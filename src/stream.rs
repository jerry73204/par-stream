//! futures-compatible parallel stream extension.

use crate::{
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
    error::{NullError, NullResult},
    rt,
};
use tokio::sync::{Mutex, Notify, Semaphore};

/// Collect multiple streams into single stream.
///
/// ```rust
/// use futures::stream::StreamExt;
/// use par_stream::ParStreamExt;
/// use std::collections::HashSet;
///
/// async fn main_async() {
///     let orig = futures::stream::iter(0..1000);
///
///     // scatter stream items to two receivers
///     let rx1 = orig.scatter(None);
///     let rx2 = rx1.clone();
///
///     // gather back from two receivers
///     let values: HashSet<_> = par_stream::gather(None, vec![rx1, rx2]).collect().await;
///
///     // the gathered values have equal content with the original
///     assert_eq!(values, (0..1000).collect::<HashSet<_>>());
/// }
///
/// # #[cfg(feature = "runtime-async-std")]
/// # #[async_std::main]
/// # async fn main() {
/// #     main_async().await
/// # }
/// #
/// # #[cfg(feature = "runtime-tokio")]
/// # #[tokio::main]
/// # async fn main() {
/// #     main_async().await
/// # }
/// #
/// # #[cfg(feature = "runtime-smol")]
/// # fn main() {
/// #     smol::block_on(main_async())
/// # }
/// ```
pub fn gather<S>(
    buf_size: impl Into<Option<usize>>,
    streams: impl IntoIterator<Item = S>,
) -> Gather<S::Item>
where
    S: 'static + StreamExt + Unpin + Send,
    S::Item: Send,
{
    let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
    let (output_tx, output_rx) = async_channel::bounded(buf_size);

    let futs = streams.into_iter().map(|mut stream| {
        let output_tx = output_tx.clone();
        async move {
            while let Some(item) = stream.next().await {
                output_tx.send(item).await?;
            }
            Ok(())
        }
    });
    let gather_fut = futures::future::try_join_all(futs);

    Gather {
        fut: Some(Box::pin(gather_fut)),
        output_rx,
    }
}

/// An extension trait for streams providing combinators for parallel processing.
pub trait ParStreamExt {
    /// Converts the stream to a cloneable receiver that receiving items in fan-out pattern.
    ///
    /// When a receiver is cloned, it creates a separate internal buffer, so that a background
    /// worker clones and passes each stream item to available receiver buffers. It can be used
    /// to _fork_ a stream into copies and pass them to concurrent workers.
    ///
    /// The internal buffer size is determined by the `buf_size`. If `buf_size` is `None`,
    /// the buffer size will be unbounded. A background worker is started and keeps
    /// copying item to available receivers. The background worker halts when all receivers
    /// are dropped.
    ///
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     let orig: Vec<_> = (0..1000).collect();
    ///
    ///     let rx1 = futures::stream::iter(orig.clone()).tee(1);
    ///     let rx2 = rx1.clone();
    ///     let rx3 = rx1.clone();
    ///
    ///     let fut1 = rx1.map(|val| val).collect();
    ///     let fut2 = rx2.map(|val| val * 2).collect();
    ///     let fut3 = rx3.map(|val| val * 3).collect();
    ///
    ///     let (vec1, vec2, vec3): (Vec<_>, Vec<_>, Vec<_>) = futures::join!(fut1, fut2, fut3);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn tee<T>(mut self, buf_size: impl Into<Option<usize>>) -> Tee<T>
    where
        Self: 'static + Stream<Item = T> + Sized + Unpin + Send,
        T: 'static + Send + Clone,
    {
        let buf_size = buf_size.into();
        let (tx, rx) = match buf_size {
            Some(buf_size) => async_channel::bounded(buf_size),
            None => async_channel::unbounded(),
        };
        let sender_set = Arc::new(flurry::HashSet::new());
        let guard = sender_set.guard();
        sender_set.insert(ByAddress(Arc::new(tx)), &guard);

        let future = {
            let sender_set = sender_set.clone();

            let future = rt::spawn(async move {
                while let Some(item) = self.next().await {
                    let futures: Vec<_> = sender_set
                        .pin()
                        .iter()
                        .map(|tx| {
                            let tx = tx.clone();
                            let item = item.clone();
                            async move {
                                let result = tx.send(item).await;
                                (result, tx)
                            }
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;
                    let success_count = results
                        .iter()
                        .filter(|(result, tx)| {
                            let ok = result.is_ok();
                            if !ok {
                                sender_set.pin().remove(tx);
                            }
                            ok
                        })
                        .count();

                    if success_count == 0 {
                        break;
                    }
                }
            });

            Arc::new(Mutex::new(Some(future)))
        };

        Tee {
            future,
            sender_set: Arc::downgrade(&sender_set),
            receiver: rx,
            buf_size,
        }
    }

    /// Gives the current iteration count that may overflow to zero as well as the next value.
    fn wrapping_enumerate<T>(self) -> WrappingEnumerate<T, Self>
    where
        Self: Stream<Item = T> + Sized + Unpin,
    {
        WrappingEnumerate {
            stream: self,
            counter: 0,
        }
    }

    /// Reorder the input items paired with a iteration count.
    ///
    /// The combinator asserts the input item has tuple type `(usize, T)`.
    /// It reorders the items according to the first value of input tuple.
    ///
    /// It is usually combined with [ParStreamExt::wrapping_enumerate], then
    /// applies a series of unordered parallel mapping, and finally reorders the values
    /// back by this method. It avoids reordering the values after each parallel mapping step.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // add enumerated index that does not panic on overflow
    ///         .wrapping_enumerate()
    ///         // double the values in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value * 2) }
    ///         })
    ///         // add values by one in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value + 1) }
    ///         })
    ///         // reorder the values by enumerated index
    ///         .reorder_enumerated()
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn reorder_enumerated<T>(self) -> ReorderEnumerated<T, Self>
    where
        Self: Stream<Item = (usize, T)> + Unpin + Sized,
    {
        ReorderEnumerated {
            stream: self,
            counter: 0,
            buffer: HashMap::new(),
        }
    }

    /// Computes new items from the stream asynchronously in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_then(None, move |value| {
    ///             // cloned needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the future is sent to a parallel worker
    ///             async move { value * (*cloned_outer) }
    ///         })
    ///         // the collected values will be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_then<T, F, Fut>(self, config: impl IntoParStreamParams, mut f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let indexed_f = move |(index, item)| {
            let fut = f(item);
            fut.map(move |output| (index, output))
        };

        let stream = self
            .wrapping_enumerate()
            .par_then_unordered(config, indexed_f)
            .reorder_enumerated();

        ParMap {
            stream: Box::pin(stream),
        }
    }

    /// Creates a parallel stream with in-local thread initializer.
    fn par_then_init<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut f: MapF,
    ) -> ParMap<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let init = init_f();

        let stream = self
            .wrapping_enumerate()
            .par_then_unordered(config, move |(index, item)| {
                let fut = f(init.clone(), item);
                fut.map(move |output| (index, output))
            })
            .reorder_enumerated();

        ParMap {
            stream: Box::pin(stream),
        }
    }

    /// Computes new items from the stream asynchronously in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The order of output items is not guaranteed to respect the order of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::collections::HashSet;
    ///
    /// async fn main_async() {
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_then_unordered(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the future is sent to a parallel worker
    ///             async move { value * (*cloned_outer) }
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<HashSet<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<HashSet<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_then_unordered<T, F, Fut>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        ParMapUnordered::new(self, config, f)
    }

    /// Creates a stream analogous to [par_then_unordered](ParStreamExt::par_then_unordered) with
    /// in-local thread initializer.
    fn par_then_init_unordered<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let init = init_f();
        ParMapUnordered::new(self, config, move |item| map_f(init.clone(), item))
    }

    /// Computes new items in a function in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a closure
    /// and the closure will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_map(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the closure is sent to parallel worker
    ///             move || value * (*cloned_outer)
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_map<T, F, Func>(self, config: impl IntoParStreamParams, mut f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        self.par_then(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Creates a parallel stream analogous to [par_map](ParStreamExt::par_map) with
    /// in-local thread initializer.
    fn par_map_init<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut f: MapF,
    ) -> ParMap<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let init = init_f();

        self.par_then(config, move |item| {
            let func = f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Computes new items in a function in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::collections::HashSet;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_map_unordered(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the closure is sent to parallel worker
    ///             move || value * (*cloned_outer)
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<HashSet<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<HashSet<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_map_unordered<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        self.par_then_unordered(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Creates a parallel stream analogous to [par_map_unordered](ParStreamExt::par_map_unordered) with
    /// in-local thread initializer.
    fn par_map_init_unordered<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut f: MapF,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let init = init_f();

        self.par_then_unordered(config, move |item| {
            let func = f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Reduces the input items into single value in parallel.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// The `buf_size` is the size of buffer that stores the temporary reduced values.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// Unlike [StreamExt::fold], the method does not combine the values sequentially.
    /// Instead, the parallel workers greedly take two values from the buffer, reduce to
    /// one value, and push back to the buffer.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///     let sum = futures::stream::iter(1..=1000)
    ///         // sum up the values in parallel
    ///         .par_reduce(None, None, move |lhs, rhs| {
    ///             // the closure is sent to parallel worker
    ///             async move { lhs + rhs }
    ///         })
    ///         .await;
    ///     assert_eq!(sum, (1 + 1000) * 1000 / 2);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_reduce<F, Fut>(
        mut self,
        limit: impl Into<Option<usize>>,
        buf_size: impl Into<Option<usize>>,
        mut f: F,
    ) -> ParReduce<Self::Item>
    where
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = Self::Item> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let limit = match limit.into() {
            None | Some(0) => num_cpus::get(),
            Some(num) => num,
        };
        let buf_size = match buf_size.into() {
            None | Some(0) => limit,
            Some(num) => num,
        };

        let fused = Arc::new(Notify::new());
        let counter = Arc::new(Semaphore::new(buf_size));
        let (buf_tx, mut buf_rx) = async_channel::bounded(buf_size);
        let (job_tx, job_rx) = async_channel::bounded(limit);
        let (output_tx, output_rx) = futures::channel::oneshot::channel();

        let buffering_fut = {
            let counter = counter.clone();
            let fused = fused.clone();
            let buf_tx = buf_tx.clone();

            async move {
                while let Some(item) = self.next().await {
                    let permit = counter.clone().acquire_owned().await;
                    buf_tx.send((item, permit)).await?;
                }
                fused.notify_one();
                Ok(())
            }
        };

        let pairing_fut = async move {
            let (lhs_item, lhs_permit) = loop {
                let (lhs_item, lhs_permit) = buf_rx.recv().await?;
                let (rhs_item, rhs_permit) = tokio::select! {
                    rhs = &mut buf_rx.next() => rhs.ok_or(NullError)?,
                    _ = fused.notified() => {
                        break (lhs_item, lhs_permit);
                    }
                };

                // forget one permit to allow new incoming items
                mem::drop(rhs_permit);

                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await?;
            };

            if counter.available_permits() <= buf_size - 2 {
                let (rhs_item, rhs_permit) = buf_rx.recv().await?;
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await?;
            }

            while counter.available_permits() <= buf_size - 2 {
                let (lhs_item, lhs_permit) = buf_rx.recv().await?;
                let (rhs_item, rhs_permit) = buf_rx.recv().await?;
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await?;
            }

            let (item, _permit) = buf_rx.recv().await?;
            output_tx.send(item).map_err(|_| ())?;

            Ok(())
        };

        let reduce_futs: Vec<_> = (0..limit)
            .map(|_| {
                let job_rx = job_rx.clone();
                let buf_tx = buf_tx.clone();

                let fut = async move {
                    while let Ok((fut, permit)) = job_rx.recv().await {
                        let output = fut.await;
                        buf_tx.send((output, permit)).await?;
                    }
                    Ok(())
                };
                rt::spawn(fut).map(|result| result.unwrap())
            })
            .collect();

        let par_reduce_fut = futures::future::try_join3(
            buffering_fut,
            pairing_fut,
            futures::future::try_join_all(reduce_futs),
        );

        ParReduce {
            fut: Some(Box::pin(par_reduce_fut)),
            output_rx,
        }
    }

    /// Distributes input items to specific workers and compute new items with respect to the input order.
    ///
    ///
    /// The `buf_size` is the size of input buffer before each mapping function.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// `routing_fn` assigns input items to specific indexes of mapping functions.
    /// `routing_fn` is executed on the calling thread.
    ///
    /// `map_fns` is a vector of mapping functions, each of which produces an asynchronous closure.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::{future::Future, pin::Pin};
    ///
    /// async fn main_async() {
    ///     let map_fns: Vec<
    ///         Box<dyn FnMut(usize) -> Pin<Box<dyn Future<Output = usize> + Send>> + Send>,
    ///     > = vec![
    ///         // even number processor
    ///         Box::new(|even_value| Box::pin(async move { even_value / 2 })),
    ///         // odd number processor
    ///         Box::new(|odd_value| Box::pin(async move { odd_value * 2 + 1 })),
    ///     ];
    ///
    ///     let transformed = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_routing(
    ///             None,
    ///             move |value| {
    ///                 // distribute the value according to its parity
    ///                 if value % 2 == 0 {
    ///                     0
    ///                 } else {
    ///                     1
    ///                 }
    ///             },
    ///             map_fns,
    ///         )
    ///         // the collected values may NOT be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000)
    ///         .map(|value| {
    ///             if value % 2 == 0 {
    ///                 value / 2
    ///             } else {
    ///                 value * 2 + 1
    ///             }
    ///         })
    ///         .collect::<Vec<_>>();
    ///     assert_eq!(transformed, expect);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn par_routing<F1, F2, Fut, T>(
        mut self,
        buf_size: impl Into<Option<usize>>,
        mut routing_fn: F1,
        mut map_fns: Vec<F2>,
    ) -> ParRouting<T>
    where
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
    {
        let buf_size = match buf_size.into() {
            None | Some(0) => num_cpus::get(),
            Some(size) => size,
        };

        let (reorder_tx, reorder_rx) = async_channel::bounded(buf_size);
        let (output_tx, output_rx) = async_channel::bounded(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_channel::bounded(buf_size);
                    let reorder_tx = reorder_tx.clone();

                    let map_fut = rt::spawn(async move {
                        while let Ok((counter, fut)) = map_rx.recv().await {
                            let output = fut.await;
                            reorder_tx.send((counter, output)).await?;
                        }
                        Ok(())
                    })
                    .map(|result| result.unwrap());

                    map_txs.push(map_tx);
                    map_futs.push(map_fut);
                    (map_txs, map_futs)
                });

        let routing_fut = async move {
            let mut counter = 0u64;

            while let Some(item) = self.next().await {
                let index = routing_fn(&item);
                let map_fn = map_fns
                    .get_mut(index)
                    .expect("the routing function returns an invalid index");
                let map_tx = map_txs.get_mut(index).unwrap();
                let fut = map_fn(item);
                map_tx.send((counter, fut)).await?;

                counter = counter.wrapping_add(1);
            }

            Ok(())
        };

        let reorder_fut = async move {
            let mut counter = 0u64;
            let mut pool = HashMap::new();

            while let Ok((index, output)) = reorder_rx.recv().await {
                if index != counter {
                    pool.insert(index, output);
                    continue;
                }

                output_tx.send(output).await?;
                counter = counter.wrapping_add(1);

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await?;
                    counter = counter.wrapping_add(1);
                }
            }

            Ok(())
        };

        let par_routing_fut = futures::future::try_join3(
            routing_fut,
            reorder_fut,
            futures::future::try_join_all(map_futs),
        );

        ParRouting {
            fut: Some(Box::pin(par_routing_fut)),
            output_rx,
        }
    }

    /// Distributes input items to specific workers and compute new items without respecting the input order.
    ///
    ///
    /// The `buf_size` is the size of input buffer before each mapping function.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// `routing_fn` assigns input items to specific indexes of mapping functions.
    /// `routing_fn` is executed on the calling thread.
    ///
    /// `map_fns` is a vector of mapping functions, each of which produces an asynchronous closure.
    fn par_routing_unordered<F1, F2, Fut, T>(
        mut self,
        buf_size: impl Into<Option<usize>>,
        mut routing_fn: F1,
        mut map_fns: Vec<F2>,
    ) -> ParRoutingUnordered<T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let buf_size = match buf_size.into() {
            None | Some(0) => num_cpus::get(),
            Some(size) => size,
        };

        let (output_tx, output_rx) = async_channel::bounded(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_channel::bounded(buf_size);
                    let output_tx = output_tx.clone();

                    let map_fut = rt::spawn(async move {
                        while let Ok(fut) = map_rx.recv().await {
                            let output = fut.await;
                            output_tx.send(output).await?;
                        }
                        Ok(())
                    })
                    .map(|result| result.unwrap());

                    map_txs.push(map_tx);
                    map_futs.push(map_fut);
                    (map_txs, map_futs)
                });

        let routing_fut = async move {
            while let Some(item) = self.next().await {
                let index = routing_fn(&item);
                let map_fn = map_fns
                    .get_mut(index)
                    .expect("the routing function returns an invalid index");
                let map_tx = map_txs.get_mut(index).unwrap();
                let fut = map_fn(item);
                map_tx.send(fut).await?;
            }
            Ok(())
        };

        let par_routing_fut =
            futures::future::try_join(routing_fut, futures::future::try_join_all(map_futs));

        ParRoutingUnordered {
            fut: Some(Box::pin(par_routing_fut)),
            output_rx,
        }
    }

    /// Splits the stream into a receiver and a future.
    ///
    /// The returned future scatters input items into the receiver and its clones,
    /// and should be manually awaited by user.
    ///
    /// The returned receiver can be cloned and distributed to resepctive workers.
    ///
    /// It lets user to write custom workers that receive items from the same stream.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// async fn main_async() {
    ///     let orig = futures::stream::iter(1isize..=1000);
    ///
    ///     // scatter the items
    ///     let rx1 = orig.scatter(None);
    ///     let rx2 = rx1.clone();
    ///
    ///     // collect the values concurrently
    ///     let (values1, values2): (Vec<_>, Vec<_>) = futures::join!(rx1.collect(), rx2.collect());
    ///
    ///     // the total item count is equal to the original set
    ///     assert_eq!(values1.len() + values2.len(), 1000);
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn scatter(mut self, buf_size: impl Into<Option<usize>>) -> Scatter<Self::Item>
    where
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
        let (tx, rx) = async_channel::bounded(buf_size);

        let future = rt::spawn(async move {
            while let Some(item) = self.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        Scatter {
            future: Arc::new(Mutex::new(Some(future))),
            receiver: rx,
        }
    }

    /// Runs an asynchronous task on each element of an stream in parallel.
    fn par_for_each<F, Fut>(self, config: impl IntoParStreamParams, f: F) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
    {
        ParForEach::new(self, config, f)
    }

    /// Creates a parallel stream analogous to [par_for_each](ParStreamExt::par_for_each) with a
    /// in-local thread initializer.
    fn par_for_each_init<B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
    {
        let init = init_f();
        ParForEach::new(self, config, move |item| map_f(init.clone(), item))
    }

    /// Runs an blocking task on each element of an stream in parallel.
    fn par_for_each_blocking<F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
    {
        self.par_for_each(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Creates a parallel stream analogous to [par_for_each_blocking](ParStreamExt::par_for_each_blocking) with a
    /// in-local thread initializer.
    fn par_for_each_blocking_init<B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut f: MapF,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
    {
        let init = init_f();

        self.par_for_each(config, move |item| {
            let func = f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }
}

impl<S> ParStreamExt for S where S: Stream {}

// scatter

pub use scatter::*;

mod scatter {
    use super::*;

    /// A stream combinator returned from [scatter()](ParStreamExt::scatter).
    #[derive(Debug)]
    pub struct Scatter<T> {
        pub(super) future: Arc<Mutex<Option<rt::JoinHandle<()>>>>,
        pub(super) receiver: async_channel::Receiver<T>,
    }

    impl<T> Stream for Scatter<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Ok(mut future_opt) = self.future.try_lock() {
                if let Some(future) = &mut *future_opt {
                    if Pin::new(future).poll(cx).is_ready() {
                        *future_opt = None;
                    }
                }
            }

            Pin::new(&mut self.receiver).poll_next(cx)
        }
    }

    impl<T> Clone for Scatter<T> {
        fn clone(&self) -> Self {
            Self {
                future: self.future.clone(),
                receiver: self.receiver.clone(),
            }
        }
    }
}

// tee

pub use tee::*;

mod tee {
    use super::*;

    /// A stream combinator returned from [tee()](ParStreamExt::tee).
    #[derive(Debug)]
    pub struct Tee<T> {
        pub(super) buf_size: Option<usize>,
        pub(super) future: Arc<Mutex<Option<rt::JoinHandle<()>>>>,
        pub(super) sender_set: Weak<flurry::HashSet<ByAddress<Arc<async_channel::Sender<T>>>>>,
        pub(super) receiver: async_channel::Receiver<T>,
    }

    impl<T> Clone for Tee<T>
    where
        T: 'static + Send,
    {
        fn clone(&self) -> Self {
            let buf_size = self.buf_size;
            let (tx, rx) = match buf_size {
                Some(buf_size) => async_channel::bounded(buf_size),
                None => async_channel::unbounded(),
            };
            let sender_set = self.sender_set.clone();

            if let Some(sender_set) = sender_set.upgrade() {
                let guard = sender_set.guard();
                sender_set.insert(ByAddress(Arc::new(tx)), &guard);
            }

            Self {
                future: self.future.clone(),
                sender_set,
                receiver: rx,
                buf_size,
            }
        }
    }

    impl<T> Stream for Tee<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Ok(mut future_opt) = self.future.try_lock() {
                if let Some(future) = &mut *future_opt {
                    if Pin::new(future).poll(cx).is_ready() {
                        *future_opt = None;
                    }
                }
            }

            Pin::new(&mut self.receiver).poll_next(cx)
        }
    }
}

// wrapping_enumerate

pub use wrapping_enumerate::*;

mod wrapping_enumerate {
    use super::*;

    /// A stream combinator returned from [wrapping_enumerate()](ParStreamExt::wrapping_enumerate).
    #[pin_project(project = WrappingEnumerateProj)]
    #[derive(Debug)]
    pub struct WrappingEnumerate<T, S>
    where
        S: Stream<Item = T> + Unpin,
    {
        #[pin]
        pub(super) stream: S,
        pub(super) counter: usize,
    }

    impl<T, S> Stream for WrappingEnumerate<T, S>
    where
        S: Stream<Item = T> + Unpin,
    {
        type Item = (usize, T);

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let WrappingEnumerateProj { stream, counter } = self.project();

            match stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let index = *counter;
                    *counter = counter.wrapping_add(1);
                    Poll::Ready(Some((index, item)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

// reorder_enumerated

pub use reorder_enumerated::*;

mod reorder_enumerated {
    use super::*;

    /// A stream combinator returned from [reorder_enumerated()](ParStreamExt::reorder_enumerated).
    #[pin_project]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ReorderEnumerated<T, S>
    where
        S: Stream<Item = (usize, T)> + Unpin,
    {
        #[pin]
        pub(super) stream: S,
        pub(super) counter: usize,
        pub(super) buffer: HashMap<usize, T>,
    }

    impl<T, S> Stream for ReorderEnumerated<T, S>
    where
        S: Stream<Item = (usize, T)> + Unpin,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let this = self.project();
            let stream = this.stream;
            let counter = this.counter;
            let buffer = this.buffer;

            let buffered_item_opt = buffer.remove(counter);
            if buffered_item_opt.is_some() {
                *counter = counter.wrapping_add(1);
            }

            match (stream.poll_next(cx), buffered_item_opt) {
                (Poll::Ready(Some((index, item))), Some(buffered_item)) => {
                    assert!(
                        *counter <= index,
                        "the enumerated index {} appears more than once",
                        index
                    );
                    buffer.insert(index, item);
                    Poll::Ready(Some(buffered_item))
                }
                (Poll::Ready(Some((index, item))), None) => match (*counter).cmp(&index) {
                    Ordering::Less => {
                        buffer.insert(index, item);
                        Poll::Pending
                    }
                    Ordering::Equal => {
                        *counter = counter.wrapping_add(1);
                        Poll::Ready(Some(item))
                    }
                    Ordering::Greater => {
                        panic!("the enumerated index {} appears more than once", index)
                    }
                },
                (_, Some(buffered_item)) => Poll::Ready(Some(buffered_item)),
                (Poll::Ready(None), None) => {
                    if buffer.is_empty() {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
                (Poll::Pending, None) => Poll::Pending,
            }
        }
    }
}

// par_map

pub use par_map::*;

mod par_map {
    use super::*;

    /// A stream combinator returned from [par_map()](ParStreamExt::par_map) and its siblings.
    #[pin_project]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParMap<T> {
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    }

    impl<T> Stream for ParMap<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            self.project().stream.poll_next(cx)
        }
    }
}

// par_map_unordered

pub use par_map_unordered::*;

mod par_map_unordered {
    use super::*;

    /// A stream combinator returned from [par_map_unordered()](ParStreamExt::par_map_unordered) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParMapUnordered<T> {
        #[derivative(Debug = "ignore")]
        pub(super) fut:
            Option<Pin<Box<dyn Future<Output = (NullResult<()>, NullResult<Vec<()>>)> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<T>,
    }

    impl<T> ParMapUnordered<T> {
        pub fn new<S, F, Fut>(mut stream: S, config: impl IntoParStreamParams, mut f: F) -> Self
        where
            T: 'static + Send,
            F: 'static + FnMut(S::Item) -> Fut + Send,
            Fut: 'static + Future<Output = T> + Send,
            S: 'static + StreamExt + Sized + Unpin + Send,
            S::Item: Send,
        {
            let ParStreamParams {
                num_workers,
                buf_size,
            } = config.into_par_stream_params();
            let (map_tx, map_rx) = async_channel::bounded(buf_size);
            let (output_tx, output_rx) = async_channel::bounded(buf_size);

            let map_fut = async move {
                while let Some(item) = stream.next().await {
                    let fut = f(item);
                    map_tx.send(fut).await?;
                }
                Ok(())
            };

            let worker_futs: Vec<_> = (0..num_workers)
                .map(|_| {
                    let map_rx = map_rx.clone();
                    let output_tx = output_tx.clone();

                    let worker_fut = async move {
                        while let Ok(fut) = map_rx.recv().await {
                            let output = fut.await;
                            output_tx.send(output).await?;
                        }
                        Ok(())
                    };
                    rt::spawn(worker_fut).map(|result| result.unwrap())
                })
                .collect();

            let par_then_fut =
                futures::future::join(map_fut, futures::future::try_join_all(worker_futs));

            Self {
                fut: Some(Box::pin(par_then_fut)),
                output_rx,
            }
        }
    }

    impl<T> Stream for ParMapUnordered<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// par_reduce

pub use par_reduce::*;

mod par_reduce {
    use super::*;

    /// A stream combinator returned from [par_reduce()](ParStreamExt::par_reduce).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParReduce<T> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<((), (), Vec<()>)>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: futures::channel::oneshot::Receiver<T>,
    }

    impl<T> Future for ParReduce<T> {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx)
                .poll(cx)
                .map(|result| result.unwrap());

            if poll.is_pending() {
                should_wake |= true;
            }

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// par_routing

pub use par_routing::*;

mod par_routing {
    use super::*;

    /// A stream combinator returned from [par_routing()](ParStreamExt::par_routing).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParRouting<T> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<((), (), Vec<()>)>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<T>,
    }

    impl<T> Stream for ParRouting<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// par_routing_unordered

pub use par_routing_unordered::*;

mod par_routing_unordered {
    use super::*;

    /// A stream combinator returned from [par_routing_unordered()](ParStreamExt::par_routing_unordered).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParRoutingUnordered<T> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<((), Vec<()>)>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<T>,
    }

    impl<T> Stream for ParRoutingUnordered<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// gather

pub use gather::*;

mod gather {
    use super::*;

    /// A stream combinator returned from [gather()](gather()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct Gather<T>
    where
        T: Send,
    {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<Vec<()>>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<T>,
    }

    impl<T> Stream for Gather<T>
    where
        T: Send,
    {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// for each

pub use par_for_each::*;

mod par_for_each {
    use super::*;

    /// A stream combinator returned from [par_for_each()](ParStreamExt::par_for_each) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParForEach {
        #[derivative(Debug = "ignore")]
        fut: Option<Pin<Box<dyn Future<Output = (NullResult<()>, Vec<()>)> + Send>>>,
    }

    impl ParForEach {
        pub fn new<St, F, Fut>(mut stream: St, config: impl IntoParStreamParams, mut f: F) -> Self
        where
            St: 'static + Stream + Unpin + Sized + Send,
            St::Item: Send,
            F: 'static + FnMut(St::Item) -> Fut + Send,
            Fut: 'static + Future<Output = ()> + Send,
        {
            let ParStreamParams {
                num_workers,
                buf_size,
            } = config.into_par_stream_params();
            let (map_tx, map_rx) = async_channel::bounded(buf_size);

            let map_fut = async move {
                while let Some(item) = stream.next().await {
                    let fut = f(item);
                    map_tx.send(fut).await?;
                }
                Ok(())
            };

            let worker_futs: Vec<_> = (0..num_workers)
                .map(|_| {
                    let map_rx = map_rx.clone();

                    let worker_fut = async move {
                        while let Ok(fut) = map_rx.recv().await {
                            fut.await;
                        }
                    };
                    rt::spawn(worker_fut).map(|result| result.unwrap())
                })
                .collect();

            let join_fut = futures::future::join(map_fut, futures::future::join_all(worker_futs));

            Self {
                fut: Some(Box::pin(join_fut)),
            }
        }
    }

    impl Future for ParForEach {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(_) => {
                        self.fut = None;
                        Poll::Ready(())
                    }
                },
                None => Poll::Ready(()),
            }
        }
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;
    use std::time::Duration;

    #[tokio::test]
    async fn par_then_output_is_ordered_test() {
        let max = 1000u64;
        futures::stream::iter((0..max).into_iter())
            .par_then(None, |value| async move {
                async_std::task::sleep(std::time::Duration::from_millis(value % 20)).await;
                value
            })
            .fold(0u64, |expect, found| async move {
                assert_eq!(expect, found);
                expect + 1
            })
            .await;
    }

    #[tokio::test]
    async fn par_then_unordered_test() {
        let max = 1000u64;
        let mut values = futures::stream::iter((0..max).into_iter())
            .par_then_unordered(None, |value| async move {
                async_std::task::sleep(std::time::Duration::from_millis(value % 20)).await;
                value
            })
            .collect::<Vec<_>>()
            .await;
        values.sort();
        values.into_iter().fold(0, |expect, found| {
            assert_eq!(expect, found);
            expect + 1
        });
    }

    #[tokio::test]
    async fn par_reduce_test() {
        let max = 100000u64;
        let sum = futures::stream::iter((1..=max).into_iter())
            .par_reduce(None, None, |lhs, rhs| async move { lhs + rhs })
            .await;
        assert_eq!(sum, (1 + max) * max / 2);
    }

    #[tokio::test]
    async fn enumerate_reorder_test() {
        let max = 1000u64;
        let iterator = (0..max).rev().step_by(2);

        let lhs = futures::stream::iter(iterator.clone())
            .wrapping_enumerate()
            .par_then_unordered(None, |(index, value)| async move {
                async_std::task::sleep(std::time::Duration::from_millis(value % 20)).await;
                (index, value)
            })
            .reorder_enumerated();
        let rhs = futures::stream::iter(iterator.clone());

        let is_equal =
            async_std::stream::StreamExt::all(&mut lhs.zip(rhs), |(lhs_value, rhs_value)| {
                lhs_value == rhs_value
            })
            .await;
        assert!(is_equal);
    }

    #[tokio::test]
    async fn for_each_test() {
        use std::sync::atomic::{self, AtomicUsize};

        {
            let sum = Arc::new(AtomicUsize::new(0));
            {
                let sum = sum.clone();
                futures::stream::iter(1..=1000)
                    .par_for_each(None, move |value| {
                        let sum = sum.clone();
                        async move {
                            sum.fetch_add(value, atomic::Ordering::SeqCst);
                        }
                    })
                    .await;
            }
            assert_eq!(sum.load(atomic::Ordering::SeqCst), (1 + 1000) * 1000 / 2);
        }

        {
            let sum = Arc::new(AtomicUsize::new(0));
            futures::stream::iter(1..=1000)
                .par_for_each_init(
                    None,
                    || sum.clone(),
                    move |sum, value| {
                        let sum = sum.clone();
                        async move {
                            sum.fetch_add(value, atomic::Ordering::SeqCst);
                        }
                    },
                )
                .await;
            assert_eq!(sum.load(atomic::Ordering::SeqCst), (1 + 1000) * 1000 / 2);
        }
    }

    #[tokio::test]
    async fn tee_halt_test() {
        let mut rx1 = futures::stream::iter(0..).tee(1);
        let mut rx2 = rx1.clone();

        assert_eq!(rx1.next().await, Some(0));
        assert_eq!(rx2.next().await, Some(0));

        // drop rx1
        drop(rx1);

        // the following should not block
        assert_eq!(rx2.next().await, Some(1));
        assert_eq!(rx2.next().await, Some(2));
        assert_eq!(rx2.next().await, Some(3));
        assert_eq!(rx2.next().await, Some(4));
        assert_eq!(rx2.next().await, Some(5));
    }

    #[tokio::test]
    async fn tee_test() {
        let orig: Vec<_> = (0..100).collect();

        let rx1 = futures::stream::iter(orig.clone()).tee(1);
        let rx2 = rx1.clone();
        let rx3 = rx1.clone();

        let fut1 = rx1
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                tokio::time::sleep(Duration::from_millis(millis)).await;
                val
            })
            .collect();
        let fut2 = rx2
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                tokio::time::sleep(Duration::from_millis(millis)).await;
                val * 2
            })
            .collect();
        let fut3 = rx3
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                tokio::time::sleep(Duration::from_millis(millis)).await;
                val * 3
            })
            .collect();

        let (vec1, vec2, vec3): (Vec<_>, Vec<_>, Vec<_>) = futures::join!(fut1, fut2, fut3);

        assert!(orig.iter().zip(&vec1).all(|(&orig, &val)| orig == val));
        assert!(orig.iter().zip(&vec2).all(|(&orig, &val)| orig * 2 == val));
        assert!(orig.iter().zip(&vec3).all(|(&orig, &val)| orig * 3 == val));
    }
}
