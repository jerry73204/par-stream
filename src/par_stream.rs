use crate::{
    broadcast::BroadcastGuard,
    common::*,
    config::{self, BufSize, NumWorkers, ParParams},
    index_stream::IndexStreamExt as _,
    rt,
    scatter::Scatter,
    shared_stream::Shared,
    stream::StreamExt as _,
    tee::Tee,
    utils,
};

/// An extension trait that provides parallel processing combinators on streams.
pub trait ParStreamExt
where
    Self: 'static + Send + Stream,
    Self::Item: 'static + Send,
{
    fn spawned<B>(self, buf_size: B) -> BoxStream<'static, Self::Item>
    where
        B: Into<BufSize>;

    /// The combinator maintains a collection of concurrent workers, each consuming as many elements as it likes,
    /// and produces the next stream element.
    ///
    /// ```rust
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::mem;
    ///
    /// async fn main_async() {
    ///     let data = vec![1, 2, -3, 4, 5, -6, 7, 8];
    ///     stream::iter(data).par_batching(None, |_worker_index, mut stream| async move {
    ///         while let Some(value) = stream.next().await {
    ///             if value > 0 {
    ///                 return Some((value, stream));
    ///             }
    ///         }
    ///         None
    ///     });
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
    fn par_batching<T, P, F, Fut>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        F: 'static + Send + Clone + FnMut(usize, Shared<Self>) -> Fut,
        Fut: 'static + Future<Output = Option<(T, Shared<Self>)>> + Send,
        T: 'static + Send,
        P: Into<ParParams>;

    /// Converts the stream to a cloneable receiver that receiving items in fan-out pattern.
    ///
    /// When a receiver is cloned, it creates a separate internal buffer, so that a background
    /// worker clones and passes each stream item to available receiver buffers. It can be used
    /// to _fork_ a stream into copies and pass them to concurrent workers.
    ///
    /// Each receiver maintains an internal buffer with `buf_size`. If one of the receiver buffer
    /// is full, the stream will halt until the blocking buffer spare the space.
    ///
    /// ```rust
    /// use futures::{join, prelude::*};
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     let orig: Vec<_> = (0..1000).collect();
    ///
    ///     let rx1 = stream::iter(orig.clone()).tee(1);
    ///     let rx2 = rx1.clone();
    ///     let rx3 = rx1.clone();
    ///
    ///     let fut1 = rx1.map(|val| val).collect();
    ///     let fut2 = rx2.map(|val| val * 2).collect();
    ///     let fut3 = rx3.map(|val| val * 3).collect();
    ///
    ///     let (vec1, vec2, vec3): (Vec<_>, Vec<_>, Vec<_>) = join!(fut1, fut2, fut3);
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
    fn tee<B>(self, buf_size: B) -> Tee<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>;

    /// Converts to a [guard](BroadcastGuard) that can create receivers,
    /// each receiving cloned elements from this stream.
    ///
    /// The generated receivers can produce elements only after the guard is dropped.
    /// It ensures the receivers start receiving elements at the mean time.
    ///
    /// Each receiver maintains an internal buffer with `buf_size`. If one of the receiver buffer
    /// is full, the stream will halt until the blocking buffer spare the space.
    ///
    /// ```rust
    /// use futures::{join, prelude::*};
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     let mut guard = stream::iter(0..).broadcast(2);
    ///     let rx1 = guard.register();
    ///     let rx2 = guard.register();
    ///     guard.finish(); // drop the guard
    ///
    ///     let (ret1, ret2): (Vec<_>, Vec<_>) =
    ///         join!(rx1.take(100).collect(), rx2.take(100).collect());
    ///     let expect: Vec<_> = (0..100).collect();
    ///
    ///     assert_eq!(ret1, expect);
    ///     assert_eq!(ret2, expect);
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
    fn broadcast<B>(self, buf_size: B) -> BroadcastGuard<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>;

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
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     let doubled: Vec<_> = stream::iter(0..1000)
    ///         // doubles the values in parallel
    ///         .par_then(None, move |value| async move { value * 2 })
    ///         // the collected values will be ordered
    ///         .collect()
    ///         .await;
    ///     let expect: Vec<_> = (0..1000).map(|value| value * 2).collect();
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
    fn par_then<T, P, F, Fut>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>;

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
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::collections::HashSet;
    ///
    /// async fn main_async() {
    ///     let doubled: HashSet<_> = stream::iter(0..1000)
    ///         // doubles the values in parallel
    ///         .par_then_unordered(None, move |value| {
    ///             // the future is sent to a parallel worker
    ///             async move { value * 2 }
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect()
    ///         .await;
    ///     let expect: HashSet<_> = (0..1000).map(|value| value * 2).collect();
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
    fn par_then_unordered<T, P, F, Fut>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>;

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
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///     let doubled: Vec<_> = stream::iter(0..1000)
    ///         // doubles the values in parallel
    ///         .par_map(None, move |value| {
    ///             // the closure is sent to parallel worker
    ///             move || value * 2
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect()
    ///         .await;
    ///     let expect: Vec<_> = (0..1000).map(|value| value * 2).collect();
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
    fn par_map<T, P, F, Func>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>;

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
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::collections::HashSet;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///
    ///     let doubled: HashSet<_> = stream::iter(0..1000)
    ///         // doubles the values in parallel
    ///         .par_map_unordered(None, move |value| {
    ///             // the closure is sent to parallel worker
    ///             move || value * 2
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect()
    ///         .await;
    ///     let expect: HashSet<_> = (0..1000).map(|value| value * 2).collect();
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
    fn par_map_unordered<T, P, F, Func>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>;

    /// Reduces the input items into single value in parallel.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// The `buf_size` is the size of buffer that stores the temporary reduced values.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// Unlike [fold()](futures::stream::StreamExt::fold), the method does not combine the values sequentially.
    /// Instead, the parallel workers greedly take two values from the buffer, reduce to
    /// one value, and push back to the buffer.
    ///
    /// ```rust
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     // the variable will be shared by parallel workers
    ///     let sum = stream::iter(1..=1000)
    ///         // sum up the values in parallel
    ///         .par_reduce(None, move |lhs, rhs| {
    ///             // the closure is sent to parallel worker
    ///             async move { lhs + rhs }
    ///         })
    ///         .await;
    ///     assert_eq!(sum, Some((1 + 1000) * 1000 / 2));
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
    fn par_reduce<P, F, Fut>(
        self,
        params: P,
        reduce_fn: F,
    ) -> BoxFuture<'static, Option<Self::Item>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Self::Item> + Send;

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
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
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
    ///     let transformed: Vec<_> = stream::iter(0..1000)
    ///         // doubles the values in parallel
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
    ///         .collect()
    ///         .await;
    ///     let expect: Vec<_> = (0..1000)
    ///         .map(|value| {
    ///             if value % 2 == 0 {
    ///                 value / 2
    ///             } else {
    ///                 value * 2 + 1
    ///             }
    ///         })
    ///         .collect();
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
    fn par_routing<T, B, F1, F2, Fut>(
        self,
        buf_size: B,
        routing_fn: F1,
        map_fns: Vec<F2>,
    ) -> BoxStream<'static, T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        B: Into<BufSize>;

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
    fn par_routing_unordered<T, B, F1, F2, Fut>(
        self,
        buf_size: B,
        routing_fn: F1,
        map_fns: Vec<F2>,
    ) -> BoxStream<'static, T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        B: Into<BufSize>;

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
    /// use futures::{join, prelude::*};
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     let orig = stream::iter(1isize..=1000);
    ///
    ///     // scatter the items
    ///     let rx1 = orig.scatter(None);
    ///     let rx2 = rx1.clone();
    ///
    ///     // collect the values concurrently
    ///     let (values1, values2): (Vec<_>, Vec<_>) = join!(rx1.collect(), rx2.collect());
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
    fn scatter<B>(self, buf_size: B) -> Scatter<Self::Item>
    where
        B: Into<BufSize>;

    /// Runs an asynchronous task on each element of an stream in parallel.
    fn par_for_each<N, F, Fut>(self, num_workers: N, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
        N: Into<NumWorkers>;

    /// Creates a parallel stream analogous to [par_for_each](ParStreamExt::par_for_each) with a
    /// Runs an blocking task on each element of an stream in parallel.
    fn par_for_each_blocking<N, F, Func>(self, num_workers: N, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
        N: Into<NumWorkers>;
}

impl<S> ParStreamExt for S
where
    S: 'static + Send + Stream,
    S::Item: 'static + Send,
{
    fn spawned<B>(self, buf_size: B) -> BoxStream<'static, Self::Item>
    where
        B: Into<BufSize>,
    {
        let (tx, rx) = utils::channel(buf_size.into().get());

        rt::spawn(async move {
            let _ = self.map(Ok).forward(tx.into_sink()).await;
        });

        rx.into_stream().boxed()
    }

    fn par_batching<T, P, F, Fut>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        F: 'static + Send + Clone + FnMut(usize, Shared<Self>) -> Fut,
        Fut: 'static + Future<Output = Option<(T, Shared<Self>)>> + Send,
        T: 'static + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let (output_tx, output_rx) = utils::channel(buf_size);
        let stream = self.shared();

        (0..num_workers).for_each(move |worker_index| {
            let output_tx = output_tx.clone();
            let f = f.clone();
            let stream = stream.clone();

            rt::spawn(async move {
                let _ = stream::repeat(())
                    .stateful_then((stream, f), |(stream, mut f), ()| async move {
                        f(worker_index, stream)
                            .await
                            .map(move |(item, stream)| ((stream, f), item))
                    })
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });

        output_rx.into_stream().boxed()
    }

    fn pull_routing<B, K, Q, F>(self, buf_size: B, key_fn: F) -> PullBuilder<Self, K, F, Q>
    where
        Self: 'static + Send + Stream,
        Self::Item: 'static + Send,
        F: 'static + Send + FnMut(&Self::Item) -> Q,
        K: 'static + Send + Hash + Eq + Borrow<Q>,
        Q: Send + Hash + Eq,
        B: Into<BufSize>,
    {
        PullBuilder::new(self, buf_size, key_fn)
    }

    fn tee<B>(self, buf_size: B) -> Tee<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>,
    {
        Tee::new(self, buf_size)
    }

    fn broadcast<B>(self, buf_size: B) -> BroadcastGuard<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>,
    {
        BroadcastGuard::new(self, buf_size)
    }

    fn par_then<T, P, F, Fut>(self, params: P, mut f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>,
    {
        let indexed_f = move |(index, item)| {
            let fut = f(item);
            fut.map(move |output| (index, output))
        };

        self.enumerate()
            .par_then_unordered(params, indexed_f)
            .reorder_enumerated()
            .boxed()
    }

    fn par_then_unordered<T, P, F, Fut>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let stream = self
            .stateful_map(f, |mut f, item| {
                let fut = f(item);
                Some((f, fut))
            })
            .shared();

        (0..num_workers).for_each(move |_| {
            let stream = stream.clone();
            let output_tx = output_tx.clone();

            rt::spawn(async move {
                let _ = stream
                    .then(|fut| fut)
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });
        output_rx.into_stream().boxed()
    }

    fn par_map<T, P, F, Func>(self, params: P, mut f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>,
    {
        self.enumerate()
            .par_map_unordered(params, move |(index, item)| {
                let job = f(item);
                move || (index, job())
            })
            .reorder_enumerated()
            .boxed()
    }

    fn par_map_unordered<T, P, F, Func>(self, params: P, f: F) -> BoxStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let stream = self
            .stateful_map(f, |mut f, item| {
                let func = f(item);
                Some((f, func))
            })
            .shared();
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(move |_| {
            let mut stream = stream.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || {
                while let Some(job) = rt::block_on(stream.next()) {
                    let output = job();
                    let result = output_tx.send(output);
                    if result.is_err() {
                        break;
                    }
                }
            });
        });

        output_rx.into_stream().boxed()
    }

    fn par_reduce<P, F, Fut>(
        self,
        params: P,
        reduce_fn: F,
    ) -> BoxFuture<'static, Option<Self::Item>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Self::Item> + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let stream = self.shared();

        // phase 1
        let phase_1_future = async move {
            let reducer_futures = {
                let reduce_fn = reduce_fn.clone();

                (0..num_workers).map(move |_| {
                    let stream = stream.clone();
                    let mut reduce_fn = reduce_fn.clone();

                    rt::spawn(
                        async move { stream.reduce(|fold, item| reduce_fn(fold, item)).await },
                    )
                })
            };
            let values = future::join_all(reducer_futures).await;

            (values, reduce_fn)
        };

        // phase 2
        let phase_2_future = async move {
            let (values, reduce_fn) = phase_1_future.await;

            let (pair_tx, pair_rx) = utils::channel(buf_size);
            let (feedback_tx, feedback_rx) = flume::bounded(num_workers);

            let mut count = 0;

            for value in values {
                if let Some(value) = value {
                    feedback_tx.send_async(value).await.map_err(|_| ()).unwrap();
                    count += 1;
                }
            }

            let pairing_future = {
                rt::spawn(async move {
                    while count >= 2 {
                        let first = feedback_rx.recv_async().await.unwrap();
                        let second = feedback_rx.recv_async().await.unwrap();
                        pair_tx.send_async((first, second)).await.unwrap();
                        count -= 1;
                    }

                    match count {
                        0 => None,
                        1 => {
                            let output = feedback_rx.recv_async().await.unwrap();
                            Some(output)
                        }
                        _ => unreachable!(),
                    }
                })
            };

            let worker_futures = (0..num_workers).map(move |_| {
                let pair_rx = pair_rx.clone();
                let feedback_tx = feedback_tx.clone();
                let mut reduce_fn = reduce_fn.clone();

                rt::spawn(async move {
                    while let Ok((first, second)) = pair_rx.recv_async().await {
                        let reduced = reduce_fn(first, second).await;
                        feedback_tx
                            .send_async(reduced)
                            .await
                            .map_err(|_| ())
                            .unwrap();
                    }
                })
            });

            let (output, _) = join!(pairing_future, future::join_all(worker_futures));

            output
        };

        phase_2_future.boxed()
    }

    fn par_routing<T, B, F1, F2, Fut>(
        self,
        buf_size: B,
        mut routing_fn: F1,
        mut map_fns: Vec<F2>,
    ) -> BoxStream<'static, T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        B: Into<BufSize>,
    {
        let buf_size = buf_size.into();
        let buf_size = match buf_size {
            BufSize::Default => Some(config::scale_positive(
                map_fns.len(),
                config::get_buf_size_scale(),
            )),
            _ => buf_size.get(),
        };

        let (reorder_tx, reorder_rx) = utils::channel(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        let mut map_txs: Vec<_> = map_fns
            .iter()
            .map(|_| {
                let (map_tx, map_rx) = utils::channel(buf_size);
                let reorder_tx = reorder_tx.clone();

                rt::spawn(async move {
                    while let Ok((counter, fut)) = map_rx.recv_async().await {
                        let output = fut.await;
                        if reorder_tx.send_async((counter, output)).await.is_err() {
                            break;
                        };
                    }
                });

                map_tx
            })
            .collect();

        let routing_fut = async move {
            let mut counter = 0u64;
            let mut stream = self.boxed();

            while let Some(item) = stream.next().await {
                let index = routing_fn(&item);
                let map_fn = map_fns
                    .get_mut(index)
                    .expect("the routing function returns an invalid index");
                let map_tx = map_txs.get_mut(index).unwrap();
                let fut = map_fn(item);
                if map_tx.send_async((counter, fut)).await.is_err() {
                    break;
                };

                counter = counter.wrapping_add(1);
            }
        };

        let reorder_fut = async move {
            let mut counter = 0u64;
            let mut pool = HashMap::new();

            while let Ok((index, output)) = reorder_rx.recv_async().await {
                if index != counter {
                    pool.insert(index, output);
                    continue;
                }

                if output_tx.send_async(output).await.is_err() {
                    break;
                };
                counter = counter.wrapping_add(1);

                while let Some(output) = pool.remove(&counter) {
                    if output_tx.send_async(output).await.is_err() {
                        break;
                    };
                    counter = counter.wrapping_add(1);
                }
            }
        };

        let join_fut = future::join(routing_fut, reorder_fut).boxed();

        utils::join_future_stream(join_fut, output_rx.into_stream()).boxed()
    }

    fn par_routing_unordered<T, B, F1, F2, Fut>(
        self,
        buf_size: B,
        mut routing_fn: F1,
        mut map_fns: Vec<F2>,
    ) -> BoxStream<'static, T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        B: Into<BufSize>,
    {
        let buf_size = buf_size.into();
        let buf_size = match buf_size {
            BufSize::Default => Some(config::scale_positive(
                map_fns.len(),
                config::get_buf_size_scale(),
            )),
            _ => buf_size.get(),
        };

        let (output_tx, output_rx) = utils::channel(buf_size);

        let mut map_txs: Vec<_> = map_fns
            .iter()
            .map(|_| {
                let (map_tx, map_rx) = utils::channel(buf_size);
                let output_tx = output_tx.clone();

                rt::spawn(async move {
                    while let Ok(fut) = map_rx.recv_async().await {
                        let output = fut.await;
                        if output_tx.send_async(output).await.is_err() {
                            break;
                        };
                    }
                });

                map_tx
            })
            .collect();

        let routing_fut = async move {
            let mut stream = self.boxed();

            while let Some(item) = stream.next().await {
                let index = routing_fn(&item);
                let map_fn = map_fns
                    .get_mut(index)
                    .expect("the routing function returns an invalid index");
                let map_tx = map_txs.get_mut(index).unwrap();
                let fut = map_fn(item);
                if map_tx.send_async(fut).await.is_err() {
                    break;
                };
            }
        };

        utils::join_future_stream(routing_fut, output_rx.into_stream()).boxed()
    }

    fn scatter<B>(self, buf_size: B) -> Scatter<Self::Item>
    where
        B: Into<BufSize>,
    {
        Scatter::new(self, buf_size)
    }

    fn par_for_each<N, F, Fut>(self, num_workers: N, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
        N: Into<NumWorkers>,
    {
        let num_workers = num_workers.into().get();
        let stream = self
            .stateful_map(f, |mut f, item| {
                let fut = f(item);
                Some((f, fut))
            })
            .shared();

        let worker_futures =
            (0..num_workers).map(move |_| rt::spawn(stream.clone().for_each(|fut| fut)));

        future::join_all(worker_futures).map(|_| ()).boxed()
    }

    fn par_for_each_blocking<N, F, Func>(self, num_workers: N, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
        N: Into<NumWorkers>,
    {
        let num_workers = num_workers.into().get();
        let stream = self
            .stateful_map(f, |mut f, item| {
                let func = f(item);
                Some((f, func))
            })
            .shared();

        let worker_futs: Vec<_> = (0..num_workers)
            .map(move |_| {
                let mut stream = stream.clone();

                rt::spawn_blocking(move || {
                    while let Some(job) = rt::block_on(stream.next()) {
                        job();
                    }
                })
            })
            .collect();

        future::join_all(worker_futs).map(|_| ()).boxed()
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::izip;
    use rand::prelude::*;
    use std::time::Duration;

    #[tokio::test]
    async fn broadcast_test() {
        let mut guard = stream::iter(0..).broadcast(2);
        let rx1 = guard.register();
        let rx2 = guard.register();
        guard.finish();

        let (ret1, ret2): (Vec<_>, Vec<_>) =
            join!(rx1.take(100).collect(), rx2.take(100).collect());

        izip!(ret1, 0..100).for_each(|(lhs, rhs)| {
            assert_eq!(lhs, rhs);
        });
        izip!(ret2, 0..100).for_each(|(lhs, rhs)| {
            assert_eq!(lhs, rhs);
        });
    }

    #[tokio::test]
    async fn par_batching_test() {
        let mut rng = rand::thread_rng();
        let data: Vec<u32> = (0..10000).map(|_| rng.gen_range(0..10)).collect();

        let sums: Vec<_> = stream::iter(data)
            .par_batching(None, |_, mut stream| async move {
                let mut sum = 0;

                while let Some(val) = stream.next().await {
                    sum += val;

                    if sum >= 1000 {
                        break;
                    }
                }

                Some((sum, stream))
            })
            .collect()
            .await;

        assert!(sums.iter().all(|&sum| sum >= 1000));
    }

    #[tokio::test]
    async fn par_then_output_is_ordered_test() {
        let max = 1000u64;
        stream::iter(0..max)
            .par_then(None, |value| async move {
                rt::sleep(Duration::from_millis(value % 20)).await;
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
        let mut values: Vec<_> = stream::iter((0..max).into_iter())
            .par_then_unordered(None, |value| async move {
                rt::sleep(Duration::from_millis(value % 20)).await;
                value
            })
            .collect()
            .await;
        values.sort();
        values.into_iter().fold(0, |expect, found| {
            assert_eq!(expect, found);
            expect + 1
        });
    }

    #[tokio::test]
    async fn par_reduce_test() {
        {
            let sum: Option<u64> = stream::iter(iter::empty())
                .par_reduce(None, |lhs, rhs| async move { lhs + rhs })
                .await;
            assert!(sum.is_none());
        }

        {
            let max = 100_000u64;
            let sum = stream::iter((1..=max).into_iter())
                .par_reduce(None, |lhs, rhs| async move { lhs + rhs })
                .await;
            assert_eq!(sum, Some((1 + max) * max / 2));
        }
    }

    #[tokio::test]
    async fn reorder_index_haling_test() {
        let indexes = vec![5, 2, 1, 0, 6, 4, 3];
        let output: Vec<_> = stream::iter(indexes)
            .then(|index| async move {
                rt::sleep(Duration::from_millis(20)).await;
                (index, index)
            })
            .reorder_enumerated()
            .collect()
            .await;
        assert_eq!(&output, &[0, 1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn enumerate_reorder_test() {
        let max = 1000u64;
        let iterator = (0..max).rev().step_by(2);

        let lhs = stream::iter(iterator.clone())
            .enumerate()
            .par_then_unordered(None, |(index, value)| async move {
                rt::sleep(std::time::Duration::from_millis(value % 20)).await;
                (index, value)
            })
            .reorder_enumerated();
        let rhs = stream::iter(iterator.clone());

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
            stream::iter(1..=1000)
                .par_for_each(None, {
                    let sum = sum.clone();
                    move |value| {
                        let sum = sum.clone();
                        async move {
                            sum.fetch_add(value, atomic::Ordering::SeqCst);
                        }
                    }
                })
                .await;
            assert_eq!(sum.load(atomic::Ordering::SeqCst), (1 + 1000) * 1000 / 2);
        }

        {
            let sum = Arc::new(AtomicUsize::new(0));
            stream::iter(1..=1000)
                .par_for_each_blocking(None, {
                    let sum = sum.clone();
                    move |value| {
                        let sum = sum.clone();
                        move || {
                            sum.fetch_add(value, atomic::Ordering::SeqCst);
                        }
                    }
                })
                .await;
            assert_eq!(sum.load(atomic::Ordering::SeqCst), (1 + 1000) * 1000 / 2);
        }
    }

    #[tokio::test]
    async fn tee_halt_test() {
        let mut rx1 = stream::iter(0..).tee(1);
        let mut rx2 = rx1.clone();

        assert!(rx1.next().await.is_some());
        assert!(rx2.next().await.is_some());

        // drop rx1
        drop(rx1);

        // the following should not block
        assert!(rx2.next().await.is_some());
        assert!(rx2.next().await.is_some());
        assert!(rx2.next().await.is_some());
        assert!(rx2.next().await.is_some());
        assert!(rx2.next().await.is_some());
    }

    #[tokio::test]
    async fn tee_test() {
        let orig: Vec<_> = (0..100).collect();

        let rx1 = stream::iter(orig.clone()).tee(1);
        let rx2 = rx1.clone();
        let rx3 = rx1.clone();

        let fut1 = rx1
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                rt::sleep(Duration::from_millis(millis)).await;
                val
            })
            .collect();
        let fut2 = rx2
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                rt::sleep(Duration::from_millis(millis)).await;
                val * 2
            })
            .collect();
        let fut3 = rx3
            .then(|val| async move {
                let millis = rand::thread_rng().gen_range(0..5);
                rt::sleep(Duration::from_millis(millis)).await;
                val * 3
            })
            .collect();

        let (vec1, vec2, vec3): (Vec<_>, Vec<_>, Vec<_>) = join!(fut1, fut2, fut3);

        // the collected method is possibly losing some of first few elements
        let start1 = orig.len() - vec1.len();
        let start2 = orig.len() - vec2.len();
        let start3 = orig.len() - vec3.len();

        assert!(orig[start1..]
            .iter()
            .zip(&vec1)
            .all(|(&orig, &val)| orig == val));
        assert!(orig[start2..]
            .iter()
            .zip(&vec2)
            .all(|(&orig, &val)| orig * 2 == val));
        assert!(orig[start3..]
            .iter()
            .zip(&vec3)
            .all(|(&orig, &val)| orig * 3 == val));
    }
}
