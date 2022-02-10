use crate::{
    broadcast::BroadcastBuilder,
    builder::ParBuilder,
    common::*,
    config::{BufSize, ParParams},
    index_stream::{IndexStreamExt as _, ReorderEnumerated},
    pull::PullBuilder,
    rt,
    stream::StreamExt as _,
    tee::Tee,
    utils,
};
use flume::r#async::RecvStream;

/// Stream for the [par_then()](ParStreamExt::par_then) method.
pub type ParThen<T> = ReorderEnumerated<RecvStream<'static, (usize, T)>, T>;

/// Stream for the [par_map()](ParStreamExt::par_map) method.
pub type ParMap<T> = ReorderEnumerated<RecvStream<'static, (usize, T)>, T>;

/// The trait extends [Stream](futures::stream::Stream) types with parallel processing combinators.
pub trait ParStreamExt
where
    Self: 'static + Send + Stream,
    Self::Item: 'static + Send,
{
    /// Moves the stream to a spawned worker and forwards stream items to a channel with `buf_size`.
    ///
    /// It returns a receiver [stream](RecvStream) that buffers the items. The receiver stream is
    /// cloneable so that items are sent in anycast manner.
    ///
    /// This combinator is similar to [shared()](crate::stream::StreamExt::shared).
    /// The difference is that `spawned()` spawns a worker that actively forwards stream
    /// items to the channel, and the receivers shares the channel. The `shared()` combinator
    /// directly poll the underlying stream whenever a receiver polls in lock-free manner.
    /// The choice of these combinator depends on the performance considerations.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// // Creates two sharing handles to the stream
    /// let stream = stream::iter(0..100);
    /// let recv1 = stream.spawned(None); // spawn with default buffer size
    /// let recv2 = recv1.clone(); // creates the second receiver
    ///
    /// // Consumes the shared streams individually
    /// let collect1 = par_stream::rt::spawn(recv1.collect());
    /// let collect2 = par_stream::rt::spawn(recv2.collect());
    /// let (vec1, vec2): (Vec<_>, Vec<_>) = futures::join!(collect1, collect2);
    ///
    /// // Checks that the combined values of two vecs are equal to original values
    /// let mut all_vec: Vec<_> = vec1.into_iter().chain(vec2).collect();
    /// all_vec.sort();
    /// itertools::assert_equal(all_vec, 0..100);
    /// # })
    /// ```
    fn spawned<B>(self, buf_size: B) -> RecvStream<'static, Self::Item>
    where
        B: Into<BufSize>;

    /// Maps this stream’s items to a different type on an blocking thread.
    ///
    /// The combinator iteratively maps the stream items and places the output
    /// items to a channel with `buf_size`. The function `f` is executed on a
    /// separate blocking thread to prevent from blocking the asynchronous runtime.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::{prelude::*, stream};
    /// use par_stream::prelude::*;
    ///
    /// let vec: Vec<_> = stream::iter(0..100)
    ///     .map_blocking(None, |_| {
    ///         // runs a CPU-bounded work here
    ///         (0..1000).sum::<u64>()
    ///     })
    ///     .collect()
    ///     .await;
    /// # })
    /// ```
    fn map_blocking<B, T, F>(self, buf_size: B, f: F) -> RecvStream<'static, T>
    where
        B: Into<BufSize>,
        T: Send,
        F: 'static + Send + FnMut(Self::Item) -> T;

    /// Creates a builder that routes each input item according to `key_fn` to a destination receiver.
    ///
    /// Call [`builder.register("key")`](PullBuilder::register) to obtain the receiving stream for that key.
    /// The builder must be finished by [`builder.build()`](PullBuilder::build) so that receivers start
    /// consuming items. [`builder.build()`](PullBuilder::build) also returns a special leaking receiver
    /// for items which key is not registered or target receiver is closed. Dropping the builder without
    /// [`builder.build()`](PullBuilder::build) will cause receivers to get empty input.
    fn pull_routing<B, K, Q, F>(self, buf_size: B, key_fn: F) -> PullBuilder<Self, K, F, Q>
    where
        Self: 'static + Send + Stream,
        Self::Item: 'static + Send,
        F: 'static + Send + FnMut(&Self::Item) -> Q,
        K: 'static + Send + Hash + Eq + Borrow<Q>,
        Q: Send + Hash + Eq,
        B: Into<BufSize>;

    /// Creates a builder that setups parallel tasks.
    fn par_builder(self) -> ParBuilder<Self>;

    /// The combinator maintains a collection of concurrent workers, each consuming as many elements as it likes,
    /// for each output element.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// let data = vec![1, 2, -3, 4, 5, -6, 7, 8];
    /// stream::iter(data).par_batching(None, |_worker_index, rx| async move {
    ///     while let Ok(value) = rx.recv_async().await {
    ///         if value > 0 {
    ///             return Some((value, rx));
    ///         }
    ///     }
    ///     None
    /// });
    /// # })
    /// ```
    fn par_batching<T, P, F, Fut>(self, params: P, f: F) -> RecvStream<'static, T>
    where
        Self: Sized,
        F: 'static + Send + Clone + FnMut(usize, flume::Receiver<Self::Item>) -> Fut,
        Fut: 'static + Future<Output = Option<(T, flume::Receiver<Self::Item>)>> + Send,
        T: 'static + Send,
        P: Into<ParParams>;

    /// Converts the stream to cloneable receivers, each receiving a copy for each input item.
    ///
    /// It spawns a task to consume the stream, and forwards item copies to receivers.
    /// The `buf_size` sets the interal channel size. Dropping a receiver does not cause another
    /// receiver to stop.
    ///
    /// Receivers are not guaranteed to get the same initial item due to the time difference
    /// among receiver creation time. Use [broadcast()](crate::par_stream::ParStreamExt::broadcast)
    /// instead if you need this guarantee.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::{join, prelude::*};
    /// use par_stream::prelude::*;
    ///
    /// let orig: Vec<_> = (0..1000).collect();
    ///
    /// let rx1 = stream::iter(orig.clone()).tee(1);
    /// let rx2 = rx1.clone();
    /// let rx3 = rx1.clone();
    ///
    /// let fut1 = rx1.map(|val| val).collect();
    /// let fut2 = rx2.map(|val| val * 2).collect();
    /// let fut3 = rx3.map(|val| val * 3).collect();
    ///
    /// let (vec1, vec2, vec3): (Vec<_>, Vec<_>, Vec<_>) = join!(fut1, fut2, fut3);
    /// # })
    /// ```
    fn tee<B>(self, buf_size: B) -> Tee<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>;

    /// Creates a [builder](BroadcastBuilder) to register broadcast receivers.
    ///
    /// Call [builder.register()](BroadcastBuilder::register) to create a receiver.
    /// Once the registration is done. [builder.build()](BroadcastBuilder::build) must
    /// be called so that receivers start comsuming item copies. If the builder is droppped
    /// without build, receivers get empty input.
    ///
    /// Each receiver maintains an internal buffer of `buf_size`. The `send_all` configures
    /// the behavior if any one of receiver closes. If `send_all` is true, closing of one receiver
    /// casues the other receivers to stop, otherwise it does not.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::{join, prelude::*};
    /// use par_stream::prelude::*;
    ///
    /// let mut builder = stream::iter(0..).broadcast(2, true);
    /// let rx1 = builder.register();
    /// let rx2 = builder.register();
    /// builder.build();
    ///
    /// let (ret1, ret2): (Vec<_>, Vec<_>) = join!(rx1.take(100).collect(), rx2.take(100).collect());
    /// let expect: Vec<_> = (0..100).collect();
    ///
    /// assert_eq!(ret1, expect);
    /// assert_eq!(ret2, expect);
    /// # })
    /// ```
    fn broadcast<B>(self, buf_size: B, send_all: bool) -> BroadcastBuilder<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>;

    /// Runs an asynchronous task on parallel workers and produces items respecting the input order.
    ///
    /// The `params` sets the worker pool size and output buffer size.
    /// Each parallel worker shares the stream and executes a future for each input item.
    /// Output items are gathered to a channel and are reordered respecting to input order.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// let doubled: Vec<_> = stream::iter(0..1000)
    ///     // doubles the values in parallel
    ///     .par_then(None, move |value| async move { value * 2 })
    ///     // the collected values will be ordered
    ///     .collect()
    ///     .await;
    /// let expect: Vec<_> = (0..1000).map(|value| value * 2).collect();
    /// assert_eq!(doubled, expect);
    /// # })
    /// ```
    fn par_then<T, P, F, Fut>(self, params: P, f: F) -> ParThen<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>;

    /// Runs an asynchronous task on parallel workers and produces items without respecting input order.
    ///
    /// The `params` sets the worker pool size and output buffer size.
    /// Each parallel worker shares the stream and executes a future for each input item.
    /// The worker forwards the output to a channel as soon as it finishes.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::collections::HashSet;
    ///
    /// let doubled: HashSet<_> = stream::iter(0..1000)
    ///     // doubles the values in parallel
    ///     .par_then_unordered(None, move |value| {
    ///         // the future is sent to a parallel worker
    ///         async move { value * 2 }
    ///     })
    ///     // the collected values may NOT be ordered
    ///     .collect()
    ///     .await;
    /// let expect: HashSet<_> = (0..1000).map(|value| value * 2).collect();
    /// assert_eq!(doubled, expect);
    /// # })
    /// ```
    fn par_then_unordered<T, P, F, Fut>(self, params: P, f: F) -> RecvStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        P: Into<ParParams>;

    /// Runs a blocking task on parallel workers and produces items respecting the input order.
    ///
    /// The `params` sets the worker pool size and output buffer size.
    /// Each parallel worker shares the stream and executes a future for each input item.
    /// Output items are gathered to a channel and are reordered respecting to input order.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// // the variable will be shared by parallel workers
    /// let doubled: Vec<_> = stream::iter(0..1000)
    ///     // doubles the values in parallel
    ///     .par_map(None, move |value| {
    ///         // the closure is sent to parallel worker
    ///         move || value * 2
    ///     })
    ///     // the collected values may NOT be ordered
    ///     .collect()
    ///     .await;
    /// let expect: Vec<_> = (0..1000).map(|value| value * 2).collect();
    /// assert_eq!(doubled, expect);
    /// # })
    /// ```
    fn par_map<T, P, F, Func>(self, params: P, f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>;

    /// Runs a blocking task on parallel workers and produces items without respecting input order.
    ///
    /// The `params` sets the worker pool size and output buffer size.
    /// Each parallel worker shares the stream and executes a future for each input item.
    /// The worker forwards the output to a channel as soon as it finishes.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::collections::HashSet;
    ///
    /// // the variable will be shared by parallel workers
    ///
    /// let doubled: HashSet<_> = stream::iter(0..1000)
    ///     // doubles the values in parallel
    ///     .par_map_unordered(None, move |value| {
    ///         // the closure is sent to parallel worker
    ///         move || value * 2
    ///     })
    ///     // the collected values may NOT be ordered
    ///     .collect()
    ///     .await;
    /// let expect: HashSet<_> = (0..1000).map(|value| value * 2).collect();
    /// assert_eq!(doubled, expect);
    /// # })
    /// ```
    fn par_map_unordered<T, P, F, Func>(self, params: P, f: F) -> RecvStream<'static, T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        P: Into<ParParams>;

    /// Reduces the input stream into a single value in parallel.
    ///
    /// It maintains a parallel worker pool of `num_workers`. Each worker reduces
    /// the input items from the stream into a single value. Once all parallel worker
    /// finish, the values from each worker are reduced into one in treefold manner.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// // the variable will be shared by parallel workers
    /// let sum = stream::iter(1..=1000)
    ///     // sum up the values in parallel
    ///     .par_reduce(None, move |lhs, rhs| {
    ///         // the closure is sent to parallel worker
    ///         async move { lhs + rhs }
    ///     })
    ///     .await;
    /// assert_eq!(sum, Some((1 + 1000) * 1000 / 2));
    /// # })
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

    /// Runs an asynchronous task on parallel workers.
    fn par_for_each<P, F, Fut>(self, params: P, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
        P: Into<ParParams>;

    /// Runs a blocking task on parallel workers.
    fn par_for_each_blocking<P, F, Func>(self, params: P, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
        P: Into<ParParams>;
}

impl<S> ParStreamExt for S
where
    S: 'static + Send + Stream,
    S::Item: 'static + Send,
{
    fn spawned<B>(self, buf_size: B) -> RecvStream<'static, Self::Item>
    where
        B: Into<BufSize>,
    {
        let (tx, rx) = utils::channel(buf_size.into().get());

        rt::spawn(async move {
            let _ = self.map(Ok).forward(tx.into_sink()).await;
        });

        rx.into_stream()
    }

    fn map_blocking<B, T, F>(self, buf_size: B, mut f: F) -> RecvStream<'static, T>
    where
        B: Into<BufSize>,
        T: Send,
        F: 'static + Send + FnMut(Self::Item) -> T,
    {
        let buf_size = buf_size.into().get();
        let mut stream = self.boxed();
        let (output_tx, output_rx) = utils::channel(buf_size);

        rt::spawn_blocking(move || {
            while let Some(input) = rt::block_on(stream.next()) {
                let output = f(input);
                if output_tx.send(output).is_err() {
                    break;
                }
            }
        });

        output_rx.into_stream()
    }

    fn par_builder(self) -> ParBuilder<Self> {
        ParBuilder::new(self)
    }

    fn par_batching<T, P, F, Fut>(self, params: P, f: F) -> RecvStream<'static, T>
    where
        F: 'static + Send + Clone + FnMut(usize, flume::Receiver<Self::Item>) -> Fut,
        Fut: 'static + Future<Output = Option<(T, flume::Receiver<Self::Item>)>> + Send,
        T: 'static + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let (input_tx, input_rx) = utils::channel(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        rt::spawn(async move {
            let _ = self.map(Ok).forward(input_tx.into_sink()).await;
        });

        (0..num_workers).for_each(move |worker_index| {
            let output_tx = output_tx.clone();
            let f = f.clone();
            let input_rx = input_rx.clone();

            rt::spawn(async move {
                let _ = stream::repeat(())
                    .stateful_then((input_rx, f), |(input_rx, mut f), ()| async move {
                        f(worker_index, input_rx)
                            .await
                            .map(move |(item, input_rx)| ((input_rx, f), item))
                    })
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });

        output_rx.into_stream()
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

    fn broadcast<B>(self, buf_size: B, send_all: bool) -> BroadcastBuilder<Self::Item>
    where
        Self::Item: Clone,
        B: Into<BufSize>,
    {
        BroadcastBuilder::new(self, buf_size, send_all)
    }

    fn par_then<T, P, F, Fut>(self, params: P, mut f: F) -> ParThen<T>
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
    }

    fn par_then_unordered<T, P, F, Fut>(self, params: P, f: F) -> RecvStream<'static, T>
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
            .spawned(buf_size);

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
        output_rx.into_stream()
    }

    fn par_map<T, P, F, Func>(self, params: P, mut f: F) -> ParMap<T>
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
    }

    fn par_map_unordered<T, P, F, Func>(self, params: P, f: F) -> RecvStream<'static, T>
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
            .spawned(buf_size);
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

        output_rx.into_stream()
    }

    fn par_reduce<P, F, Fut>(
        self,
        params: P,
        reduce_fn: F,
    ) -> BoxFuture<'static, Option<Self::Item>>
    where
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Self::Item> + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let stream = self.spawned(buf_size);

        // phase 1
        let phase_1_future = {
            let reduce_fn = reduce_fn.clone();
            async move {
                let reducer_futures = (0..num_workers).map(move |_| {
                    let reduce_fn = reduce_fn.clone();
                    let stream = stream.clone();

                    rt::spawn(async move { stream.reduce(reduce_fn).await })
                });

                future::join_all(reducer_futures).await
            }
        };

        // phase 2
        let phase_2_future = async move {
            let values = phase_1_future.await;

            let (pair_tx, pair_rx) = utils::channel(num_workers);
            let (feedback_tx, feedback_rx) = flume::bounded(num_workers);

            let mut count = 0;

            for value in values.into_iter().flatten() {
                feedback_tx.send_async(value).await.map_err(|_| ()).unwrap();
                count += 1;
            }

            let pairing_future = rt::spawn(async move {
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
            });

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

    fn par_for_each<P, F, Fut>(self, params: P, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let stream = self
            .stateful_map(f, |mut f, item| {
                let fut = f(item);
                Some((f, fut))
            })
            .spawned(buf_size);

        let worker_futures =
            (0..num_workers).map(move |_| rt::spawn(stream.clone().for_each(|fut| fut)));

        future::join_all(worker_futures).map(|_| ()).boxed()
    }

    fn par_for_each_blocking<P, F, Func>(self, params: P, f: F) -> BoxFuture<'static, ()>
    where
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() + Send,
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
            .spawned(buf_size);

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
    use crate::utils::async_test;
    use rand::prelude::*;
    use std::time::Duration;

    async_test! {
        async fn par_batching_test() {
            let mut rng = rand::thread_rng();
            let data: Vec<u32> = (0..10000).map(|_| rng.gen_range(0..10)).collect();

            let sums: Vec<_> = stream::iter(data)
                .par_batching(None, |_, rx| async move {
                    let mut sum = rx.recv_async().await.ok()?;

                    while let Ok(val) = rx.recv_async().await {
                        sum += val;

                        if sum >= 1000 {
                            return Some((sum, rx));
                        }
                    }

                    None
                })
                .collect()
                .await;

            assert!(sums.iter().all(|&sum| sum >= 1000));
        }


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
}
