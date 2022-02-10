//! Builder types for [par_builder](crate::par_stream::ParStreamExt::par_builder).

mod fn_factory;
mod future_factory;

pub use fn_factory::*;
pub use future_factory::*;

use crate::{
    common::*,
    config::ParParams,
    index_stream::{IndexStreamExt as _, ReorderEnumerated},
    par_stream::ParStreamExt as _,
    rt, utils,
};
use flume::r#async::RecvStream;
use tokio::sync::broadcast;

pub type UnorderedStream<T> = RecvStream<'static, T>;
pub type OrderedStream<T> = ReorderEnumerated<RecvStream<'static, (usize, T)>, T>;

/// Parallel stream builder created by [par_builder](crate::par_stream::ParStreamExt::par_builder).
pub struct ParBuilder<St>
where
    St: ?Sized + Stream,
{
    stream: St,
}

/// The parallel stream builder with scheduled asynchronous tasks.
pub struct ParAsyncBuilder<St, Fac>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    Fac: FutureFactory<St::Item>,
    Fac::Fut: 'static + Send + Future,
    <Fac::Fut as Future>::Output: Send,
{
    fac: Fac,
    stream: St,
}

/// The parallel stream builder with scheduled asynchronous tasks and a blocking task in the end.
pub struct ParAsyncTailBlockBuilder<St, FutFac, FnFac, Out>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    FutFac: FutureFactory<St::Item>,
    FutFac::Fut: 'static + Send + Future,
    <FutFac::Fut as Future>::Output: Send,
    FnFac: FnFactory<<FutFac::Fut as Future>::Output, Out>,
    FnFac::Fn: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    fut_fac: FutFac,
    fn_fac: FnFac,
    _phantom: PhantomData<Out>,
    stream: St,
}

/// The parallel stream builder with scheduled blocking tasks.
pub struct ParBlockingBuilder<St, Fac, Out>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    Fac: FnFactory<St::Item, Out>,
    Fac::Fn: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    fac: Fac,
    _phantom: PhantomData<Out>,
    stream: St,
}

impl<St> ParBuilder<St>
where
    St: Stream,
{
    /// Creates a builder instance from a stream.
    pub fn new(stream: St) -> Self {
        Self { stream }
    }

    /// Schedules an asynchronous task.
    pub fn map_async<Fut, Fac>(self, fac: Fac) -> ParAsyncBuilder<St, Fac>
    where
        St::Item: Send,
        Fac: 'static + Send + FnMut(St::Item) -> Fut,
        Fut: 'static + Send + Future,
        Fut::Output: Send,
    {
        let Self { stream } = self;

        ParAsyncBuilder { fac, stream }
    }

    /// Schedules a blocking task.
    pub fn map_blocking<Fac, Func, Out>(self, fac: Fac) -> ParBlockingBuilder<St, Fac, Out>
    where
        St::Item: 'static + Send,
        Fac: 'static + Send + FnMut(St::Item) -> Func,
        Func: 'static + Send + FnOnce() -> Out,
        Out: Send,
    {
        let Self { stream } = self;

        ParBlockingBuilder {
            fac,
            stream,
            _phantom: PhantomData,
        }
    }
}

impl<St, Fac> ParAsyncBuilder<St, Fac>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FutureFactory<St::Item>,
    Fac::Fut: 'static + Send + Future,
    <Fac::Fut as Future>::Output: 'static + Send,
{
    /// Schedule an asynchronous task.
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<St, ComposeFutureFactory<St::Item, Fac, NewFac>>
    where
        NewFac: 'static + Send + Clone + FnMut(<Fac::Fut as Future>::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fac: orig_fac,
            stream,
            ..
        } = self;

        ParAsyncBuilder {
            fac: orig_fac.compose(new_fac),
            stream,
        }
    }

    /// Schedules a blocking task.
    ///
    /// The worker thread will pass the blocking task to a blocking thread and
    /// wait for the task to finish. It will introduce overhead on spawning blocking threads.
    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncTailBlockBuilder<St, Fac, NewFac, NewOut>
    where
        NewFac: 'static + Send + Clone + FnMut(<Fac::Fut as Future>::Output) -> NewFunc,
        NewFunc: 'static + Send + FnOnce() -> NewOut,
        NewFunc::Output: 'static + Send,
    {
        let Self {
            fac: orig_fac,
            stream,
            ..
        } = self;

        ParAsyncTailBlockBuilder {
            fut_fac: orig_fac,
            fn_fac: new_fac,
            _phantom: PhantomData,
            stream,
        }
    }

    /// Creates a stream that runs scheduled parallel tasks, which output does not respect the input order.
    pub fn build_unordered_stream<P>(
        self,
        params: P,
    ) -> UnorderedStream<<Fac::Fut as Future>::Output>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let stream = stream.map(move |item| fac.generate(item)).spawned(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

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

    /// Creates a stream that runs scheduled parallel tasks, which output respects the input order.
    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<<Fac::Fut as Future>::Output>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let stream = stream
            .map(move |item| fac.generate(item))
            .enumerate()
            .spawned(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(move |_| {
            let stream = stream.clone();
            let output_tx = output_tx.clone();

            rt::spawn(async move {
                let _ = stream
                    .then(|(index, fut)| async move { (index, fut.await) })
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });

        output_rx.into_stream().reorder_enumerated()
    }
}

impl<St, Fac> ParAsyncBuilder<St, Fac>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FutureFactory<St::Item>,
    Fac::Fut: 'static + Send + Future<Output = ()>,
{
    /// Runs parallel tasks on each stream item.
    pub async fn for_each<P>(self, params: P)
    where
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let Self {
            mut fac, stream, ..
        } = self;
        let stream = stream.map(move |item| fac.generate(item)).spawned(buf_size);

        let worker_futures = (0..num_workers).map(move |_| {
            let stream = stream.clone();

            rt::spawn(async move {
                stream.for_each(|fut| fut).await;
            })
        });

        future::join_all(worker_futures).await;
    }
}

impl<St, Fac, Error> ParAsyncBuilder<St, Fac>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FutureFactory<St::Item>,
    Fac::Fut: 'static + Send + Future<Output = Result<(), Error>>,
    Error: 'static + Send,
{
    /// Runs parallel tasks on each stream item.
    pub async fn try_for_each<P>(self, params: P) -> Result<(), Error>
    where
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let Self {
            mut fac, stream, ..
        } = self;
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);
        let stream = stream
            .take_until(async move {
                let _ = terminate_rx.recv().await;
            })
            .map(move |item| fac.generate(item))
            .spawned(buf_size);

        let worker_futures = (0..num_workers).map(move |_| {
            let stream = stream.clone();
            let terminate_tx = terminate_tx.clone();

            rt::spawn(async move {
                let result = stream.map(Ok).try_for_each(|fut| fut).await;

                if result.is_err() {
                    let _ = terminate_tx.send(());
                }

                result
            })
        });

        future::try_join_all(worker_futures).await?;
        Ok(())
    }
}

impl<St, Fac, Out> ParBlockingBuilder<St, Fac, Out>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnFactory<St::Item, Out>,
    Fac::Fn: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    /// Schedule an asynchronous task.
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<
        St,
        ComposeFutureFactory<St::Item, impl FnMut(St::Item) -> rt::JoinHandle<Out>, NewFac>,
    >
    where
        NewFac: Send + Clone + FnMut(Out) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fac: mut orig_fac,
            stream,
            ..
        } = self;

        let orig_fac_async = move |input: St::Item| rt::spawn_blocking(orig_fac.generate(input));

        ParAsyncBuilder {
            fac: orig_fac_async.compose(new_fac),
            stream,
        }
    }

    /// Schedule a blocking task.
    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParBlockingBuilder<St, BoxFnFactory<St::Item, NewOut>, NewOut>
    where
        NewFac: 'static + Send + Clone + FnMut(Out) -> NewFunc,
        NewFunc: 'static + Send + FnOnce() -> NewOut,
        NewFunc::Output: 'static + Send,
    {
        let Self {
            fac: orig_fac,
            stream,
            ..
        } = self;

        ParBlockingBuilder {
            fac: orig_fac.chain(new_fac),
            _phantom: PhantomData,
            stream,
        }
    }

    /// Creates a stream that runs scheduled parallel tasks, which output does not respect the input order.
    pub fn build_unordered_stream<P>(self, params: P) -> UnorderedStream<Out>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let stream = stream.map(move |item| fac.generate(item)).spawned(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(move |_| {
            let mut stream = stream.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || {
                while let Some(func) = rt::block_on(stream.next()) {
                    let output = func();
                    let result = output_tx.send(output);
                    if result.is_err() {
                        break;
                    }
                }
            });
        });

        output_rx.into_stream()
    }

    /// Creates a stream that runs scheduled parallel tasks, which output respects the input order.
    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<Out>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let stream = stream
            .map(move |item| fac.generate(item))
            .enumerate()
            .spawned(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(move |_| {
            let mut stream = stream.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || {
                while let Some((index, func)) = rt::block_on(stream.next()) {
                    let output = func();
                    let result = output_tx.send((index, output));
                    if result.is_err() {
                        break;
                    }
                }
            });
        });

        output_rx.into_stream().reorder_enumerated()
    }
}

impl<St, Fac> ParBlockingBuilder<St, Fac, ()>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnFactory<St::Item, ()>,
    Fac::Fn: 'static + Send + FnOnce(),
{
    /// Runs parallel tasks on each stream item.
    pub async fn for_each<P>(self, params: P)
    where
        P: Into<ParParams>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let stream = stream.map(move |item| fac.generate(item)).spawned(buf_size);

        let worker_futures = (0..num_workers).map(move |_| {
            let mut stream = stream.clone();

            rt::spawn_blocking(move || {
                while let Some(func) = rt::block_on(stream.next()) {
                    func();
                }
            })
        });

        future::join_all(worker_futures).await;
    }
}

// impl<St, Fac, Error> ParBlockingBuilder<St, Fac, ()>
// where
//     St: 'static + Send + Stream,
//     St::Item: 'static + Send,
//     Fac: 'static + Send + FnFactory<St::Item, Result<(), Error>>,
//     Fac::Fn: 'static + Send + FnOnce() -> Result<(), Error>,
//     Error: 'static + Send,
// {
//     /// Runs parallel tasks on each stream item.
//     pub async fn try_for_each<N>(self, num_workers: N) -> Result<(), Error>
//     where
//         N: Into<NumWorkers>,
//     {
//         let Self {
//             mut fac, stream, ..
//         } = self;
//         let num_workers = num_workers.into().get();
//         let (terminate_tx, mut terminate_rx) = broadcast::channel(1);
//         let stream = stream.take_until(async move {
//             let _ = terminate_rx.recv().await;
//         }).map(move |item| fac.generate(item)).shared();

//         let worker_futures = (0..num_workers).map(move |_| {
//             let mut stream = stream.clone();
//             let terminate_tx =  terminate_tx.clone();

//             rt::spawn_blocking(move || {
//                 while let Some(func) = rt::block_on(stream.next()) {
//                     let result = func();

//                     if result.is_err() {
//                         let _ = terminate_rx.send(());
//                         return result;
//                     }
//                 }

//                 Ok(())
//             })
//         });

//         future::try_join_all(worker_futures).await?;
//         Ok(())
//     }
// }

impl<St, FutFac, FnFac, Out> ParAsyncTailBlockBuilder<St, FutFac, FnFac, Out>
where
    St: Stream,
    St::Item: 'static + Send,
    FutFac: 'static + Send + FutureFactory<St::Item>,
    FutFac::Fut: 'static + Send + Future,
    <FutFac::Fut as Future>::Output: 'static + Send,
    FnFac: 'static + Send + Clone + FnFactory<<FutFac::Fut as Future>::Output, Out>,
    FnFac::Fn: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    /// Schedule an asynchronous task.
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<St, BoxFutureFactory<'static, St::Item, NewFut::Output>>
    where
        NewFac: 'static + Send + Clone + FnMut(Out) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fut_fac,
            mut fn_fac,
            stream,
            ..
        } = self;

        let fn_fac_async = move |input: <FutFac::Fut as Future>::Output| {
            rt::spawn_blocking(fn_fac.generate(input))
        };

        ParAsyncBuilder {
            fac: fut_fac.compose(fn_fac_async).compose(new_fac).boxed(),
            stream,
        }
    }

    /// Schedule a blocking task.
    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncTailBlockBuilder<
        St,
        FutFac,
        BoxFnFactory<<FutFac::Fut as Future>::Output, NewOut>,
        NewOut,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Out) -> NewFunc,
        NewFunc: 'static + Send + FnOnce() -> NewOut,
        NewFunc::Output: 'static + Send,
    {
        let Self {
            fut_fac,
            fn_fac,
            stream,
            ..
        } = self;

        ParAsyncTailBlockBuilder {
            fut_fac,
            fn_fac: fn_fac.chain(new_fac),
            _phantom: PhantomData,
            stream,
        }
    }

    /// Creates a stream that runs scheduled parallel tasks, which output does not respect the input order.
    pub fn build_unordered_stream<P>(self, params: P) -> UnorderedStream<Out>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        self.into_async_builder().build_unordered_stream(params)
    }

    /// Creates a stream that runs scheduled parallel tasks, which output respects the input order.
    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<Out>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        self.into_async_builder().build_ordered_stream(params)
    }

    fn into_async_builder(self) -> ParAsyncBuilder<St, BoxFutureFactory<'static, St::Item, Out>> {
        let Self {
            fut_fac,
            mut fn_fac,
            stream,
            ..
        } = self;

        let fn_fac_async = move |input: <FutFac::Fut as Future>::Output| {
            rt::spawn_blocking(fn_fac.generate(input))
        };

        ParAsyncBuilder {
            fac: fut_fac.compose(fn_fac_async).boxed(),
            stream,
        }
    }
}

impl<St, FutFac, FnFac> ParAsyncTailBlockBuilder<St, FutFac, FnFac, ()>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    FutFac: 'static + Send + FutureFactory<St::Item>,
    FutFac::Fut: 'static + Send + Future,
    <FutFac::Fut as Future>::Output: 'static + Send,
    FnFac: 'static + Send + Clone + FnFactory<<FutFac::Fut as Future>::Output, ()>,
    FnFac::Fn: 'static + Send + FnOnce(),
{
    /// Runs parallel tasks on each stream item.
    pub async fn for_each<P>(self, params: P)
    where
        P: Into<ParParams>,
    {
        self.into_async_builder().for_each(params).await;
    }
}

// impl<St, FutFac, FnFac, Error> ParAsyncTailBlockBuilder<St, FutFac, FnFac, ()>
// where
//     St: 'static + Send + Stream,
//     St::Item: 'static + Send,
//     FutFac: 'static + Send + FutureFactory<St::Item>,
//     FutFac::Fut: 'static + Send + Future,
//     <FutFac::Fut as Future>::Output: 'static + Send,
//     FnFac: 'static + Send + Clone + FnFactory<<FutFac::Fut as Future>::Output, Result<(), Error>>,
//     FnFac::Fn: 'static + Send + FnOnce() -> Result<(), Error>,
// {
//     /// Runs parallel tasks on each stream item.
//     pub async fn try_for_each<N>(self, num_workers: N)
//     where
//         N: Into<NumWorkers>,
//     {
//         self.into_async_builder().try_for_each(num_workers).await;
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::async_test;

    async_test! {
        async fn par_builder_blocking_test() {
            let vec: Vec<_> = stream::iter(1u64..=1000)
                .par_builder()
                .map_blocking(|val| move || val.pow(5))
                .map_blocking(|val| move || val + 1)
                .build_ordered_stream(None)
                .collect()
                .await;
            let expect: Vec<_> = (1u64..=1000).map(|val| val.pow(5) + 1).collect();

            assert_eq!(vec, expect);
        }

        async fn par_builder_async_test() {
            let vec: Vec<_> = stream::iter(1u64..=1000)
                .par_builder()
                .map_async(|val| async move { val.pow(5) })
                .map_async(|val| async move { val + 1 })
                .build_ordered_stream(None)
                .collect()
                .await;
            let expect: Vec<_> = (1u64..=1000).map(|val| val.pow(5) + 1).collect();

            assert_eq!(vec, expect);
        }

        async fn par_builder_mixed_async_blocking_test() {
            {
                let vec: Vec<_> = stream::iter(1u64..=1000)
                    .par_builder()
                    .map_async(|val| async move { val.pow(5) })
                    .map_blocking(|val| move || val + 1)
                    .build_ordered_stream(None)
                    .collect()
                    .await;
                let expect: Vec<_> = (1u64..=1000).map(|val| val.pow(5) + 1).collect();

                assert_eq!(vec, expect);
            }

            {
                let vec: Vec<_> = stream::iter(1u64..=1000)
                    .par_builder()
                    .map_blocking(|val| move || val.pow(5))
                    .map_async(|val| async move { val + 1 })
                    .build_ordered_stream(None)
                    .collect()
                    .await;
                let expect: Vec<_> = (1u64..=1000).map(|val| val.pow(5) + 1).collect();

                assert_eq!(vec, expect);
            }

            {
                let vec: Vec<_> = stream::iter(1u64..=1000)
                    .par_builder()
                    .map_blocking(|val| move || val.pow(5))
                    .map_async(|val| async move { val + 1 })
                    .map_blocking(|val| move || val / 2)
                    .build_ordered_stream(None)
                    .collect()
                    .await;
                let expect: Vec<_> = (1u64..=1000).map(|val| (val.pow(5) + 1) / 2).collect();

                assert_eq!(vec, expect);
            }

            {
                let vec: Vec<_> = stream::iter(1u64..=1000)
                    .par_builder()
                    .map_async(|val| async move { val.pow(5) })
                    .map_blocking(|val| move || val + 1)
                    .map_async(|val| async move { val / 2 })
                    .build_ordered_stream(None)
                    .collect()
                    .await;
                let expect: Vec<_> = (1u64..=1000).map(|val| (val.pow(5) + 1) / 2).collect();

                assert_eq!(vec, expect);
            }
        }

        // #[tokio::test]
        // async fn par_unfold_builder_async_test() {
        //     let vec: Vec<_> = super::par_unfold_builder(|| async move {

        //     }).into_stream().collect();
        // }
    }
}
