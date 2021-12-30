mod fn_factory;
mod future_factory;

pub use fn_factory::*;
pub use future_factory::*;

use crate::{
    common::*,
    config::{NumWorkers, ParParams},
    index_stream::{IndexStreamExt as _, ReorderEnumerated},
    rt,
    stream::StreamExt as _,
    utils,
};
use flume::r#async::RecvStream;

pub type UnorderedStream<T> = RecvStream<'static, T>;
pub type OrderedStream<T> = ReorderEnumerated<RecvStream<'static, (usize, T)>, T>;

pub struct ParBuilder<St>
where
    St: ?Sized + Stream,
{
    stream: St,
}

pub struct ParAsyncBuilder<St, Fac, Fut>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    Fac: FutureFactory<St::Item, Fut::Output, Fut>,
    Fut: 'static + Send + Future,
    Fut::Output: Send,
{
    fac: Fac,
    _phantom: PhantomData<Fut>,
    stream: St,
}

pub struct ParAsyncTailBlockBuilder<St, FutFac, Fut, FnFac, Func, Out>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    FutFac: FutureFactory<St::Item, Fut::Output, Fut>,
    Fut: 'static + Send + Future,
    Fut::Output: Send,
    FnFac: FnFactory<Fut::Output, Out, Func>,
    Func: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    fut_fac: FutFac,
    fn_fac: FnFac,
    _phantom: PhantomData<(Fut, Func, Out)>,
    stream: St,
}

pub struct ParBlockingBuilder<St, Fac, Func, Out>
where
    St: ?Sized + Stream,
    St::Item: 'static + Send,
    Fac: FnFactory<St::Item, Out, Func>,
    Func: 'static + Send + FnOnce() -> Out,
    Out: 'static + Send,
{
    fac: Fac,
    _phantom: PhantomData<(Func, Out)>,
    stream: St,
}

impl<St> ParBuilder<St>
where
    St: Stream,
{
    pub fn new(stream: St) -> Self {
        Self { stream }
    }

    pub fn map_async<Fac, Fut>(self, fac: Fac) -> ParAsyncBuilder<St, Fac, Fut>
    where
        St::Item: Send,
        Fac: 'static + Send + FnMut(St::Item) -> Fut,
        Fut: Send + Future,
        Fut::Output: Send,
    {
        let Self { stream } = self;

        ParAsyncBuilder {
            fac,
            stream,
            _phantom: PhantomData,
        }
    }

    pub fn map_blocking<Fac, Func, Out>(self, fac: Fac) -> ParBlockingBuilder<St, Fac, Func, Out>
    where
        St::Item: 'static + Send,
        Fac: 'static + Send + FnMut(St::Item) -> Func,
        Func: Send + FnOnce() -> Out,
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

impl<St, Fac, Fut> ParAsyncBuilder<St, Fac, Fut>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Fut,
    Fut: 'static + Send + Future,
    Fut::Output: 'static + Send,
{
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<
        St,
        BoxFutureFactory<St::Item, NewFut::Output>,
        BoxFuture<'static, NewFut::Output>,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Fut::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fac: orig_fac,
            stream,
            ..
        } = self;

        ParAsyncBuilder {
            fac: orig_fac.chain(new_fac),
            _phantom: PhantomData,
            stream,
        }
    }

    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncTailBlockBuilder<St, Fac, Fut, NewFac, NewFunc, NewOut>
    where
        NewFac: 'static + Send + Clone + FnMut(Fut::Output) -> NewFunc,
        NewFunc: 'static + Send + FnOnce() -> NewOut,
        NewFunc::Output: 'static + Send,
    {
        let Self {
            fac: orig_fac,
            stream,
            ..
        } = self;

        // let new_fac_async = move |input: Fut::Output| rt::spawn_blocking(new_fac(input));
        ParAsyncTailBlockBuilder {
            fut_fac: orig_fac,
            fn_fac: new_fac,
            _phantom: PhantomData,
            stream,
        }
    }

    pub fn build_unordered_stream<P>(self, params: P) -> UnorderedStream<Fut::Output>
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

        let stream = stream.map(move |item| fac(item)).shared();
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

    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<Fut::Output>
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

        let stream = stream.map(move |item| fac(item)).enumerate().shared();
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

impl<St, Fac, Fut> ParAsyncBuilder<St, Fac, Fut>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Fut,
    Fut: 'static + Send + Future<Output = ()>,
{
    pub async fn for_each<N>(self, num_workers: N)
    where
        N: Into<NumWorkers>,
    {
        let num_workers = num_workers.into().get();
        let Self {
            mut fac, stream, ..
        } = self;
        let stream = stream.map(move |item| fac(item)).shared();

        let worker_futures = (0..num_workers).map(move |_| {
            let stream = stream.clone();

            rt::spawn(async move {
                stream.for_each(|fut| fut).await;
            })
        });

        future::join_all(worker_futures).await;
    }
}

impl<St, Fac, Func, Out> ParBlockingBuilder<St, Fac, Func, Out>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Func,
    Func: 'static + Send + FnOnce() -> Out,
    Func::Output: 'static + Send,
{
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<
        St,
        BoxFutureFactory<St::Item, NewFut::Output>,
        BoxFuture<'static, NewFut::Output>,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fac: mut orig_fac,
            stream,
            ..
        } = self;

        let orig_fac_async = move |input: St::Item| rt::spawn_blocking(orig_fac(input));

        ParAsyncBuilder {
            fac: orig_fac_async.chain(new_fac),
            _phantom: PhantomData,
            stream,
        }
    }

    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParBlockingBuilder<
        St,
        BoxFnFactory<St::Item, NewOut>,
        Box<dyn FnOnce() -> NewOut + Send>,
        NewOut,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFunc,
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

    pub fn build_unordered_stream<P>(self, params: P) -> UnorderedStream<Func::Output>
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

        let stream = stream.map(move |item| fac(item)).shared();
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

    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<Func::Output>
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

        let stream = stream.map(move |item| fac(item)).enumerate().shared();
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

impl<St, Fac, Func> ParBlockingBuilder<St, Fac, Func, ()>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Func,
    Func: 'static + Send + FnOnce() -> (),
{
    pub async fn for_each<N>(self, num_workers: N)
    where
        N: Into<NumWorkers>,
    {
        let Self {
            mut fac, stream, ..
        } = self;
        let num_workers = num_workers.into().get();
        let stream = stream.map(move |item| fac(item)).shared();

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

impl<St, FutFac, Fut, FnFac, Func, Out> ParAsyncTailBlockBuilder<St, FutFac, Fut, FnFac, Func, Out>
where
    St: Stream,
    St::Item: 'static + Send,
    FutFac: 'static + Send + FnMut(St::Item) -> Fut,
    Fut: 'static + Send + Future,
    Fut::Output: 'static + Send,
    FnFac: 'static + Send + Clone + FnMut(Fut::Output) -> Func,
    Func: 'static + Send + FnOnce() -> Out,
    Func::Output: 'static + Send,
{
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<
        St,
        BoxFutureFactory<St::Item, NewFut::Output>,
        BoxFuture<'static, NewFut::Output>,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            fut_fac,
            mut fn_fac,
            stream,
            ..
        } = self;

        let fn_fac_async = move |input: Fut::Output| rt::spawn_blocking(fn_fac(input));

        ParAsyncBuilder {
            fac: fut_fac.chain(fn_fac_async).chain(new_fac),
            _phantom: PhantomData,
            stream,
        }
    }

    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncTailBlockBuilder<
        St,
        FutFac,
        Fut,
        BoxFnFactory<Fut::Output, NewOut>,
        Box<dyn FnOnce() -> NewOut + Send>,
        NewOut,
    >
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFunc,
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

    pub fn build_unordered_stream<P>(self, params: P) -> UnorderedStream<Func::Output>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        self.into_async_builder().build_unordered_stream(params)
    }

    pub fn build_ordered_stream<P>(self, params: P) -> OrderedStream<Func::Output>
    where
        St: 'static + Send,
        P: Into<ParParams>,
    {
        self.into_async_builder().build_ordered_stream(params)
    }

    fn into_async_builder(
        self,
    ) -> ParAsyncBuilder<
        St,
        BoxFutureFactory<St::Item, Func::Output>,
        BoxFuture<'static, Func::Output>,
    > {
        let Self {
            fut_fac,
            mut fn_fac,
            stream,
            ..
        } = self;

        let fn_fac_async = move |input: Fut::Output| rt::spawn_blocking(fn_fac(input));

        ParAsyncBuilder {
            fac: fut_fac.chain(fn_fac_async),
            _phantom: PhantomData,
            stream,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::par_stream::ParStreamExt as _;

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
}
