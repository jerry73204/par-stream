use crate::common::*;
use crate::config::BufSize;
use crate::config::NumWorkers;
use crate::fn_factory::{BoxFnFactory, FnFactory};
use crate::future_factory::{BoxFutureFactory, FutureFactory};
use crate::rt;
use crate::utils;

pub struct ParBuilder<St>
where
    St: ?Sized,
{
    pub input_buf_size: BufSize,
    pub num_workers: NumWorkers,
    pub stream: St,
}

impl<St> ParBuilder<St>
where
    St: Stream,
{
    pub fn map_async<Fac, Fut>(self, fac: Fac) -> ParAsyncBuilder<St, Fac>
    where
        Fac: FnMut(St::Item) -> Fut,
        Fut: Future,
    {
        let Self {
            input_buf_size,
            num_workers,
            stream,
        } = self;

        ParAsyncBuilder {
            input_buf_size,
            num_workers,
            fac,
            stream,
        }
    }

    pub fn map_blocking<Fac, Func>(self, fac: Fac) -> ParBlockingBuilder<St, Fac>
    where
        Fac: FnMut(St::Item) -> Func,
        Func: FnOnce(),
    {
        let Self {
            input_buf_size,
            num_workers,
            stream,
        } = self;

        ParBlockingBuilder {
            input_buf_size,
            num_workers,
            fac,
            stream,
        }
    }
}

pub struct ParAsyncBuilder<St, Fac>
where
    St: ?Sized,
{
    pub input_buf_size: BufSize,
    pub num_workers: NumWorkers,
    pub fac: Fac,
    pub stream: St,
}

impl<St, Fac, Fut> ParAsyncBuilder<St, Fac>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Fut,
    Fut: 'static + Send + Future,
    Fut::Output: 'static + Send,
{
    pub fn map_async<NewOut, NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<St, BoxFutureFactory<St::Item, NewFut::Output>>
    where
        NewFac: 'static + Send + Clone + FnMut(Fut::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            input_buf_size,
            num_workers,
            fac: orig_fac,
            stream,
        } = self;

        ParAsyncBuilder {
            input_buf_size,
            num_workers,
            fac: orig_fac.chain(new_fac),
            stream,
        }
    }

    pub fn map_blocking<NewOut, NewFac, NewFunc>(
        self,
        mut new_fac: NewFac,
    ) -> ParAsyncBuilder<St, BoxFutureFactory<St::Item, NewFunc::Output>>
    where
        NewFac: 'static + Send + Clone + FnMut(Fut::Output) -> NewFunc,
        NewFunc: 'static + Send + FnOnce(),
        NewFunc::Output: 'static + Send,
    {
        let Self {
            input_buf_size,
            num_workers,
            fac: orig_fac,
            stream,
        } = self;

        let new_fac_async = move |input: Fut::Output| rt::spawn_blocking(new_fac(input));

        ParAsyncBuilder {
            input_buf_size,
            num_workers,
            fac: orig_fac.chain(new_fac_async),
            stream,
        }
    }
}

impl<St, Fac, Fut> ParAsyncBuilder<St, Fac>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Fut,
    Fut: 'static + Send + Future<Output = ()>,
{
    pub async fn for_each(self) {
        let Self {
            input_buf_size,
            num_workers,
            mut fac,
            stream,
        } = self;
        let num_workers = num_workers.get();
        let (tx, rx) = utils::channel(input_buf_size.get());

        let input_future = rt::spawn(async move {
            let _ = stream
                .map(|item| {
                    let fut = fac(item);
                    Ok(fut)
                })
                .forward(tx.into_sink())
                .await;
        });

        let worker_futures = (0..num_workers).map(|_| {
            let rx = rx.clone();

            rt::spawn(async move {
                rx.into_stream().for_each(|fut| fut).await;
            })
        });

        join!(input_future, future::join_all(worker_futures));
    }
}

pub struct ParBlockingBuilder<St, Fac>
where
    St: ?Sized,
{
    pub input_buf_size: BufSize,
    pub num_workers: NumWorkers,
    pub fac: Fac,
    pub stream: St,
}

impl<St, Fac, Func> ParBlockingBuilder<St, Fac>
where
    St: Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Func,
    Func: 'static + Send + FnOnce(),
    Func::Output: 'static + Send,
{
    pub fn map_async<NewFac, NewFut>(
        self,
        new_fac: NewFac,
    ) -> ParAsyncBuilder<St, BoxFutureFactory<St::Item, NewFut::Output>>
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFut,
        NewFut: 'static + Send + Future,
        NewFut::Output: 'static + Send,
    {
        let Self {
            input_buf_size,
            num_workers,
            fac: mut orig_fac,
            stream,
        } = self;

        let orig_fac_async = move |input: St::Item| rt::spawn_blocking(orig_fac(input));

        ParAsyncBuilder {
            input_buf_size,
            num_workers,
            fac: orig_fac_async.chain(new_fac),
            stream,
        }
    }

    pub fn map_blocking<NewFac, NewFunc>(
        self,
        new_fac: NewFac,
    ) -> ParBlockingBuilder<St, BoxFnFactory<St::Item, NewFunc::Output>>
    where
        NewFac: 'static + Send + Clone + FnMut(Func::Output) -> NewFunc,
        NewFunc: 'static + Send + FnOnce(),
        NewFunc::Output: 'static + Send,
    {
        let Self {
            input_buf_size,
            num_workers,
            fac: orig_fac,
            stream,
        } = self;

        ParBlockingBuilder {
            input_buf_size,
            num_workers,
            fac: orig_fac.chain(new_fac),
            stream,
        }
    }
}

impl<St, Fac, Func> ParBlockingBuilder<St, Fac>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    Fac: 'static + Send + FnMut(St::Item) -> Func,
    Func: 'static + Send + FnOnce() -> (),
{
    pub async fn for_each(self) {
        let Self {
            input_buf_size,
            num_workers,
            mut fac,
            stream,
        } = self;
        let num_workers = num_workers.get();
        let (tx, rx) = utils::channel(input_buf_size.get());

        let input_future = rt::spawn(async move {
            let _ = stream
                .map(|item| {
                    let func = fac(item);
                    Ok(func)
                })
                .forward(tx.into_sink())
                .await;
        });

        let worker_futures = (0..num_workers).map(|_| {
            let rx = rx.clone();

            rt::spawn_blocking(move || {
                while let Ok(func) = rx.recv() {
                    func();
                }
            })
        });

        join!(input_future, future::join_all(worker_futures));
    }
}
