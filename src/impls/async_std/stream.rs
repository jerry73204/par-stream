use crate::{
    base::ParStreamExt as _,
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
};
pub use tokio::sync::{Notify, Semaphore};

pub fn par_gather<S>(
    streams: impl IntoIterator<Item = S>,
    buf_size: impl Into<Option<usize>>,
) -> ParGather<S::Item>
where
    S: 'static + StreamExt + Unpin + Send,
    S::Item: Send,
{
    let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());
    let (output_tx, output_rx) = async_std::channel::bounded(buf_size);

    let futs = streams.into_iter().map(|mut stream| {
        let output_tx = output_tx.clone();
        async move {
            while let Some(item) = stream.next().await {
                output_tx.send(item).await.unwrap();
            }
        }
    });
    let gather_fut = futures::future::join_all(futs);

    ParGather {
        fut: Some(Box::pin(gather_fut)),
        output_rx,
    }
}

pub trait ParStreamExt {
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
            async_std::task::spawn_blocking(func)
        })
    }

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
            async_std::task::spawn_blocking(func)
        })
    }

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
            async_std::task::spawn_blocking(func)
        })
    }

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
            async_std::task::spawn_blocking(func)
        })
    }

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
        let (buf_tx, mut buf_rx) = async_std::channel::bounded(buf_size);
        let (job_tx, job_rx) = async_std::channel::bounded(limit);
        let (output_tx, output_rx) = futures::channel::oneshot::channel();

        let buffering_fut = {
            let counter = counter.clone();
            let fused = fused.clone();
            let buf_tx = buf_tx.clone();

            async move {
                while let Some(item) = self.next().await {
                    let permit = counter.clone().acquire_owned().await;
                    buf_tx.send((item, permit)).await.unwrap();
                }
                fused.notify_one();
            }
        };

        let pairing_fut = async move {
            let (lhs_item, lhs_permit) = loop {
                let (lhs_item, lhs_permit) = buf_rx.next().await.unwrap();
                let (rhs_item, rhs_permit) = tokio::select! {
                    rhs = &mut buf_rx.next() => rhs.unwrap(),
                    _ = fused.notified() => {
                        break (lhs_item, lhs_permit);
                    }
                };

                // forget one permit to allow new incoming items
                mem::drop(rhs_permit);

                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await.unwrap();
            };

            if counter.available_permits() <= buf_size - 2 {
                let (rhs_item, rhs_permit) = buf_rx.next().await.unwrap();
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await.unwrap();
            }

            while counter.available_permits() <= buf_size - 2 {
                let (lhs_item, lhs_permit) = buf_rx.next().await.unwrap();
                let (rhs_item, rhs_permit) = buf_rx.next().await.unwrap();
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await.unwrap();
            }

            let (item, _permit) = buf_rx.next().await.unwrap();
            let _ = output_tx.send(item);
        };

        let reduce_futs = (0..limit)
            .map(|_| {
                let job_rx = job_rx.clone();
                let buf_tx = buf_tx.clone();

                let fut = async move {
                    while let Ok((fut, permit)) = job_rx.recv().await {
                        let output = fut.await;
                        buf_tx.send((output, permit)).await.unwrap();
                    }
                };
                async_std::task::spawn(fut)
            })
            .collect::<Vec<_>>();

        let par_reduce_fut = futures::future::join3(
            buffering_fut,
            pairing_fut,
            futures::future::join_all(reduce_futs),
        );

        ParReduce {
            fut: Some(Box::pin(par_reduce_fut)),
            output_rx,
        }
    }

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

        let (reorder_tx, reorder_rx) = async_std::channel::bounded(buf_size);
        let (output_tx, output_rx) = async_std::channel::bounded(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_std::channel::bounded(buf_size);
                    let reorder_tx = reorder_tx.clone();

                    let map_fut = async_std::task::spawn(async move {
                        while let Ok((counter, fut)) = map_rx.recv().await {
                            let output = fut.await;
                            reorder_tx.send((counter, output)).await.unwrap();
                        }
                    });

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
                map_tx.send((counter, fut)).await.unwrap();

                counter = counter.wrapping_add(1);
            }
        };

        let reorder_fut = async move {
            let mut counter = 0u64;
            let mut pool = HashMap::new();

            while let Ok((index, output)) = reorder_rx.recv().await {
                if index != counter {
                    pool.insert(index, output);
                    continue;
                }

                output_tx.send(output).await.unwrap();
                counter = counter.wrapping_add(1);

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await.unwrap();
                    counter = counter.wrapping_add(1);
                }
            }
        };

        let par_routing_fut = futures::future::join3(
            routing_fut,
            reorder_fut,
            futures::future::join_all(map_futs),
        );

        ParRouting {
            fut: Some(Box::pin(par_routing_fut)),
            output_rx,
        }
    }

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

        let (output_tx, output_rx) = async_std::channel::bounded(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_std::channel::bounded(buf_size);
                    let output_tx = output_tx.clone();

                    let map_fut = async_std::task::spawn(async move {
                        while let Ok(fut) = map_rx.recv().await {
                            let output = fut.await;
                            output_tx.send(output).await.unwrap();
                        }
                    });

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
                map_tx.send(fut).await.unwrap();
            }
        };

        let par_routing_fut =
            futures::future::join(routing_fut, futures::future::join_all(map_futs));

        ParRoutingUnordered {
            fut: Some(Box::pin(par_routing_fut)),
            output_rx,
        }
    }

    fn par_scatter(
        mut self,
        buf_size: impl Into<Option<usize>>,
    ) -> (
        Pin<Box<dyn Future<Output = ()>>>,
        async_std::channel::Receiver<Self::Item>,
    )
    where
        Self: 'static + StreamExt + Sized + Unpin,
    {
        let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());
        let (tx, rx) = async_std::channel::bounded(buf_size);

        let scatter_fut = Box::pin(async move {
            while let Some(item) = self.next().await {
                tx.send(item).await.unwrap();
            }
        });

        (scatter_fut, rx)
    }

    fn par_for_each<F, Fut>(self, config: impl IntoParStreamParams, f: F) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
    {
        ParForEach::new(self, config, f)
    }

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

    fn par_for_each_blocking<F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> () + Send,
    {
        self.par_for_each(config, move |item| {
            let func = f(item);
            async_std::task::spawn_blocking(func)
        })
    }

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
        Func: 'static + FnOnce() -> () + Send,
    {
        let init = init_f();

        self.par_for_each(config, move |item| {
            let func = f(init.clone(), item);
            async_std::task::spawn_blocking(func)
        })
    }
}

impl<S> ParStreamExt for S where S: Stream {}

// par_map

#[pin_project]
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParMap<T> {
    #[pin]
    #[derivative(Debug = "ignore")]
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
}

impl<T> Stream for ParMap<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

// par_map_unordered

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParMapUnordered<T> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::channel::Receiver<T>,
}

impl<T> ParMapUnordered<T> {
    fn new<S, F, Fut>(mut stream: S, config: impl IntoParStreamParams, mut f: F) -> Self
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
        let (map_tx, map_rx) = async_std::channel::bounded(buf_size);
        let (output_tx, output_rx) = async_std::channel::bounded(buf_size);

        let map_fut = async move {
            while let Some(item) = stream.next().await {
                let fut = f(item);
                map_tx.send(fut).await.unwrap();
            }
        };

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let output_tx = output_tx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        let output = fut.await;
                        output_tx.send(output).await.unwrap();
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect();

        let par_then_fut = futures::future::join(map_fut, futures::future::join_all(worker_futs));

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

// par_reduce

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParReduce<T> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: futures::channel::oneshot::Receiver<T>,
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

        if let Poll::Pending = poll {
            should_wake |= true;
        }

        if should_wake {
            cx.waker().wake_by_ref();
        }

        poll
    }
}

// par_routing

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParRouting<T> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::channel::Receiver<T>,
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

// par_routing_unordered

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParRoutingUnordered<T> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::channel::Receiver<T>,
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

// par_gather

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParGather<T>
where
    T: Send,
{
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = Vec<()>> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::channel::Receiver<T>,
}

impl<T> Stream for ParGather<T>
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

// for each

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ParForEach {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)> + Send>>>,
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
        let (map_tx, map_rx) = async_std::channel::bounded(buf_size);

        let map_fut = async move {
            while let Some(item) = stream.next().await {
                let fut = f(item);
                map_tx.send(fut).await.unwrap();
            }
        };

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        fut.await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
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

// tests

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
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

    #[async_std::test]
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

    #[async_std::test]
    async fn par_reduce_test() {
        let max = 100000u64;
        let sum = futures::stream::iter((1..=max).into_iter())
            .par_reduce(None, None, |lhs, rhs| async move { lhs + rhs })
            .await;
        assert_eq!(sum, (1 + max) * max / 2);
    }

    #[async_std::test]
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

    #[async_std::test]
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
}
