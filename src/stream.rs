use crate::common::*;

/// Collect multiple streams into single stream.
pub fn par_gather<S>(
    streams: impl IntoIterator<Item = S>,
    buf_size: impl Into<Option<usize>>,
) -> ParGather<S::Item>
where
    S: 'static + StreamExt + Unpin,
{
    let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());
    let (output_tx, output_rx) = async_std::sync::channel(buf_size);

    let futs = streams.into_iter().map(|mut stream| {
        let output_tx = output_tx.clone();
        async move {
            while let Some(item) = stream.next().await {
                output_tx.send(item).await;
            }
        }
    });
    let gather_fut = futures::future::join_all(futs);

    ParGather {
        fut: Some(Box::pin(gather_fut)),
        output_rx,
    }
}

/// An extension trait for [Stream](Stream) that provides parallel combinator functions.
pub trait ParStreamExt {
    /// Computes new items from the stream asynchronously in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    fn par_then<T, F, Fut>(mut self, limit: impl Into<Option<usize>>, mut f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin,
    {
        let limit = match limit.into() {
            None | Some(0) => num_cpus::get(),
            Some(num) => num,
        };
        let (map_tx, map_rx) = async_std::sync::channel(limit);
        let (reorder_tx, reorder_rx) = async_std::sync::channel(limit);
        let (output_tx, output_rx) = async_std::sync::channel(limit);

        let map_fut = async move {
            let mut counter = 0u64;
            while let Some(item) = self.next().await {
                let fut = f(item);
                map_tx.send((counter, fut)).await;
                counter = counter.overflowing_add(1).0;
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

                output_tx.send(output).await;
                counter = counter.overflowing_add(1).0;

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await;
                    counter = counter.overflowing_add(1).0;
                }
            }
        };

        let worker_futs = (0..limit)
            .map(|_| {
                let map_rx = map_rx.clone();
                let reorder_tx = reorder_tx.clone();

                let worker_fut = async move {
                    while let Ok((index, fut)) = map_rx.recv().await {
                        let output = fut.await;
                        reorder_tx.send((index, output)).await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect::<Vec<_>>();

        let par_then_fut =
            futures::future::join3(map_fut, reorder_fut, futures::future::join_all(worker_futs));

        ParMap {
            fut: Some(Box::pin(par_then_fut)),
            output_rx,
        }
    }

    /// Computes new items from the stream asynchronously in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The order of output items is not guaranteed to respect the order of input items.
    fn par_then_unordered<T, F, Fut>(
        mut self,
        limit: impl Into<Option<usize>>,
        mut f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin,
    {
        let limit = match limit.into() {
            None | Some(0) => num_cpus::get(),
            Some(num) => num,
        };
        let (map_tx, map_rx) = async_std::sync::channel(limit);
        let (output_tx, output_rx) = async_std::sync::channel(limit);

        let map_fut = async move {
            while let Some(item) = self.next().await {
                let fut = f(item);
                map_tx.send(fut).await;
            }
        };

        let worker_futs = (0..limit)
            .map(|_| {
                let map_rx = map_rx.clone();
                let output_tx = output_tx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        let output = fut.await;
                        output_tx.send(output).await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect::<Vec<_>>();

        let par_then_fut = futures::future::join(map_fut, futures::future::join_all(worker_futs));

        ParMapUnordered {
            fut: Some(Box::pin(par_then_fut)),
            output_rx,
        }
    }

    /// Computes new items in a function in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    fn par_map<T, F, Func>(self, limit: impl Into<Option<usize>>, mut f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin,
    {
        self.par_then(limit, move |item| {
            let func = f(item);
            async_std::task::spawn_blocking(func)
        })
    }

    /// Computes new items in a function in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    fn par_map_unordered<T, F, Func>(
        self,
        limit: impl Into<Option<usize>>,
        mut f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin,
    {
        self.par_then_unordered(limit, move |item| {
            let func = f(item);
            async_std::task::spawn_blocking(func)
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
    /// Unlike [StreamExt::fold], the method may not combine the values sequentially.
    fn par_reduce<F, Fut>(
        mut self,
        limit: impl Into<Option<usize>>,
        buf_size: impl Into<Option<usize>>,
        mut f: F,
    ) -> ParReduce<Self::Item>
    where
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut,
        Fut: 'static + Future<Output = Self::Item> + Send,
        Self: 'static + StreamExt + Sized + Unpin,
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
        let (buf_tx, mut buf_rx) = async_std::sync::channel(buf_size);
        let (job_tx, job_rx) = async_std::sync::channel(limit);
        let (output_tx, output_rx) = futures::channel::oneshot::channel();

        let buffering_fut = {
            let counter = counter.clone();
            let fused = fused.clone();
            let buf_tx = buf_tx.clone();

            async move {
                while let Some(item) = self.next().await {
                    let permit = counter.clone().acquire_owned().await;
                    buf_tx.send((item, permit)).await;
                }
                fused.notify();
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
                job_tx.send((fut, lhs_permit)).await;
            };

            if counter.available_permits() <= buf_size - 2 {
                let (rhs_item, rhs_permit) = buf_rx.next().await.unwrap();
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await;
            }

            while counter.available_permits() <= buf_size - 2 {
                let (lhs_item, lhs_permit) = buf_rx.next().await.unwrap();
                let (rhs_item, rhs_permit) = buf_rx.next().await.unwrap();
                mem::drop(rhs_permit);
                let fut = f(lhs_item, rhs_item);
                job_tx.send((fut, lhs_permit)).await;
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
                        buf_tx.send((output, permit)).await;
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
    fn par_routing<F1, F2, Fut, T>(
        mut self,
        buf_size: impl Into<Option<usize>>,
        mut routing_fn: F1,
        mut map_fns: Vec<F2>,
    ) -> ParRouting<T>
    where
        Self: 'static + StreamExt + Sized + Unpin,
        F1: 'static + FnMut(&Self::Item) -> usize,
        F2: 'static + FnMut(Self::Item) -> Fut,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
    {
        let buf_size = match buf_size.into() {
            None | Some(0) => num_cpus::get(),
            Some(size) => size,
        };

        let (reorder_tx, reorder_rx) = async_std::sync::channel(buf_size);
        let (output_tx, output_rx) = async_std::sync::channel(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_std::sync::channel(buf_size);
                    let reorder_tx = reorder_tx.clone();

                    let map_fut = async_std::task::spawn(async move {
                        while let Ok((counter, fut)) = map_rx.recv().await {
                            let output = fut.await;
                            reorder_tx.send((counter, output)).await;
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
                map_tx.send((counter, fut)).await;

                counter = counter.overflowing_add(1).0;
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

                output_tx.send(output).await;
                counter = counter.overflowing_add(1).0;

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await;
                    counter = counter.overflowing_add(1).0;
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
        Self: 'static + StreamExt + Sized + Unpin,
        F1: 'static + FnMut(&Self::Item) -> usize,
        F2: 'static + FnMut(Self::Item) -> Fut,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
    {
        let buf_size = match buf_size.into() {
            None | Some(0) => num_cpus::get(),
            Some(size) => size,
        };

        let (output_tx, output_rx) = async_std::sync::channel(buf_size);

        let (mut map_txs, map_futs) =
            map_fns
                .iter()
                .fold((vec![], vec![]), |(mut map_txs, mut map_futs), _| {
                    let (map_tx, map_rx) = async_std::sync::channel(buf_size);
                    let output_tx = output_tx.clone();

                    let map_fut = async_std::task::spawn(async move {
                        while let Ok(fut) = map_rx.recv().await {
                            let output = fut.await;
                            output_tx.send(output).await;
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
                map_tx.send(fut).await;
            }
        };

        let par_routing_fut =
            futures::future::join(routing_fut, futures::future::join_all(map_futs));

        ParRoutingUnordered {
            fut: Some(Box::pin(par_routing_fut)),
            output_rx,
        }
    }

    /// Gives the current iteration count that may overflow to zero as well as the next value.
    fn overflowing_enumerate<T>(self) -> OverflowingEnumerate<T, Self>
    where
        Self: Stream<Item = T> + Sized + Unpin,
    {
        OverflowingEnumerate {
            stream: self,
            counter: 0,
        }
    }

    /// Reorder the input items paired with a iteration count.
    ///
    /// The type of input item must be a tuple `(usize, T)`.
    /// It reorders the items according to the first elemnt of tupple.
    /// It is usually combined with [ParStreamExt::overflowing_enumerate].
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

    /// Splits the stream into a clonable receiver and a future that scatters input items into the receiver and its clones.
    fn par_scatter(
        mut self,
        buf_size: impl Into<Option<usize>>,
    ) -> (
        Box<dyn Future<Output = ()>>,
        async_std::sync::Receiver<Self::Item>,
    )
    where
        Self: 'static + StreamExt + Sized + Unpin,
    {
        let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());
        let (tx, rx) = async_std::sync::channel(buf_size);

        let scatter_fut = Box::new(async move {
            while let Some(item) = self.next().await {
                tx.send(item).await;
            }
        });

        (scatter_fut, rx)
    }
}

impl<S> ParStreamExt for S where S: Stream {}

// par_map

pub struct ParMap<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)>>>>,
    output_rx: async_std::sync::Receiver<T>,
}

impl<T> Stream for ParMap<T> {
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

// par_map_unordered

pub struct ParMapUnordered<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)>>>>,
    output_rx: async_std::sync::Receiver<T>,
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

pub struct ParReduce<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)>>>>,
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

pub struct ParRouting<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)>>>>,
    output_rx: async_std::sync::Receiver<T>,
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

pub struct ParRoutingUnordered<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)>>>>,
    output_rx: async_std::sync::Receiver<T>,
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

pub struct ParGather<T> {
    fut: Option<Pin<Box<dyn Future<Output = Vec<()>>>>>,
    output_rx: async_std::sync::Receiver<T>,
}

impl<T> Stream for ParGather<T> {
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

// overflowing_enumerate

pub struct OverflowingEnumerate<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    stream: S,
    counter: usize,
}

impl<T, S> Stream for OverflowingEnumerate<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    type Item = (usize, T);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let index = self.counter;
                self.counter = self.counter.overflowing_add(1).0;
                Poll::Ready(Some((index, item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// reorder_enumerated

pub struct ReorderEnumerated<T, S>
where
    S: Stream<Item = (usize, T)> + Unpin,
{
    stream: S,
    counter: usize,
    buffer: HashMap<usize, T>,
}

impl<T, S> Stream for ReorderEnumerated<T, S>
where
    S: Stream<Item = (usize, T)> + Unpin,
    T: Unpin,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let Self {
            stream,
            counter,
            buffer,
        } = &mut *self;

        let buffered_item_opt = buffer.remove(counter);
        if let Some(_) = buffered_item_opt {
            *counter = counter.overflowing_add(1).0;
        }

        match (Pin::new(stream).poll_next(cx), buffered_item_opt) {
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
                    *counter = counter.overflowing_add(1).0;
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

// tests

mod tests {
    use super::*;

    #[async_std::test]
    async fn par_then_output_is_ordered_test() {
        let max = 1000u64;
        futures::stream::iter((0..max).into_iter())
            .par_then(None, |value| {
                async move {
                    async_std::task::sleep(std::time::Duration::from_millis(value % 50)).await;
                    value
                }
            })
            .fold(0u64, |expect, found| {
                async move {
                    assert_eq!(expect, found);
                    expect + 1
                }
            })
            .await;
    }

    #[async_std::test]
    async fn par_then_unordered_test() {
        let max = 1000u64;
        let mut values = futures::stream::iter((0..max).into_iter())
            .par_then_unordered(None, |value| {
                async move {
                    async_std::task::sleep(std::time::Duration::from_millis(value % 100)).await;
                    value
                }
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
            .overflowing_enumerate()
            .par_then_unordered(None, |(index, value)| {
                async move {
                    async_std::task::sleep(std::time::Duration::from_millis(value % 100)).await;
                    (index, value)
                }
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

}
