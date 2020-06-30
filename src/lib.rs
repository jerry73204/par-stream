use futures::stream::{Stream, StreamExt};
use std::{
    collections::HashMap,
    future::Future,
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{Notify, Semaphore};

pub trait ParStreamExt {
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
        let (gather_tx, gather_rx) = async_std::sync::channel(limit);

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

                gather_tx.send(output).await;
                counter = counter.overflowing_add(1).0;

                while let Some(output) = pool.remove(&counter) {
                    gather_tx.send(output).await;
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
            gather_rx,
        }
    }

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
        let (gather_tx, gather_rx) = async_std::sync::channel(limit);

        let map_fut = async move {
            while let Some(item) = self.next().await {
                let fut = f(item);
                map_tx.send(fut).await;
            }
        };

        let worker_futs = (0..limit)
            .map(|_| {
                let map_rx = map_rx.clone();
                let gather_tx = gather_tx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        let output = fut.await;
                        gather_tx.send(output).await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect::<Vec<_>>();

        let par_then_fut = futures::future::join(map_fut, futures::future::join_all(worker_futs));

        ParMapUnordered {
            fut: Some(Box::pin(par_then_fut)),
            gather_rx,
        }
    }

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
}

impl<S> ParStreamExt for S where S: Stream {}

// par_map

pub struct ParMap<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)>>>>,
    gather_rx: async_std::sync::Receiver<T>,
}

impl<T> Stream for ParMap<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(fut) = self.fut.as_mut() {
            match Pin::new(fut).poll(cx) {
                Poll::Pending => (),
                Poll::Ready(_) => self.fut = None,
            }
        }
        Pin::new(&mut self.gather_rx).poll_next(cx)
    }
}

// par_map_unordered

pub struct ParMapUnordered<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)>>>>,
    gather_rx: async_std::sync::Receiver<T>,
}

impl<T> Stream for ParMapUnordered<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(fut) = self.fut.as_mut() {
            match Pin::new(fut).poll(cx) {
                Poll::Pending => (),
                Poll::Ready(_) => self.fut = None,
            }
        }
        Pin::new(&mut self.gather_rx).poll_next(cx)
    }
}

// par_fold

pub struct ParReduce<T> {
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)>>>>,
    output_rx: futures::channel::oneshot::Receiver<T>,
}

impl<T> Future for ParReduce<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(fut) = self.fut.as_mut() {
            match Pin::new(fut).poll(cx) {
                Poll::Pending => (),
                Poll::Ready(_) => self.fut = None,
            }
        }
        Pin::new(&mut self.output_rx)
            .poll(cx)
            .map(|result| result.unwrap())
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
}
