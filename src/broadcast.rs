use crate::{
    common::*, config::BufSize, index_stream::IndexStreamExt as _, rt, stream::StreamExt as _,
    utils,
};
use tokio::sync::{oneshot, watch};

/// The build type returned from [broadcast()](crate::par_stream::ParStreamExt::broadcast).
///
/// It is used to register new broadcast receivers. Each receiver consumes copies
/// of items of the stream. The builder is finished by `guard.build()` so that
/// registered receivers can start consuming data. Otherwise, the receivers
/// take empty input.
#[derive(Debug)]
pub struct BroadcastBuilder<T> {
    pub(super) buf_size: Option<usize>,
    pub(super) ready_rx: watch::Receiver<()>,
    pub(super) senders_tx: Option<oneshot::Sender<Vec<flume::Sender<(usize, T)>>>>,
    pub(super) senders: Option<Vec<flume::Sender<(usize, T)>>>,
}

impl<T> BroadcastBuilder<T>
where
    T: 'static + Send + Clone,
{
    pub fn new<B, St>(stream: St, buf_size: B, send_all: bool) -> BroadcastBuilder<T>
    where
        St: 'static + Send + Stream<Item = T>,
        B: Into<BufSize>,
    {
        let (senders_tx, senders_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = watch::channel(());

        rt::spawn(async move {
            // wait for receiver list to be ready
            let senders: Vec<flume::Sender<(usize, T)>> = match senders_rx.await {
                Ok(senders) => senders,
                Err(_) => return,
            };

            // tell subscribers to be ready
            if ready_tx.send(()).is_err() {
                return;
            }

            let num_senders = senders.len();

            match num_senders {
                0 => {
                    // fall through for zero senders
                }
                1 => {
                    // fast path for single sender
                    let sender = senders.into_iter().next().unwrap();
                    let _ = stream.enumerate().map(Ok).forward(sender.into_sink()).await;
                }
                _ => {
                    // merge senders into a sink
                    let sink =
                        futures::sink::unfold(senders, |senders, item: (usize, T)| async move {
                            // let each sender sends a copy of the item
                            let futures: stream::FuturesUnordered<_> = senders
                                .into_iter()
                                .map(|tx| {
                                    let item = item.clone();

                                    async move {
                                        let result = tx.send_async(item).await;

                                        // if sending is successful, return the sender back
                                        result.map(move |()| tx)
                                    }
                                })
                                .collect();

                            // collect senders back
                            let senders: Vec<_> = futures
                                .filter_map(|tx| future::ready(tx.ok()))
                                .collect()
                                .await;

                            // finish sink if
                            // case 1: send_all == true, no senders fail
                            // case 2: send_all == false, there are successful sender(s)
                            let n_remaining_senders = senders.len();

                            if (!send_all && n_remaining_senders > 0)
                                || (send_all && (n_remaining_senders == num_senders))
                            {
                                Ok(senders)
                            } else {
                                Err(flume::SendError(()))
                            }
                        });

                    let _ = stream.enumerate().map(Ok).forward(sink).await;
                }
            }
        });

        BroadcastBuilder {
            buf_size: buf_size.into().get(),
            ready_rx,
            senders_tx: Some(senders_tx),
            senders: Some(vec![]),
        }
    }

    /// Creates a new receiver.
    pub fn register(&mut self) -> BroadcastStream<T> {
        let Self {
            buf_size,
            ref ready_rx,
            ref mut senders,
            ..
        } = *self;
        let senders = senders.as_mut().unwrap();
        let mut ready_rx = ready_rx.clone();

        let (tx, rx) = utils::channel(buf_size);
        senders.push(tx);

        let stream = rx
            .into_stream()
            .reorder_enumerated()
            .wait_until(async move { ready_rx.changed().await.is_ok() })
            .boxed();

        BroadcastStream { stream }
    }

    /// Finish the builder to start broadcasting.
    pub fn build(mut self) {
        let senders_tx = self.senders_tx.take().unwrap();
        let senders = self.senders.take().unwrap();
        senders_tx.send(senders).unwrap();
    }
}

/// The receiver that consumes broadcasted messages from the stream.
#[pin_project]
pub struct BroadcastStream<T> {
    #[pin]
    pub(super) stream: BoxStream<'static, T>,
}

impl<T> Stream for BroadcastStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{par_stream::ParStreamExt as _, utils::async_test};
    use itertools::izip;

    async_test! {
        async fn broadcast_test() {
            let mut builder = stream::iter(0..).broadcast(2, true);
            let rx1 = builder.register();
            let rx2 = builder.register();
            builder.build();

            let (ret1, ret2): (Vec<_>, Vec<_>) =
                join!(rx1.take(100).collect(), rx2.take(100).collect());

            izip!(ret1, 0..100).for_each(|(lhs, rhs)| {
                assert_eq!(lhs, rhs);
            });
            izip!(ret2, 0..100).for_each(|(lhs, rhs)| {
                assert_eq!(lhs, rhs);
            });
        }

        async fn broadcast_and_drop_receiver_test() {
            {
                let mut builder = stream::iter(0..).broadcast(2, false);
                let rx1 = builder.register();
                let rx2 = builder.register();
                builder.build();

                drop(rx2);

                let vec: Vec<_> = rx1.take(100).collect().await;
                izip!(vec, 0..100).for_each(|(lhs, rhs)| {
                    assert_eq!(lhs, rhs);
                });
            }

            {
                let mut builder = stream::iter(0..).broadcast(2, true);
                let mut rx1 = builder.register();
                let rx2 = builder.register();
                builder.build();

                drop(rx2);
                assert_eq!(rx1.next().await, Some(0));
                assert!(rx1.next().await.is_none());
            }
        }
    }
}
