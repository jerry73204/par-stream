use crate::{common::*, config::BufSize, rt, utils};
use tokio::sync::{oneshot, watch};

/// The build type returned from [broadcast()](ParStreamExt::broadcast).
///
/// It is used to register new broadcast receivers. Each receiver consumes copies
/// of items of the stream. The builder is finished by `guard.build()` so that
/// registered receivers can start consuming data. Otherwise, the receivers
/// take empty input.
#[derive(Debug)]
pub struct BroadcastBuilder<T> {
    pub(super) buf_size: Option<usize>,
    pub(super) ready_rx: watch::Receiver<()>,
    pub(super) senders_tx: Option<oneshot::Sender<Vec<flume::Sender<T>>>>,
    pub(super) senders: Option<Vec<flume::Sender<T>>>,
}

impl<T> BroadcastBuilder<T>
where
    T: 'static + Send + Clone,
{
    pub fn new<B, St>(stream: St, buf_size: B) -> BroadcastBuilder<T>
    where
        St: 'static + Send + Stream<Item = T>,
        B: Into<BufSize>,
    {
        let (senders_tx, senders_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = watch::channel(());

        rt::spawn(async move {
            // wait for receiver list to be ready
            let senders: Vec<flume::Sender<_>> = match senders_rx.await {
                Ok(senders) => senders,
                Err(_) => return,
            };

            // tell subscribers to be ready
            if ready_tx.send(()).is_err() {
                return;
            }

            if senders.len() == 1 {
                // fast path for single sender
                let sender = senders.into_iter().next().unwrap();
                let _ = stream.map(Ok).forward(sender.into_sink()).await;
            } else {
                debug_assert!(!senders.is_empty());

                // merge senders into a fanout sink
                let fanout = senders
                    .into_iter()
                    .map(|tx| -> BoxSink<T, flume::SendError<T>> { Box::pin(tx.into_sink()) })
                    .reduce(|fanout, sink| -> BoxSink<T, flume::SendError<T>> {
                        Box::pin(fanout.fanout(sink))
                    })
                    .unwrap();

                let _ = stream.map(Ok).forward(fanout).await;
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

        let stream = async move {
            let ok = ready_rx.changed().await.is_ok();
            Either::Left(ok)
        }
        .into_stream()
        .chain(rx.into_stream().map(Either::Right))
        .take_while(|either| {
            let ok = !matches!(either, Either::Left(false));
            future::ready(ok)
        })
        .filter_map(|either| async move {
            use Either::*;

            match either {
                Left(_) => None,
                Right(item) => Some(item),
            }
        })
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
#[derive(Derivative)]
#[derivative(Debug)]
pub struct BroadcastStream<T> {
    #[derivative(Debug = "ignore")]
    pub(super) stream: BoxStream<'static, T>,
}

impl<T> Stream for BroadcastStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
