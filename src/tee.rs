use crate::{common::*, config::BufSize, rt, utils};
use dashmap::DashSet;
use tokio::sync::Mutex;

/// Stream for the [tee()](crate::par_stream::ParStreamExt::tee) method.
///
/// Cloning this stream allocates a new channel for the new receiver, so that
/// future copies of stream items are forwarded to the channel.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Tee<T>
where
    T: 'static,
{
    pub(super) buf_size: Option<usize>,
    #[derivative(Debug = "ignore")]
    pub(super) future: Arc<Mutex<Option<rt::JoinHandle<()>>>>,
    pub(super) sender_set: Weak<DashSet<ByAddress<Arc<flume::Sender<T>>>>>,
    #[derivative(Debug = "ignore")]
    pub(super) stream: flume::r#async::RecvStream<'static, T>,
}

impl<T> Tee<T>
where
    T: Send + Clone,
{
    pub fn new<B, St>(stream: St, buf_size: B) -> Tee<T>
    where
        St: 'static + Send + Stream<Item = T>,
        B: Into<BufSize>,
    {
        let buf_size = buf_size.into().get();
        let (tx, rx) = utils::channel(buf_size);
        let sender_set = Arc::new(DashSet::new());
        sender_set.insert(ByAddress(Arc::new(tx)));

        let future = {
            let sender_set = sender_set.clone();
            let mut stream = stream.boxed();

            let future = rt::spawn(async move {
                while let Some(item) = stream.next().await {
                    let futures: Vec<_> = sender_set
                        .iter()
                        .map(|tx| {
                            let tx = tx.clone();
                            let item = item.clone();
                            async move {
                                let result = tx.send_async(item).await;
                                (result, tx)
                            }
                        })
                        .collect();

                    let results = future::join_all(futures).await;
                    let success_count = results
                        .iter()
                        .filter(|(result, tx)| {
                            let ok = result.is_ok();
                            if !ok {
                                sender_set.remove(tx);
                            }
                            ok
                        })
                        .count();

                    if success_count == 0 {
                        break;
                    }
                }
            });

            Arc::new(Mutex::new(Some(future)))
        };

        Tee {
            future,
            sender_set: Arc::downgrade(&sender_set),
            stream: rx.into_stream(),
            buf_size,
        }
    }
}

impl<T> Clone for Tee<T>
where
    T: 'static + Send,
{
    fn clone(&self) -> Self {
        let buf_size = self.buf_size;
        let (tx, rx) = utils::channel(buf_size);
        let sender_set = self.sender_set.clone();

        if let Some(sender_set) = sender_set.upgrade() {
            sender_set.insert(ByAddress(Arc::new(tx)));
        }

        Self {
            future: self.future.clone(),
            sender_set,
            stream: rx.into_stream(),
            buf_size,
        }
    }
}

impl<T> Stream for Tee<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(mut future_opt) = self.future.try_lock() {
            if let Some(future) = &mut *future_opt {
                if Pin::new(future).poll(cx).is_ready() {
                    *future_opt = None;
                }
            }
        }

        match Pin::new(&mut self.stream).poll_next(cx) {
            Ready(Some(output)) => {
                cx.waker().clone().wake();
                Ready(Some(output))
            }
            Ready(None) => Ready(None),
            Pending => {
                cx.waker().clone().wake();
                Pending
            }
        }
    }
}
