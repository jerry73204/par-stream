use crate::{common::*, config::BufSize, rt, utils};
use flume::r#async::RecvStream;

/// The builder forwards each stream item according to its key to a destination receiver.
pub struct PullBuilder<St, K, F, Q = K>
where
    St: ?Sized + Stream,
{
    buf_size: Option<usize>,
    key_fn: F,
    senders: HashMap<K, flume::Sender<St::Item>>,
    _phantom: PhantomData<Q>,
    stream: St,
}

impl<St, K, Q, F> PullBuilder<St, K, F, Q>
where
    St: 'static + Send + Stream,
    St::Item: 'static + Send,
    F: 'static + Send + FnMut(&St::Item) -> Q,
    K: 'static + Send + Hash + Eq + Borrow<Q>,
    Q: Send + Hash + Eq,
{
    /// Creates the builder.
    ///
    /// The `buf_size` sets the channel size for each registered receiver.
    /// The `key_fn` is used to compute the key for each input item.
    pub fn new<B>(stream: St, buf_size: B, key_fn: F) -> Self
    where
        B: Into<BufSize>,
    {
        let buf_size = buf_size.into().get();

        Self {
            buf_size,
            key_fn,
            senders: HashMap::new(),
            _phantom: PhantomData,
            stream,
        }
    }

    /// Creates a receiver binding to the `key`.
    ///
    /// If the `key` is already registered, it returns `None`.
    pub fn register(&mut self, key: K) -> Option<RecvStream<'static, St::Item>> {
        use std::collections::hash_map::Entry as E;

        if let E::Vacant(entry) = self.senders.entry(key) {
            let (tx, rx) = utils::channel(self.buf_size);
            entry.insert(tx);
            Some(rx.into_stream())
        } else {
            None
        }
    }

    /// Finish the builder and start forwarding items to receivers.
    ///
    /// It returns a special leaking receiver that accepts items which
    /// key is not registered or the destination receiver is closed.
    pub fn build(self) -> RecvStream<'static, St::Item> {
        let Self {
            mut key_fn,
            senders,
            stream,
            buf_size,
            ..
        } = self;
        let (leak_tx, leak_rx) = utils::channel(buf_size);

        rt::spawn(async move {
            let mut stream = stream.boxed();

            while let Some(item) = stream.next().await {
                let query = key_fn(&item);
                let tx = senders.get(&query);

                if let Some(tx) = tx {
                    if let Err(err) = tx.send_async(item).await {
                        let _ = leak_tx.send_async(err.into_inner()).await;
                    }
                } else {
                    let _ = leak_tx.send_async(item).await;
                }
            }
        });

        leak_rx.into_stream()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{par_stream::ParStreamExt as _, utils::async_test};

    async_test! {
        async fn pull_routing_test() {
            let mut builder = stream::iter([("A", 1), ("B", 2), ("C", 3), ("D", 4)])
                .pull_routing(None, |&(key, _)| key);

            let stream_a = builder.register("A").unwrap();
            let stream_b = builder.register("B").unwrap();
            let stream_c = builder.register("C").unwrap();
            let stream_leak = builder.build();

            let join: Vec<Vec<_>> = future::join_all([
                stream_a.collect(),
                stream_b.collect(),
                stream_c.collect(),
                stream_leak.collect(),
            ])
                .await;

            assert_eq!(
                join,
                vec![
                    vec![("A", 1)],
                    vec![("B", 2)],
                    vec![("C", 3)],
                    vec![("D", 4)]
                ]
            );
        }
    }
}
