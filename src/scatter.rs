use crate::{common::*, config::BufSize, rt, utils};

/// A stream combinator returned from [scatter()](ParStreamExt::scatter).
#[derive(Clone)]
pub struct Scatter<T>
where
    T: 'static,
{
    pub(super) stream: flume::r#async::RecvStream<'static, T>,
}

impl<T> Scatter<T> {
    pub fn new<B, St>(stream: St, buf_size: B) -> Scatter<T>
    where
        St: 'static + Send + Stream<Item = T>,
        T: Send,
        B: Into<BufSize>,
    {
        let (tx, rx) = utils::channel(buf_size.into().get());

        rt::spawn(async move {
            let _ = stream.map(Ok).forward(tx.into_sink()).await;
        });

        Scatter {
            stream: rx.into_stream(),
        }
    }
}

impl<T> Stream for Scatter<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
