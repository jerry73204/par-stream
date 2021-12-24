use crate::common::*;

// pub(crate) type BoxedFuture<T> = BoxFuture<'static, T>;
// pub(crate) type BoxedStream<T> = BoxStream<'static, T>;

pub fn join_future_stream<F, S>(future: F, stream: S) -> impl Stream<Item = S::Item>
where
    F: Future,
    S: Stream,
{
    stream::select(future.map(|_| None).into_stream(), stream.map(Some))
        .filter_map(|item| future::ready(item))
}

pub use tokio_mpsc_receiver_ext::*;

mod tokio_mpsc_receiver_ext {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    pub trait TokioMpscReceiverExt<T> {
        fn into_stream(self) -> ReceiverStream<T>;
    }

    impl<T> TokioMpscReceiverExt<T> for mpsc::Receiver<T>
    where
        T: 'static + Send,
    {
        fn into_stream(self) -> ReceiverStream<T> {
            ReceiverStream::new(self)
        }
    }
}
