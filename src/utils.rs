use crate::common::*;

// macro_rules! declare_boxed_stream {
//     ($name:ident, $(#[$attr:meta])*) => {
//         $(#[$attr])*
//         #[derive(Derivative)]
//         #[derivative(Debug)]
//         pub struct $name<T> {
//             #[derivative(Debug = "ignore")]
//             pub(super) stream: std::pin::Pin<Box<dyn futures::Stream<Item = T> + Send>>,
//         }

//         impl<T> futures::Stream for $name<T> {
//             type Item = T;

//             fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
//                 Pin::new(&mut self.stream).poll_next(cx)
//             }
//         }

//     }
// }

// macro_rules! declare_boxed_future {
//     ($name:ident, $(#[$attr:meta])*) => {
//         $(#[$attr])*
//         #[derive(Derivative)]
//         #[derivative(Debug)]
//         pub struct $name<T> {
//             #[derivative(Debug = "ignore")]
//             pub(super) future: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>,
//         }

//         impl<T> std::future::Future for $name<T> {
//             type Output = T;

//             fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
//                 Pin::new(&mut self.future).poll(cx)
//             }
//         }

//     }
// }

// pub(crate) use declare_boxed_future;
// pub(crate) use declare_boxed_stream;

pub(crate) type BoxedFuture<T> = BoxFuture<'static, T>;
pub(crate) type BoxedStream<T> = BoxStream<'static, T>;

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

pub use flume_receiver_ext::*;

mod flume_receiver_ext {
    use super::*;

    pub trait FlumeReceiverExt<T> {
        fn into_stream(self) -> FlumeReceiverStream<T>;
    }

    impl<T> FlumeReceiverExt<T> for flume::Receiver<T>
    where
        T: 'static + Send,
    {
        fn into_stream(self) -> FlumeReceiverStream<T> {
            FlumeReceiverStream::new(self)
        }
    }

    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct FlumeReceiverStream<T> {
        #[derivative(Debug = "ignore")]
        stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    }

    impl<T> FlumeReceiverStream<T>
    where
        T: 'static + Send,
    {
        pub fn new(rx: flume::Receiver<T>) -> Self {
            let stream = futures::stream::unfold(rx, |rx| async move {
                rx.recv_async().await.ok().map(|item| (item, rx))
            });

            Self {
                stream: Box::pin(stream),
            }
        }
    }

    impl<T> Stream for FlumeReceiverStream<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// pub use async_channel_receiver_ext::*;

// mod async_channel_receiver_ext {
//     use super::*;

//     pub trait AsyncChannelReceiverExt<T> {
//         fn into_stream(self) -> AsyncChannelReceiverStream<T>;
//     }

//     impl<T> AsyncChannelReceiverExt<T> for async_channel::Receiver<T>
//     where
//         T: 'static + Send
//     {
//         fn into_stream(self) -> AsyncChannelReceiverStream<T> {
//             AsyncChannelReceiverStream::new(self)
//         }
//     }

//     #[derive(Derivative)]
//     #[derivative(Debug)]
//     pub struct AsyncChannelReceiverStream<T> {
//         #[derivative(Debug = "ignore")]
//         stream: Pin<Box<dyn Stream<Item = T> + Send>>,
//     }

//     impl<T> AsyncChannelReceiverStream<T>
//     where
//         T: 'static + Send,
//     {
//         pub fn new(rx: async_channel::Receiver<T>) -> Self {
//             let stream = futures::stream::unfold(rx, |rx| async move {
//                 rx.recv().await.ok().map(|item| (item, rx))
//             });

//             Self {
//                 stream: Box::pin(stream),
//             }
//         }
//     }

//     impl<T> Stream for AsyncChannelReceiverStream<T> {
//         type Item = T;

//         fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
//             Pin::new(&mut self.stream).poll_next(cx)
//         }
//     }
// }
