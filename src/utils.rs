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

pub(crate) type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(crate) type BoxedStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct AsyncChannelReceiverStream<T> {
    #[derivative(Debug = "ignore")]
    stream: Pin<Box<dyn Stream<Item = T> + Send>>,
    // rx: Arc<async_channel::Receiver<T>>,
}

impl<T> AsyncChannelReceiverStream<T>
where
    T: 'static + Send,
{
    pub fn new(rx: async_channel::Receiver<T>) -> Self {
        let rx = Arc::new(rx);

        let stream = {
            let rx = rx.clone();

            futures::stream::unfold(rx, |rx| async move {
                rx.recv().await.ok().map(|item| (item, rx))
            })
        };

        Self {
            stream: Box::pin(stream),
            // rx,
        }
    }
}

impl<T> Stream for AsyncChannelReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

// impl<T> Clone for AsyncChannelReceiverStream<T>
// where
//     T: 'static + Send,
// {
//     fn clone(&self) -> Self {
//         let rx = (*self.rx).clone();
//         Self::new(rx)
//     }
// }
