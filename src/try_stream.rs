use crate::common::*;

/// An extension trait that controls ordering of items of fallible streams.
pub trait TryStreamExt
where
    Self: TryStream,
{
    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_enumerate(self) -> TryEnumerate<Self, Self::Ok, Self::Error>
    where
        Self: Sized,
    {
        TryEnumerate {
            stream: self,
            counter: 0,
            fused: false,
            _phantom: PhantomData,
        }
    }

    fn catch_error(
        self,
    ) -> (
        BoxStream<'static, Self::Ok>,
        BoxFuture<'static, Result<(), Self::Error>>,
    )
    where
        Self: 'static + Send,
        Self::Ok: 'static + Send,
        Self::Error: 'static + Send;
}

impl<S, T, E> TryStreamExt for S
where
    S: Stream<Item = Result<T, E>>,
{
    fn catch_error(self) -> (BoxStream<'static, T>, BoxFuture<'static, Result<(), E>>)
    where
        S: 'static + Send,
        T: 'static + Send,
        E: 'static + Send,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future = rx.map(|result| match result {
            Ok(err) => Err(err),
            Err(_) => Ok(()),
        });
        let stream = self.scan(Some(tx), |tx, result| {
            let output = match result {
                Ok(val) => Some(val),
                Err(err) => {
                    let _ = tx.take().unwrap().send(err);
                    None
                }
            };

            future::ready(output)
        });

        (stream.boxed(), future.boxed())
    }
}

// try_enumerate

pub use try_enumerate::*;

mod try_enumerate {
    use super::*;

    /// A fallible stream combinator returned from [try_enumerate()](crate::try_stream::TryStreamExt::try_enumerate).
    #[pin_project(project = TryEnumerateProj)]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryEnumerate<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) counter: usize,
        pub(super) fused: bool,
        pub(super) _phantom: PhantomData<(T, E)>,
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: S,
    }

    impl<S, T, E> Stream for TryEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        type Item = Result<(usize, T), E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let TryEnumerateProj {
                stream,
                fused,
                counter,
                ..
            } = self.project();

            if *fused {
                return Ready(None);
            }

            let poll = stream.poll_next(cx);
            match poll {
                Ready(Some(Ok(item))) => {
                    let index = *counter;
                    *counter += 1;
                    Ready(Some(Ok((index, item))))
                }
                Ready(Some(Err(err))) => {
                    *fused = true;
                    Ready(Some(Err(err)))
                }
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }

    impl<S, T, E> FusedStream for TryEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        fn is_terminated(&self) -> bool {
            self.fused
        }
    }
}
