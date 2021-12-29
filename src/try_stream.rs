use crate::common::*;

/// An extension trait that controls ordering of items of fallible streams.
pub trait TryStreamExt
where
    Self: TryStream,
{
    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_enumerate(self) -> TryEnumerate<Self, Self::Ok, Self::Error>;

    fn take_until_error(self) -> TakeUntilError<Self, Self::Ok, Self::Error>;

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

    fn try_stateful_then<B, U, F, Fut>(
        self,
        init: B,
        f: F,
    ) -> TryStatefulThen<Self, B, Self::Ok, U, Self::Error, F, Fut>;

    fn try_stateful_map<B, U, F>(
        self,
        init: B,
        f: F,
    ) -> TryStatefulMap<Self, B, Self::Ok, U, Self::Error, F>;
}

impl<S, T, E> TryStreamExt for S
where
    S: Stream<Item = Result<T, E>>,
{
    fn try_enumerate(self) -> TryEnumerate<Self, T, E> {
        TryEnumerate {
            counter: 0,
            fused: false,
            _phantom: PhantomData,
            stream: self,
        }
    }

    fn take_until_error(self) -> TakeUntilError<Self, T, E> {
        TakeUntilError {
            _phantom: PhantomData,
            is_terminated: false,
            stream: self,
        }
    }

    fn try_stateful_then<B, U, F, Fut>(
        self,
        init: B,
        f: F,
    ) -> TryStatefulThen<Self, B, T, U, E, F, Fut> {
        TryStatefulThen {
            stream: self,
            future: None,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

    fn try_stateful_map<B, U, F>(self, init: B, f: F) -> TryStatefulMap<Self, B, T, U, E, F> {
        TryStatefulMap {
            stream: self,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

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

pub use take_until_error::*;
mod take_until_error {
    use super::*;

    #[pin_project]
    pub struct TakeUntilError<St, T, E>
    where
        St: ?Sized,
    {
        pub(super) _phantom: PhantomData<(T, E)>,
        pub(super) is_terminated: bool,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, T, E> Stream for TakeUntilError<St, T, E>
    where
        St: Stream<Item = Result<T, E>>,
    {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.project();

            Ready({
                if *this.is_terminated {
                    None
                } else {
                    if let Some(result) = ready!(this.stream.poll_next(cx)) {
                        if result.is_err() {
                            *this.is_terminated = true;
                        }
                        Some(result)
                    } else {
                        *this.is_terminated = true;
                        None
                    }
                }
            })
        }
    }
}

pub use try_stateful_then::*;
mod try_stateful_then {
    use super::*;

    /// Stream for the [`try_stateful_then`](super::TryStreamExt::try_stateful_then) method.
    #[pin_project]
    pub struct TryStatefulThen<St, B, T, U, E, F, Fut>
    where
        St: ?Sized,
    {
        #[pin]
        pub(super) future: Option<Fut>,
        pub(super) state: Option<B>,
        pub(super) f: F,
        pub(super) _phantom: PhantomData<(T, U, E)>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, B, T, U, E, F, Fut> Stream for TryStatefulThen<St, B, T, U, E, F, Fut>
    where
        St: Stream<Item = Result<T, E>>,
        F: FnMut(B, T) -> Fut,
        Fut: Future<Output = Result<Option<(B, U)>, E>>,
    {
        type Item = Result<U, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Poll::Ready(loop {
                if let Some(fut) = this.future.as_mut().as_pin_mut() {
                    let result = ready!(fut.poll(cx));
                    this.future.set(None);

                    match result {
                        Ok(Some((state, item))) => {
                            *this.state = Some(state);
                            break Some(Ok(item));
                        }
                        Ok(None) => {
                            break None;
                        }
                        Err(err) => break Some(Err(err)),
                    }
                } else if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(Ok(item))) => {
                            this.future.set(Some((this.f)(state, item)));
                        }
                        Ready(Some(Err(err))) => break Some(Err(err)),
                        Ready(None) => break None,
                        Pending => {
                            *this.state = Some(state);
                            return Pending;
                        }
                    }
                } else {
                    break None;
                }
            })
        }
    }
}

pub use try_stateful_map::*;
mod try_stateful_map {
    use super::*;

    /// Stream for the [`try_stateful_map`](super::TryStreamExt::try_stateful_map) method.
    #[pin_project]
    pub struct TryStatefulMap<St, B, T, U, E, F>
    where
        St: ?Sized,
    {
        pub(super) state: Option<B>,
        pub(super) f: F,
        pub(super) _phantom: PhantomData<(T, U, E)>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, B, T, U, E, F> Stream for TryStatefulMap<St, B, T, U, E, F>
    where
        St: Stream<Item = Result<T, E>>,
        F: FnMut(B, T) -> Result<Option<(B, U)>, E>,
    {
        type Item = Result<U, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Poll::Ready(loop {
                if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(Ok(in_item))) => {
                            let result = (this.f)(state, in_item);

                            match result {
                                Ok(Some((state, out_item))) => {
                                    *this.state = Some(state);
                                    break Some(Ok(out_item));
                                }
                                Ok(None) => {
                                    break None;
                                }
                                Err(err) => break Some(Err(err)),
                            }
                        }
                        Ready(Some(Err(err))) => break Some(Err(err)),
                        Ready(None) => break None,
                        Pending => {
                            *this.state = Some(state);
                            return Pending;
                        }
                    }
                } else {
                    break None;
                }
            })
        }
    }
}

pub use try_enumerate::*;
mod try_enumerate {
    use super::*;

    /// A fallible stream combinator returned from [try_enumerate()](crate::try_stream::TryStreamExt::try_enumerate).
    #[derive(Derivative)]
    #[derivative(Debug)]
    #[pin_project]
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
            let mut this = self.project();

            Ready({
                if *this.fused {
                    None
                } else {
                    match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                        Some(Ok(item)) => {
                            let index = *this.counter;
                            *this.counter += 1;
                            Some(Ok((index, item)))
                        }
                        Some(Err(err)) => {
                            *this.fused = true;
                            Some(Err(err))
                        }
                        None => None,
                    }
                }
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn take_until_error_test() {
        {
            let vec: Vec<Result<(), ()>> = stream::empty().take_until_error().collect().await;
            assert_eq!(vec, []);
        }

        {
            let vec: Vec<Result<_, ()>> = stream::iter([Ok(0), Ok(1), Ok(2), Ok(3)])
                .take_until_error()
                .collect()
                .await;
            assert_eq!(vec, [Ok(0), Ok(1), Ok(2), Ok(3)]);
        }

        {
            let vec: Vec<Result<_, _>> = stream::iter([Ok(0), Ok(1), Err(2), Ok(3)])
                .take_until_error()
                .collect()
                .await;
            assert_eq!(vec, [Ok(0), Ok(1), Err(2),]);
        }
    }

    #[tokio::test]
    async fn try_stateful_then_test() {
        {
            let values: Result<Vec<_>, ()> = stream::iter([Ok(3), Ok(1), Ok(4), Ok(1)])
                .try_stateful_then(0, |acc, val| async move {
                    let new_acc = acc + val;
                    Ok(Some((new_acc, new_acc)))
                })
                .try_collect()
                .await;

            assert_eq!(values, Ok(vec![3, 4, 8, 9]));
        }

        {
            let mut stream = stream::iter([Ok(3), Ok(1), Err(()), Ok(1)])
                .try_stateful_then(0, |acc, val| async move {
                    let new_acc = acc + val;
                    Ok(Some((new_acc, new_acc)))
                })
                .boxed();

            assert_eq!(stream.next().await, Some(Ok(3)));
            assert_eq!(stream.next().await, Some(Ok(4)));
            assert_eq!(stream.next().await, Some(Err(())));
            assert_eq!(stream.next().await, None);
        }

        {
            let mut stream = stream::iter([Ok(3), Ok(1), Ok(4), Ok(1), Err(())])
                .try_stateful_then(0, |acc, val| async move {
                    let new_acc = acc + val;
                    if new_acc != 8 {
                        Ok(Some((new_acc, new_acc)))
                    } else {
                        Err(())
                    }
                })
                .boxed();

            assert_eq!(stream.next().await, Some(Ok(3)));
            assert_eq!(stream.next().await, Some(Ok(4)));
            assert_eq!(stream.next().await, Some(Err(())));
            assert_eq!(stream.next().await, None);
        }

        {
            let mut stream = stream::iter([Ok(3), Ok(1), Ok(4), Ok(1), Err(())])
                .try_stateful_then(0, |acc, val| async move {
                    let new_acc = acc + val;
                    if new_acc != 8 {
                        Ok(Some((new_acc, new_acc)))
                    } else {
                        Ok(None)
                    }
                })
                .boxed();

            assert_eq!(stream.next().await, Some(Ok(3)));
            assert_eq!(stream.next().await, Some(Ok(4)));
            assert_eq!(stream.next().await, None);
        }
    }
}
