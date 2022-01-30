use crate::common::*;
use tokio::sync::oneshot;

/// The trait extends [TryStream](futures::stream::TryStream) types with combinators.
pub trait TryStreamExt
where
    Self: TryStream,
{
    /// Create a fallible stream that gives the current iteration count.
    ///
    /// # Overflow Behavior
    /// The method does no guarding against overflows, so enumerating more than `usize::MAX`
    /// elements either produces the wrong result or panics. If debug assertions are enabled, a panic is guaranteed.
    ///
    /// Panics
    /// The returned iterator might panic if the to-be-returned index would overflow a `usize`.
    fn try_enumerate(self) -> TryEnumerate<Self, Self::Ok, Self::Error>;

    /// Takes elements until an `Err(_)`.
    fn take_until_error(self) -> TakeUntilError<Self, Self::Ok, Self::Error>;

    /// Split the stream of `Result<T, E>` to a stream of `T` and a future of `Result<(), E>`.
    ///
    /// The method returns `(future, stream)`. If this combinator encoutners an `Err`,
    /// `future.await` returns that error, and returned `stream` fuses. If the input
    /// stream is depleted without error, `future.await` resolves to `Ok(())`.
    fn catch_error(self) -> (ErrorNotify<Self::Error>, CatchError<Self>);

    /// Similar to [and_then](futures::stream::TryStreamExt::and_then) but with a state.
    fn try_stateful_then<B, U, F, Fut>(
        self,
        init: B,
        f: F,
    ) -> TryStatefulThen<Self, B, Self::Ok, U, Self::Error, F, Fut>
    where
        F: FnMut(B, Self::Ok) -> Fut,
        Fut: Future<Output = Result<Option<(B, U)>, Self::Error>>;

    /// Similar to [map](futures::stream::StreamExt::map) but with a state and is fallible.
    fn try_stateful_map<B, U, F>(
        self,
        init: B,
        f: F,
    ) -> TryStatefulMap<Self, B, Self::Ok, U, Self::Error, F>
    where
        F: FnMut(B, Self::Ok) -> Result<Option<(B, U)>, Self::Error>;
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
    ) -> TryStatefulThen<Self, B, T, U, E, F, Fut>
    where
        F: FnMut(B, T) -> Fut,
        Fut: Future<Output = Result<Option<(B, U)>, E>>,
    {
        TryStatefulThen {
            stream: self,
            future: None,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

    fn try_stateful_map<B, U, F>(self, init: B, f: F) -> TryStatefulMap<Self, B, T, U, E, F>
    where
        F: FnMut(B, T) -> Result<Option<(B, U)>, E>,
    {
        TryStatefulMap {
            stream: self,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

    fn catch_error(self) -> (ErrorNotify<E>, CatchError<S>) {
        let (tx, rx) = oneshot::channel();
        let stream = CatchError {
            sender: Some(tx),
            stream: self,
        };
        let notify = ErrorNotify { receiver: rx };

        (notify, stream)
    }
}

pub use take_until_error::*;
mod take_until_error {
    use super::*;

    /// Stream for the [`take_until_error`](super::TryStreamExt::take_until_error) method.
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
                } else if let Some(result) = ready!(this.stream.poll_next(cx)) {
                    if result.is_err() {
                        *this.is_terminated = true;
                    }
                    Some(result)
                } else {
                    *this.is_terminated = true;
                    None
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

            Poll::Ready({
                if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(Ok(in_item))) => {
                            let result = (this.f)(state, in_item);

                            match result {
                                Ok(Some((state, out_item))) => {
                                    *this.state = Some(state);
                                    Some(Ok(out_item))
                                }
                                Ok(None) => None,
                                Err(err) => Some(Err(err)),
                            }
                        }
                        Ready(Some(Err(err))) => Some(Err(err)),
                        Ready(None) => None,
                        Pending => {
                            *this.state = Some(state);
                            return Pending;
                        }
                    }
                } else {
                    None
                }
            })
        }
    }
}

pub use try_enumerate::*;
mod try_enumerate {
    use super::*;

    /// Stream for the [try_enumerate()](crate::try_stream::TryStreamExt::try_enumerate) method.
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

pub use catch_error::*;
mod catch_error {
    use super::*;

    /// Stream for the [`catch_error`](super::TryStreamExt::catch_error) method.
    #[pin_project]
    pub struct CatchError<St>
    where
        St: ?Sized + TryStream,
    {
        pub(super) sender: Option<oneshot::Sender<St::Error>>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St> Stream for CatchError<St>
    where
        St: TryStream,
    {
        type Item = St::Ok;

        fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
            let this = self.project();

            Ready({
                if let Some(sender) = this.sender.take() {
                    match this.stream.try_poll_next(ctx) {
                        Ready(Some(Ok(item))) => {
                            *this.sender = Some(sender);
                            Some(item)
                        }
                        Ready(Some(Err(err))) => {
                            let _ = sender.send(err);
                            None
                        }
                        Ready(None) => {
                            drop(sender);
                            None
                        }
                        Pending => {
                            *this.sender = Some(sender);
                            return Pending;
                        }
                    }
                } else {
                    None
                }
            })
        }
    }

    /// Future for the [`catch_error`](super::TryStreamExt::catch_error) method.
    #[pin_project]
    pub struct ErrorNotify<E> {
        #[pin]
        pub(super) receiver: oneshot::Receiver<E>,
    }

    impl<E> ErrorNotify<E> {
        pub fn try_catch(mut self) -> ControlFlow<Result<(), E>, Self> {
            use oneshot::error::TryRecvError::*;

            match self.receiver.try_recv() {
                Ok(err) => Break(Err(err)),
                Err(Empty) => Continue(self),
                Err(Closed) => Break(Ok(())),
            }
        }
    }

    impl<E> Future for ErrorNotify<E> {
        type Output = Result<(), E>;

        fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
            let this = self.project();

            Ready(match ready!(this.receiver.poll(ctx)) {
                Ok(err) => Err(err),
                Err(_) => Ok(()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::async_test;

    async_test! {
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


        async fn catch_error_test() {
            {
                let (notify, stream) = stream::empty::<Result<(), ()>>().catch_error();

                let vec: Vec<_> = stream.collect().await;
                let result = notify.await;

                assert_eq!(vec, []);
                assert_eq!(result, Ok(()));
            }

            {
                let (notify, stream) =
                    stream::iter([Result::<_, ()>::Ok(0), Ok(1), Ok(2), Ok(3)]).catch_error();

                let vec: Vec<_> = stream.collect().await;
                let result = notify.await;

                assert_eq!(vec, [0, 1, 2, 3]);
                assert_eq!(result, Ok(()));
            }

            {
                let (notify, stream) = stream::iter([Ok(0), Ok(1), Err(2), Ok(3)]).catch_error();

                let vec: Vec<_> = stream.collect().await;
                let result = notify.await;

                assert_eq!(vec, [0, 1]);
                assert_eq!(result, Err(2));
            }

            {
                let (notify, mut stream) = stream::empty::<Result<(), ()>>().catch_error();

                let notify = match notify.try_catch() {
                    Continue(notify) => notify,
                    _ => unreachable!(),
                };

                assert_eq!(stream.next().await, None);
                assert!(matches!(notify.try_catch(), Break(Ok(()))));
            }

            {
                let (notify, mut stream) = stream::iter([Result::<_, ()>::Ok(0)]).catch_error();

                let notify = match notify.try_catch() {
                    Continue(notify) => notify,
                    _ => unreachable!(),
                };

                assert_eq!(stream.next().await, Some(0));
                let notify = match notify.try_catch() {
                    Continue(notify) => notify,
                    _ => unreachable!(),
                };

                assert_eq!(stream.next().await, None);
                assert!(matches!(notify.try_catch(), Break(Ok(()))));
            }

            {
                let (notify, mut stream) = stream::iter([Ok(0), Err(2)]).catch_error();

                let notify = match notify.try_catch() {
                    Continue(notify) => notify,
                    _ => unreachable!(),
                };

                assert_eq!(stream.next().await, Some(0));
                let notify = match notify.try_catch() {
                    Continue(notify) => notify,
                    _ => unreachable!(),
                };

                assert_eq!(stream.next().await, None);
                assert!(matches!(notify.try_catch(), Break(Err(2))));
            }
        }
    }
}
