use crate::common::*;

pub trait StreamExt
where
    Self: Stream,
{
    fn reduce<F, Fut>(self, f: F) -> Reduce<Self, F, Fut>;

    /// A combinator that consumes as many elements as it likes, and produces the next stream element.
    ///
    /// The function f([receiver](flume::Receiver), [sender](flume::Sender)) takes one or more elements
    /// by calling `receiver.recv().await`,
    /// It returns `Some(item)` if an input element is available, otherwise it returns `None`.
    /// Calling `sender.send(item).await` will produce an output element. It returns `Ok(())` when success,
    /// or returns `Err(item)` if the output stream is closed.
    ///
    /// ```rust
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::mem;
    ///
    /// async fn main_async() {
    ///     let data = vec![1, 2, -3, 4, 5, -6, 7, 8];
    ///     let mut stream = stream::iter(data)
    ///         .batching(|mut stream| async move {
    ///             let mut buffer = vec![];
    ///             while let Some(value) = stream.next().await {
    ///                 buffer.push(value);
    ///                 if value < 0 {
    ///                     break;
    ///                 }
    ///             }
    ///
    ///             (!buffer.is_empty()).then(|| (buffer, stream))
    ///         })
    ///         .boxed();
    ///
    ///     assert_eq!(stream.next().await, Some(vec![1, 2, -3]));
    ///     assert_eq!(stream.next().await, Some(vec![4, 5, -6]));
    ///     assert_eq!(stream.next().await, Some(vec![7, 8]));
    ///     assert!(stream.next().await.is_none());
    /// }
    ///
    /// # #[cfg(feature = "runtime-async-std")]
    /// # #[async_std::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-tokio")]
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     main_async().await
    /// # }
    /// #
    /// # #[cfg(feature = "runtime-smol")]
    /// # fn main() {
    /// #     smol::block_on(main_async())
    /// # }
    /// ```
    fn batching<T, F, Fut>(self, f: F) -> Batching<Self, T, F, Fut>
    where
        Self: Sized,
        F: 'static + Send + FnMut(Self) -> Fut,
        Fut: 'static + Future<Output = Option<(T, Self)>> + Send,
        T: 'static + Send;

    fn stateful_then<T, B, F, Fut>(self, init: B, f: F) -> StatefulThen<Self, B, T, F, Fut>;

    fn stateful_map<T, B, F>(self, init: B, f: F) -> StatefulMap<Self, B, T, F>;
}

impl<S> StreamExt for S
where
    S: Stream,
{
    fn reduce<F, Fut>(self, f: F) -> Reduce<Self, F, Fut> {
        Reduce {
            fold: None,
            f,
            future: None,
            stream: self,
        }
    }

    fn batching<T, F, Fut>(self, f: F) -> Batching<Self, T, F, Fut> {
        Batching {
            f,
            future: None,
            stream: Some(self),
            _phantom: PhantomData,
        }
    }

    fn stateful_then<T, B, F, Fut>(self, init: B, f: F) -> StatefulThen<Self, B, T, F, Fut> {
        StatefulThen {
            stream: self,
            future: None,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

    fn stateful_map<T, B, F>(self, init: B, f: F) -> StatefulMap<Self, B, T, F> {
        StatefulMap {
            stream: self,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }
}

pub use batching::*;
mod batching {
    use super::*;

    /// Stream for the [`batching`](super::StreamExt::batching) method.
    #[pin_project]
    pub struct Batching<St, T, F, Fut> {
        pub(super) f: F,
        #[pin]
        pub(super) future: Option<Fut>,
        pub(super) stream: Option<St>,
        pub(super) _phantom: PhantomData<T>,
    }

    impl<St, T, F, Fut> Stream for Batching<St, T, F, Fut>
    where
        F: FnMut(St) -> Fut,
        Fut: Future<Output = Option<(T, St)>>,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            if let Some(stream) = this.stream.take() {
                let new_future = (this.f)(stream);
                this.future.set(Some(new_future));
            }

            Ready(loop {
                if let Some(mut future) = this.future.as_pin_mut() {
                    match ready!(future.poll_unpin(cx)) {
                        Some((item, stream)) => {
                            let new_future = (this.f)(stream);
                            future.set(new_future);
                            break Some(item);
                        }
                        None => break None,
                    }
                } else {
                    break None;
                }
            })
        }
    }
}

pub use stateful_then::*;
mod stateful_then {
    use super::*;

    /// Stream for the [`stateful_then`](super::StreamExt::stateful_then) method.
    #[pin_project]
    pub struct StatefulThen<St, B, T, F, Fut>
    where
        St: ?Sized,
    {
        #[pin]
        pub(super) future: Option<Fut>,
        pub(super) state: Option<B>,
        pub(super) f: F,
        pub(super) _phantom: PhantomData<T>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, B, T, F, Fut> Stream for StatefulThen<St, B, T, F, Fut>
    where
        St: Stream,
        F: FnMut(B, St::Item) -> Fut,
        Fut: Future<Output = Option<(B, T)>>,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Poll::Ready(loop {
                if let Some(fut) = this.future.as_mut().as_pin_mut() {
                    let output = ready!(fut.poll(cx));
                    this.future.set(None);

                    if let Some((state, item)) = output {
                        *this.state = Some(state);
                        break Some(item);
                    } else {
                        break None;
                    }
                } else if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(item)) => {
                            this.future.set(Some((this.f)(state, item)));
                        }
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

pub use stateful_map::*;
mod stateful_map {
    use super::*;

    /// Stream for the [`stateful_map`](super::StreamExt::stateful_map) method.
    #[pin_project]
    pub struct StatefulMap<St, B, T, F>
    where
        St: ?Sized,
    {
        pub(super) state: Option<B>,
        pub(super) f: F,
        pub(super) _phantom: PhantomData<T>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, B, T, F> Stream for StatefulMap<St, B, T, F>
    where
        St: Stream,
        F: FnMut(B, St::Item) -> Option<(B, T)>,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Poll::Ready(loop {
                if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(in_item)) => {
                            if let Some((state, out_item)) = (this.f)(state, in_item) {
                                *this.state = Some(state);
                                break Some(out_item);
                            } else {
                                break None;
                            }
                        }
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

use reduce::*;
mod reduce {
    use super::*;

    #[pin_project]
    pub struct Reduce<St, F, Fut>
    where
        St: ?Sized + Stream,
    {
        pub(super) fold: Option<St::Item>,
        pub(super) f: F,
        #[pin]
        pub(super) future: Option<Fut>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, F, Fut> Future for Reduce<St, F, Fut>
    where
        St: Stream,
        F: FnMut(St::Item, St::Item) -> Fut,
        Fut: Future<Output = St::Item>,
    {
        type Output = Option<St::Item>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut this = self.project();

            Ready(loop {
                if let Some(mut future) = this.future.as_mut().as_pin_mut() {
                    let fold = ready!(future.poll_unpin(cx));
                    this.future.set(None);
                    *this.fold = Some(fold);
                } else if let Some(item) = ready!(this.stream.poll_next_unpin(cx)) {
                    if let Some(fold) = this.fold.take() {
                        let future = (this.f)(fold, item);
                        this.future.set(Some(future));
                    } else {
                        *this.fold = Some(item);
                    }
                } else {
                    break this.fold.take();
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reduce_test() {
        {
            let output = stream::iter(1..=10)
                .reduce(|lhs, rhs| async move { lhs + rhs })
                .await;
            assert_eq!(output, Some(55));
        }

        {
            let output = future::ready(1)
                .into_stream()
                .reduce(|lhs, rhs| async move { lhs + rhs })
                .await;
            assert_eq!(output, Some(1));
        }

        {
            let output = stream::empty::<usize>()
                .reduce(|lhs, rhs| async move { lhs + rhs })
                .await;
            assert_eq!(output, None);
        }
    }

    #[tokio::test]
    async fn stateful_then_test() {
        let vec: Vec<_> = stream::repeat(())
            .stateful_then(0, |count, ()| async move {
                (count < 10).then(|| (count + 1, count))
            })
            .collect()
            .await;

        assert_eq!(&*vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn stateful_map_test() {
        let vec: Vec<_> = stream::repeat(())
            .stateful_map(0, |count, ()| (count < 10).then(|| (count + 1, count)))
            .collect()
            .await;

        assert_eq!(&*vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn batching_test() {
        let sums: Vec<_> = stream::iter(0..10)
            .batching(|mut stream| async move {
                let mut sum = 0;

                while let Some(val) = stream.next().await {
                    sum += val;

                    if sum >= 10 {
                        return Some((sum, stream));
                    }
                }

                None
            })
            .collect()
            .await;

        assert_eq!(sums, vec![10, 11, 15]);
    }
}
