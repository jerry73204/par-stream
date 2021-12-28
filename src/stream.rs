use crate::common::*;

pub trait StreamExt
where
    Self: Stream,
{
    fn stateful_then<T, B, F, Fut>(self, init: B, f: F) -> StatefulThen<Self, B, T, F, Fut>;

    fn stateful_map<T, B, F>(self, init: B, f: F) -> StatefulMap<Self, B, T, F>;
}

impl<S> StreamExt for S
where
    S: Stream,
{
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
