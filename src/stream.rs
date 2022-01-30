use crate::{common::*, shared_stream::Shared, state_stream::StateStream};
use futures::stream::Zip;

/// Stream for the [with_state()](StreamExt::with_state) method.
pub type WithState<S, B> = Zip<S, StateStream<B>>;

/// The trait extneds [Stream](futures::stream::Stream) types with extra combinators.
pub trait StreamExt
where
    Self: Stream,
{
    /// Creates a shareable stream that can clone the ownership of the stream.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::mem;
    ///
    /// // Creates two sharing handles to the stream
    /// let stream = stream::iter(0..100);
    /// let shared1 = stream.shared();
    /// let shared2 = shared1.clone();
    ///
    /// // Consumes the shared streams individually
    /// let collect1 = par_stream::rt::spawn(shared1.collect());
    /// let collect2 = par_stream::rt::spawn(shared2.collect());
    /// let (vec1, vec2): (Vec<_>, Vec<_>) = futures::join!(collect1, collect2);
    ///
    /// // Checks that the combined values of two vecs are equal to original values
    /// let mut all_vec: Vec<_> = vec1.into_iter().chain(vec2).collect();
    /// all_vec.sort();
    /// itertools::assert_equal(all_vec, 0..100);
    /// # })
    /// ```
    fn shared(self) -> Shared<Self>;

    /// Binds the stream with a state value.
    ///
    /// It turns the stream item into `(item, state)`. The `state` is a mutable
    /// handle to the state value which is initialized to `init`. The `state` can
    /// be modified as user desires. The state must be given back by `state.send()`
    /// or be dropped so that the stream can proceed to the next iteration. If
    /// `state.close()` is called, the state is discarded and terminates the stream.
    fn with_state<B>(self, init: B) -> WithState<Self, B>
    where
        Self: Sized;

    /// Take elements after the provided future resolves to `true`, otherwise fuse the stream.
    ///
    /// The stream waits for the future `fut` to resolve. Once the future becomes ready
    /// The stream starts taking elements if it resolves to `true`. If the future returns `false`,
    /// this stream combinator will always return that the stream is done.
    ///
    /// # Examples
    ///
    /// ```
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::{prelude::*, stream, stream::StreamExt as _};
    /// use par_stream::prelude::*;
    ///
    /// use std::{
    ///     sync::{
    ///         atomic::{AtomicBool, Ordering::*},
    ///         Arc,
    ///     },
    ///     time::Duration,
    /// };
    ///
    /// let is_ready = Arc::new(AtomicBool::new(false));
    ///
    /// stream::iter(0..10)
    ///     .wait_until(async {
    ///         par_stream::rt::sleep(Duration::from_millis(200)).await;
    ///         is_ready.store(true, SeqCst);
    ///         true
    ///     })
    ///     .for_each(|_| async {
    ///         assert!(is_ready.load(SeqCst));
    ///     })
    ///     .await;
    /// # })
    /// ```
    fn wait_until<Fut>(self, fut: Fut) -> WaitUntil<Self, Fut>
    where
        Fut: Future<Output = bool>;

    /// Reduces the stream items into a single value.
    ///
    /// The `f(item, item) -> item` returns a future that reduces two stream items into one value.
    /// If the stream is empty, this stream combinator resolves to `None`. Otherwise it resolves to
    /// the reduced value `Some(value)`.
    fn reduce<F, Fut>(self, f: F) -> Reduce<Self, F, Fut>;

    /// The combinator consumes as many items from the stream as it likes for each output item.
    ///
    /// The function `f(stream) -> Option<(output, stream)>` returns a future that takes values
    /// from the stream, and returns combined values and the stream back. If it returns `None`,
    /// this stream combinator stops producing future values.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::mem;
    ///
    /// let data = vec![1, 2, -3, 4, 5, -6, 7, 8];
    /// let mut stream = stream::iter(data)
    ///     .batching(|mut stream| async move {
    ///         let mut buffer = vec![];
    ///         while let Some(value) = stream.next().await {
    ///             buffer.push(value);
    ///             if value < 0 {
    ///                 break;
    ///             }
    ///         }
    ///
    ///         (!buffer.is_empty()).then(|| (buffer, stream))
    ///     })
    ///     .boxed();
    ///
    /// assert_eq!(stream.next().await, Some(vec![1, 2, -3]));
    /// assert_eq!(stream.next().await, Some(vec![4, 5, -6]));
    /// assert_eq!(stream.next().await, Some(vec![7, 8]));
    /// assert!(stream.next().await.is_none());
    /// # })
    /// ```
    fn batching<T, F, Fut>(self, f: F) -> Batching<Self, T, F, Fut>
    where
        Self: Sized,
        F: 'static + Send + FnMut(Self) -> Fut,
        Fut: 'static + Future<Output = Option<(T, Self)>> + Send,
        T: 'static + Send;

    /// Similar to [batching](StreamExt::batching) but with a state.
    ///
    /// The batching funtion `f(state, stream) -> Option<(output, state, stream)>` returns a future
    /// that takes items from stream as many as it wants, and returns the output and gives the state
    /// and stream back.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::{stream, stream::StreamExt as _};
    /// use par_stream::StreamExt as _;
    ///
    /// let vec: Vec<_> = stream::iter([1i32, 1, 1, -1, -1, 1])
    ///     .stateful_batching(None, |mut sum: Option<i32>, mut stream| async move {
    ///         while let Some(val) = stream.next().await {
    ///             match &mut sum {
    ///                 Some(sum) => {
    ///                     if sum.signum() == val.signum() {
    ///                         *sum += val;
    ///                     } else {
    ///                         return Some((*sum, Some(val), stream));
    ///                     }
    ///                 }
    ///                 sum => *sum = Some(val),
    ///             }
    ///         }
    ///
    ///         match sum {
    ///             Some(sum) => Some((sum, None, stream)),
    ///             None => None,
    ///         }
    ///     })
    ///     .collect()
    ///     .await;
    ///
    /// assert_eq!(vec, [3, -2, 1]);
    /// # })
    /// ```
    fn stateful_batching<T, B, F, Fut>(self, init: B, f: F) -> StatefulBatching<Self, B, T, F, Fut>
    where
        Self: Sized + Stream,
        F: FnMut(B, Self) -> Fut,
        Fut: Future<Output = Option<(T, B, Self)>>;

    /// Similar to [then](futures::StreamExt::then) but with a state.
    fn stateful_then<T, B, F, Fut>(self, init: B, f: F) -> StatefulThen<Self, B, T, F, Fut>
    where
        Self: Stream,
        F: FnMut(B, Self::Item) -> Fut,
        Fut: Future<Output = Option<(B, T)>>;

    /// Similar to [map](futures::StreamExt::map) but with a state.
    fn stateful_map<T, B, F>(self, init: B, f: F) -> StatefulMap<Self, B, T, F>
    where
        Self: Stream,
        F: FnMut(B, Self::Item) -> Option<(B, T)>;
}

impl<S> StreamExt for S
where
    S: Stream,
{
    fn shared(self) -> Shared<Self> {
        Shared::new(self)
    }

    fn with_state<B>(self, init: B) -> WithState<Self, B>
    where
        Self: Sized,
    {
        self.zip(StateStream::new(init))
    }

    fn reduce<F, Fut>(self, f: F) -> Reduce<Self, F, Fut> {
        Reduce {
            fold: None,
            f,
            future: None,
            stream: self,
        }
    }

    fn wait_until<Fut>(self, fut: Fut) -> WaitUntil<Self, Fut>
    where
        Fut: Future<Output = bool>,
    {
        WaitUntil::new(self, fut)
    }

    fn batching<T, F, Fut>(self, f: F) -> Batching<Self, T, F, Fut> {
        Batching {
            f,
            future: None,
            stream: Some(self),
            _phantom: PhantomData,
        }
    }

    fn stateful_batching<T, B, F, Fut>(self, init: B, f: F) -> StatefulBatching<Self, B, T, F, Fut>
    where
        Self: Stream,
        F: FnMut(B, Self) -> Fut,
        Fut: Future<Output = Option<(T, B, Self)>>,
    {
        StatefulBatching {
            state: Some((init, self)),
            future: None,
            f,
            _phantom: PhantomData,
        }
    }

    fn stateful_then<T, B, F, Fut>(self, init: B, f: F) -> StatefulThen<Self, B, T, F, Fut>
    where
        Self: Stream,
        F: FnMut(B, Self::Item) -> Fut,
        Fut: Future<Output = Option<(B, T)>>,
    {
        StatefulThen {
            stream: self,
            future: None,
            state: Some(init),
            f,
            _phantom: PhantomData,
        }
    }

    fn stateful_map<T, B, F>(self, init: B, f: F) -> StatefulMap<Self, B, T, F>
    where
        Self: Stream,
        F: FnMut(B, Self::Item) -> Option<(B, T)>,
    {
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
        pub(super) _phantom: PhantomData<T>,
        pub(super) stream: Option<St>,
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

            Ready({
                if let Some(mut future) = this.future.as_pin_mut() {
                    match ready!(future.poll_unpin(cx)) {
                        Some((item, stream)) => {
                            let new_future = (this.f)(stream);
                            future.set(new_future);
                            Some(item)
                        }
                        None => None,
                    }
                } else {
                    None
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

            Poll::Ready({
                if let Some(state) = this.state.take() {
                    match this.stream.as_mut().poll_next(cx) {
                        Ready(Some(in_item)) => {
                            if let Some((state, out_item)) = (this.f)(state, in_item) {
                                *this.state = Some(state);
                                Some(out_item)
                            } else {
                                None
                            }
                        }
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

pub use stateful_batching::*;
mod stateful_batching {
    use super::*;

    /// Stream for the [`stateful_batching`](super::StreamExt::stateful_batching) method.
    #[pin_project]
    pub struct StatefulBatching<St, B, T, F, Fut> {
        pub(super) f: F,
        pub(super) _phantom: PhantomData<T>,
        #[pin]
        pub(super) future: Option<Fut>,
        pub(super) state: Option<(B, St)>,
    }

    impl<St, B, T, F, Fut> Stream for StatefulBatching<St, B, T, F, Fut>
    where
        St: Stream,
        F: FnMut(B, St) -> Fut,
        Fut: Future<Output = Option<(T, B, St)>>,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Poll::Ready(loop {
                if let Some(fut) = this.future.as_mut().as_pin_mut() {
                    let output = ready!(fut.poll(cx));
                    this.future.set(None);

                    if let Some((item, state, stream)) = output {
                        *this.state = Some((state, stream));
                        break Some(item);
                    } else {
                        break None;
                    }
                } else if let Some((state, stream)) = this.state.take() {
                    this.future.set(Some((this.f)(state, stream)));
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

use wait_until::*;
mod wait_until {
    use super::*;

    #[pin_project]
    pub struct WaitUntil<St, Fut>
    where
        St: ?Sized + Stream,
        Fut: Future<Output = bool>,
    {
        pub(super) is_fused: bool,
        #[pin]
        pub(super) future: Option<Fut>,
        #[pin]
        pub(super) stream: St,
    }

    impl<St, Fut> WaitUntil<St, Fut>
    where
        St: Stream,
        Fut: Future<Output = bool>,
    {
        pub(super) fn new(stream: St, fut: Fut) -> Self {
            Self {
                stream,
                future: Some(fut),
                is_fused: false,
            }
        }
    }

    impl<St, Fut> Stream for WaitUntil<St, Fut>
    where
        St: Stream,
        Fut: Future<Output = bool>,
    {
        type Item = St::Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Ready(loop {
                if *this.is_fused {
                    break None;
                } else if let Some(future) = this.future.as_mut().as_pin_mut() {
                    let ok = ready!(future.poll(cx));
                    this.future.set(None);

                    if !ok {
                        *this.is_fused = true;
                        break None;
                    }
                } else {
                    break ready!(this.stream.poll_next(cx));
                }
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            if self.is_fused {
                // No future values if it is fused
                (0, Some(0))
            } else {
                let (lower, upper) = self.stream.size_hint();

                if self.future.is_some() {
                    // If future is not resolved yet, returns zero lower bound
                    (0, upper)
                } else {
                    // Returns size hint from underlying stream if the future is resolved
                    (lower, upper)
                }
            }
        }
    }

    impl<St, Fut> FusedStream for WaitUntil<St, Fut>
    where
        St: FusedStream,
        Fut: Future<Output = bool>,
    {
        fn is_terminated(&self) -> bool {
            self.is_fused || self.stream.is_terminated()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{rt, utils::async_test};
    use std::time::Instant;

    async_test! {
        async fn stream_wait_until_future_test() {
            let wait = Duration::from_millis(200);

            {
                let instant = Instant::now();
                let vec: Vec<_> = stream::iter([3, 1, 4])
                    .wait_until(async move {
                        rt::sleep(wait).await;
                        true
                    })
                    .collect()
                    .await;

                assert!(instant.elapsed() >= wait);
                assert_eq!(vec, [3, 1, 4]);
            }

            {
                let instant = Instant::now();
                let vec: Vec<_> = stream::iter([3, 1, 4])
                    .wait_until(async move {
                        rt::sleep(wait).await;
                        false
                    })
                    .collect()
                    .await;

                assert!(instant.elapsed() >= wait);
                assert_eq!(vec, []);
            }
        }


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


        async fn stateful_then_test() {
            let vec: Vec<_> = stream::repeat(())
                .stateful_then(0, |count, ()| async move {
                    (count < 10).then(|| (count + 1, count))
                })
                .collect()
                .await;

            assert_eq!(&*vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }


        async fn stateful_map_test() {
            let vec: Vec<_> = stream::repeat(())
                .stateful_map(0, |count, ()| (count < 10).then(|| (count + 1, count)))
                .collect()
                .await;

            assert_eq!(&*vec, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }


        async fn stateful_batching_test() {
            let vec: Vec<_> = stream::iter([1i32, 1, 1, -1, -1, 1])
                .stateful_batching(None, |mut sum: Option<i32>, mut stream| async move {
                    while let Some(val) = stream.next().await {
                        match &mut sum {
                            Some(sum) => {
                                if sum.signum() == val.signum() {
                                    *sum += val;
                                } else {
                                    return Some((*sum, Some(val), stream));
                                }
                            }
                            sum => *sum = Some(val),
                        }
                    }

                    match sum {
                        Some(sum) => Some((sum, None, stream)),
                        None => None,
                    }
                })
                .collect()
                .await;

            assert_eq!(vec, [3, -2, 1]);
        }


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
}
