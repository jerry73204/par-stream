use crate::common::*;

/// An extension trait that controls ordering of stream items.
pub trait IndexStreamExt
where
    Self: Stream<Item = (usize, Self::IndexedItem)>,
{
    type IndexedItem;

    /// Reorder the input items paired with a iteration count.
    ///
    /// The combinator asserts the input item has tuple type `(usize, T)`.
    /// It reorders the items according to the first value of input tuple.
    ///
    /// It is usually combined with [enumerate()](futures::StreamExt::enumerate), then
    /// applies a series of unordered parallel mapping, and finally reorders the values
    /// back by this method. It avoids reordering the values after each parallel mapping step.
    ///
    /// ```rust
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// async fn main_async() {
    ///     let doubled = stream::iter(0..1000)
    ///         // add enumerated index that does not panic on overflow
    ///         .enumerate()
    ///         // double the values in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value * 2) }
    ///         })
    ///         // add values by one in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value + 1) }
    ///         })
    ///         // reorder the values by enumerated index
    ///         .reorder_enumerated()
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
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
    fn reorder_enumerated(self) -> ReorderEnumerated<Self, Self::IndexedItem>
    where
        Self: Sized,
    {
        ReorderEnumerated {
            commit: 0,
            buffer: HashMap::new(),
            stream: self,
        }
    }
}

impl<S, T> IndexStreamExt for S
where
    S: Stream<Item = (usize, T)>,
{
    type IndexedItem = T;
}

// reorder_enumerated

pub use reorder_enumerated::*;

mod reorder_enumerated {
    use super::*;

    /// A stream combinator returned from [reorder_enumerated()](IndexStreamExt::reorder_enumerated).
    #[derive(Derivative)]
    #[derivative(Debug)]
    #[pin_project]
    pub struct ReorderEnumerated<S, T>
    where
        S: ?Sized,
    {
        pub(super) commit: usize,
        pub(super) buffer: HashMap<usize, T>,
        #[pin]
        pub(super) stream: S,
    }

    impl<S, T> Stream for ReorderEnumerated<S, T>
    where
        S: Stream<Item = (usize, T)>,
    {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Ready(loop {
                if let Some(item) = this.buffer.remove(&*this.commit) {
                    *this.commit += 1;
                    break Some(item);
                } else {
                    match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                        Some((index, item)) => match (*this.commit).cmp(&index) {
                            Less => {
                                let prev = this.buffer.insert(index, item);
                                assert!(
                                    prev.is_none(),
                                    "the index number {} appears more than once",
                                    index
                                );
                            }
                            Equal => {
                                *this.commit += 1;
                                break Some(item);
                            }
                            Greater => {
                                panic!("the index number {} appears more than once", index);
                            }
                        },
                        None => {
                            assert!(
                                this.buffer.is_empty(),
                                "the item for index number {} is missing",
                                this.commit
                            );
                            break None;
                        }
                    }
                }
            })
        }
    }
}
