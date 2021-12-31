use crate::common::*;

/// The trait extends [Stream](futures::stream::Stream) types with ordering manipulation combinators.
pub trait IndexStreamExt
where
    Self: Stream<Item = (usize, Self::IndexedItem)>,
{
    type IndexedItem;

    /// Reorders the input items `(index, item)` according to the index number and returns `item`.
    ///
    /// It can be combined with [enumerate()](futures::StreamExt::enumerate) and parallel
    /// unordered tasks.
    ///
    /// The index numbers must start from zero, be unique and contiguous. Index not starting
    /// from zero causes the stream to hang indefinitely.
    ///
    /// # Panics
    /// The repeating of an index will cause the stream to panic.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// let doubled: Vec<_> = stream::iter(0..1000)
    ///     // add index number
    ///     .enumerate()
    ///     // double the values in parallel
    ///     .par_then_unordered(None, move |(index, value)| {
    ///         // the closure is sent to parallel worker
    ///         async move { (index, value * 2) }
    ///     })
    ///     // add values by one in parallel
    ///     .par_then_unordered(None, move |(index, value)| {
    ///         // the closure is sent to parallel worker
    ///         async move { (index, value + 1) }
    ///     })
    ///     // reorder the values according to index number
    ///     .reorder_enumerated()
    ///     .collect()
    ///     .await;
    /// let expect: Vec<_> = (0..1000).map(|value| value * 2 + 1).collect();
    /// assert_eq!(doubled, expect);
    /// # })
    /// ```
    fn reorder_enumerated(self) -> ReorderEnumerated<Self, Self::IndexedItem>;
}

impl<S, T> IndexStreamExt for S
where
    S: Stream<Item = (usize, T)>,
{
    type IndexedItem = T;

    fn reorder_enumerated(self) -> ReorderEnumerated<Self, Self::IndexedItem> {
        ReorderEnumerated {
            commit: 0,
            buffer: HashMap::new(),
            stream: self,
        }
    }
}

// reorder_enumerated

pub use reorder_enumerated::*;

mod reorder_enumerated {
    use super::*;

    /// Stream for the [reorder_enumerated](IndexStreamExt::reorder_enumerated) method.
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
