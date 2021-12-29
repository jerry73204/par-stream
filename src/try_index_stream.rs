use crate::common::*;

/// An extension trait that controls ordering of items of fallible streams.
pub trait TryIndexStreamExt
where
    Self: Stream<Item = Result<(usize, Self::Ok), Self::Error>>,
{
    type Ok;
    type Error;

    /// Creates a fallible stream that reorders the items according to the iteration count.
    ///
    /// It is usually combined with [try_enumerate](crate::try_stream::TryStreamExt::try_enumerate).
    fn try_reorder_enumerated(self) -> TryReorderEnumerated<Self, Self::Ok, Self::Error>;
}

impl<S, T, E> TryIndexStreamExt for S
where
    S: Stream<Item = Result<(usize, T), E>>,
{
    type Ok = T;
    type Error = E;

    fn try_reorder_enumerated(self) -> TryReorderEnumerated<Self, T, E> {
        TryReorderEnumerated {
            stream: self,
            commit: 0,
            is_terminated: false,
            buffer: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

// try_reorder_enumerated

pub use try_reorder_enumerated::*;

mod try_reorder_enumerated {
    use super::*;

    /// A fallible stream combinator returned from [try_reorder_enumerated()](TryIndexStreamExt::try_reorder_enumerated).
    #[derive(Derivative)]
    #[derivative(Debug)]
    #[pin_project]
    pub struct TryReorderEnumerated<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) commit: usize,
        pub(super) is_terminated: bool,
        pub(super) buffer: HashMap<usize, T>,
        pub(super) _phantom: PhantomData<E>,
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: S,
    }

    impl<S, T, E> Stream for TryReorderEnumerated<S, T, E>
    where
        S: Stream<Item = Result<(usize, T), E>>,
    {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            Ready(loop {
                if *this.is_terminated {
                    break None;
                } else if let Some(item) = this.buffer.remove(this.commit) {
                    *this.commit += 1;
                    break Some(Ok(item));
                } else {
                    match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                        Some(Ok((index, item))) => match (*this.commit).cmp(&index) {
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
                                break Some(Ok(item));
                            }
                            Greater => {
                                panic!("the index number {} appears more than once", index);
                            }
                        },
                        Some(Err(err)) => {
                            *this.is_terminated = true;
                            break Some(Err(err));
                        }
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

    impl<S, T, E> FusedStream for TryReorderEnumerated<S, T, E>
    where
        Self: Stream,
    {
        fn is_terminated(&self) -> bool {
            self.is_terminated
        }
    }
}
