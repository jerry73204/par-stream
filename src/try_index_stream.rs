use crate::common::*;

/// The trait extends [TryStream](futures::stream::TryStream) types with ordering manipulation combinators.
pub trait TryIndexStreamExt
where
    Self: Stream<Item = Result<(usize, Self::Ok), Self::Error>>,
{
    type Ok;
    type Error;

    /// Reorders the input items `Ok((index, item))` according to the index number and returns `item`.
    ///
    /// It can be combined with [try_enumerate](crate::try_stream::TryStreamExt::try_enumerate) and
    /// unordered parallel tasks.
    ///
    /// If an `Err` item is received, it stops receiving future items and flushes buffered values,
    /// and sends the `Err` in the end.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    ///
    /// let result: Result<Vec<_>, _> = stream::iter(0..100)
    ///     .map(|val| if val < 50 { Ok(val) } else { Err(val) })
    ///     // add index number
    ///     .try_enumerate()
    ///     // double the values in parallel
    ///     .try_par_then_unordered(None, move |(index, value)| {
    ///         // the closure is sent to parallel worker
    ///         async move { Ok((index, value * 2)) }
    ///     })
    ///     // add values by one in parallel
    ///     .try_par_then_unordered(None, move |(index, value)| {
    ///         // the closure is sent to parallel worker
    ///         async move {
    ///             let new_val = value + 1;
    ///             if new_val < 50 {
    ///                 Ok((index, new_val))
    ///             } else {
    ///                 Err(value)
    ///             }
    ///         }
    ///     })
    ///     // reorder the values according to index number
    ///     .try_reorder_enumerated()
    ///     .try_collect()
    ///     .await;
    /// # })
    /// ```
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
            pending_error: None,
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

    /// Stream for the [try_reorder_enumerated()](TryIndexStreamExt::try_reorder_enumerated) method.
    #[derive(Derivative)]
    #[derivative(Debug)]
    #[pin_project]
    pub struct TryReorderEnumerated<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) commit: usize,
        pub(super) is_terminated: bool,
        pub(super) pending_error: Option<E>,
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
                } else if let Some(err) = this.pending_error.take() {
                    if let Some(item) = this.buffer.remove(this.commit) {
                        *this.pending_error = Some(err);
                        *this.commit += 1;
                        break Some(Ok(item));
                    } else {
                        *this.is_terminated = true;
                        break Some(Err(err));
                    }
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
                            *this.pending_error = Some(err);
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
