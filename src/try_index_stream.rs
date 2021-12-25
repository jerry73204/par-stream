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
    fn try_reorder_enumerated(self) -> TryReorderEnumerated<Self, Self::Ok, Self::Error>
    where
        Self: Sized,
    {
        TryReorderEnumerated {
            stream: self,
            commit: 0,
            fused: false,
            buffer: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

impl<S, T, E> TryIndexStreamExt for S
where
    S: Stream<Item = Result<(usize, T), E>>,
{
    type Ok = T;
    type Error = E;
}

// try_reorder_enumerated

pub use try_reorder_enumerated::*;

mod try_reorder_enumerated {
    use super::*;

    /// A fallible stream combinator returned from [try_reorder_enumerated()](TryIndexStreamExt::try_reorder_enumerated).
    #[pin_project(project = TryReorderEnumeratedProj)]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryReorderEnumerated<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) commit: usize,
        pub(super) fused: bool,
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
            let TryReorderEnumeratedProj {
                fused,
                stream,
                commit,
                buffer,
                ..
            } = self.project();

            if *fused {
                return Ready(None);
            }

            if let Some(item) = buffer.remove(commit) {
                *commit += 1;
                cx.waker().clone().wake();
                return Ready(Some(Ok(item)));
            }

            match stream.poll_next(cx) {
                Ready(Some(Ok((index, item)))) => match (*commit).cmp(&index) {
                    Less => match buffer.entry(index) {
                        hash_map::Entry::Occupied(_) => {
                            panic!("the index number {} appears more than once", index);
                        }
                        hash_map::Entry::Vacant(entry) => {
                            entry.insert(item);
                            cx.waker().clone().wake();
                            Pending
                        }
                    },
                    Equal => {
                        *commit += 1;
                        cx.waker().clone().wake();
                        Ready(Some(Ok(item)))
                    }
                    Greater => {
                        panic!("the index number {} appears more than once", index);
                    }
                },
                Ready(Some(Err(err))) => {
                    *fused = true;
                    Ready(Some(Err(err)))
                }
                Ready(None) => {
                    assert!(buffer.is_empty(), "the index numbers are not contiguous");
                    Ready(None)
                }
                Pending => Pending,
            }
        }
    }

    impl<S, T, E> FusedStream for TryReorderEnumerated<S, T, E>
    where
        Self: Stream,
    {
        fn is_terminated(&self) -> bool {
            self.fused
        }
    }
}
