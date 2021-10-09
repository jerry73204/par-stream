use crate::common::*;

/// The trait provides extensions for concurrent processing on slice-like types.
pub trait SliceExt<T> {
    /// Returns an iterator of fixed-sized chunks of the slice.
    ///
    /// Each chunk has `chunk_size` elements, expect the last chunk maybe shorter
    /// if there aren't enough elements.
    ///
    /// The yielded chunks maintain a global reference count. Each chunk refers to
    /// a mutable and exclusive sub-slice, enabling concurrent processing on input data.
    ///
    /// # Panics
    /// The method panics if `chunk_size` is zero and slice length is not zero.
    fn concurrent_chunks(mut self, chunk_size: usize) -> ConcurrentChunks<Self, T>
    where
        Self: 'static + AsMut<[T]> + Sized + Send,
        T: 'static + Send,
    {
        let len = self.as_mut().len();

        let num_chunks = if len == 0 {
            0
        } else {
            assert!(
                chunk_size > 0,
                "chunk_size must be positive for non-empty slice"
            );
            (len + chunk_size - 1) / chunk_size
        };

        unsafe { ConcurrentChunks::new_unchecked(self, chunk_size, num_chunks, len) }
    }

    /// Returns an iterator of exactly `num_chunks` fixed-sized chunks of the slice.
    ///
    /// The chunk size is determined by `num_chunks`. The last chunk maybe shorter if
    /// there aren't enough elements. If `num_chunks` is `None`, it defaults to
    /// the number of system processors.
    ///
    /// The method is a proxy of [`concurrent_chunks`](SliceExt::concurrent_chunks).
    ///
    /// # Panics
    /// The method panics if `num_chunks` is zero and slice length is not zero.
    fn concurrent_chunks_by_division(
        mut self,
        num_chunks: impl Into<Option<usize>>,
    ) -> ConcurrentChunks<Self, T>
    where
        Self: 'static + AsMut<[T]> + Sized + Send,
        T: 'static + Send,
    {
        let len = self.as_mut().len();
        let num_chunks = num_chunks.into().unwrap_or_else(|| num_cpus::get());

        let chunk_size = if len == 0 {
            0
        } else {
            assert!(num_chunks > 0, "num_chunks must be positive, but get zero");
            (len + num_chunks - 1) / num_chunks
        };

        unsafe { ConcurrentChunks::new_unchecked(self, chunk_size, num_chunks, len) }
    }

    fn concurrent_iter(self: Arc<Self>) -> ConcurrentIter<Self, T>
    where
        Self: 'static + AsRef<[T]> + Sized + Send,
    {
        let owner = ArcRef::new(self.clone()).map(|me| me.as_ref());
        let len = owner.len();

        ConcurrentIter {
            owner,
            len,
            index: 0,
        }
    }
}

impl<S, T> SliceExt<T> for S {}

pub use concurrent_iter::*;

mod concurrent_iter {
    use super::*;

    #[derive(Debug)]
    pub struct ConcurrentIter<S, T> {
        pub(super) owner: ArcRef<S, [T]>,
        pub(super) len: usize,
        pub(super) index: usize,
    }

    impl<S, T> Clone for ConcurrentIter<S, T> {
        fn clone(&self) -> Self {
            Self {
                owner: self.owner.clone(),
                len: self.len,
                index: self.index,
            }
        }
    }

    impl<S, T> Iterator for ConcurrentIter<S, T> {
        type Item = ArcRef<S, T>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index == self.len {
                return None;
            }

            let item = self.owner.clone().map(|slice| &slice[self.index]);
            self.index += 1;
            Some(item)
        }
    }
}

// concurrent_chunks

pub use concurrent_chunks::*;

mod concurrent_chunks {
    use super::*;

    /// An iterator that yields [chunks](Chunk).
    #[derive(Debug)]
    pub struct ConcurrentChunks<S, T>
    where
        S: 'static + Send,
        T: 'static + Send,
    {
        pub(super) index: usize,
        pub(super) chunk_size: usize,
        pub(super) len: usize,
        pub(super) data: Arc<S>,
        pub(super) _phantom: PhantomData<T>,
    }

    impl<S, T> ConcurrentChunks<S, T>
    where
        S: 'static + Send,
        T: 'static + Send,
    {
        pub(super) unsafe fn new_unchecked(
            mut owner: S,
            chunk_size: usize,
            num_chunks: usize,
            len: usize,
        ) -> Self
        where
            S: AsMut<[T]>,
        {
            debug_assert!(
                owner.as_mut().len() == len,
                "expect {} sized slice, but get {}",
                len,
                owner.as_mut().len()
            );
            debug_assert!(if len == 0 {
                chunk_size * num_chunks == 0
            } else {
                let residual = (chunk_size * num_chunks) as isize - len as isize;
                (0..len as isize).contains(&residual)
            });

            let data = Arc::new(owner);

            ConcurrentChunks {
                index: 0,
                chunk_size,
                len,
                data,
                _phantom: PhantomData,
            }
        }
    }

    impl<S, T> Iterator for ConcurrentChunks<S, T>
    where
        S: 'static + AsMut<[T]> + Send,
        T: 'static + Send,
    {
        type Item = Chunk<S, T>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index >= self.len {
                return None;
            }

            let start = self.index;
            let end = cmp::min(start + self.chunk_size, self.len);
            self.index = end;

            let data = self.data.clone();

            let slice = unsafe {
                let ptr = Arc::as_ptr(&data) as *mut S;
                let slice: &mut [T] = ptr.as_mut().unwrap().as_mut();
                NonNull::new_unchecked(&mut slice[start..end] as *mut [T])
            };

            Some(Chunk { data, slice })
        }
    }

    unsafe impl<S, T> Send for ConcurrentChunks<S, T>
    where
        S: 'static + Send,
        T: 'static + Send,
    {
    }

    unsafe impl<S, T> Sync for ConcurrentChunks<S, T>
    where
        S: 'static + Send,
        T: 'static + Send,
    {
    }
}

// chunk

pub use chunk::*;

mod chunk {
    use super::*;

    /// A mutable sub-slice reference-counted reference to a slice-like data.
    #[derive(Debug)]
    pub struct Chunk<S, T> {
        pub(super) data: Arc<S>,
        pub(super) slice: NonNull<[T]>,
    }

    impl<S, T> Chunk<S, T> {
        /// Consumes all chunk instances and recover the referenced data.
        ///
        /// # Panics
        /// The method panics if any one of associate chunks is missing, or
        /// the chunks refer to inconsistent data.
        pub fn into_owner(chunks: impl IntoIterator<Item = Self>) -> S
        where
            S: AsMut<[T]>,
        {
            unsafe {
                let mut chunks = chunks.into_iter();

                // obtain inner pointer from the first chunk
                let first = chunks.next().expect("the chunks must be non-empty");
                let data = first.data.clone();

                // verify if all chunks points to the same owner
                let mut chunks: Vec<_> = iter::once(first)
                    .chain(chunks.inspect(|chunk| {
                        assert_eq!(
                            Arc::as_ptr(&chunk.data),
                            Arc::as_ptr(&data),
                            "inconsistent owner of the chunks"
                        );
                    }))
                    .collect();

                // make sure no extra reference counts
                assert_eq!(
                    Arc::strong_count(&data), chunks.len() + 1,
                    "the creating iterator of the chunks must be dropped before calling this method. try `drop(iterator)`"
                );

                // sort chunks by pointer address
                chunks.sort_by_cached_key(|chunk| chunk.slice.as_ptr());

                // verify the boundary addresses
                {
                    let ptr = Arc::as_ptr(&data) as *mut S;
                    let slice = ptr.as_mut().unwrap().as_mut();
                    let range = slice.as_ptr_range();
                    assert_eq!(
                        chunks.first().unwrap().slice.as_ref().as_ptr_range().start,
                        range.start,
                        "the first chunk is missing"
                    );
                    assert_eq!(
                        chunks.last().unwrap().slice.as_ref().as_ptr_range().end,
                        range.end,
                        "the last chunk is missing"
                    );
                }

                // verify if chunks are contiguous
                chunks
                    .iter()
                    .zip(chunks.iter().skip(1))
                    .for_each(|(prev, next)| {
                        let prev_end = prev.slice.as_ref().as_ptr_range().end;
                        let next_start = next.slice.as_ref().as_ptr_range().start;
                        assert!(prev_end == next_start, "the chunks are not contiguous");
                    });

                // free chunk references
                drop(chunks);

                // recover owner
                let data = Arc::try_unwrap(data).map_err(|_| ()).unwrap();

                data
            }
        }

        /// Concatenates contiguous chunks into one chunk.
        ///
        /// # Panics
        /// The method panics if the chunks are not contiguous, or
        /// the chunks refer to inconsistent data.
        pub fn cat(chunks: impl IntoIterator<Item = Self>) -> Self
        where
            S: AsMut<[T]>,
        {
            unsafe {
                let mut chunks = chunks.into_iter();

                // obtain inner pointer from the first chunk
                let first = chunks.next().expect("the chunks must be non-empty");
                let data = first.data.clone();

                let mut chunks: Vec<_> = iter::once(first)
                    .chain(chunks.inspect(|chunk| {
                        // verify if all chunks points to the same owner
                        assert_eq!(
                            Arc::as_ptr(&chunk.data),
                            Arc::as_ptr(&data),
                            "inconsistent owner of the chunks"
                        );
                    }))
                    .collect();

                // verify if chunks are contiguous
                chunks
                    .iter()
                    .zip(chunks.iter().skip(1))
                    .for_each(|(prev, next)| {
                        let prev_end = prev.slice.as_ref().as_ptr_range().end;
                        let next_start = next.slice.as_ref().as_ptr_range().start;
                        assert!(prev_end == next_start, "the chunks are not contiguous");
                    });

                // save slice range
                let len = chunks.iter().map(|chunk| chunk.slice.as_ref().len()).sum();
                let slice_ptr: *mut T = chunks.first_mut().unwrap().as_mut().as_mut_ptr();

                // free chunk references
                drop(chunks);

                // create returning chunk
                let slice = {
                    let slice = slice::from_raw_parts_mut(slice_ptr, len);
                    NonNull::new_unchecked(slice as *mut [T])
                };

                Chunk { data, slice }
            }
        }
    }

    unsafe impl<S, T> Send for Chunk<S, T> {}
    unsafe impl<S, T> Sync for Chunk<S, T> {}

    impl<S, T> AsRef<[T]> for Chunk<S, T> {
        fn as_ref(&self) -> &[T] {
            self.deref()
        }
    }

    impl<S, T> AsMut<[T]> for Chunk<S, T> {
        fn as_mut(&mut self) -> &mut [T] {
            self.deref_mut()
        }
    }

    impl<S, T> Deref for Chunk<S, T> {
        type Target = [T];

        fn deref(&self) -> &Self::Target {
            unsafe { self.slice.as_ref() }
        }
    }

    impl<S, T> DerefMut for Chunk<S, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { self.slice.as_mut() }
        }
    }

    impl<'a, S, T> IntoIterator for &'a Chunk<S, T> {
        type Item = &'a T;
        type IntoIter = slice::Iter<'a, T>;

        fn into_iter(self) -> Self::IntoIter {
            self.deref().into_iter()
        }
    }

    impl<'a, S, T> IntoIterator for &'a mut Chunk<S, T> {
        type Item = &'a mut T;
        type IntoIter = slice::IterMut<'a, T>;

        fn into_iter(self) -> Self::IntoIter {
            self.deref_mut().into_iter()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::izip;

    #[test]
    fn merge_chunks_test() {
        let orig: Vec<_> = (0..16).collect();

        let mut chunks = orig.concurrent_chunks_by_division(3);
        let chunk1 = chunks.next().unwrap();
        let chunk2 = chunks.next().unwrap();
        let chunk3 = chunks.next().unwrap();
        drop(chunks); // decrease ref count
        let new = Chunk::into_owner(vec![chunk3, chunk1, chunk2]);

        assert!(izip!(new, 0..16).all(|(lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn concat_chunks_test() {
        let orig: Vec<_> = (0..25).collect();

        let mut chunks = orig.concurrent_chunks_by_division(4);
        let chunk1 = chunks.next().unwrap();
        let chunk2 = chunks.next().unwrap();
        let chunk3 = chunks.next().unwrap();
        let chunk4 = chunks.next().unwrap();
        drop(chunks); // decrease ref count

        let chunk12 = Chunk::cat(vec![chunk1, chunk2]);
        assert!(izip!(&chunk12, 0..14).all(|(&lhs, rhs)| lhs == rhs));

        let chunk34 = Chunk::cat(vec![chunk3, chunk4]);
        assert!(izip!(&chunk34, 14..25).all(|(&lhs, rhs)| lhs == rhs));

        let chunk1234 = Chunk::cat(vec![chunk12, chunk34]);
        assert!(izip!(&chunk1234, 0..25).all(|(&lhs, rhs)| lhs == rhs));

        let new = Chunk::into_owner(vec![chunk1234]);
        assert!(izip!(&new, 0..25).all(|(&lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn concurrent_chunks_test() {
        let vec: Vec<_> = (0..16).collect();
        let chunks: Vec<_> = vec.concurrent_chunks_by_division(3).collect();
        assert_eq!(chunks.len(), 3);
        assert!(izip!(&chunks[0], 0..6).all(|(&lhs, rhs)| lhs == rhs));
        assert!(izip!(&chunks[1], 6..12).all(|(&lhs, rhs)| lhs == rhs));
        assert!(izip!(&chunks[2], 12..16).all(|(&lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn empty_concurrent_chunks_test() {
        assert_eq!([(); 0].concurrent_chunks(2).count(), 0);
        assert_eq!([(); 0].concurrent_chunks_by_division(None).count(), 0);
    }
}
