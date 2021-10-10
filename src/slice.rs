use crate::common::*;

/// The trait provides extensions for concurrent processing on slice-like types.
pub trait SliceExt<T> {
    /// Splits the slice-like data into two sub-slices, divided at specified index.
    ///
    /// # Panics
    /// The method panics if the index is out of bound.
    fn concurrent_split_at(self, index: usize) -> (Chunk<Self, T>, Chunk<Self, T>)
    where
        Self: 'static + AsMut<[T]> + Sized + Send,
        T: 'static + Send,
    {
        unsafe {
            let data = Arc::new(self);
            let ptr = Arc::as_ptr(&data) as *mut Self;
            let slice: &mut [T] = ptr.as_mut().unwrap().as_mut();
            let lslice = NonNull::new_unchecked(&mut slice[0..index] as *mut [T]);
            let rslice = NonNull::new_unchecked(&mut slice[index..] as *mut [T]);

            (
                Chunk {
                    data: data.clone(),
                    slice: lslice,
                },
                Chunk {
                    data,
                    slice: rslice,
                },
            )
        }
    }

    /// Returns an iterator of roughly fixed-sized chunks of the slice.
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
        assert!(
            len == 0 || chunk_size > 0,
            "chunk_size must be positive for non-empty slice"
        );

        ConcurrentChunks {
            index: 0,
            chunk_size,
            end: len,
            data: Arc::new(self),
            _phantom: PhantomData,
        }
    }

    /// Returns an iterator of roughly `division` roughly fixed-sized chunks of the slice.
    ///
    /// The chunk size is determined by `division`. The last chunk maybe shorter if
    /// there aren't enough elements. If `division` is `None`, it defaults to
    /// the number of system processors.
    ///
    /// # Panics
    /// The method panics if `division` is zero and slice length is not zero.
    fn concurrent_chunks_by_division(
        mut self,
        division: impl Into<Option<usize>>,
    ) -> ConcurrentChunks<Self, T>
    where
        Self: 'static + AsMut<[T]> + Sized + Send,
        T: 'static + Send,
    {
        let len = self.as_mut().len();
        let division = division.into().unwrap_or_else(|| num_cpus::get());

        let chunk_size = if len == 0 {
            0
        } else {
            assert!(
                division > 0,
                "division must be positive for non-empty slice, but get zero"
            );
            (len + division - 1) / division
        };

        ConcurrentChunks {
            index: 0,
            chunk_size,
            end: len,
            data: Arc::new(self),
            _phantom: PhantomData,
        }
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
        pub(super) end: usize,
        pub(super) data: Arc<S>,
        pub(super) _phantom: PhantomData<T>,
    }

    impl<S, T> ConcurrentChunks<S, T>
    where
        S: 'static + Send,
        T: 'static + Send,
    {
        /// Obtains the guard that is used to recover the owning data.
        pub fn guard(&self) -> ConcurrentChunksGuard<S> {
            ConcurrentChunksGuard {
                data: self.data.clone(),
            }
        }

        /// Gets the reference count on the owning data.
        pub fn ref_count(&self) -> usize {
            Arc::strong_count(&self.data)
        }
    }

    impl<S, T> Iterator for ConcurrentChunks<S, T>
    where
        S: 'static + AsMut<[T]> + Send,
        T: 'static + Send,
    {
        type Item = Chunk<S, T>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index >= self.end {
                return None;
            }

            let start = self.index;
            let end = cmp::min(start + self.chunk_size, self.end);
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

    /// The guard is used to recover the owning data from [ConcurrentChunks].
    #[derive(Debug)]
    pub struct ConcurrentChunksGuard<S> {
        pub(super) data: Arc<S>,
    }

    impl<S> ConcurrentChunksGuard<S>
    where
        S: Send,
    {
        /// Tries to recover the owning data.
        ///
        /// The method succeeds if the referencing chunk iterator and all chunks are dropped.
        /// Otherwise, it returns the guard intact.
        pub fn try_unwrap(self) -> Result<S, Self> {
            Arc::try_unwrap(self.data).map_err(|data| ConcurrentChunksGuard { data })
        }

        pub fn unwrap(self) -> S
        where
            S: Debug,
        {
            self.try_unwrap().unwrap()
        }

        /// Gets the reference count on the owning data.
        pub fn ref_count(&self) -> usize {
            Arc::strong_count(&self.data)
        }
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
        /// Splits the chunk into two sub-chunks, divided at specified index.
        ///
        /// # Panics
        /// The method panics if the index is out of bound.
        pub fn split_at(mut self, index: usize) -> (Chunk<S, T>, Chunk<S, T>)
        where
            S: AsMut<[T]>,
            T: 'static + Send,
        {
            unsafe {
                let data = self.data;
                let slice: &mut [T] = self.slice.as_mut();
                let lslice = NonNull::new_unchecked(&mut slice[0..index] as *mut [T]);
                let rslice = NonNull::new_unchecked(&mut slice[index..] as *mut [T]);

                (
                    Chunk {
                        data: data.clone(),
                        slice: lslice,
                    },
                    Chunk {
                        data,
                        slice: rslice,
                    },
                )
            }
        }

        /// Returns an iterator of roughly  fixed-sized chunks of the chunk.
        ///
        /// Each chunk has `chunk_size` elements, expect the last chunk maybe shorter
        /// if there aren't enough elements.
        ///
        /// The yielded chunks maintain a global reference count on owning data. Each chunk refers to
        /// a mutable and exclusive sub-slice, enabling concurrent processing on input data.
        ///
        /// # Panics
        /// The method panics if `chunk_size` is zero and slice length is not zero.
        pub fn chunks(self, chunk_size: usize) -> ConcurrentChunks<S, T>
        where
            S: 'static + AsMut<[T]> + Sized + Send,
            T: 'static + Send,
        {
            unsafe {
                let data = self.data;
                let data_ptr = Arc::as_ptr(&data) as *mut S;
                let data_slice = data_ptr.as_mut().unwrap().as_mut();

                let slice_len = self.slice.as_ref().len();
                let slice_ptr = self.slice.as_ref().as_ptr();
                let start = slice_ptr.offset_from(data_slice.as_ptr()) as usize;

                assert!(
                    slice_len == 0 || chunk_size > 0,
                    "chunk_size must be positive for non-empty slice"
                );

                ConcurrentChunks {
                    chunk_size,
                    index: start,
                    end: start + slice_len,
                    data,
                    _phantom: PhantomData,
                }
            }
        }

        /// Returns an iterator of roughly `division` roughly fixed-sized chunks of the chunk.
        ///
        /// The chunk size is determined by `division`. The last chunk maybe shorter if
        /// there aren't enough elements. If `division` is `None`, it defaults to
        /// the number of system processors.
        ///
        /// # Panics
        /// The method panics if `division` is zero and slice length is not zero.
        pub fn chunks_by_division(
            self,
            division: impl Into<Option<usize>>,
        ) -> ConcurrentChunks<S, T>
        where
            S: 'static + AsMut<[T]> + Sized + Send,
            T: 'static + Send,
        {
            unsafe {
                let division = division.into().unwrap_or_else(|| num_cpus::get());

                let data = self.data;
                let data_ptr = Arc::as_ptr(&data) as *mut S;
                let data_slice = data_ptr.as_mut().unwrap().as_mut();

                let slice_len = self.slice.as_ref().len();
                let slice_ptr = self.slice.as_ref().as_ptr();
                let start = slice_ptr.offset_from(data_slice.as_ptr()) as usize;

                let chunk_size = if slice_len == 0 {
                    0
                } else {
                    assert!(division > 0, "division must be positive, but get zero");
                    (slice_len + division - 1) / division
                };

                ConcurrentChunks {
                    index: start,
                    chunk_size,
                    end: start + slice_len,
                    data,
                    _phantom: PhantomData,
                }
            }
        }

        /// Returns the guard that is used to recover the owning data.
        pub fn guard(&self) -> ConcurrentChunksGuard<S> {
            ConcurrentChunksGuard {
                data: self.data.clone(),
            }
        }

        /// Gets the reference count on the owning data.
        pub fn ref_count(&self) -> usize {
            Arc::strong_count(&self.data)
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

        pub fn into_arc_ref(self) -> ArcRef<S, [T]> {
            unsafe {
                let Self { data, slice } = self;
                ArcRef::new(data).map(|_| slice.as_ref())
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
        let _ = chunks.next().unwrap();
        let _ = chunks.next().unwrap();
        let _ = chunks.next().unwrap();

        let guard = chunks.guard();
        drop(chunks);
        let new = guard.try_unwrap().unwrap();

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

        let guard = chunk1234.guard();
        drop(chunk1234);
        let new = guard.try_unwrap().unwrap();

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

    #[test]
    fn split_at_test() {
        let vec: Vec<_> = (0..16).collect();
        let (lslice, rslice) = vec.concurrent_split_at(5);
        assert!(izip!(&lslice, 0..5).all(|(&lhs, rhs)| lhs == rhs));
        assert!(izip!(&rslice, 5..16).all(|(&lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn chunks_of_chunk_test() {
        let owner: Vec<_> = (0..9).collect();
        let mut chunks = owner.concurrent_chunks_by_division(2);

        let chunk1 = chunks.next().unwrap();
        let chunk2 = chunks.next().unwrap();
        assert!(chunks.next().is_none());
        drop(chunks);

        let mut chunks = chunk1.chunks(3);
        let chunk3 = chunks.next().unwrap();
        let chunk4 = chunks.next().unwrap();
        assert!(chunks.next().is_none());
        assert_eq!(&*chunk3, &[0, 1, 2]);
        assert_eq!(&*chunk4, &[3, 4]);
        drop(chunks);

        let mut chunks = chunk2.chunks_by_division(3);
        let chunk5 = chunks.next().unwrap();
        let chunk6 = chunks.next().unwrap();
        assert!(chunks.next().is_none());
        assert_eq!(&*chunk5, &[5, 6]);
        assert_eq!(&*chunk6, &[7, 8]);

        let chunk7 = Chunk::cat(vec![chunk4, chunk5]);
        assert_eq!(&*chunk7, &[3, 4, 5, 6]);

        let (chunk8, chunk9) = chunk7.split_at(1);
        assert_eq!(&*chunk8, &[3]);
        assert_eq!(&*chunk9, &[4, 5, 6]);

        // if the ref count is correct, the data should be recovered
        let guard = chunk6.guard();

        drop(chunks);
        drop(chunk3);
        drop(chunk6);
        drop(chunk8);
        drop(chunk9);

        let owner = guard.try_unwrap().unwrap();
        assert_eq!(owner, (0..9).collect::<Vec<_>>());
    }
}
