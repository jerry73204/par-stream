use crate::common::*;

pub trait SliceExt<T>
where
    Self: AsRef<[T]>,
{
    /// Returns an iterator of exactly `num_divisions` chunks of the slice.
    ///
    /// If `num_divisions` is `None`, it defaults to the number of system processors.
    fn chunks_by_division(&self, num_divisions: impl Into<Option<usize>>) -> slice::Chunks<'_, T> {
        let num_divisions = num_divisions.into().unwrap_or_else(|| num_cpus::get());
        assert!(
            num_divisions > 0,
            "num_divisions must be positive, but get zero"
        );

        let slice = self.as_ref();
        let len = slice.len();
        let chunk_size = (len + num_divisions - 1) / num_divisions;
        slice.chunks(chunk_size)
    }
}

impl<S, T> SliceExt<T> for S where S: AsRef<[T]> {}

pub trait SliceMutExt<T>
where
    Self: AsMut<[T]>,
{
    /// Returns an iterator of exactly `num_divisions` mutable chunks of the slice.
    ///
    /// If `num_divisions` is `None`, it defaults to the number of system processors.
    fn chunks_by_division_mut(
        &mut self,
        num_divisions: impl Into<Option<usize>>,
    ) -> slice::ChunksMut<'_, T> {
        let num_divisions = num_divisions.into().unwrap_or_else(|| num_cpus::get());
        assert!(
            num_divisions > 0,
            "num_divisions must be positive, but get zero"
        );

        let slice = self.as_mut();
        let len = slice.len();
        let chunk_size = (len + num_divisions - 1) / num_divisions;
        slice.chunks_mut(chunk_size)
    }
}

impl<S, T> SliceMutExt<T> for S where S: AsMut<[T]> {}
