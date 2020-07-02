use crate::common::*;

/// An extension trait for [Future](Future) that provides combinator functions for parallelism.
pub trait ParFuture {
    /// Spawn a task that polls the given future.
    fn spawned<T>(self) -> JoinHandle<T>
    where
        Self: 'static + Future<Output = T> + Send + Sized,
        T: 'static + Send,
    {
        async_std::task::spawn(self)
    }
}

impl<T, F> ParFuture for F where F: Future<Output = T> {}
