use crate::common::*;

/// An extension trait for [Future](Future) that provides combinator functions for parallelism.
///
/// It simplifies the writing of spawning a parallel task. The following code are equivalent.
///
/// ```rust
/// use futures::stream::StreamExt;
/// use par_stream::ParFuture;
///
/// #[async_std::main]
/// async fn main() {
///     // original
///     async_std::task::spawn(async move {
///         println!("a parallel task");
///     })
///     .await;
///
///     // using the method
///     async move {
///         println!("a parallel task");
///     }
///     .spawned()
///     .await;
/// }
/// ```
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
