//! Asynchronous runtime methods.

use crate::common::*;

#[cfg(not(any(
    all(
        feature = "runtime-async-std",
        not(feature = "runtime-tokio"),
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        feature = "runtime-tokio",
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        not(feature = "runtime-tokio"),
        feature = "runtime-smol"
    ),
)))]
compile_error!("one of 'runtime-async-std', 'runtime-tokio', 'runtime-smol' feature must be enabled for this crate");

#[cfg(not(any(
    all(
        feature = "runtime-async-std",
        not(feature = "runtime-tokio"),
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        feature = "runtime-tokio",
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        not(feature = "runtime-tokio"),
        feature = "runtime-smol"
    ),
)))]
pub use rt_dummy::*;

#[cfg(all(
    not(feature = "runtime-async-std"),
    feature = "runtime-tokio",
    not(feature = "runtime-smol")
))]
pub use rt_tokio::*;

#[cfg(all(
    feature = "runtime-async-std",
    not(feature = "runtime-tokio"),
    not(feature = "runtime-smol")
))]
pub use rt_async_std::*;

#[cfg(all(
    not(feature = "runtime-async-std"),
    not(feature = "runtime-tokio"),
    feature = "runtime-smol"
))]
pub use rt_smol::*;
#[cfg(not(any(
    all(
        feature = "runtime-async-std",
        not(feature = "runtime-tokio"),
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        feature = "runtime-tokio",
        not(feature = "runtime-smol")
    ),
    all(
        not(feature = "runtime-async-std"),
        not(feature = "runtime-tokio"),
        feature = "runtime-smol"
    ),
)))]
mod rt_dummy {
    use super::*;

    pub fn spawn<F>(_: F) -> JoinHandle<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        panic!();
    }

    pub fn spawn_blocking<F, R>(_: F) -> JoinHandle<R>
    where
        F: 'static + Send + FnOnce() -> R,
        R: 'static + Send,
    {
        panic!();
    }

    pub async fn sleep(duration: Duration) {
        panic!();
    }

    pub fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        panic!();
    }

    pub fn block_on_executor<F>(future: F) -> F::Output
    where
        F: Future,
    {
        panic!();
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T> {
        _phantom: PhantomData<T>,
    }

    impl<T> Future for JoinHandle<T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!();
        }
    }
}

#[cfg(all(
    not(feature = "runtime-async-std"),
    feature = "runtime-tokio",
    not(feature = "runtime-smol")
))]
mod rt_tokio {
    use super::*;
    use tokio::runtime::{Handle, Runtime};

    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        JoinHandle(tokio::spawn(future))
    }

    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: 'static + Send + FnOnce() -> R,
        R: 'static + Send,
    {
        JoinHandle(tokio::task::spawn_blocking(f))
    }

    pub async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    pub fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        Handle::current().block_on(future)
    }

    pub fn block_on_executor<F>(future: F) -> F::Output
    where
        F: Future,
    {
        Runtime::new().unwrap().block_on(future)
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T>(tokio::task::JoinHandle<T>);

    impl<T> Future for JoinHandle<T> {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0).poll(cx).map(|result| result.unwrap())
        }
    }
}

#[cfg(all(
    feature = "runtime-async-std",
    not(feature = "runtime-tokio"),
    not(feature = "runtime-smol")
))]
mod rt_async_std {
    use super::*;

    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        JoinHandle(async_std::task::spawn(future))
    }

    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: 'static + Send + FnOnce() -> R,
        R: 'static + Send,
    {
        JoinHandle(async_std::task::spawn_blocking(f))
    }

    pub async fn sleep(duration: Duration) {
        async_std::task::sleep(duration).await;
    }

    pub fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        async_std::task::block_on(future)
    }

    pub fn block_on_executor<F>(future: F) -> F::Output
    where
        F: Future,
    {
        async_std::task::block_on(future)
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T>(async_std::task::JoinHandle<T>);

    impl<T> Future for JoinHandle<T> {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0).poll(cx)
        }
    }
}

#[cfg(all(
    not(feature = "runtime-async-std"),
    not(feature = "runtime-tokio"),
    feature = "runtime-smol"
))]
mod rt_smol {
    use super::*;

    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: 'static + Future + Send,
        F::Output: 'static + Send,
    {
        JoinHandle::Task(smol::spawn(future))
    }

    pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: 'static + Send + FnOnce() -> R,
        R: 'static + Send,
    {
        JoinHandle::UnBlock(Box::pin(smol::unblock(f)))
    }

    pub async fn sleep(duration: Duration) {
        smol::Timer::after(duration).await;
    }

    #[derive(Derivative)]
    #[derivative(Debug)]
    pub enum JoinHandle<T> {
        Task(smol::Task<T>),
        UnBlock(#[derivative(Debug = "ignore")] Pin<Box<dyn Future<Output = T> + Send>>),
    }

    pub fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        smol::block_on(future)
    }

    pub fn block_on_executor<F>(future: F) -> F::Output
    where
        F: Future,
    {
        smol::block_on(future)
    }

    impl<T> Future for JoinHandle<T> {
        type Output = T;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match &mut *self {
                Self::Task(task) => Pin::new(task).poll(cx),
                Self::UnBlock(future) => Pin::new(future).poll(cx),
            }
        }
    }
}
