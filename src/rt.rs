use crate::common::*;

#[cfg(feature = "runtime_tokio")]
pub use rt_tokio::*;

#[cfg(feature = "runtime_async-std")]
pub use rt_async_std::*;

#[cfg(feature = "runtime_tokio")]
mod rt_tokio {
    use super::*;

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

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T>(tokio::task::JoinHandle<T>);

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0)
                .poll(cx)
                .map(|result| result.map_err(|err| JoinError(err)))
        }
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinError(tokio::task::JoinError);
}

#[cfg(feature = "runtime_async-std")]
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

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinHandle<T>(async_std::task::JoinHandle<T>);

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0).poll(cx).map(|output| Ok(output))
        }
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct JoinError {
        _private: [u8; 0],
    }
}
