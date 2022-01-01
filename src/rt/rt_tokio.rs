use crate::common::*;
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

pub struct JoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| result.unwrap())
    }
}
