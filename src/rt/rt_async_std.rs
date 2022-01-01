use crate::common::*;

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

pub struct JoinHandle<T>(async_std::task::JoinHandle<T>);

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
