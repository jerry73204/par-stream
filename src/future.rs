use crate::common::*;

pub trait ParFuture {
    fn spawned<T>(self) -> JoinHandle<T>
    where
        Self: 'static + Future<Output = T> + Send + Sized,
        T: 'static + Send,
    {
        async_std::task::spawn(self)
    }
}

impl<T, F> ParFuture for F where F: Future<Output = T> {}
