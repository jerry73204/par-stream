use crate::common::*;
use std::any::Any;

static GLOBAL_RUNTIME: OnceCell<Box<dyn Runtime>> = OnceCell::new();

pub(crate) type BoxAny<'a> = Box<dyn 'a + Send + Any>;

pub unsafe trait Runtime
where
    Self: 'static + Sync + Send,
{
    fn block_on<'a>(&self, fut: BoxFuture<'a, BoxAny<'static>>) -> BoxAny<'static>;

    fn block_on_executor<'a>(&self, fut: BoxFuture<'a, BoxAny<'static>>) -> BoxAny<'static>;

    fn spawn(&self, fut: BoxFuture<'static, BoxAny<'static>>) -> Box<dyn SpawnHandle>;

    fn spawn_blocking(
        &self,
        f: Box<dyn FnOnce() -> BoxAny<'static> + Send>,
    ) -> Box<dyn SpawnHandle>;

    fn sleep(&self, dur: Duration) -> Box<dyn SleepHandle>;
}

pub unsafe trait SpawnHandle
where
    Self: Send + Future<Output = BoxAny<'static>> + Unpin,
{
}

pub unsafe trait SleepHandle
where
    Self: Send + Future<Output = ()> + Unpin,
{
}

#[allow(dead_code)]
pub(crate) fn get_global_runtime() -> &'static Box<dyn Runtime> {
    GLOBAL_RUNTIME
        .get()
        .expect("global runtime is not set, did you call set_global_runtime()?")
}

/// Sets the global runtime from a [Runtime] object.
///
/// It is effective only when none of `runtime-*` feature is enabled.
pub fn set_global_runtime<R>(runtime: R) -> Result<(), &'static str>
where
    R: Runtime,
{
    GLOBAL_RUNTIME
        .set(Box::new(runtime))
        .map_err(|_| "set_global_runtime() cannot be called more than once")
}
