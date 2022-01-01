use crate::common::*;
use std::any::Any;

static GLOBAL_RUNTIME: OnceCell<Box<dyn Runtime>> = OnceCell::new();

type BoxAny<'a> = Box<dyn 'a + Send + Any>;

pub unsafe trait Runtime
where
    Self: 'static + Sync + Send,
{
    fn block_on<'a>(&self, fut: BoxFuture<'a, BoxAny<'static>>) -> BoxAny<'static>;

    fn block_on_executor<'a>(&self, fut: BoxFuture<'a, BoxAny<'static>>) -> BoxAny<'static>;

    fn spawn(&self, fut: BoxFuture<'static, BoxAny<'static>>) -> Box<dyn SpawnHandle>;

    fn spawn_blocking(&self, f: Box<dyn FnOnce() -> BoxAny<'static> + Send>) -> Box<dyn SpawnHandle>;

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

fn get_global_runtime() -> &'static Box<dyn Runtime> {
    GLOBAL_RUNTIME
        .get()
        .expect("global runtime is not set, did you call set_global_runtime()?")
}

pub fn set_global_runtime<R>(runtime: R) -> Result<(), &'static str>
where
    R: Runtime,
{
    GLOBAL_RUNTIME
        .set(Box::new(runtime))
        .map_err(|_| "set_global_runtime() cannot be called more than once")
}

pub fn spawn<Fut>(fut: Fut) -> JoinHandle<Fut::Output>
where
    Fut: 'static + Future + Send,
    Fut::Output: 'static + Send,
{
    let future = async move {
        let output = get_global_runtime()
            .spawn(
                async move {
                    let output: BoxAny<'static> = Box::new(fut.await);
                    output
                }
                .boxed(),
            )
            .await;

        let output =
            BoxAny::<'static>::downcast::<Fut::Output>(output).expect("interal error: unable downcast Box");
        *output
    }
    .boxed();

    JoinHandle {
        future,
        _phantom: PhantomData,
    }
}

pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: 'static + Send + FnOnce() -> R,
    R: 'static + Send,
{
    let future = async move {
        let output = get_global_runtime()
            .spawn_blocking({
                let f: Box<dyn FnOnce() -> BoxAny<'static> + Send> = Box::new(move || {
                    let output: BoxAny<'static> = Box::new(f());
                    output
                });
                f
            })
            .await;

        let output = BoxAny::<'static>::downcast::<R>(output).expect("interal error: unable downcast Box");
        *output
    }
    .boxed();

    JoinHandle {
        future,
        _phantom: PhantomData,
    }
}

pub async fn sleep(dur: Duration) {
    get_global_runtime().sleep(dur).await;
}

pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: 'static + Send,
{
    let output = get_global_runtime().block_on(
        async move {
            let output: BoxAny<'static> = Box::new(future.await);
            output
        }
        .boxed(),
    );

    let output = BoxAny::<'static>::downcast::<F::Output>(output).expect("interal error: unable downcast Box");
    *output
}

pub fn block_on_executor<F>(future: F) -> F::Output
where
    F: 'static + Future + Send,
    F::Output: 'static + Send,
{
    let output = get_global_runtime().block_on(
        async {
            let output: BoxAny<'static> = Box::new(future.await);
            output
        }
        .boxed(),
    );

    let output = BoxAny::<'static>::downcast::<F::Output>(output).expect("interal error: unable downcast Box");
    *output
}

#[repr(transparent)]
#[pin_project]
pub struct JoinHandle<T> {
    _phantom: PhantomData<T>,
    #[pin]
    future: BoxFuture<'static, T>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll_unpin(cx)
    }
}
