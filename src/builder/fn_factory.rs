pub type BoxFnFactory<In, Out> = Box<dyn FnMut(In) -> BoxFn<'static, Out> + Send>;
pub(crate) type BoxFn<'a, T> = Box<dyn FnOnce() -> T + Send + 'a>;

pub trait FnFactory<In, Out>
where
    Self::Fn: 'static + Send + FnOnce() -> Out,
    In: 'static + Send,
    Out: 'static + Send,
{
    type Fn;

    fn generate(&mut self, input: In) -> Self::Fn;

    fn boxed(self) -> BoxFnFactory<In, Out>
    where
        Self: 'static + Send;

    fn chain<GOut, G>(self, other: G) -> BoxFnFactory<In, GOut>
    where
        Self: 'static + Send + Sized,
        G: 'static + Send + Clone + FnFactory<Out, GOut>,
        GOut: 'static + Send,
        G::Fn: 'static + Send + FnOnce() -> GOut;
}

impl<F, In, Out, Func> FnFactory<In, Out> for F
where
    F: FnMut(In) -> Func,
    Func: 'static + Send + FnOnce() -> Out,
    In: 'static + Send,
    Out: 'static + Send,
{
    type Fn = Func;

    fn generate(&mut self, input: In) -> Self::Fn {
        self(input)
    }

    fn boxed(mut self) -> BoxFnFactory<In, Out>
    where
        Self: 'static + Send,
    {
        Box::new(move |input: In| -> BoxFn<'static, Out> { Box::new(self.generate(input)) })
    }

    fn chain<GOut, G>(mut self, other: G) -> BoxFnFactory<In, GOut>
    where
        Self: 'static + Send + Sized,
        G: 'static + Send + Clone + FnFactory<Out, GOut>,
        GOut: 'static + Send,
    {
        Box::new(move |input: In| -> BoxFn<'static, GOut> {
            let func1 = self.generate(input);
            let mut g = other.clone();

            Box::new(move || {
                let mid = func1();
                let func2 = g.generate(mid);
                func2()
            })
        })
    }
}
