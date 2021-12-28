use crate::common::*;

pub type BoxFnFactory<In, Out> = Box<dyn FnMut(In) -> Box<dyn FnOnce() -> Out + Send> + Send>;

pub trait FnFactory<In, Out, Func>
where
    Self: 'static + Send + FnMut(In) -> Func,
    Func: 'static + Send + FnOnce() -> Out,
    In: 'static + Send,
    Out: 'static + Send,
{
    fn generate(&mut self, input: In) -> Func;

    fn boxed(self) -> BoxFnFactory<In, Out>;

    fn chain<GOut, G, GFunc>(self, other: G) -> BoxFnFactory<In, GOut>
    where
        Self: Sized + Send,
        G: Send + Clone + FnFactory<Out, GOut, GFunc>,
        GOut: 'static + Send,
        GFunc: 'static + Send + FnOnce() -> GOut;
}

impl<F, In, Out, Func> FnFactory<In, Out, Func> for F
where
    F: 'static + Send + FnMut(In) -> Func,
    Func: 'static + Send + FnOnce() -> Out,
    In: 'static + Send,
    Out: 'static + Send,
{
    fn generate(&mut self, input: In) -> Func {
        self(input)
    }

    fn boxed(mut self) -> BoxFnFactory<In, Out> {
        Box::new(move |input: In| -> Box<dyn FnOnce() -> Out + Send> {
            Box::new(self.generate(input))
        })
    }

    fn chain<GOut, G, GFunc>(mut self, other: G) -> BoxFnFactory<In, GOut>
    where
        Self: Sized,
        G: Clone + FnFactory<Out, GOut, GFunc>,
        GOut: 'static + Send,
        GFunc: 'static + Send + FnOnce() -> GOut,
    {
        Box::new(move |input: In| -> Box<dyn FnOnce() -> GOut + Send> {
            let func1 = self.generate(input);
            let mut g = other.clone();

            Box::new(move || {
                let mid = func1();
                let func2 = g.generate(mid);
                let out = func2();
                out
            })
        })
    }
}
