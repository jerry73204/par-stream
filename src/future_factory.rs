use crate::common::*;

pub type BoxFutureFactory<In, Out> = Box<dyn FnMut(In) -> BoxFuture<'static, Out> + Send>;

pub trait FutureFactory<In, Out, Fut>
where
    Self: 'static + Send + FnMut(In) -> Fut,
    Fut: 'static + Send + Future<Output = Out>,
    In: 'static + Send,
    Out: 'static + Send,
{
    fn generate(&mut self, input: In) -> Fut;

    fn boxed(self) -> BoxFutureFactory<In, Out>;

    fn chain<GOut, G, GFut>(self, other: G) -> BoxFutureFactory<In, GOut>
    where
        Self: Sized + Send,
        G: Send + Clone + FutureFactory<Out, GOut, GFut>,
        GOut: 'static + Send,
        GFut: 'static + Send + Future<Output = GOut>;
}

impl<F, In, Out, Fut> FutureFactory<In, Out, Fut> for F
where
    F: 'static + Send + FnMut(In) -> Fut,
    Fut: 'static + Send + Future<Output = Out>,
    In: 'static + Send,
    Out: 'static + Send,
{
    fn generate(&mut self, input: In) -> Fut {
        self(input)
    }

    fn boxed(mut self) -> BoxFutureFactory<In, Out> {
        Box::new(move |input: In| self.generate(input).boxed())
    }

    fn chain<GOut, G, GFut>(mut self, other: G) -> BoxFutureFactory<In, GOut>
    where
        Self: Sized,
        G: Clone + FutureFactory<Out, GOut, GFut>,
        GOut: 'static + Send,
        GFut: 'static + Send + Future<Output = GOut>,
    {
        Box::new(move |input: In| {
            let fut1 = self.generate(input);
            let mut g = other.clone();

            async move {
                let mid = fut1.await;
                let fut2 = g.generate(mid);
                let out = fut2.await;
                out
            }
            .boxed()
        })
    }
}
