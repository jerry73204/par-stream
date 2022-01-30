use crate::common::*;

pub type BoxFutureFactory<'a, In, Out> = Box<dyn 'a + FnMut(In) -> BoxFuture<'static, Out> + Send>;

pub use future_factory_::*;
mod future_factory_ {
    use super::*;

    pub trait FutureFactory<In>
    where
        In: 'static + Send,
        Self::Fut: 'static + Send + Future,
        <Self::Fut as Future>::Output: 'static + Send,
    {
        type Fut;

        fn generate(&mut self, input: In) -> Self::Fut;

        fn boxed<'a>(mut self) -> BoxFutureFactory<'a, In, <Self::Fut as Future>::Output>
        where
            Self: 'a + Sized + Send,
        {
            Box::new(move |input: In| self.generate(input).boxed())
        }

        fn compose<G>(self, other: G) -> ComposeFutureFactory<In, Self, G>
        where
            Self: Sized,
            G: 'static + Send + Clone + FutureFactory<<Self::Fut as Future>::Output>,
            <G::Fut as Future>::Output: Send,
        {
            ComposeFutureFactory::new(self, other)
        }
    }

    impl<F, In, Fut> FutureFactory<In> for F
    where
        F: FnMut(In) -> Fut,
        In: 'static + Send,
        Fut: 'static + Send + Future,
        Fut::Output: 'static + Send,
    {
        type Fut = Fut;

        fn generate(&mut self, input: In) -> Self::Fut {
            self(input)
        }
    }
}

pub use compose_future_factory::*;
mod compose_future_factory {
    use super::*;

    pub struct ComposeFutureFactory<In, F, G>
    where
        F: FutureFactory<In>,
        G: FutureFactory<<F::Fut as Future>::Output>,
        In: 'static + Send,
        <F::Fut as Future>::Output: 'static + Send,
        <G::Fut as Future>::Output: 'static + Send,
        F::Fut: 'static + Send + Future,
        G::Fut: 'static + Send + Future,
    {
        f: F,
        g: G,
        _phantom: PhantomData<In>,
    }

    impl<In, F, G> ComposeFutureFactory<In, F, G>
    where
        F: FutureFactory<In>,
        G: FutureFactory<<F::Fut as Future>::Output>,
        In: 'static + Send,
        <F::Fut as Future>::Output: 'static + Send,
        <G::Fut as Future>::Output: 'static + Send,
        F::Fut: 'static + Send + Future,
        G::Fut: 'static + Send + Future,
    {
        pub fn new(f: F, g: G) -> Self {
            Self {
                f,
                g,
                _phantom: PhantomData,
            }
        }
    }

    impl<In, F, G> FutureFactory<In> for ComposeFutureFactory<In, F, G>
    where
        F: FutureFactory<In>,
        G: 'static + Send + Clone + FutureFactory<<F::Fut as Future>::Output>,
        In: 'static + Send,
        <F::Fut as Future>::Output: 'static + Send,
        <G::Fut as Future>::Output: 'static + Send,
        F::Fut: 'static + Send + Future,
        G::Fut: 'static + Send + Future,
    {
        type Fut = FutFacChain<F::Fut, G>;

        fn generate(&mut self, input: In) -> FutFacChain<F::Fut, G> {
            FutFacChain::new(self.f.generate(input), self.g.clone())
        }
    }

    impl<In, F, G> Clone for ComposeFutureFactory<In, F, G>
    where
        F: Clone + FutureFactory<In>,
        G: Send + Clone + FutureFactory<<F::Fut as Future>::Output>,
        In: 'static + Send,
        <F::Fut as Future>::Output: 'static + Send,
        <G::Fut as Future>::Output: 'static + Send,
        F::Fut: 'static + Send + Future,
        G::Fut: 'static + Send + Future,
    {
        fn clone(&self) -> Self {
            Self {
                f: self.f.clone(),
                g: self.g.clone(),
                _phantom: PhantomData,
            }
        }
    }
}

pub use fut_fac_chain::*;
mod fut_fac_chain {
    use super::*;

    #[pin_project]
    pub struct FutFacChain<Fut, Fac>
    where
        Fut::Output: 'static + Send,
        <Fac::Fut as Future>::Output: 'static + Send,
        Fut: Future,
        Fac::Fut: 'static + Send + Future,
        Fac: FutureFactory<Fut::Output>,
    {
        #[pin]
        ffut: Option<Fut>,
        #[pin]
        gfut: Option<Fac::Fut>,
        fac: Fac,
    }

    impl<Fut, Fac> FutFacChain<Fut, Fac>
    where
        Fut::Output: 'static + Send,
        <Fac::Fut as Future>::Output: 'static + Send,
        Fut: Future,
        Fac::Fut: 'static + Send + Future,
        Fac: FutureFactory<Fut::Output>,
    {
        pub fn new(fut: Fut, fac: Fac) -> Self {
            Self {
                ffut: Some(fut),
                gfut: None,
                fac,
            }
        }
    }

    impl<Fut, Fac> Future for FutFacChain<Fut, Fac>
    where
        Fut::Output: 'static + Send,
        <Fac::Fut as Future>::Output: 'static + Send,
        Fut: Future,
        Fac::Fut: 'static + Send + Future,
        Fac: FutureFactory<Fut::Output>,
    {
        type Output = <Fac::Fut as Future>::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let mut this = self.project();

            Ready(loop {
                if let Some(ffut) = this.ffut.as_mut().as_pin_mut() {
                    let fout = ready!(ffut.poll(cx));
                    this.ffut.set(None);
                    let gfut = this.fac.generate(fout);
                    this.gfut.set(Some(gfut))
                } else if let Some(gfut) = this.gfut.as_mut().as_pin_mut() {
                    let gout = ready!(gfut.poll(cx));
                    this.gfut.set(None);
                    break gout;
                } else {
                    unreachable!()
                }
            })
        }
    }
}
