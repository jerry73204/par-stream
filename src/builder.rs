// use crate::common::*;
// use crate::par_stream::ParStreamExt as _;
// use crate::rt;

// type BoxFactory<In, Out> = Box<dyn Factory<In, Out, BoxFuture<'static, Out>>>;

// pub trait Factory<In, Out, Fut>
// where
//     Self: 'static,
//     In: 'static + Send,
//     Out: 'static + Send,
//     Fut: 'static + Send,
// {
//     fn generate(&self, input: In) -> Fut;

//     fn boxed(self) -> BoxFactory<In, Out>;

//     fn chain<GOut, G, GFut>(self, other: G) -> BoxFactory<In, GOut>
//     where
//         Self: Sized + Send + Clone,
//         G: Send + Clone + Factory<Out, GOut, GFut>,
//         GOut: 'static + Send,
//         GFut: 'static + Send + Future<Output = GOut>;
// }

// impl<F, In, Out, Fut> Factory<In, Out, Fut> for F
// where
//     F: 'static + Fn(In) -> Fut,
//     Fut: 'static + Send + Future<Output = Out>,
//     In: 'static + Send,
//     Out: 'static + Send,
// {
//     fn generate(&self, input: In) -> Fut {
//         self(input)
//     }

//     fn boxed(self) -> BoxFactory<In, Out> {
//         Box::new(move |input: In| self.generate(input).boxed())
//     }

//     fn chain<GOut, G, GFut>(self, other: G) -> BoxFactory<In, GOut>
//     where
//         Self: Sized + Send + Clone,
//         G: Send + Clone + Factory<Out, GOut, GFut>,
//         GOut: 'static + Send,
//         GFut: 'static + Send + Future<Output = GOut>,
//     {
//         Box::new(move |input: In| {
//             let f = self.clone();
//             let g = other.clone();

//             async move {
//                 let fut1 = f.generate(input);
//                 let mid = fut1.await;
//                 let fut2 = g.generate(mid);
//                 let out = fut2.await;
//                 out
//             }
//             .boxed()
//         })
//     }
// }

// pub struct ParBuilder<St, In, Out, Fac, Fut>
// where
//     St: ?Sized,
// {
//     factory: Fac,
//     _phantom: PhantomData<(In, Out, Fut)>,
//     stream: St,
// }

// impl<St, Fac, In, Out, Fut> ParBuilder<St, In, Out, Fac, Fut>
// where
//     St: ?Sized + Stream<Item = In>,
//     Fac: Factory<In, Out, Fut>,
//     In: 'static + Send,
//     Out: 'static + Send,
//     Fut: 'static + Send + Future<Output = Out>,
// {
//     // pub fn into_stream(self) -> impl Stream<Item = Out>
//     // where
//     //     St: Sized,
//     //     Fac: FnMut(In) -> Fut,
//     // {
//     //     let Self {stream, factory, ..} = self;
//     //     stream.then(factory)
//     // }

//     // pub fn spawn_single(self, buf_size: usize) -> impl Stream<Item = Out>
//     // where
//     //     St: 'static + Sized + Send,
//     //     Fac: Send + FnMut(In) -> Fut,
//     // {
//     //     let Self {stream, factory, ..} = self;
//     //     stream.then(factory).spawned(buf_size)
//     // }

//     pub fn map<FOut, F>(
//         self,
//         f: F,
//     ) -> ParBuilder<
//         St,
//         In,
//         FOut,
//         impl Factory<In, FOut, BoxFuture<'static, FOut>>,
//         BoxFuture<'static, FOut>,
//     >
//     where
//         St: Sized,
//         F: 'static + Send + Clone + Fn(Out) -> FOut,
//         Fac: Send + Clone,
//         FOut: 'static + Send,
//     {
//         let Self {
//             factory, stream, ..
//         } = self;

//         let new_factory = move |input: In| {
//             let factory = factory.clone();
//             let f = f.clone();

//             async move {
//                 let fut = factory.generate(input);
//                 let out = fut.await;
//                 let fout = f(out);
//                 fout
//             }
//             .boxed()
//         };

//         ParBuilder {
//             factory: new_factory,
//             _phantom: PhantomData,
//             stream,
//         }
//     }

//     pub fn then<FOut, F, FFut>(
//         self,
//         f: F,
//     ) -> ParBuilder<
//         St,
//         In,
//         FOut,
//         impl Factory<In, FOut, BoxFuture<'static, FOut>>,
//         BoxFuture<'static, FOut>,
//     >
//     where
//         St: Sized,
//         F: 'static + Send + Clone + Fn(Out) -> FFut,
//         FFut: Send + Future<Output = FOut>,
//         Fac: Send + Clone,
//         FOut: 'static + Send,
//     {
//         let Self {
//             factory, stream, ..
//         } = self;

//         let new_factory = move |input: In| {
//             let factory = factory.clone();
//             let f = f.clone();

//             async move {
//                 let fut = factory.generate(input);
//                 let out = fut.await;
//                 let ffut = f(out);
//                 let fout = ffut.await;
//                 fout
//             }
//             .boxed()
//         };

//         ParBuilder {
//             factory: new_factory,
//             _phantom: PhantomData,
//             stream,
//         }
//     }

//     pub fn block<FOut, F, FFn>(
//         self,
//         f: F,
//     ) -> ParBuilder<
//         St,
//         In,
//         FOut,
//         impl Factory<In, FOut, BoxFuture<'static, FOut>>,
//         BoxFuture<'static, FOut>,
//     >
//     where
//         St: Sized,
//         F: 'static + Send + Clone + Fn(Out) -> FFn,
//         FFn: 'static + Send + FnOnce() -> FOut,
//         Fac: Send + Clone,
//         FOut: 'static + Send,
//     {
//         let Self {
//             factory, stream, ..
//         } = self;

//         let new_factory = move |input: In| {
//             let factory = factory.clone();
//             let f = f.clone();

//             async move {
//                 let fut = factory.generate(input);
//                 let out = fut.await;
//                 let ffn = f(out);
//                 let fout = rt::spawn_blocking(ffn).await;
//                 fout
//             }
//             .boxed()
//         };

//         ParBuilder {
//             factory: new_factory,
//             _phantom: PhantomData,
//             stream,
//         }
//     }
// }
