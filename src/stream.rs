//! futures-compatible parallel stream extension.

use crate::{base, common::*, config::IntoParStreamParams, impls};

/// Collect multiple streams into single stream.
///
/// ```rust
/// use futures::stream::StreamExt;
/// use par_stream::ParStreamExt;
/// use std::collections::HashSet;
///
/// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
/// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
/// async fn main() {
///     let outer = Box::new(2);
///
///     // scatter to two receivers
///     let (scatter_fut, rx1) = futures::stream::iter(0..1000).par_scatter(None);
///     let rx2 = rx1.clone();
///
///     // gather back from two receivers
///     let gather_fut = par_stream::par_gather(vec![rx1, rx2], None).collect::<HashSet<_>>();
///
///     // collect the items from respective workers
///     let ((), values) = futures::join!(scatter_fut, gather_fut);
///
///     // the gathered values have exactly the same size with the stream
///     assert_eq!(values, (0..1000).collect::<HashSet<_>>());
/// }
/// ```
pub fn par_gather<S>(
    streams: impl IntoIterator<Item = S>,
    buf_size: impl Into<Option<usize>>,
) -> ParGather<S::Item>
where
    S: 'static + StreamExt + Unpin + Send,
    S::Item: Send,
{
    impls::stream::par_gather(streams, buf_size)
}

/// An extension trait for [Stream](Stream) that provides parallel combinator functions.
pub trait ParStreamExt {
    /// Computes new items from the stream asynchronously in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_then(None, move |value| {
    ///             // cloned needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the future is sent to a parallel worker
    ///             async move { value * (*cloned_outer) }
    ///         })
    ///         // the collected values will be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    /// ```
    fn par_then<T, F, Fut>(self, config: impl IntoParStreamParams, f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_then(self, config, f)
    }

    /// Creates a parallel stream with in-local thread initializer.
    fn par_then_init<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        f: MapF,
    ) -> ParMap<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_then_init(self, config, init_f, f)
    }

    /// Computes new items from the stream asynchronously in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The order of output items is not guaranteed to respect the order of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::collections::HashSet;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_then_unordered(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the future is sent to a parallel worker
    ///             async move { value * (*cloned_outer) }
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<HashSet<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<HashSet<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    /// ```
    fn par_then_unordered<T, F, Fut>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_then_unordered(self, config, f)
    }

    /// Creates a stream analogous to [par_then_unordered](ParStreamExt::par_then_unordered) with
    /// in-local thread initializer.
    fn par_then_init_unordered<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_then_init_unordered(self, config, init_f, map_f)
    }

    /// Computes new items in a function in parallel with respect to the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a closure
    /// and the closure will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     // the variable will be shared by parallel workers
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_map(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the closure is sent to parallel worker
    ///             move || value * (*cloned_outer)
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    /// ```
    fn par_map<T, F, Func>(self, config: impl IntoParStreamParams, f: F) -> ParMap<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_map(self, config, f)
    }

    /// Creates a parallel stream analogous to [par_map](ParStreamExt::par_map) with
    /// in-local thread initializer.
    fn par_map_init<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        f: MapF,
    ) -> ParMap<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_map_init(self, config, init_f, f)
    }

    /// Computes new items in a function in parallel without respecting the input order.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    /// The method guarantees the order of output items obeys that of input items.
    ///
    /// Each parallel task runs in two-stage manner. The `f` closure is invoked in the
    /// main thread and lets you clone over outer varaibles. Then, `f` returns a future
    /// and the future will be sent to a parallel worker.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::collections::HashSet;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     // the variable will be shared by parallel workers
    ///     let outer = Box::new(2);
    ///
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_map_unordered(None, move |value| {
    ///             // clone needed variables in the main thread
    ///             let cloned_outer = outer.clone();
    ///
    ///             // the closure is sent to parallel worker
    ///             move || value * (*cloned_outer)
    ///         })
    ///         // the collected values may NOT be ordered
    ///         .collect::<HashSet<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2).collect::<HashSet<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    /// ```
    fn par_map_unordered<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_map_unordered(self, config, f)
    }

    /// Creates a parallel stream analogous to [par_map_unordered](ParStreamExt::par_map_unordered) with
    /// in-local thread initializer.
    fn par_map_init_unordered<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        f: MapF,
    ) -> ParMapUnordered<T>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> T + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_map_init_unordered(self, config, init_f, f)
    }

    /// Reduces the input items into single value in parallel.
    ///
    /// The `limit` is the number of parallel workers.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// The `buf_size` is the size of buffer that stores the temporary reduced values.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// Unlike [StreamExt::fold], the method does not combine the values sequentially.
    /// Instead, the parallel workers greedly take two values from the buffer, reduce to
    /// one value, and push back to the buffer.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     // the variable will be shared by parallel workers
    ///     let sum = futures::stream::iter(1..=1000)
    ///         // sum up the values in parallel
    ///         .par_reduce(None, None, move |lhs, rhs| {
    ///             // the closure is sent to parallel worker
    ///             async move { lhs + rhs }
    ///         })
    ///         .await;
    ///     assert_eq!(sum, (1 + 1000) * 1000 / 2);
    /// }
    /// ```
    fn par_reduce<F, Fut>(
        self,
        limit: impl Into<Option<usize>>,
        buf_size: impl Into<Option<usize>>,
        f: F,
    ) -> ParReduce<Self::Item>
    where
        F: 'static + FnMut(Self::Item, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = Self::Item> + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_reduce(self, limit, buf_size, f)
    }

    /// Distributes input items to specific workers and compute new items with respect to the input order.
    ///
    ///
    /// The `buf_size` is the size of input buffer before each mapping function.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// `routing_fn` assigns input items to specific indexes of mapping functions.
    /// `routing_fn` is executed on the calling thread.
    ///
    /// `map_fns` is a vector of mapping functions, each of which produces an asynchronous closure.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    /// use std::{future::Future, pin::Pin};
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     let map_fns: Vec<
    ///         Box<dyn FnMut(usize) -> Pin<Box<dyn Future<Output = usize> + Send>> + Send>,
    ///     > = vec![
    ///         // even number processor
    ///         Box::new(|even_value| Box::pin(async move { even_value / 2 })),
    ///         // odd number processor
    ///         Box::new(|odd_value| Box::pin(async move { odd_value * 2 + 1 })),
    ///     ];
    ///
    ///     let transformed = futures::stream::iter(0..1000)
    ///         // doubles the values in parallel up to maximum number of cores
    ///         .par_routing(
    ///             None,
    ///             move |value| {
    ///                 // distribute the value according to its parity
    ///                 if value % 2 == 0 {
    ///                     0
    ///                 } else {
    ///                     1
    ///                 }
    ///             },
    ///             map_fns,
    ///         )
    ///         // the collected values may NOT be ordered
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000)
    ///         .map(|value| {
    ///             if value % 2 == 0 {
    ///                 value / 2
    ///             } else {
    ///                 value * 2 + 1
    ///             }
    ///         })
    ///         .collect::<Vec<_>>();
    ///     assert_eq!(transformed, expect);
    /// }
    /// ```
    fn par_routing<F1, F2, Fut, T>(
        self,
        buf_size: impl Into<Option<usize>>,
        routing_fn: F1,
        map_fns: Vec<F2>,
    ) -> ParRouting<T>
    where
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
    {
        impls::stream::ParStreamExt::par_routing(self, buf_size, routing_fn, map_fns)
    }

    /// Distributes input items to specific workers and compute new items without respecting the input order.
    ///
    ///
    /// The `buf_size` is the size of input buffer before each mapping function.
    /// If it is `0` or `None`, it defaults the number of cores on system.
    ///
    /// `routing_fn` assigns input items to specific indexes of mapping functions.
    /// `routing_fn` is executed on the calling thread.
    ///
    /// `map_fns` is a vector of mapping functions, each of which produces an asynchronous closure.
    fn par_routing_unordered<F1, F2, Fut, T>(
        self,
        buf_size: impl Into<Option<usize>>,
        routing_fn: F1,
        map_fns: Vec<F2>,
    ) -> ParRoutingUnordered<T>
    where
        F1: 'static + FnMut(&Self::Item) -> usize + Send,
        F2: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = T> + Send,
        T: 'static + Send,
        Self: 'static + StreamExt + Sized + Unpin + Send,
        Self::Item: Send,
    {
        impls::stream::ParStreamExt::par_routing_unordered(self, buf_size, routing_fn, map_fns)
    }

    /// Gives the current iteration count that may overflow to zero as well as the next value.
    fn wrapping_enumerate<T>(self) -> WrappingEnumerate<T, Self>
    where
        Self: Stream<Item = T> + Sized + Unpin,
    {
        base::ParStreamExt::wrapping_enumerate(self)
    }

    /// Reorder the input items paired with a iteration count.
    ///
    /// The combinator asserts the input item has tuple type `(usize, T)`.
    /// It reorders the items according to the first value of input tuple.
    ///
    /// It is usually combined with [ParStreamExt::wrapping_enumerate], then
    /// applies a series of unordered parallel mapping, and finally reorders the values
    /// back by this method. It avoids reordering the values after each parallel mapping step.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     let doubled = futures::stream::iter(0..1000)
    ///         // add enumerated index that does not panic on overflow
    ///         .wrapping_enumerate()
    ///         // double the values in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value * 2) }
    ///         })
    ///         // add values by one in parallel
    ///         .par_then_unordered(None, move |(index, value)| {
    ///             // the closure is sent to parallel worker
    ///             async move { (index, value + 1) }
    ///         })
    ///         // reorder the values by enumerated index
    ///         .reorder_enumerated()
    ///         .collect::<Vec<_>>()
    ///         .await;
    ///     let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
    ///     assert_eq!(doubled, expect);
    /// }
    /// ```
    fn reorder_enumerated<T>(self) -> ReorderEnumerated<T, Self>
    where
        Self: Stream<Item = (usize, T)> + Unpin + Sized,
    {
        base::ParStreamExt::reorder_enumerated(self)
    }

    /// Splits the stream into a receiver and a future.
    ///
    /// The returned future scatters input items into the receiver and its clones,
    /// and should be manually awaited by user.
    ///
    /// The returned receiver can be cloned and distributed to resepctive workers.
    ///
    /// It lets user to write custom workers that receive items from the same stream.
    ///
    /// ```rust
    /// use futures::stream::StreamExt;
    /// use par_stream::ParStreamExt;
    ///
    /// # #[cfg_attr(feature = "runtime_async-std", async_std::main)]
    /// # #[cfg_attr(feature = "runtime_tokio", tokio::main)]
    /// async fn main() {
    ///     let outer = Box::new(2);
    ///
    ///     let (scatter_fut, rx1) = futures::stream::iter(0..1000).par_scatter(None);
    ///     let rx2 = rx1.clone();
    ///
    ///     // first parallel worker
    ///     let worker1 = async_std::task::spawn(async move {
    ///         let mut values = vec![];
    ///         while let Ok(value) = rx1.recv().await {
    ///             values.push(value);
    ///         }
    ///         values
    ///     });
    ///
    ///     // second parallel worker
    ///     let worker2 = async_std::task::spawn(async move {
    ///         let mut values = vec![];
    ///         while let Ok(value) = rx2.recv().await {
    ///             values.push(value);
    ///         }
    ///         values
    ///     });
    ///
    ///     // collect the items from respective workers
    ///     let ((), values1, values2) = futures::join!(scatter_fut, worker1, worker2);
    ///
    ///     // the union of collected values have exactly the same size with the stream
    ///     assert_eq!(values1.len() + values2.len(), 1000);
    /// }
    /// ```
    fn par_scatter(
        self,
        buf_size: impl Into<Option<usize>>,
    ) -> (
        Pin<Box<dyn Future<Output = ()>>>,
        async_std::channel::Receiver<Self::Item>,
    )
    where
        Self: 'static + StreamExt + Sized + Unpin,
    {
        impls::stream::ParStreamExt::par_scatter(self, buf_size)
    }

    /// Runs an asynchronous task on each element of an stream in parallel.
    fn par_for_each<F, Fut>(self, config: impl IntoParStreamParams, f: F) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
    {
        impls::stream::ParStreamExt::par_for_each(self, config, f)
    }

    /// Creates a parallel stream analogous to [par_for_each](ParStreamExt::par_for_each) with a
    /// in-local thread initializer.
    fn par_for_each_init<B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Fut + Send,
        Fut: 'static + Future<Output = ()> + Send,
    {
        impls::stream::ParStreamExt::par_for_each_init(self, config, init_f, map_f)
    }

    /// Runs an blocking task on each element of an stream in parallel.
    fn par_for_each_blocking<F, Func>(self, config: impl IntoParStreamParams, f: F) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        F: 'static + FnMut(Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> () + Send,
    {
        impls::stream::ParStreamExt::par_for_each_blocking(self, config, f)
    }

    /// Creates a parallel stream analogous to [par_for_each_blocking](ParStreamExt::par_for_each_blocking) with a
    /// in-local thread initializer.
    fn par_for_each_blocking_init<B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        f: MapF,
    ) -> ParForEach
    where
        Self: 'static + Stream + Unpin + Sized + Send,
        Self::Item: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Item) -> Func + Send,
        Func: 'static + FnOnce() -> () + Send,
    {
        impls::stream::ParStreamExt::par_for_each_blocking_init(self, config, init_f, f)
    }
}

impl<S> ParStreamExt for S where S: Stream {}

// par_map

pub use impls::stream::ParMap;

// par_map_unordered

pub use impls::stream::ParMapUnordered;

// par_reduce

pub use impls::stream::ParReduce;

// par_routing

pub use impls::stream::ParRouting;

// par_routing_unordered

pub use impls::stream::ParRoutingUnordered;

// par_gather

pub use impls::stream::ParGather;

// wrapping_enumerate

pub use base::WrappingEnumerate;

// reorder_enumerated

pub use base::ReorderEnumerated;

// for each

pub use impls::stream::ParForEach;
