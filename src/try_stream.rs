use crate::{base, common::*, config::IntoParStreamParams, impls};

/// An extension trait for [TryStream](TryStream) that provides parallel combinator functions.
pub trait TryParStreamExt {
    /// Fallible parallel stream.
    fn try_par_then<T, F, Fut>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_then(self, config, f)
    }

    /// Fallible parallel stream with in-local thread initializer.
    fn try_par_then_init<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_then_init(self, config, init_f, map_f)
    }

    /// Fallible parallel stream that does not respect the ordering of input items.
    fn try_par_then_unordered<T, F, Fut>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_then_unordered(self, config, f)
    }

    /// An parallel stream analogous to [try_par_then_unordered](TryParStreamExt::try_par_then_unordered) with
    /// in-local thread initializer
    fn try_par_then_init_unordered<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_then_init_unordered(self, config, init_f, map_f)
    }

    /// Fallible parallel stream that runs blocking workers.
    fn try_par_map<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_map(self, config, f)
    }

    /// Fallible parallel stream that runs blocking workers with in-local thread initializer.
    fn try_par_map_init<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_map_init(self, config, init_f, map_f)
    }

    /// A parallel stream that analogous to [try_par_map](TryParStreamExt::try_par_map) without respecting
    /// the order of input items.
    fn try_par_map_unordered<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_map_unordered(self, config, f)
    }

    /// A parallel stream that analogous to [try_par_map_unordered](TryParStreamExt::try_par_map_unordered) with
    /// in-local thread initializer.
    fn try_par_map_init_unordered<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_map_init_unordered(self, config, init_f, map_f)
    }

    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_wrapping_enumerate<T, E>(self) -> TryWrappingEnumerate<T, E, Self>
    where
        Self: Stream<Item = Result<T, E>> + Sized + Unpin + Send,
    {
        base::TryParStreamExt::try_wrapping_enumerate(self)
    }

    /// Creates a fallible stream that reorders the items according to the iteration count.
    ///
    /// It is usually combined with [try_wrapping_enumerate](TryParStreamExt::try_wrapping_enumerate).
    fn try_reorder_enumerated<T, E>(self) -> TryReorderEnumerated<T, E, Self>
    where
        Self: Stream<Item = Result<(usize, T), E>> + Sized + Unpin + Send,
    {
        base::TryParStreamExt::try_reorder_enumerated(self)
    }

    /// Runs this stream to completion, executing asynchronous closure for each element on the stream
    /// in parallel.
    fn try_par_for_each<F, Fut>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParForEach<Self::Error>
    where
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        impls::try_stream::TryParStreamExt::try_par_for_each(self, config, f)
    }

    /// Runs an fallible blocking task on each element of an stream in parallel.
    fn try_par_for_each_init<B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), Self::Error>> + Send,
    {
        impls::try_stream::TryParStreamExt::try_par_for_each_init(self, config, init_f, map_f)
    }

    fn try_par_for_each_blocking<F, Func>(
        self,
        config: impl IntoParStreamParams,
        f: F,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), Self::Error> + Send,
    {
        impls::try_stream::TryParStreamExt::try_par_for_each_blocking(self, config, f)
    }

    /// Creates a fallible parallel stream analogous to [try_par_for_each_blocking](TryParStreamExt::try_par_for_each_blocking)
    /// with a in-local thread initializer.
    fn try_par_for_each_blocking_init<B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        init_f: InitF,
        f: MapF,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), Self::Error> + Send,
    {
        impls::try_stream::TryParStreamExt::try_par_for_each_blocking_init(self, config, init_f, f)
    }
}

impl<S> TryParStreamExt for S where S: TryStream {}

// try_par_then

pub use impls::try_stream::TryParMap;

// try_par_then_unordered

pub use impls::try_stream::TryParMapUnordered;

// try_par_for_each

pub use impls::try_stream::TryParForEach;

// try_wrapping_enumerate

pub use base::TryWrappingEnumerate;

// try_reorder_enumerated

pub use base::TryReorderEnumerated;
