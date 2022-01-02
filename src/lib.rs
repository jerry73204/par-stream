//! Parallel processing libray for asynchronous streams.
//!
//! # Runtime Configuration
//!
//! The following cargo features select the backend runtime for parallel workers.
//! At most one of them can be specified, otherwise the crate raises a compile error.
//!
//! - `runtime-tokio` enables the [tokio] multi-threaded runtime.
//! - `runtime-async-std` enables the [async-std](async_std) default runtime.
//!
//! Please read [Using Custom Runtime](#using-custom-runtime) if you would like to provide a custom runtime.
//!
//! # Extension Traits
//!
//! Extension traits extends existing [Stream](futures::Stream) with extra combinators to existing streams.
//! They can be imported from [prelude] for convenience.
//!
//! ```rust
//! use par_stream::prelude::*;
//! ```
//!
//! Stream combinators are provided by distinct traits according to the capability of the stream.
//!
//!
//! ### Traits for non-parallel stream item manipulation
//!
//! - [StreamExt](crate::StreamExt) requires `Self: Stream`
//! - [TryStreamExt](crate::TryStreamExt) requires `Self: TryStream`
//!
//! ### Traits for stream element ordering
//!
//! - [IndexStreamExt](crate::IndexStreamExt) requires `Self: Stream<Item = (usize, T)>`
//! - [TryIndexStreamExt](crate::TryIndexStreamExt) requires `Stream<Item = Result<(usize, T), E>>`
//!
//! ### Traits for parallel processing
//!
//! - [ParStreamExt](crate::ParStreamExt) requires
//!   - `Self: 'static + Send + Stream` and
//!   - `Self::Item: 'static + Send`
//! - [TryParStreamExt](crate::TryParStreamExt) requires
//!   - `Self: 'static + Send + Stream<Item = Result<T, E>>`,
//!   - `T: 'static + Send` and
//!   - `E: 'static + Send`
//!
//! # Parallel Processing
//!
//! These combinators run parallel tasks on the stream, either in ordered/unordered and fallible or not manner.
//! - [`par_map()`](ParStreamExt::par_map) runs parallel blocking task respecting input order.
//! - [`par_then()`](ParStreamExt::par_then) runs parallel asynchronous task respecting input order.
//! - [`par_map_unordered()`](ParStreamExt::par_map_unordered) runs parallel blocking task without respecting input order.
//! - [`par_then_unordered()`](ParStreamExt::par_then_unordered) runs parallel asynchronous task without respecting input order.
//! - [`try_par_map()`](TryParStreamExt::try_par_map), [`try_par_then()`](TryParStreamExt::try_par_then),
//!   [`try_par_then_unordered()`](TryParStreamExt::try_par_then_unordered) are the fallible variances.
//!
//! Chaining the combinators above establishes a parallel processing dataflow.
//!
//! ```
//! # par_stream::rt::block_on_executor(async move {
//! use futures::stream::{self, StreamExt as _};
//! use par_stream::{IndexStreamExt as _, ParStreamExt as _};
//!
//! let vec: Vec<_> = stream::iter(0i64..1000)
//!     // a series of unordered parallel tasks
//!     .par_then(None, |val| async move { val.pow(2) })
//!     .par_then(None, |val| async move { val * 2 })
//!     .par_then(None, |val| async move { val + 1 })
//!     .collect()
//!     .await;
//!
//! itertools::assert_equal(vec, (0i64..1000).map(|val| val.pow(2) * 2 + 1));
//! # })
//! ```
//!
//! # Unordered Parallel Processing
//!
//! The crate provides item reordering combinators.
//!
//! - [`reorder_enumerated()`](IndexStreamExt::reorder_enumerated) reorders the items `(index, value)`
//!   according to the index number.
//! - [`try_reorder_enumerated()`](TryIndexStreamExt::try_reorder_enumerated) is the fallible coutnerpart.
//!
//! They can be combined with either
//! [enumerate()](futures::StreamExt::enumerate) from [futures] crate or the fallible counterpart
//! [try_enumerate()](TryStreamExt::try_enumerate) from this crate
//! to establish an unordered data processing flow.
//!
//! ```
//! # par_stream::rt::block_on_executor(async move {
//! use futures::stream::{self, StreamExt as _};
//! use par_stream::{IndexStreamExt as _, ParStreamExt as _};
//!
//! let vec: Vec<_> = stream::iter(0i64..1000)
//!     // add index number to each item
//!     .enumerate()
//!     // a series of unordered parallel tasks
//!     .par_then_unordered(None, |(index, val)| async move { (index, val.pow(2)) })
//!     .par_then_unordered(None, |(index, val)| async move { (index, val * 2) })
//!     .par_then_unordered(None, |(index, val)| async move { (index, val + 1) })
//!     // reorder the items back by index number
//!     .reorder_enumerated()
//!     .collect()
//!     .await;
//!
//! itertools::assert_equal(vec, (0i64..1000).map(|val| val.pow(2) * 2 + 1));
//! # })
//! ```
//!
//! # Anycast Pattern
//!
//! - [`shared()`](StreamExt::shared) creates stream handles that can be sent to multiple receivers.
//!   Polling the handle will poll the underlying stream in lock-free manner. By consuming the handle,
//!   the receiver takes a portion of stream items.
//! - [`spawned()`](ParStreamExt::spawned) spawns an active worker to forward stream items to a channel.
//!   The channel can be cloned and be sent to multiple receivers, so that each receiver takes a portion of stream items.
//!
//! Both `shared()` and `spawned()` splits the ownership of the stream into multiple receivers. They differ in
//! performance considerations. The `spawned()` methods spawns an active worker and allocates an extra buffer while
//! `shared()` does not. In most cases, their performance are comparable.
//!
//! The combinators can work with [`select()`](futures::stream::select) to construct a scatter-gather dataflow.
//!
//! ```rust
//! # par_stream::rt::block_on_executor(async move {
//! use futures::stream::{self, StreamExt as _};
//! use par_stream::{ParStreamExt as _, StreamExt as _};
//! use std::collections::HashSet;
//!
//! let stream = futures::stream::iter(0..1000);
//!
//! // scatter stream items to two receivers
//! let share1 = stream.shared(); // or stream.scatter(buf_size)
//! let share2 = share1.clone();
//!
//! // process elements in separate parallel workers
//! let receiver1 = share1.map(|val| val * 2).spawned(None);
//! let receiver2 = share2.map(|val| val * 2).spawned(None);
//!
//! // gather values back from receivers
//! let mut vec: Vec<_> = stream::select(receiver1, receiver2).collect().await;
//!
//! // verify output values
//! vec.sort();
//! itertools::assert_equal(vec, (0..2000).step_by(2));
//! # })
//! ```
//!
//! # Broadcast Pattern
//!
//! - [`broadcast()`](ParStreamExt::broadcast) broadcasts copies of stream items to receivers.
//!   Receivers are registered before starting taking items and are guaranteed to start from the first item.
//! - [`tee()`](ParStreamExt::tee) is similar to `broadcast()`, but can register new receiver after starting taking items.
//!   Receivers are not guaranteed to start from the first item.
//!
//! The `broadcast()` can work with [zip()](futures::StreamExt::zip) to construct a broadcast-join dataflow.
//!
//! ```rust
//! # par_stream::rt::block_on_executor(async move {
//! use futures::prelude::*;
//! use par_stream::prelude::*;
//!
//! let data = vec![2, -1, 3, 5];
//! let stream = futures::stream::iter(data.clone());
//!
//! // broadcast the stream into three receivers
//! let mut builder = stream.broadcast(None, true);
//! let rx1 = builder.register();
//! let rx2 = builder.register();
//! let rx3 = builder.register();
//! builder.build(); // finish the builder to start consuming items
//!
//! // spawn a parallel processor for each receiver
//! let stream1 = rx1.map(|v| v * 2).spawned(None);
//! let stream2 = rx2.map(|v| v * 3).spawned(None);
//! let stream3 = rx3.map(|v| v * 5).spawned(None);
//!
//! // collect output values
//! let vec: Vec<_> = stream1
//!     .zip(stream2)
//!     .zip(stream3)
//!     .map(|((v1, v2), v3)| (v1, v2, v3))
//!     .collect()
//!     .await;
//!
//! // verify output values
//! assert_eq!(vec, [(4, 6, 10), (-2, -3, -5), (6, 9, 15), (10, 15, 25)]);
//! # })
//! ```
//!
//! # Parallel Data Generation
//!
//! The following combniators spawn parallel workers, each producing items individually.
//!
//! - [`par_unfold`](par_unfold) produces values from a future.
//! - [`par_unfold_blocking`](par_unfold_blocking) produces values from a blocking function.
//! - [`try_par_unfold`](try_par_unfold) and [`try_par_unfold_blocking`](try_par_unfold_blocking) are fallible counterparts.
//!
//! # Parameters
//!
//! Combinators may require extra parameters to configure the number of workers and buffer size.
//!
//! - `N: Into<NumWorkers>` for `par_for_each<N, F>(n: N, f: F)`
//! - `B: Into<BufSize>` for `scatter<B>(b: B)`
//! - `P: Into<ParParams>` for `par_then<P, F>(p: P, f: F)`
//!
//! [`N: Into<NumWorkers>`](NumWorkers) accepts the following values.
//! - `None`: default value, it sets to the number of logical system processors.
//! - `8` (integer): fixed number of workers.
//! - `2.0` (floating number): sets to the scaling of the number of logical system processors.
//!
//! [`B: Into<BufSize>`](BufSize) accepts the following values.
//! - `None`: default value, it sets to the double of logical system processors.
//! - `8` (integer): fixed buffer size.
//! - `2.0` (floating number): sets to the scaling of the number of logical system processors.
//!
//! [`P: Into<ParParms>`](ParParams) is combination of worker size and buffer size. It accepts the following values.
//! - `None`: default value, it sets to default values of worker size and buffer size.
//! - `8` (integer): fixed worker size, and buffer size is contant multiple of worker size.
//! - `2.0` (floating number): sets the worker size to the scaling of logical system processors, and buffer size is contant multiple of worker size.
//! - [`ParParamsConfig`](ParParamsConfig): manual configuration.
//!
//! # Utility Combinators
//!
//! The crate provides several utility stream combinators that coule make your life easier :).
//!
//! - [`with_state`](StreamExt::with_state) binds a stream with a state value.
//! - [`wait_until`](StreamExt::wait_until) lets a stream to wait until a future resolves.
//! - [`reduce`](StreamExt::reduce) reduces the stream items into a single value.
//! - [`batching`](StreamExt::batching) consumes arbitrary number of input items for each output item.
//! - [`stateful_then`](StreamExt::stateful_then), [`stateful_map`](StreamExt::stateful_map), [`stateful_batching`](StreamExt::stateful_batching) are stateful counterparts.
//! - [`take_until_error`](TryStreamExt::take_until_error) causes the stream to stop taking values after an error.
//! - [`catch_error`](TryStreamExt::catch_error) splits a stream of results into a stream of unwrapped value and a future that may resolve to an error.
//!
//! # Using Custom Runtime
//!
//! To provide custom runtime implementation, declare a type that implements [Runtime](crate::rt::Runtime).
//! Then, create an instance for that type and pass to [set_global_runtime()](crate::rt::set_global_runtime).
//! The global runtime can be set at most once, and is effective only when no runtime Cargo features are enabled.
//! Otherwise [set_global_runtime()](crate::rt::set_global_runtime) returns an error.
//!
//! ```
//! use futures::future::BoxFuture;
//! use par_stream::rt::{Runtime, SleepHandle, SpawnHandle};
//! use std::{any::Any, time::Duration};
//!
//! pub struct MyRuntime {/* omit */}
//!
//! impl MyRuntime {
//!     pub fn new() -> Self {
//!         Self { /* omit */ }
//!     }
//! }
//!
//! unsafe impl Runtime for MyRuntime {
//!     fn block_on<'a>(
//!         &self,
//!         fut: BoxFuture<'a, Box<dyn Send + Any + 'static>>,
//!     ) -> Box<dyn Send + Any + 'static> {
//!         todo!()
//!     }
//!
//!     fn block_on_executor<'a>(
//!         &self,
//!         fut: BoxFuture<'a, Box<dyn Send + Any + 'static>>,
//!     ) -> Box<dyn Send + Any + 'static> {
//!         todo!()
//!     }
//!
//!     fn spawn(
//!         &self,
//!         fut: BoxFuture<'static, Box<dyn Send + Any + 'static>>,
//!     ) -> Box<dyn SpawnHandle> {
//!         todo!()
//!     }
//!
//!     fn spawn_blocking(
//!         &self,
//!         f: Box<dyn FnOnce() -> Box<dyn Send + Any + 'static> + Send>,
//!     ) -> Box<dyn SpawnHandle> {
//!         todo!()
//!     }
//!
//!     fn sleep(&self, dur: Duration) -> Box<dyn SleepHandle> {
//!         todo!()
//!     }
//! }
//!
//! par_stream::rt::set_global_runtime(MyRuntime::new()).unwrap();
//! ```

mod broadcast;
pub mod builder;
mod common;
mod config;
mod functions;
mod index_stream;
mod par_stream;
mod pull;
pub mod rt;
mod shared_stream;
pub mod state_stream;
mod stream;
mod tee;
mod try_index_stream;
mod try_par_stream;
mod try_stream;
mod utils;

pub use crate::par_stream::*;
pub use broadcast::*;
pub use config::*;
pub use functions::*;
pub use index_stream::*;
pub use pull::*;
pub use shared_stream::*;
pub use stream::*;
pub use tee::*;
pub use try_index_stream::*;
pub use try_par_stream::*;
pub use try_stream::*;

crate::utils::has_tokio! {
    pub use tokio;
}

crate::utils::has_async_std! {
    pub use async_std;
}

/// Commonly used traits.
pub mod prelude {

    pub use super::{
        index_stream::IndexStreamExt, stream::StreamExt, try_index_stream::TryIndexStreamExt,
        try_stream::TryStreamExt,
    };

    pub use super::{par_stream::ParStreamExt, try_par_stream::TryParStreamExt};
}
