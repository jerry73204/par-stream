//! Asynchronous parallel streams analogous to [rayon](https://github.com/rayon-rs/rayon).
//!
//! # Cargo Features
//!
//! The following cargo features select the backend runtime for concurrent workers.
//! One of them must be specified, otherwise the crate raises a compile error.
//!
//! - `runtime-tokio` enables the [tokio] multi-threaded runtime.
//! - `runtime-async-std` enables the [async-std](async_std) default runtime.
//! - `runtime-smol` enables the [smol] default runtime.
//!
//! # Combinators
//!
//! ## Usage
//!
//! The crate provides extension traits to add new combinators to existing [streams](futures::stream::Stream),
//! that are targeted for parallel computing and concurrent data processing. Most traits can be found at [`prelude`](prelude).
//!
//! The extension traits can be imported from [`prelude`](prelude).
//!
//! ```rust
//! use par_stream::prelude::*;
//! ```
//!
//! ## Parallel Processing
//!
//! - [`stream.par_map(config, map_fn)`](ParStreamExt::par_map) processes stream items in parallel closures.
//! - [`stream.par_then(config, fut)`](ParStreamExt::par_then) processes stream items in parallel futures.
//! - [`par_map_unordered()`](ParStreamExt::par_map_unordered) and [`par_then_unordered()`](ParStreamExt::par_then_unordered)
//!   are unordered variances.
//! - [`try_par_map()`](FallibleParStreamExt::try_par_map), [`try_par_then()`](FallibleParStreamExt::try_par_then),
//!   [`try_par_then_unordered()`](FallibleParStreamExt::try_par_then_unordered) are the fallible variances.
//!
//! ## Distributing Patterns
//!
//! - [`stream.broadcast(buf_size)`](ParStreamExt::broadcast) broadcasts copies of elements to multiple receivers.
//! - [`stream.tee(buf_size)`](ParStreamExt::tee) creates copies the stream at any time.
//!   Unlike [`broadcast()`](ParStreamExt::broadcast), receivers can start consuming at different points.
//! - [`stream.scatter(buf_size)`](ParStreamExt::scatter) sends each element to one of existing receivers.
//! - [`gather(buf_size, streams)`](gather) merges multiple streams into one stream.
//!
//! ### Scatter-Gather Pattern
//!
//! The combinators can construct a scatter-gather pattern that passes each to one of concurrent workers,
//! and gathers the outputs together.
//!
//! ```rust
//! # use futures::stream::StreamExt;
//! # use par_stream::ParStreamExt;
//! # use std::collections::HashSet;
//!
//! async fn main_async() {
//!     let orig = futures::stream::iter(0..1000);
//!
//!     // scatter stream items to two receivers
//!     let rx1 = orig.scatter(None);
//!     let rx2 = rx1.clone();
//!
//!     // gather back from two receivers
//!     let values: HashSet<_> = par_stream::gather(None, vec![rx1, rx2]).collect().await;
//!
//!     // the gathered values have equal content with the original
//!     assert_eq!(values, (0..1000).collect::<HashSet<_>>());
//! }
//!
//! # #[cfg(feature = "runtime-async-std")]
//! # #[async_std::main]
//! # async fn main() {
//! #     main_async().await
//! # }
//! #
//! # #[cfg(feature = "runtime-tokio")]
//! # #[tokio::main]
//! # async fn main() {
//! #     main_async().await
//! # }
//! #
//! # #[cfg(feature = "runtime-smol")]
//! # fn main() {
//! #     smol::block_on(main_async())
//! # }
//! ```
//!
//! ### Broadcast-Join Pattern
//!
//! Another example is to construct a tee-zip pattern that clones each element to
//! several concurrent workers, and pairs up outputs from each worker.
//!
//! ```rust
//! # use futures::prelude::*;
//! # use par_stream::prelude::*;
//! use par_stream::join;
//!
//! async fn main_async() {
//!     let data = vec![2, -1, 3, 5];
//!
//!     let mut guard = futures::stream::iter(data.clone()).broadcast(None);
//!     let rx1 = guard.register();
//!     let rx2 = guard.register();
//!     let rx3 = guard.register();
//!     guard.finish();
//!
//!     let join = par_stream::join!(rx1.map(|v| v * 2), rx2.map(|v| v * 3), rx3.map(|v| v * 5));
//!
//!     let collected: Vec<_> = join.collect().await;
//!     assert_eq!(
//!         collected,
//!         vec![((4, 6), 10), ((-2, -3), -5), ((6, 9), 15), ((10, 16), 25)]
//!     );
//! }
//!
//! # #[cfg(feature = "runtime-async-std")]
//! # #[async_std::main]
//! # async fn main() {
//! #     main_async().await
//! # }
//! #
//! # #[cfg(feature = "runtime-tokio")]
//! # #[tokio::main]
//! # async fn main() {
//! #     main_async().await
//! # }
//! #
//! # #[cfg(feature = "runtime-smol")]
//! # fn main() {
//! #     smol::block_on(main_async())
//! # }
//! ```
//!
//! ## Item Ordering
//!
//! - [`stream.wrapping_enumerate()`](IndexedStreamExt::wrapping_enumerate) is like [`enumerate()`](futures::StreamExt::enumerate),
//!   but wraps around to zero after reaching [usize::MAX].
//! - [`stream.reorder_enumerated()`](IndexedStreamExt::reorder_enumerated) accepts a `(usize, T)` typed stream and
//!   reorder the items according to the index number.
//! - [`stream.try_wrapping_enumerate()`](FallibleIndexedStreamExt::try_wrapping_enumerate) and
//!   [`stream.try_reorder_enumerated()`](FallibleIndexedStreamExt::try_reorder_enumerated) are fallible counterparts.
//!
//! The item ordering combinators are usually combined with unordered concurrent processing methods,
//! allowing on-demand data passing between stages.
//!
//! ```ignore
//! stream
//!     // mark items with index numbers
//!     .enumerate()
//!     // a series of unordered maps
//!     .par_then_unordered(config, map_fn)
//!     .par_then_unordered(config, map_fn)
//!     .par_then_unordered(config, map_fn)
//!     // reorder the items back by indexes
//!     .reorder_enumerated()
//! ```
//!
//! ## Configure Number of Workers
//!
//! The `config` parameter of [`stream.par_map(config, map_fn)`](ParStreamExt::par_map) controls
//! the number of concurrent workers and internal buffer size. It accepts the following values.
//!
//! - `None`: The number of workers defaults to the number of system processors.
//! - `10` or non-zero integers: 10 workers.
//! - `2.5` or non-zero floating points: The number of worker is 2.5 times the system processors.
//! - `(10, 15)`: 10 workers and internal buffer size 15.
//!
//! If the buffer size is not specified, the default is the double of number of workers.

/// Commonly used traits.
pub mod prelude {
    pub use super::{
        stream::{IndexedStreamExt, ParStreamExt},
        try_stream::{FallibleIndexedStreamExt, FallibleParStreamExt},
    };
}

mod common;
mod config;
mod macros;
mod rt;
mod stream;
mod try_stream;
mod utils;

pub use config::*;
pub use stream::*;
pub use try_stream::*;
