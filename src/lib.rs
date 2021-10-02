//! Asynchronous parallel streams analogous to [rayon](https://github.com/rayon-rs/rayon).
//!
//! # Combinators
//!
//! The [ParStreamExt](ParStreamExt) and [TryParStreamExt](TryParStreamExt) traits add
//! new combinators to existing [streams](futures::stream::Stream), that are targeted for
//! parallel computing and data processing.
//!
//! ## Parallel Processing
//!
//! - [`stream.par_map(config, fn)`](ParStreamExt::par_map) processes stream items in parallel closures.
//! - [`stream.par_then(config, fut)`](ParStreamExt::par_then) processes stream items in parallel futures.
//! - [`par_map_unordered()`](ParStreamExt::par_map_unordered) and [`par_then_unordered()`](ParStreamExt::par_then_unordered)
//!   are unordered variances.
//! - [`try_par_map()`](TryParStreamExt::try_par_map), [`try_par_then()`](TryParStreamExt::try_par_then),
//!   [`try_par_then_unordered()`](TryParStreamExt::try_par_then_unordered) are the fallible variances.
//!
//! ## Distributing Patterns
//!
//! - [`stream.tee(buf_size)`](ParStreamExt::tee) creates a copy of a stream.
//! - [`stream.scatter(buf_size)`](ParStreamExt::scatter) forks a stream into parts.
//! - [`gather(buf_size, streams)`](gather) merges multiple streams into one stream.
//!
//! ## Ordering
//!
//! - [`stream.wrapping_enumerate()`](ParStreamExt::wrapping_enumerate) is like [`enumerate()`](futures::StreamExt::enumerate),
//!   but wraps around to zero after reaching [usize::MAX].
//! - [`stream.reorder_enumerated()`](ParStreamExt::reorder_enumerated) accepts a `(usize, T)` typed stream and
//!   reorder the items according to the index number.
//! - [`stream.try_wrapping_enumerate()`](TryParStreamExt::try_wrapping_enumerate) and
//!   [`stream.try_reorder_enumerated()`](TryParStreamExt::try_reorder_enumerated) are fallible counterparts.
//!
//! ## Configure Number of Workers
//!
//! The `config` parameter of [`stream.par_map(config, fn)`](ParStreamExt::par_map) controls
//! the number of concurrent workers and internal buffer size. It accepts the following values.
//!
//! - `None`: The number of workers defaults to the number of system processors.
//! - `10` or non-zero integers: 10 workers.
//! - `2.5` or non-zero floating points: The number of worker is 2.5 times the system processors.
//! - `(10, 15)`: 10 workers and internal buffer size 15.
//!
//! If the buffer size is not specified, the default is the double of number of workers.
//!
//! # Cargo Features
//!
//! The following cargo features select the backend runtime for concurrent workers.
//! One of them must be specified, otherwise the crate raises a compile error.
//!
//! - `runtime-tokio` uses the multi-threaded [tokio] runtime.
//! - `runtime-async-std` uses the default [async-std](async_std) runtime.
//! - `runtime-smol` uses the default [smol] runtime.

/// Commonly used traits.
pub mod prelude {
    pub use super::{
        slice::{SliceExt, SliceMutExt},
        stream::ParStreamExt,
        try_stream::TryParStreamExt,
    };
}

mod common;
mod config;
mod error;
mod rt;
mod slice;
mod stream;
mod try_stream;

pub use config::*;
pub use stream::*;
pub use try_stream::*;
