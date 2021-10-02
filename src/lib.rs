//! Asynchronous parallel streams analogous to [rayon](https://github.com/rayon-rs/rayon).
//!
//! The [ParStreamExt](ParStreamExt) and [TryParStreamExt](TryParStreamExt) traits extends
//! existing [Stream](futures::stream::Stream)s with parallel stream combinators.
//!
//! The followings are common combinators.
//! - [`stream.par_map(config, map_fn)`](ParStreamExt::par_map) processes stream items in parallel closures.
//! - [`stream.par_then(config, map_fut)`](ParStreamExt::par_then) processes stream items in parallel futures.
//! - [`par_map_unordered()`](ParStreamExt::par_map_unordered) and [`par_then_unordered()`](ParStreamExt::par_then_unordered) are unordered variances.
//! - [`try_par_map()`](TryParStreamExt::try_par_map), [`try_par_then()`](TryParStreamExt::try_par_then), [`try_par_then_unordered()`](TryParStreamExt::try_par_then_unordered)
//!   are the fallible variances.
//!
//! The `config` parameter configures the worker pool size. It accepts the following values.
//!
//! - `None`: The worker pool size scales to the number of system CPUs, and double size of input buffer.
//! - `10` or non-zero integers: Scales the worker pool size to absolute 10, and double size of input buffer.
//! - `2.3` or non-zero floating points: Scale the number of workers to 2.3 times the number of system CPUs, and double size of input buffer.
//! - `(10, 15)`: Scales to absolute 10 workers, and sets the input buffer size to 15.

/// Commonly used traits.
pub mod prelude {
    pub use super::{stream::ParStreamExt, try_stream::TryParStreamExt};
}

mod common;
mod config;
mod error;
mod rt;
mod stream;
mod try_stream;

pub use config::*;
pub use stream::*;
pub use try_stream::*;
