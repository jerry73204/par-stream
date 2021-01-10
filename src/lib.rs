//! Asynchronous parallel streams analogous to [rayon](https://github.com/rayon-rs/rayon).
//!
//! The [ParStreamExt](ParStreamExt) and [TryParStreamExt](TryParStreamExt) traits extends
//! existing [Stream](futures::stream::Stream)s with parallel stream combinators.
//!
//! The followings are common combinators.
//! - [`stream.par_then(limit, map_fut)`](ParStreamExt::par_then) processes stream items to parallel futures.
//! - [`stream.par_map(limit, map_fn)`](ParStreamExt::par_map) processes stream items to parallel closures.
//! - [`stream.par_then_unordered(limit, map_fut)`](ParStreamExt::par_then_unordered) is unordered version of [`stream.par_then`](ParStreamExt::par_then).
//! - [`stream.par_then_init(limit, init_fut, map_fut)`](ParStreamExt::par_then_init) accepts an extra in-local thread initializer.
//! - [`stream.try_par_then(limit, map_fut)`](TryParStreamExt::try_par_then) is the fallible version of [`stream.par_then`](ParStreamExt::par_then).
//!
//! The `limit` parameter configures the worker pool size. It accepts the following values.
//!
//! - `None`: The worker pool size scales to the number of system CPUs, and double size of input buffer.
//! - `10` or non-zero integers: Scales the worker pool size to absolute 10, and double size of input buffer.
//! - `2.3` or non-zero floating points: Scale the number of workers to 2.3 times the number of system CPUs, and double size of input buffer.
//! - `(10, 15)`: Scales to absolute 10 workers, and sets the input buffer size to 15.

mod base;
mod common;
mod config;
mod impls;
mod stream;
mod try_stream;

pub use config::*;
pub use stream::*;
pub use try_stream::*;
