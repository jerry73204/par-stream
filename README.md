# par-stream: Asynchronous Parallel Stream for Rust

An Rust implementation of asynchronous parallel streams analogous to [rayon](https://github.com/rayon-rs/rayon).

It is a re-design of [async-rs/parallel-stream](https://github.com/async-rs/parallel-stream).
It guarantees the compatibility with [futures](https://github.com/rust-lang/futures-rs) stream by providing extension traits.
Unlike parallel-stream, it does not introduce incompatible types.

## Usage

The crate is not published to crates.io yet. Add the Git repository to your `Cargo.toml` to include this crate.

```toml
[dependencies]
par-stream = { git = "https://github.com/jerry73204/par-stream.git" }
```

## Features

### Easy usage

Add one line and you can obtain parallel combinators on existing [futures]((https://github.com/rust-lang/futures-rs)) stream.

```rust
use par_stream::ParStreamExt;
```

### Parallel combinators

- `stream.par_then(limit, map_fut)` processes stream items to parallel futures.
- `stream.par_map(limit, map_fn)` processes stream items to parallel closures.
- `stream.par_then_unordered(limit, map_fut)` and `stream.par_map_unordered(limit, map_fn)` are unordered correspondings of above.
- `stream.par_then_init(limit, init_fut, map_fut)` accepts an extra in-local thread initializer.
- `stream.try_par_then(limit, map_fut)` is the fallible version of `stream.par_then(limit, map_fut)`.

The `limit` parameter configures the worker pool size. It accepts the following values:

- `None`: The worker pool size scales to the number of system CPUs, and double size of input buffer.
- `10` or non-zero integers: Scales the worker pool size to absolute 10, and double size of input buffer.
- `2.3` or non-zero floating points: Scale the number of workers to 2.3 times the number of system CPUs, and double size of input buffer.
- `(10, 15)`: Scales to absolute 10 workers, and sets the input buffer size to 15.

### Scatter and gather combinators

The feature is convenient to work with your custom organization of parallel workers.

`stream.par_scatter(buf_size)` allows you to convert a stream to a scattering worker and a clonable receiver.
You can distribute cloned receivers to respective workers to share a stream.

`par_gather(streams, buf_size)` gathers multiple streams into one stream.

```rust
let (scatter_fut, rx) = stream.par_scatter(buf_size);

let rx1 = rx.clone();
let rx2 = rx.clone();

let stream1 = worker1(rx1);
let stream2 = worker1(rx2);

let gathered_stream = par_stream::par_gather(vec![stream1, stream2], buf_size);
```

### Control the ordering of stream items

The combination of `stream.overflowing_enumerate()` and `stream.reorder_enumerated()`
enable you to control the ordering of the stream items.

It gives the way to mark items with index numbers, apply to multiple unordered parallel tasks,
and reorder them back. It effectively avoids reordering after each parallel task.

```rust
stream
    // mark items with index numbers
    .overflowing_enumerate()
    // a series of unordered maps
    .par_then_unordered(limit, map_fut1)
    .par_then_unordered(limit, map_fut2)
    .par_then_unordered(limit, map_fut3)
    // reorder the items back by indexes
    .reorder_enumerated()
```

## Example

Please visit the [example](example) directory to see usages of the crate.

## License

MIT License. See [LICENSE](LICENSE.txt) file.
