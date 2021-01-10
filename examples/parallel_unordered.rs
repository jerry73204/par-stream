use futures::stream::StreamExt;
use par_stream::ParStreamExt;

async fn main_async() {
    // the variable will be captured by parallel workers
    let scale = Box::new(2usize);
    let addition = Box::new(1usize);

    let doubled = futures::stream::iter(0..1000)
        // add indexes that does not panic on overflow
        .wrapping_enumerate()
        // unordered parallel tasks on futures
        .par_then_unordered(None, move |(index, value)| {
            // cloned needed variables in the main thread
            let cloned = *scale;

            // the future is sent to a parallel worker
            async move { (index, value * cloned) }
        })
        // unordered parallel tasks on closures
        .par_map_unordered(None, move |(index, value)| {
            // cloned needed variables in the main thread
            let cloned = *addition;

            // the future is sent to a parallel worker
            move || (index, value + cloned)
        })
        // reorder the values back by indexes
        .reorder_enumerated()
        // call `collect()` from futures crate
        .collect::<Vec<_>>()
        .await;

    // the output will be ordered
    let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
    assert_eq!(doubled, expect);
}

#[cfg(feature = "runtime_async-std")]
#[async_std::main]
async fn main() {
    main_async().await
}

#[cfg(feature = "runtime_tokio")]
#[tokio::main]
async fn main() {
    main_async().await
}
