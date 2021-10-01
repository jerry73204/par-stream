use futures::stream::StreamExt;
use par_stream::ParStreamExt;

async fn main_async() {
    // the variable will be captured by parallel workers
    let scale = Box::new(2usize);
    let addition = Box::new(1usize);

    let doubled = futures::stream::iter(0..1000)
        // parallel tasks on futures
        .par_then(None, move |value| {
            // cloned needed variables in the main thread
            let cloned = *scale;

            // the future is sent to a parallel worker
            async move { value * cloned }
        })
        // parallel tasks on closures
        .par_map(None, move |value| {
            // cloned needed variables in the main thread
            let cloned = *addition;

            // the future is sent to a parallel worker
            move || value + cloned
        })
        // call `collect()` from futures crate
        .collect::<Vec<_>>()
        .await;

    // the output will be ordered
    let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
    assert_eq!(doubled, expect);
}

#[cfg(feature = "runtime-async-std")]
#[async_std::main]
async fn main() {
    main_async().await
}

#[cfg(feature = "runtime-tokio")]
#[tokio::main]
async fn main() {
    main_async().await
}
