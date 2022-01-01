use futures::stream::StreamExt as _;
use par_stream::ParStreamExt as _;

fn main() {
    par_stream::rt::block_on_executor(async move {
        // the variable will be captured by parallel workers
        let scale = Box::new(2usize);
        let addition = Box::new(1usize);

        let doubled: Vec<_> = futures::stream::iter(0..1000)
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
            .collect()
            .await;

        // the output will be ordered
        let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
        assert_eq!(doubled, expect);
    });
}
