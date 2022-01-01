use futures::stream::StreamExt as _;
use par_stream::prelude::*;

fn main() {
    par_stream::rt::block_on_executor(async move {
        // the variable will be captured by parallel workers
        let scale = Box::new(2usize);
        let addition = Box::new(1usize);

        let doubled: Vec<_> = futures::stream::iter(0..1000)
            // add indexes that does not panic on overflow
            .enumerate()
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
            .collect()
            .await;

        // the output will be ordered
        let expect = (0..1000).map(|value| value * 2 + 1).collect::<Vec<_>>();
        assert_eq!(doubled, expect);
    });
}
