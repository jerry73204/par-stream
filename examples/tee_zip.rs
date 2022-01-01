use futures::stream::StreamExt as _;
use itertools::izip;
use par_stream::ParStreamExt as _;

fn main() {
    par_stream::rt::block_on_executor(async move {
        let rx1 = futures::stream::iter(1isize..=10).tee(2);
        let rx2 = rx1.clone();

        let values: Vec<_> = rx1
            .map(|val| val * 2)
            .zip(rx2.map(|val| val * 3))
            .collect()
            .await;

        assert!(izip!(1..=1000, values)
            .all(|(orig, (val1, val2))| orig * 2 == val1 && orig * 3 == val2));
    });
}
