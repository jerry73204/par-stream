use futures::stream::{self, StreamExt as _};
use par_stream::ParStreamExt as _;

fn main() {
    par_stream::rt::block_on_executor(async move {
        let rx1 = futures::stream::iter(1isize..=1000).spawned(None);
        let rx2 = rx1.clone();

        // gather from workers
        let gathered_values: Vec<_> = stream::select(rx1, rx2.map(|val| -val)).collect().await;

        // summary
        let n_pos = gathered_values
            .iter()
            .cloned()
            .filter(|&value| value > 0)
            .count();
        let n_neg = gathered_values
            .iter()
            .cloned()
            .filter(|&value| value < 0)
            .count();

        println!("worker1 process {} numbers", n_pos);
        println!("worker2 process {} numbers", n_neg);
    });
}
