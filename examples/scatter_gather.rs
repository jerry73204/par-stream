use futures::stream::StreamExt;
use par_stream::ParStreamExt;

async fn main_async() {
    let rx1 = futures::stream::iter(1isize..=1000).scatter(None);
    let rx2 = rx1.clone();

    // gather from workers
    let gathered_values: Vec<_> = par_stream::gather(vec![rx1.boxed(), rx2.map(|val| -val).boxed()], None)
        .collect()
        .await;

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
