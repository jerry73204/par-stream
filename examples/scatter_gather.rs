use futures::stream::StreamExt;
use par_stream::ParStreamExt;

async fn main_async() {
    let (scatter_fut, scatter_rx1) = futures::stream::iter(1isize..=1000).par_scatter(None);
    let scatter_rx2 = scatter_rx1.clone();

    // first parallel worker
    let (worker1_tx, worker1_rx) = async_std::channel::bounded(4);
    let worker1 = async_std::task::spawn(async move {
        while let Ok(value) = scatter_rx1.recv().await {
            worker1_tx.send(value).await.unwrap();
        }
    });

    // second parallel worker
    let (worker2_tx, worker2_rx) = async_std::channel::bounded(4);
    let worker2 = async_std::task::spawn(async move {
        while let Ok(value) = scatter_rx2.recv().await {
            worker2_tx.send(-value).await.unwrap();
        }
    });

    // gather from workers
    let gather_fut = par_stream::par_gather(vec![worker1_rx, worker2_rx], None).collect::<Vec<_>>();

    // join parallel tasks
    let ((), (), (), gathered_values) = futures::join!(scatter_fut, worker1, worker2, gather_fut);

    // summary
    let n_pos: isize = gathered_values
        .iter()
        .cloned()
        .filter_map(|value| if value > 0 { Some(1) } else { None })
        .sum();
    let n_neg: isize = gathered_values
        .iter()
        .cloned()
        .filter_map(|value| if value < 0 { Some(1) } else { None })
        .sum();

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
