use futures::stream::StreamExt as _;
use itertools::izip;
use par_stream::ParStreamExt as _;

async fn main_async() {
    let rx1 = futures::stream::iter(1isize..=10).tee(2);
    let rx2 = rx1.clone();

    let values: Vec<_> = rx1
        .map(|val| val * 2)
        .zip(rx2.map(|val| val * 3))
        .collect()
        .await;

    assert!(
        izip!(1..=1000, values).all(|(orig, (val1, val2))| orig * 2 == val1 && orig * 3 == val2)
    );
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

#[cfg(feature = "runtime-smol")]
fn main() {
    smol::block_on(main_async())
}
