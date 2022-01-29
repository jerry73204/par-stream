use futures::{
    future,
    stream::{self, StreamExt as _},
};
use par_stream::{rt, Shared};
use rand::{prelude::*, rngs::OsRng};
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opts {
    pub num_jobs: usize,
    pub num_workers: usize,
    pub in_buf_size: usize,
    pub out_buf_size: usize,
    pub pow: u32,
    #[structopt(long)]
    pub spawn: bool,
}

fn main() {
    par_stream::rt::block_on_executor(async move {
        let opts = Opts::from_args();

        let elapsed_notifier = shared_stream_by_notifier_test(&opts).await;
        println!("elapsed for notifier\t{:?}ms", elapsed_notifier.as_millis());

        let elapsed_channel = shared_stream_by_channel_test(&opts).await;
        println!("elapsed for channel\t{:?}ms", elapsed_channel.as_millis());
    });
}

async fn shared_stream_by_notifier_test(opts: &Opts) -> Duration {
    let pow = opts.pow;
    let spawn = opts.spawn;

    let stream = stream::repeat(())
        .take(opts.num_jobs)
        .map(|()| -> u64 { OsRng.gen() });
    let stream = Shared::new(stream);
    let (out_tx, out_rx) = flume::bounded(opts.out_buf_size);

    let worker_futures = (0..(opts.num_workers)).map(move |_| {
        let out_tx = out_tx.clone();
        let stream = stream.clone();

        rt::spawn(async move {
            let _ = stream
                .then(|val| task(val, pow, spawn))
                .map(Ok)
                .forward(out_tx.into_sink())
                .await;
        })
    });

    let output_future = rt::spawn(async move {
        out_rx
            .into_stream()
            .fold(0u64, |sum, val| future::ready(sum.wrapping_add(val)))
            .await
    });

    let instant = std::time::Instant::now();
    futures::join!(output_future, future::join_all(worker_futures));

    instant.elapsed()
}

async fn shared_stream_by_channel_test(opts: &Opts) -> Duration {
    let pow = opts.pow;
    let spawn = opts.spawn;

    let stream = stream::repeat(())
        .take(opts.num_jobs)
        .map(|()| -> u64 { OsRng.gen() });
    let (in_tx, in_rx) = flume::bounded(opts.in_buf_size);
    let (out_tx, out_rx) = flume::bounded(opts.out_buf_size);

    let input_future = rt::spawn(async move {
        let _ = stream.map(Ok).forward(in_tx.into_sink()).await;
    });

    let worker_futures = (0..(opts.num_workers)).map(move |_| {
        let in_rx = in_rx.clone();
        let out_tx = out_tx.clone();

        rt::spawn(async move {
            let _ = in_rx
                .into_stream()
                .then(|val| task(val, pow, spawn))
                .map(Ok)
                .forward(out_tx.into_sink())
                .await;
        })
    });

    let output_future = rt::spawn(async move {
        out_rx
            .into_stream()
            .fold(0u64, |sum, val| future::ready(sum.wrapping_add(val)))
            .await
    });

    let instant = std::time::Instant::now();
    futures::join!(
        input_future,
        output_future,
        future::join_all(worker_futures)
    );
    instant.elapsed()
}

async fn task(input: u64, pow: u32, spawn: bool) -> u64 {
    if spawn {
        rt::spawn_blocking(move || compute(input, pow)).await
    } else {
        compute(input, pow)
    }
}

fn compute(input: u64, pow: u32) -> u64 {
    (0..pow).fold(1u64, move |product, _| product.wrapping_mul(input))
}
