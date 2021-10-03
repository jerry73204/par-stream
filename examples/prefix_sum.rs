use futures::{stream, stream::StreamExt};
use itertools::{chain, izip};
use par_stream::{prelude::*, Chunk};
use rand::prelude::*;
use std::{convert::identity, iter, sync::Arc, time::Instant};

const LEN: usize = 100_000_000;
const CHUNK_LEN: usize = 65536;

async fn main_async() {
    // fill array with random numbers
    let instant = Instant::now();
    let array = {
        let array = vec![0i32; LEN];

        let chunks: Vec<_> = stream::iter(array.concurrent_chunks_by_division(None))
            .par_map_unordered(None, |mut chunk| {
                move || {
                    let mut rng = rand::thread_rng();
                    chunk.iter_mut().for_each(|elem| *elem = rng.gen());
                    chunk
                }
            })
            .collect()
            .await;
        let array = Chunk::into_owner(chunks);

        array
    };
    eprintln!("random vec generation:\t{:?}", instant.elapsed());

    // benchmark non-concurrent prefix-sum

    let instant = Instant::now();
    let array_std: Vec<_> = array
        .iter()
        .scan(0, |sum, &val| {
            *sum += val;
            Some(*sum)
        })
        .collect();
    eprintln!("single thread:\t{:?}", instant.elapsed());

    // concurrent prefix-sum
    let instant = Instant::now();
    let array_concurrent = {
        // compute prefix-sum with chunks

        let mut chunks0: Vec<_> = stream::iter(array.concurrent_chunks(CHUNK_LEN))
            .par_map(None, |mut chunk| {
                move || {
                    chunk
                        .iter_mut()
                        .scan(0, |sum, val| {
                            *sum += *val;
                            *val = *sum;
                            Some(())
                        })
                        .for_each(identity);
                    chunk
                }
            })
            .collect()
            .await;
        let mut chunks1: Vec<_> = vec![0; LEN].concurrent_chunks(CHUNK_LEN).collect();

        // reverse chunks so we can skip chunks easily
        chunks0.reverse();
        chunks1.reverse();

        let offsets = {
            let num_chunks = chunks0.len();
            let max_power = round_to_power_of_2(num_chunks).trailing_zeros();
            (0..max_power).map(|power| 1 << power)
        };

        for offset in offsets {
            // swap buffer
            let chunks0_ = chunks1;
            let chunks1_ = Arc::new(chunks0);

            let chunks0_: Vec<_> = stream::iter(izip!(
                chain!(
                    chunks1_.clone().concurrent_iter().skip(offset).map(Some),
                    iter::repeat(None).take(offset)
                ),
                chunks1_.clone().concurrent_iter(),
                chunks0_
            ))
            .par_map(None, |(lchunk1, rchunk1, mut chunk0)| {
                move || {
                    match lchunk1 {
                        Some(lchunk1) => {
                            let addition = *lchunk1.last().unwrap();

                            izip!(&*rchunk1, &mut chunk0).for_each(|(&src, tgt)| {
                                *tgt = src + addition;
                            });
                        }
                        None => {
                            chunk0.copy_from_slice(&*rchunk1);
                        }
                    }

                    chunk0
                }
            })
            .collect()
            .await;

            chunks0 = chunks0_;
            chunks1 = Arc::try_unwrap(chunks1_).unwrap();
        }

        chunks0.reverse();
        Chunk::into_owner(chunks0)
    };
    eprintln!("multi-thread:\t{:?}", instant.elapsed());

    // validate results
    assert_eq!(array_std, array_concurrent);
}

fn round_to_power_of_2(val: usize) -> usize {
    let val = val as u64;
    let val = val - 1;
    let val = val | val >> 1;
    let val = val | val >> 2;
    let val = val | val >> 4;
    let val = val | val >> 8;
    let val = val | val >> 16;
    let val = val | val >> 32;
    let val = val + 1;
    val as usize
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
