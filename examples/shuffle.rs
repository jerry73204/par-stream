//! The example demonstrates the MergeShuffle algorithm by Axel Bacher et al.
//!
//! See the paper _MergeShuffle: A Very Fast, Parallel Random Permutation Algorithm_
//! for the description of this algorithm.
//! https://arxiv.org/abs/1508.03167

use concurrent_slice::{Chunk, ConcurrentSlice};
use futures::{stream, stream::StreamExt as _};
use par_stream::prelude::*;
use rand::prelude::*;
use std::{mem, time::Instant};

const LEN: usize = 100_000_000;
const MIN_CHUNK_LEN: usize = 25_000_000;

fn main() {
    par_stream::rt::block_on_executor(async move {
        let array: Vec<_> = (0..LEN).collect();

        // benchmark Fisher-Yates shuffling
        let array_single = array.clone();

        let instant = Instant::now();

        let _array_single = {
            let mut array = array_single;
            fisher_yates(&mut array);
            array
        };
        eprintln!("single:\t{:?}", instant.elapsed());

        // benchmark parallel algorithm
        let instant = Instant::now();
        let _array_concurrent = {
            // shuffle each chunk locally
            let mut chunks: Vec<_> = stream::iter(array.concurrent_chunks(MIN_CHUNK_LEN))
                .par_map(None, |mut chunk| {
                    move || {
                        fisher_yates(&mut chunk);
                        chunk
                    }
                })
                .collect()
                .await;

            // merge shuffle
            while chunks.len() > 1 {
                let chunks_: Vec<_> = stream::iter(chunks)
                    .chunks(2)
                    .par_map(None, |pair| {
                        move || {
                            let mut pair = pair.into_iter();

                            let lchunk = pair.next().unwrap();
                            let rchunk = match pair.next() {
                                Some(rchunk) => rchunk, // non-tail case
                                None => {
                                    // tail case
                                    return lchunk;
                                }
                            };
                            let llen = lchunk.len();

                            // merge chunk pair into one chunk
                            let mut chunk = Chunk::cat(vec![lchunk, rchunk]);
                            let len = chunk.len();

                            let mut rng = rand::thread_rng();
                            let mut lidx = 0;
                            let mut ridx = llen;

                            while lidx < ridx && ridx < len {
                                if rng.gen() {
                                    let (lslice, rslice) = chunk.split_at_mut(ridx);
                                    mem::swap(&mut lslice[lidx], &mut rslice[0]);
                                    ridx += 1;
                                }
                                lidx += 1;
                            }

                            while lidx < len {
                                let oidx = rng.gen_range(0..=lidx);
                                if oidx != lidx {
                                    let (lslice, rslice) = chunk.split_at_mut(oidx + 1);
                                    mem::swap(
                                        lslice.last_mut().unwrap(),
                                        &mut rslice[lidx - oidx - 1],
                                    );
                                }
                                lidx += 1;
                            }

                            chunk
                        }
                    })
                    .collect()
                    .await;

                // swap even and odd chunks
                chunks = chunks_;
            }

            // merge chunks back to array
            let guard = chunks[0].guard();
            drop(chunks);
            guard.try_unwrap().unwrap()
        };
        eprintln!("parallel:\t{:?}", instant.elapsed());
    });
}

fn fisher_yates<T>(array: &mut [T]) {
    let mut rng = rand::thread_rng();
    let len = array.len();

    (0..len).for_each(|lidx| {
        let ridx = rng.gen_range(lidx..len);

        if lidx != ridx {
            let (larray, rarray) = array.split_at_mut(lidx + 1);
            mem::swap(larray.last_mut().unwrap(), &mut rarray[ridx - lidx - 1]);
        }
    });
}
