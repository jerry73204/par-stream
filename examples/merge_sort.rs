use concurrent_slice::{Chunk, ConcurrentSlice};
use futures::{stream, stream::StreamExt as _};
use itertools::izip;
use par_stream::prelude::*;
use rand::prelude::*;
use std::time::Instant;

const LEN: usize = 100_000_000;
const MIN_CHUNK_LEN: usize = 2048;

fn main() {
    par_stream::rt::block_on_executor(async move {
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
            let guard = chunks[0].guard();
            drop(chunks);
            guard.try_unwrap().unwrap()
        };
        eprintln!("random vec generation:\t{:?}", instant.elapsed());

        // benchmark non-concurrent sorting
        let mut array_std = array.clone();

        let instant = Instant::now();
        let array_std = {
            array_std.sort();
            array_std
        };
        eprintln!("std sort:\t{:?}", instant.elapsed());

        // parallel merge sort
        let instant = Instant::now();
        let array_concurrent = {
            // generate chunks
            let chunks0: Vec<_> = array.concurrent_chunks(MIN_CHUNK_LEN).collect();
            let mut chunks1: Vec<_> = vec![0; LEN].concurrent_chunks(MIN_CHUNK_LEN).collect();

            // sort within chunks
            let mut chunks0: Vec<_> = stream::iter(chunks0)
                .par_map(None, |mut chunk| {
                    move || {
                        chunk.sort();
                        chunk
                    }
                })
                .collect()
                .await;

            // merge sort
            while chunks0.len() > 1 {
                let (chunks0_, chunks1_): (Vec<_>, Vec<_>) = stream::iter(izip!(chunks0, chunks1))
                    .chunks(2)
                    .par_map(None, |pair| {
                        move || {
                            let mut pair = pair.into_iter();

                            let (lchunk0, mut lchunk1) = pair.next().unwrap();
                            let (rchunk0, rchunk1) = match pair.next() {
                                Some(chunk) => chunk, // non-tail case
                                None => {
                                    // tail case
                                    lchunk1.copy_from_slice(&*lchunk0);
                                    return (lchunk0, lchunk1);
                                }
                            };

                            // source slices
                            let mut lslice0: &[_] = &*lchunk0;
                            let mut rslice0: &[_] = &*rchunk0;

                            // target slice
                            let mut chunk1 = Chunk::cat(vec![lchunk1, rchunk1]);
                            let mut slice1 = &mut *chunk1;

                            // merge sorted source slices into the target slice
                            loop {
                                match (lslice0.first(), rslice0.first()) {
                                    (Some(&lval), Some(&rval)) => {
                                        if lval <= rval {
                                            slice1[0] = lval;
                                            lslice0 = &lslice0[1..];
                                        } else {
                                            slice1[0] = rval;
                                            rslice0 = &rslice0[1..];
                                        }
                                        slice1 = &mut slice1[1..];
                                    }
                                    (Some(_), None) => {
                                        slice1.clone_from_slice(lslice0);
                                        break;
                                    }
                                    (None, Some(_)) => {
                                        slice1.clone_from_slice(rslice0);
                                        break;
                                    }
                                    (None, None) => break,
                                }
                            }

                            // merge source chunks
                            let chunk0 = Chunk::cat(vec![lchunk0, rchunk0]);

                            (chunk0, chunk1)
                        }
                    })
                    .unzip()
                    .await;

                // swap even and odd chunks
                chunks0 = chunks1_;
                chunks1 = chunks0_;
            }

            // merge chunks back to array
            let guard = chunks0[0].guard();
            drop(chunks0);
            guard.try_unwrap().unwrap()
        };
        eprintln!("merge sort:\t{:?}", instant.elapsed());

        // verify results
        assert_eq!(
            array_std, array_concurrent,
            "the outputs of std sort and merge-sort differ"
        );
    });
}
