use crate::{
    common::*,
    config::{BufSize, ParParams},
    rt,
    stream::StreamExt as _,
    try_stream::{TakeUntilError, TryStreamExt as _},
    utils,
};
use flume::r#async::RecvStream;
use tokio::sync::broadcast;

/// Stream for the [try_par_unfold()] method.
pub type TryParUnfold<T, E> = TakeUntilError<RecvStream<'static, Result<T, E>>, T, E>;

/// Stream for the [try_par_unfold_blocking()] method.
pub type TryParUnfoldBlocking<T, E> = TakeUntilError<RecvStream<'static, Result<T, E>>, T, E>;

// // par_unfold_builder

// pub use par_unfold_builder::*;
// mod par_unfold_builder {
//     use super::*;

//     pub fn par_unfold_builder<State, Out, Fut, F>(f: F) -> ParUnfoldAsyncBuilder<State, Out, F>
//     where
//         F: FnMut(State) -> Fut,
//         Fut: 'static + Send + Future<Output = Option<(State, Out)>>,
//         State: 'static + Send,
//         Out: 'static + Send,
//     {
//         ParUnfoldAsyncBuilder::new(f)
//     }
// }

// // par_unfold_blocking_builder

// pub use par_unfold_blocking_builder::*;
// mod par_unfold_blocking_builder {
//     use super::*;

//     pub fn par_unfold_blocking_builder<State, Out, Func, F>(
//         f: F,
//     ) -> ParUnfoldBlockingBuilder<State, Out, F>
//     where
//         F: Send + FnMut(State) -> Func,
//         Func: 'static + Send + FnOnce() -> Option<(State, Out)>,
//         State: 'static + Send,
//         Out: 'static + Send,
//     {
//         ParUnfoldBlockingBuilder::new(f)
//     }
// }

// iter_blocking

pub use iter_blocking::*;

mod iter_blocking {
    use super::*;

    /// Converts an [Iterator] into a [Stream] by consuming the iterator in a blocking thread.
    ///
    /// It is useful when consuming the iterator is computationally expensive and involves blocking code.
    /// It prevents blocking the asynchronous context when consuming the returned stream.
    pub fn iter_blocking<B, I>(buf_size: B, iter: I) -> RecvStream<'static, I::Item>
    where
        B: Into<BufSize>,
        I: 'static + IntoIterator + Send,
        I::Item: Send,
    {
        let buf_size = buf_size.into().get();
        let (tx, rx) = utils::channel(buf_size);

        rt::spawn_blocking(move || {
            for item in iter.into_iter() {
                if tx.send(item).is_err() {
                    break;
                }
            }
        });

        rx.into_stream()
    }
}

// par_unfold

pub use par_unfold::*;

mod par_unfold {
    use super::*;

    /// Produce stream elements from a parallel asynchronous task.
    ///
    /// This function spawns a set of parallel workers. Each worker produces and places
    /// items to an output buffer. The worker pool size and buffer size is determined by
    /// `params`.
    ///
    /// Each worker receives a copy of initialized state `init`, then iteratively calls the function
    /// `f(worker_index, State) -> Fut`. The `Fut` is a future that returns `Option<(output, State)>`.
    /// The future updates the state and produces an output item.
    ///
    /// If a worker receives a `None`, the worker with that worker index will halt, but it does not halt
    /// the other workers. The output stream terminates after every worker halts.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::sync::{
    ///     atomic::{AtomicUsize, Ordering::*},
    ///     Arc,
    /// };
    ///
    /// let mut vec: Vec<_> = par_stream::par_unfold(
    ///     None,
    ///     Arc::new(AtomicUsize::new(0)),
    ///     |_, counter| async move {
    ///         let output = counter.fetch_add(1, SeqCst);
    ///         (output < 1000).then(|| (output, counter))
    ///     },
    /// )
    /// .collect()
    /// .await;
    ///
    /// vec.sort();
    /// itertools::assert_equal(vec, 0..1000);
    /// # })
    /// ```
    pub fn par_unfold<Item, State, P, F, Fut>(
        params: P,
        init: State,
        f: F,
    ) -> RecvStream<'static, Item>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, State) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Option<(Item, State)>> + Send,
        Item: 'static + Send,
        State: 'static + Send + Clone,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(|worker_index| {
            let output_tx = output_tx.clone();
            let state = init.clone();
            let f = f.clone();

            rt::spawn(async move {
                let _ = stream::unfold((state, f), |(state, mut f)| async move {
                    f(worker_index, state)
                        .await
                        .map(|(item, state)| (item, (state, f)))
                })
                .map(Ok)
                .forward(output_tx.into_sink())
                .await;
            });
        });

        output_rx.into_stream()
    }

    /// Produce stream elements from a parallel blocking task.
    ///
    /// This function spawns a set of parallel workers. Each worker produces and places
    /// items to an output buffer. The worker pool size and buffer size is determined by
    /// `params`.
    ///
    /// Each worker receives a copy of initialized state `init`, then iteratively calls the function
    /// `f(worker_index, State) -> Option<(output, State)>` to update the state and produce an output item.
    ///
    /// If a worker receives a `None`, the worker with that worker index will halt, but it does not halt
    /// the other workers. The output stream terminates after every worker halts.
    ///
    /// ```rust
    /// # par_stream::rt::block_on_executor(async move {
    /// use futures::prelude::*;
    /// use par_stream::prelude::*;
    /// use std::sync::{
    ///     atomic::{AtomicUsize, Ordering::*},
    ///     Arc,
    /// };
    ///
    /// let mut vec: Vec<_> =
    ///     par_stream::par_unfold_blocking(None, Arc::new(AtomicUsize::new(0)), move |_, counter| {
    ///         let output = counter.fetch_add(1, SeqCst);
    ///         (output < 1000).then(|| (output, counter))
    ///     })
    ///     .collect()
    ///     .await;
    ///
    /// vec.sort();
    /// itertools::assert_equal(vec, 0..1000);
    /// # })
    /// ```
    pub fn par_unfold_blocking<Item, State, P, F>(
        params: P,
        init: State,
        f: F,
    ) -> RecvStream<'static, Item>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, State) -> Option<(Item, State)> + Send + Clone,
        Item: 'static + Send,
        State: 'static + Send + Clone,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (output_tx, output_rx) = utils::channel(buf_size);

        (0..num_workers).for_each(|worker_index| {
            let mut f = f.clone();
            let mut state = init.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || {
                while let Some((item, new_state)) = f(worker_index, state) {
                    if output_tx.send(item).is_ok() {
                        state = new_state;
                    } else {
                        break;
                    }
                }
            });
        });

        output_rx.into_stream()
    }
}

// sync

pub use sync::*;

mod sync {
    use super::*;
    use std::{cmp::Reverse, collections::BinaryHeap};

    #[derive(Derivative)]
    #[derivative(PartialEq, Eq, PartialOrd, Ord)]
    struct KV<K, V> {
        pub key: K,
        pub index: usize,
        #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
        pub value: V,
    }

    /// Synchronize streams by pairing up keys of each stream item.
    ///
    /// The `key_fn` constructs the key for each item.
    /// The input items are grouped by their keys in the interal buffer until
    /// all items with the key arrives. The finished items are yielded in type
    /// `Ok((stream_index, item))` in monotonic manner.
    ///
    /// If any one of the `streams` generates a non-monotonic item. The item is
    /// yielded as `Err((stream_index, item))` immediately.
    pub fn sync_by_key<I, F, K, S>(
        buf_size: impl Into<Option<usize>>,
        key_fn: F,
        streams: I,
    ) -> BoxStream<'static, Result<(usize, S::Item), (usize, S::Item)>>
    where
        I: IntoIterator<Item = S>,
        S: 'static + Stream + Send,
        S::Item: 'static + Send,
        F: 'static + Fn(&S::Item) -> K + Send,
        K: 'static + Clone + Ord + Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);

        let streams: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(stream_index, stream)| stream.map(move |item| (stream_index, item)).boxed())
            .collect();
        let num_streams = streams.len();

        match num_streams {
            0 => {
                // The case that no stream provided, return empty stream
                return stream::empty().boxed();
            }
            1 => {
                // Fast path for single stream case
                return streams.into_iter().next().unwrap().map(Ok).boxed();
            }
            _ => {
                // Fall through for multiple streams
            }
        }

        let mut input_stream =
            stream::select_all(streams).stateful_map(key_fn, |key_fn, (index, item)| {
                let key = key_fn(&item);
                Some((key_fn, (index, key, item)))
            });
        let (output_tx, output_rx) = utils::channel(buf_size);

        rt::spawn(async move {
            let mut heap: BinaryHeap<Reverse<KV<K, S::Item>>> = BinaryHeap::new();
            let mut min_items: Vec<Option<K>> = vec![None; num_streams];
            let mut threshold: Option<K>;

            'worker: loop {
                'input: while let Some((index, key, item)) = input_stream.next().await {
                    // update min item for that stream
                    {
                        let prev = &mut min_items[index];
                        match prev {
                            Some(prev) if *prev <= key => {
                                *prev = key.clone();
                            }
                            Some(_) => {
                                let ok = output_tx.send_async(Err((index, item))).await.is_ok();
                                if !ok {
                                    break 'worker;
                                }
                                continue 'input;
                            }
                            None => *prev = Some(key.clone()),
                        }
                    }

                    // save item
                    heap.push(Reverse(KV {
                        index,
                        key,
                        value: item,
                    }));

                    // update global threshold
                    threshold = min_items.iter().min().unwrap().clone();

                    // pop items below threshold
                    if let Some(threshold) = &threshold {
                        'output: while let Some(Reverse(KV { key, .. })) = heap.peek() {
                            if key < threshold {
                                let KV { value, index, .. } = heap.pop().unwrap().0;
                                let ok = output_tx.send(Ok((index, value))).is_ok();
                                if !ok {
                                    break 'worker;
                                }
                            } else {
                                break 'output;
                            }
                        }
                    }
                }

                // send remaining items
                for Reverse(KV { index, value, .. }) in heap {
                    let ok = output_tx.send(Ok((index, value))).is_ok();
                    if !ok {
                        break 'worker;
                    }
                }

                break;
            }
        });

        output_rx.into_stream().boxed()
    }
}

// try_sync

pub use try_sync::*;

mod try_sync {
    use super::*;
    use std::{cmp::Reverse, collections::BinaryHeap};

    #[derive(Derivative)]
    #[derivative(PartialEq, Eq, PartialOrd, Ord)]
    struct KV<K, V> {
        pub key: K,
        pub index: usize,
        #[derivative(PartialEq = "ignore", PartialOrd = "ignore", Ord = "ignore")]
        pub value: V,
    }

    /// Synchronize streams by pairing up keys of each stream item. It is fallible counterpart of [sync_by_key](crate::sync_by_key).
    ///
    /// The `key_fn` constructs the key for each item.
    /// The input items are grouped by their keys in the interal buffer until
    /// all items with the key arrives. The finished items are yielded in type
    /// `Ok(Ok((stream_index, item)))` in monotonic manner.
    ///
    /// If any one of the `streams` generates a non-monotonic item. The item is
    /// yielded as `Ok(Err((stream_index, item)))` immediately.
    ///
    /// When an error is receiver from one of the `streams`. The returned stream
    /// yields `Err(err)` and no longer produce future items.
    pub fn try_sync_by_key<I, F, K, T, E, S>(
        buf_size: impl Into<Option<usize>>,
        key_fn: F,
        streams: I,
    ) -> BoxStream<'static, Result<Result<(usize, T), (usize, T)>, E>>
    where
        I: IntoIterator<Item = S>,
        S: 'static + Stream<Item = Result<T, E>> + Send,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + Fn(&T) -> K + Send,
        K: 'static + Clone + Ord + Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);

        let streams: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(index, stream)| stream.map_ok(move |item| (index, item)).boxed())
            .collect();
        let num_streams = streams.len();

        match num_streams {
            0 => {
                // The case that no stream provided, return empty stream
                return stream::empty().boxed();
            }
            1 => {
                // Fast path for single stream case
                return streams
                    .into_iter()
                    .next()
                    .unwrap()
                    .and_then(|item| async move { Ok(Ok(item)) })
                    .boxed();
            }
            _ => {
                // Fall through for multiple streams
            }
        }

        let (output_tx, output_rx) = utils::channel(buf_size);
        let mut input_stream =
            stream::select_all(streams).stateful_map(key_fn, |key_fn, result| {
                let result = result.map(|(index, item)| {
                    let key = key_fn(&item);
                    (index, key, item)
                });

                Some((key_fn, result))
            });

        rt::spawn(async move {
            let mut heap: BinaryHeap<Reverse<KV<K, T>>> = BinaryHeap::new();
            let mut min_items: Vec<Option<K>> = vec![None; num_streams];
            let mut threshold: Option<K>;

            'worker: loop {
                'input: while let Some(result) = input_stream.next().await {
                    let (index, key, item) = match result {
                        Ok(tuple) => tuple,
                        Err(err) => {
                            let _ = output_tx.send_async(Err(err)).await;
                            break 'worker;
                        }
                    };

                    // update min item for that stream
                    {
                        let prev = &mut min_items[index];
                        match prev {
                            Some(prev) if *prev <= key => {
                                *prev = key.clone();
                            }
                            Some(_) => {
                                let ok = output_tx.send(Ok(Err((index, item)))).is_ok();
                                if !ok {
                                    break 'worker;
                                }
                                continue 'input;
                            }
                            None => *prev = Some(key.clone()),
                        }
                    }

                    // save item
                    heap.push(Reverse(KV {
                        index,
                        key,
                        value: item,
                    }));

                    // update global threshold
                    threshold = min_items.iter().min().unwrap().clone();

                    // pop items below threshold
                    if let Some(threshold) = &threshold {
                        'output: while let Some(Reverse(KV { key, .. })) = heap.peek() {
                            if key < threshold {
                                let KV { value, index, .. } = heap.pop().unwrap().0;
                                let ok = output_tx.send(Ok(Ok((index, value)))).is_ok();
                                if !ok {
                                    break 'worker;
                                }
                            } else {
                                break 'output;
                            }
                        }
                    }
                }

                // send remaining items
                for Reverse(KV { index, value, .. }) in heap {
                    let ok = output_tx.send(Ok(Ok((index, value)))).is_ok();
                    if !ok {
                        break 'worker;
                    }
                }

                break;
            }
        });

        output_rx.into_stream().boxed()
    }
}

// try_par_unfold

pub use try_par_unfold::*;
mod try_par_unfold {
    use super::*;

    /// Produce stream elements from a fallible parallel asynchronous task.
    ///
    /// This function spawns a set of parallel workers. Each worker produces and places
    /// items to an output buffer. The worker pool size and buffer size is determined by
    /// `params`.
    ///
    /// Each worker receives a copy of initialized state `init`, then iteratively calls the function
    /// `f(worker_index, State) -> Fut`. The `Fut` is a future that returns `Result<Option<(output, State)>, Error>`.
    /// The future updates the state and produces an output item.
    ///
    /// If a worker receives an error `Err(_)`, the error is produced in output stream and the stream halts for ever.
    /// If a worker receives a `Ok(None)`, the worker with that worker index will halt, but it does not halt
    /// the other workers. The output stream terminates after every worker halts.
    pub fn try_par_unfold<Item, Error, State, P, F, Fut>(
        params: P,
        init: State,
        f: F,
    ) -> TryParUnfold<Item, Error>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, State) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Result<Option<(Item, State)>, Error>> + Send,
        State: 'static + Send + Clone,
        Item: 'static + Send,
        Error: 'static + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let (terminate_tx, _) = broadcast::channel::<()>(1);

        (0..num_workers).for_each(move |worker_index| {
            let f = f.clone();
            let state = init.clone();
            let output_tx = output_tx.clone();
            let mut terminate_rx = terminate_tx.subscribe();
            let terminate_tx = terminate_tx.clone();

            rt::spawn(async move {
                let _ = stream::repeat(())
                    .take_until(async move {
                        let _ = terminate_rx.recv().await;
                    })
                    .map(Ok)
                    .try_stateful_then(
                        (f, terminate_tx, state),
                        |(mut f, terminate_tx, state), ()| async move {
                            let result = f(worker_index, state).await;

                            if result.is_err() {
                                let _ = terminate_tx.send(());
                            }

                            result.map(|option| {
                                option.map(|(item, state)| ((f, terminate_tx, state), item))
                            })
                        },
                    )
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });

        output_rx.into_stream().take_until_error()
    }
}

// try_par_unfold_blocking

pub use try_par_unfold_blocking::*;
mod try_par_unfold_blocking {
    use super::*;

    /// Produce stream elements from a fallible parallel asynchronous task.
    ///
    /// This function spawns a set of parallel workers. Each worker produces and places
    /// items to an output buffer. The worker pool size and buffer size is determined by
    /// `params`.
    ///
    /// Each worker receives a copy of initialized state `init`, then iteratively calls the function
    /// `f(worker_index, State) -> Result<Option<(output, State)>, Error>`, which updates the state
    /// and produces an output item.
    ///
    /// If a worker receives an error `Err(_)`, the error is produced in output stream and the stream halts for ever.
    /// If a worker receives a `Ok(None)`, the worker with that worker index will halt, but it does not halt
    /// the other workers. The output stream terminates after every worker halts.
    pub fn try_par_unfold_blocking<Item, Error, State, P, F>(
        params: P,
        init: State,
        f: F,
    ) -> TryParUnfoldBlocking<Item, Error>
    where
        F: 'static + FnMut(usize, State) -> Result<Option<(Item, State)>, Error> + Send + Clone,
        Item: 'static + Send,
        Error: 'static + Send,
        State: 'static + Send + Clone,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let terminate = Arc::new(AtomicBool::new(false));

        (0..num_workers).for_each(|worker_index| {
            let mut f = f.clone();
            let mut state = init.clone();
            let output_tx = output_tx.clone();
            let terminate = terminate.clone();

            rt::spawn_blocking(move || loop {
                if terminate.load(Acquire) {
                    break;
                }

                match f(worker_index, state) {
                    Ok(Some((item, new_state))) => {
                        let result = output_tx.send(Ok(item));
                        if result.is_err() {
                            break;
                        }
                        state = new_state;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(err) => {
                        let _ = output_tx.send(Err(err));
                        terminate.store(true, Release);
                        break;
                    }
                }
            });
        });

        output_rx.into_stream().take_until_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::async_test;
    use rand::prelude::*;

    async_test! {


        async fn sync_test() {
            {
                let stream1 = stream::iter([1, 3, 5, 7]);
                let stream2 = stream::iter([2, 4, 6, 8]);

                let collected: Vec<_> = super::sync_by_key(None, |&val| val, [stream1, stream2])
                    .collect()
                    .await;

                assert_eq!(
                    collected,
                    [
                        Ok((0, 1)),
                        Ok((1, 2)),
                        Ok((0, 3)),
                        Ok((1, 4)),
                        Ok((0, 5)),
                        Ok((1, 6)),
                        Ok((0, 7)),
                        Ok((1, 8)),
                    ]
                );
            }

            {
                let stream1 = stream::iter([1, 2, 3]);
                let stream2 = stream::iter([2, 1, 3]);

                let (synced, leaked): (Vec<_>, Vec<_>) =
                    super::sync_by_key(None, |&val| val, [stream1, stream2])
                        .map(|result| match result {
                            Ok(item) => (Some(item), None),
                            Err(item) => (None, Some(item)),
                        })
                        .unzip()
                        .await;
                let synced: Vec<_> = synced.into_iter().flatten().collect();
                let leaked: Vec<_> = leaked.into_iter().flatten().collect();

                assert_eq!(synced, [(0, 1), (0, 2), (1, 2), (0, 3), (1, 3)]);
                assert_eq!(leaked, [(1, 1)]);
            }
        }


        async fn par_unfold_test() {
            let max_quota = 100;

            let count = super::par_unfold(
                4,
                Arc::new(AtomicUsize::new(0)),
                move |_, quota| async move {
                    let enough = quota.fetch_add(1, AcqRel) < max_quota;

                    enough.then(|| {
                        let mut rng = rand::thread_rng();
                        let val = rng.gen_range(0..10);
                        (val, quota)
                    })
                },
            )
            .count()
            .await;

            assert_eq!(count, max_quota);
        }


        async fn par_unfold_blocking_test() {
            let max_quota = 100;

            let count =
                super::par_unfold_blocking(4, Arc::new(AtomicUsize::new(0)), move |_, quota| {
                    let enough = quota.fetch_add(1, AcqRel) < max_quota;

                    enough.then(|| {
                        let mut rng = rand::thread_rng();
                        let val = rng.gen_range(0..10);
                        (val, quota)
                    })
                })
                .count()
                .await;

            assert_eq!(count, max_quota);
        }


        async fn try_sync_test() {
            {
                let stream1 = stream::iter(vec![Ok(3), Ok(1), Ok(5), Ok(7)]);
                let stream2 = stream::iter(vec![Ok(2), Ok(4), Ok(6), Err("error")]);

                let mut stream = super::try_sync_by_key(None, |&val| val, [stream1, stream2]);

                let mut prev = None;
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(Ok((index, value))) => {
                            if value & 1 == 1 {
                                assert_eq!(index, 0);
                            } else {
                                assert_eq!(index, 1);
                            }

                            if let Some(prev) = prev {
                                assert!(prev < value);
                            }
                            prev = Some(value);
                        }
                        Ok(Err((index, value))) => {
                            assert_eq!(index, 0);
                            assert_eq!(value, 1);
                        }
                        Err(err) => {
                            assert_eq!(err, "error");
                            break;
                        }
                    }
                }

                assert_eq!(stream.next().await, None);
            }
        }


        async fn try_par_unfold_test() {
            let max_quota = 100;

            let mut stream = super::try_par_unfold(
                None,
                Arc::new(AtomicUsize::new(0)),
                move |index, quota| async move {
                    let enough = quota.fetch_add(1, AcqRel) < max_quota;

                    if enough {
                        Ok(Some((index, quota)))
                    } else {
                        Err("out of quota")
                    }
                },
            );

            let mut counts = HashMap::new();

            loop {
                let result = stream.next().await;

                match result {
                    Some(Ok(index)) => {
                        *counts.entry(index).or_insert_with(|| 0) += 1;
                    }
                    Some(Err("out of quota")) => {
                        break;
                    }
                    Some(Err(_)) | None => {
                        unreachable!();
                    }
                }
            }

            assert!(stream.next().await.is_none());
            assert!(counts.values().all(|&count| count <= max_quota));
            assert!(counts.values().cloned().sum::<usize>() <= max_quota);
        }


        async fn try_par_unfold_blocking_test() {
            let max_quota = 100;

            let mut stream = super::try_par_unfold_blocking(
                None,
                Arc::new(AtomicUsize::new(0)),
                move |index, quota| {
                    let enough = quota.fetch_add(1, AcqRel) < max_quota;

                    if enough {
                        Ok(Some((index, quota)))
                    } else {
                        Err("out of quota")
                    }
                },
            );

            let mut counts = HashMap::new();

            loop {
                let result = stream.next().await;

                match result {
                    Some(Ok(index)) => {
                        *counts.entry(index).or_insert_with(|| 0) += 1;
                    }
                    Some(Err("out of quota")) => {
                        break;
                    }
                    Some(Err(_)) | None => {
                        unreachable!();
                    }
                }
            }

            assert!(stream.next().await.is_none());
            assert!(counts.values().all(|&count| count <= max_quota));
            assert!(counts.values().cloned().sum::<usize>() <= max_quota);
        }


        async fn iter_blocking_test() {
            let iter = (0..2).map(|val| {
                std::thread::sleep(Duration::from_millis(100));
                val
            });

            let vec: Vec<_> = stream::select(
                super::iter_blocking(None, iter),
                future::ready(2).into_stream(),
            )
            .collect()
            .await;

            // assuming iter_blocking() will not block the executor,
            // 2 must go before 0, 1
            assert_eq!(vec, [2, 0, 1]);
        }
    }
}
