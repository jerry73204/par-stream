use crate::{common::*, config::ParParams, rt, utils};

// par_unfold

pub use par_unfold::*;

mod par_unfold {
    use super::*;

    /// Creates a stream elements produced by multiple concurrent workers.
    ///
    /// Each worker obtains the initial state by calling `init_f(worker_index)`.
    /// Then, `unfold_f(wokrer_index, state)` consumes the state and is called repeatedly.
    /// If `unfold_f` returns `Some((output, state))`, the output is produced as stream element and
    /// the state is updated. The stream finishes `unfold_f` returns `None` on all workers.
    ///
    /// The output elements collected from workers can be arbitrary ordered. There is no
    /// ordering guarantee respecting to the order of function callings and worker indexes.
    pub fn par_unfold<Item, State, P, F, Fut>(
        config: P,
        state: State,
        f: F,
    ) -> BoxStream<'static, Item>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, Arc<State>) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Option<(Item, Arc<State>)>> + Send,
        Item: 'static + Send,
        State: 'static + Sync + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = config.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let state = Arc::new(state);

        (0..num_workers).for_each(|worker_index| {
            let output_tx = output_tx.clone();
            let state = state.clone();
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

        output_rx.into_stream().boxed()
    }

    /// Creates a stream elements produced by multiple concurrent workers. It is a blocking analogous to
    /// [par_unfold()].
    pub fn par_unfold_blocking<Item, State, P, F>(
        config: P,
        state: State,
        f: F,
    ) -> BoxStream<'static, Item>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, Arc<State>) -> Option<(Item, Arc<State>)> + Send + Clone,
        Item: 'static + Send,
        State: 'static + Send + Sync,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = config.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let state = Arc::new(state);

        (0..num_workers).for_each(|worker_index| {
            let mut f = f.clone();
            let mut state = state.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || loop {
                if let Some((item, state_)) = f(worker_index, state) {
                    if output_tx.send(item).is_ok() {
                        state = state_;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            });
        });

        output_rx.into_stream().boxed()
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
        let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());

        let streams: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(index, stream)| stream.map(move |item| (index, item)).boxed())
            .collect();
        let num_streams = streams.len();

        match num_streams {
            0 => {
                return stream::empty().boxed();
            }
            1 => {
                return streams.into_iter().next().unwrap().map(Ok).boxed();
            }
            _ => {}
        }

        let mut select_stream = stream::select_all(streams);
        let (input_tx, input_rx) = utils::channel(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        let input_future = async move {
            while let Some((index, item)) = select_stream.next().await {
                let key = key_fn(&item);
                if input_tx.send_async((index, key, item)).await.is_err() {
                    break;
                }
            }
        };

        let sync_future = rt::spawn_blocking(move || {
            let mut heap: BinaryHeap<Reverse<KV<K, S::Item>>> = BinaryHeap::new();
            let mut min_items: Vec<Option<K>> = vec![None; num_streams];
            let mut threshold: Option<K>;

            'worker: loop {
                'input: while let Ok((index, key, item)) = input_rx.recv() {
                    // update min item for that stream
                    {
                        let prev = &mut min_items[index];
                        match prev {
                            Some(prev) if *prev <= key => {
                                *prev = key.clone();
                            }
                            Some(_) => {
                                let ok = output_tx.send(Err((index, item))).is_ok();
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

        let join_future = future::join(input_future, sync_future);

        utils::join_future_stream(join_future, output_rx.into_stream()).boxed()
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
        let buf_size = buf_size.into().unwrap_or_else(|| num_cpus::get());

        let streams: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(index, stream)| stream.map_ok(move |item| (index, item)).boxed())
            .collect();
        let num_streams = streams.len();

        match num_streams {
            0 => {
                return stream::empty().boxed();
            }
            1 => {
                return streams
                    .into_iter()
                    .next()
                    .unwrap()
                    .and_then(|item| async move { Ok(Ok(item)) })
                    .boxed();
            }
            _ => {}
        }

        let mut select_stream = stream::select_all(streams);
        let (input_tx, input_rx) = utils::channel(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);

        let input_future = async move {
            while let Some(result) = select_stream.next().await {
                match result {
                    Ok((index, item)) => {
                        let key = key_fn(&item);
                        if input_tx.send_async(Ok((index, key, item))).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = input_tx.send_async(Err(err)).await;
                        break;
                    }
                }
            }
        };

        let sync_future = rt::spawn_blocking(move || {
            let mut heap: BinaryHeap<Reverse<KV<K, T>>> = BinaryHeap::new();
            let mut min_items: Vec<Option<K>> = vec![None; num_streams];
            let mut threshold: Option<K>;

            'worker: loop {
                'input: while let Ok(result) = input_rx.recv() {
                    let (index, key, item) = match result {
                        Ok(tuple) => tuple,
                        Err(err) => {
                            let _ = output_tx.send(Err(err));
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

        let join_future = future::join(input_future, sync_future);

        stream::select(
            join_future.into_stream().map(|_| None),
            output_rx.into_stream().map(|item| Some(item)),
        )
        .filter_map(|item| async move { item })
        .boxed()
    }
}

// try_par_unfold

pub use try_par_unfold::*;

mod try_par_unfold {
    use super::*;

    /// A fallible analogue to [par_unfold()](super::par_unfold()).
    pub fn try_par_unfold<Item, Error, State, P, F, Fut>(
        config: P,
        state: State,
        f: F,
    ) -> BoxStream<'static, Result<Item, Error>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(usize, Arc<State>) -> Fut + Send + Clone,
        Fut: 'static + Future<Output = Result<Option<(Item, Arc<State>)>, Error>> + Send,
        State: 'static + Sync + Send,
        Item: 'static + Send,
        Error: 'static + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = config.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let terminate = Arc::new(AtomicBool::new(false));
        let state = Arc::new(state);

        let worker_futs = (0..num_workers).map(move |worker_index| {
            let mut f = f.clone();
            let mut state = state.clone();
            let output_tx = output_tx.clone();
            let terminate = terminate.clone();

            rt::spawn(async move {
                loop {
                    if terminate.load(Acquire) {
                        break;
                    }

                    match f(worker_index, state).await {
                        Ok(Some((item, new_state))) => {
                            let result = output_tx.send_async(Ok(item)).await;
                            if result.is_err() {
                                break;
                            }
                            state = new_state;
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(err) => {
                            let _ = output_tx.send_async(Err(err)).await;
                            terminate.store(true, Release);
                            break;
                        }
                    }
                }
            })
        });

        let join_future = future::join_all(worker_futs);

        stream::select(
            output_rx.into_stream().map(Some),
            join_future.map(|_| None).into_stream(),
        )
        .filter_map(|item| async move { item })
        .scan(false, |terminated, result| {
            let output = if *terminated {
                None
            } else {
                if result.is_err() {
                    *terminated = true;
                }
                Some(result)
            };

            async move { output }
        })
        .fuse()
        .boxed()
    }

    /// A fallible analogue to [par_unfold_blocking()](super::par_unfold_blocking).
    pub fn try_par_unfold_blocking<Item, Error, State, P, F>(
        config: P,
        state: State,
        f: F,
    ) -> BoxStream<'static, Result<Item, Error>>
    where
        F: 'static
            + FnMut(usize, Arc<State>) -> Result<Option<(Item, Arc<State>)>, Error>
            + Send
            + Clone,
        Item: 'static + Send,
        Error: 'static + Send,
        State: 'static + Sync + Send,
        P: Into<ParParams>,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = config.into();
        let (output_tx, output_rx) = utils::channel(buf_size);
        let terminate = Arc::new(AtomicBool::new(false));
        let state = Arc::new(state);

        (0..num_workers).for_each(|worker_index| {
            let mut f = f.clone();
            let mut state = state.clone();
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

        output_rx
            .into_stream()
            .scan(false, |terminated, result| {
                let output = if *terminated {
                    None
                } else {
                    if result.is_err() {
                        *terminated = true;
                    }
                    Some(result)
                };

                async move { output }
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[tokio::test]
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

    #[tokio::test]
    async fn par_unfold_test() {
        let max_quota = 100;

        let count = super::par_unfold(4, AtomicUsize::new(0), move |_, quota| async move {
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
    #[tokio::test]
    async fn par_unfold_blocking_test() {
        let max_quota = 100;

        let count = super::par_unfold_blocking(4, AtomicUsize::new(0), move |_, quota| {
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

    #[tokio::test]
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

    #[tokio::test]
    async fn try_par_unfold_test() {
        let max_quota = 100;

        let mut stream =
            super::try_par_unfold(None, AtomicUsize::new(0), move |index, quota| async move {
                let enough = quota.fetch_add(1, AcqRel) < max_quota;

                if enough {
                    Ok(Some((index, quota)))
                } else {
                    Err("out of quota")
                }
            });

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

    #[tokio::test]
    async fn try_par_unfold_blocking_test() {
        let max_quota = 100;

        let mut stream =
            super::try_par_unfold_blocking(None, AtomicUsize::new(0), move |index, quota| {
                let enough = quota.fetch_add(1, AcqRel) < max_quota;

                if enough {
                    Ok(Some((index, quota)))
                } else {
                    Err("out of quota")
                }
            });

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
}
