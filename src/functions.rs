use crate::{
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
    rt, utils,
};

// iter_spawned

pub use iter_spawned::*;

mod iter_spawned {
    use super::*;

    /// Converts an [Iterator] into a [Stream] by consuming the iterator in a blocking thread.
    ///
    /// It is useful when consuming the iterator is computationally expensive and involves blocking code.
    /// It prevents blocking the asynchronous context when consuming the returned stream.
    pub fn iter_spawned<I>(buf_size: usize, iter: I) -> IterSpawned<I::Item>
    where
        I: 'static + IntoIterator + Send,
        I::Item: Send,
    {
        let (tx, rx) = flume::bounded(buf_size);

        rt::spawn_blocking(move || {
            for item in iter.into_iter() {
                if tx.send(item).is_err() {
                    break;
                }
            }
        });

        IterSpawned {
            stream: rx.into_stream(),
        }
    }

    /// Stream for the [iter_spawned] function.
    #[derive(Clone)]
    pub struct IterSpawned<T>
    where
        T: 'static,
    {
        stream: flume::r#async::RecvStream<'static, T>,
    }

    impl<T> Stream for IterSpawned<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// unfold

pub use unfold::*;

mod unfold {
    use super::*;

    /// Creates a stream with elements produced by an asynchronous function.
    ///
    /// The `init_f` function creates an initial state. Then `unfold_f` consumes the state
    /// and is called repeatedly. If `unfold_f` returns `Some(output, state)`, it produces
    /// the output as stream element and updates the state, until it returns `None`.
    pub fn unfold<IF, UF, IFut, UFut, State, Item>(
        buf_size: impl Into<Option<usize>>,
        mut init_f: IF,
        mut unfold_f: UF,
    ) -> Unfold<Item>
    where
        IF: 'static + FnMut() -> IFut + Send,
        UF: 'static + FnMut(State) -> UFut + Send,
        IFut: Future<Output = State> + Send,
        UFut: Future<Output = Option<(Item, State)>> + Send,
        State: Send,
        Item: 'static + Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
        let (data_tx, data_rx) = flume::bounded(buf_size);

        let producer_fut = rt::spawn(async move {
            let mut state = init_f().await;

            while let Some((item, new_state)) = unfold_f(state).await {
                let result = data_tx.send_async(item).await;
                if result.is_err() {
                    break;
                }
                state = new_state;
            }
        });

        let stream = stream::select(
            producer_fut
                .into_stream()
                .map(|result| {
                    if let Err(err) = result {
                        panic!("unable to spawn a worker: {:?}", err);
                    }
                    None
                })
                .fuse(),
            data_rx.into_stream().map(Some),
        )
        .filter_map(|item| async move { item })
        .boxed();

        Unfold { stream }
    }

    /// Creates a stream with elements produced by a function.
    ///
    /// The `init_f` function creates an initial state. Then `unfold_f` consumes the state
    /// and is called repeatedly. If `unfold_f` returns `Some(output, state)`, it produces
    /// the output as stream element and updates the state, until it returns `None`.
    pub fn unfold_blocking<IF, UF, State, Item>(
        buf_size: impl Into<Option<usize>>,
        mut init_f: IF,
        mut unfold_f: UF,
    ) -> Unfold<Item>
    where
        IF: 'static + FnMut() -> State + Send,
        UF: 'static + FnMut(State) -> Option<(Item, State)> + Send,
        Item: 'static + Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
        let (data_tx, data_rx) = flume::bounded(buf_size);

        let producer_fut = rt::spawn_blocking(move || {
            let mut state = init_f();

            while let Some((item, new_state)) = unfold_f(state) {
                let result = data_tx.send(item);
                if result.is_err() {
                    break;
                }
                state = new_state;
            }
        });

        let stream = stream::select(
            producer_fut
                .into_stream()
                .map(|result| {
                    if let Err(err) = result {
                        panic!("unable to spawn a worker: {:?}", err);
                    }
                    None
                })
                .fuse(),
            data_rx.into_stream().map(Some),
        )
        .filter_map(|item| async move { item })
        .boxed();

        Unfold { stream }
    }

    /// A stream combinator returned from [unfold()](super::unfold())
    /// or [unfold_blocking()](super::unfold_blocking()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct Unfold<T> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, T>,
    }

    impl<T> Stream for Unfold<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// par_unfold_unordered

pub use par_unfold_unordered::*;

mod par_unfold_unordered {
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
    pub fn par_unfold_unordered<P, IF, UF, IFut, UFut, State, Item>(
        config: P,
        mut init_f: IF,
        unfold_f: UF,
    ) -> ParUnfoldUnordered<Item>
    where
        IF: 'static + FnMut(usize) -> IFut,
        UF: 'static + FnMut(usize, State) -> UFut + Send + Clone,
        IFut: 'static + Future<Output = State> + Send,
        UFut: 'static + Future<Output = Option<(Item, State)>> + Send,
        State: Send,
        Item: 'static + Send,
        P: IntoParStreamParams,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (output_tx, output_rx) = flume::bounded(buf_size);

        let worker_futs = (0..num_workers).map(|worker_index| {
            let init_fut = init_f(worker_index);
            let mut unfold_f = unfold_f.clone();
            let output_tx = output_tx.clone();

            rt::spawn(async move {
                let mut state = init_fut.await;

                loop {
                    match unfold_f(worker_index, state).await {
                        Some((item, new_state)) => {
                            let result = output_tx.send_async(item).await;
                            if result.is_err() {
                                break;
                            }
                            state = new_state;
                        }
                        None => {
                            break;
                        }
                    }
                }
            })
        });

        let join_future = future::try_join_all(worker_futs);

        let stream = stream::select(
            output_rx.into_stream().map(Some),
            join_future.into_stream().map(|result| {
                result.unwrap();
                None
            }),
        )
        .filter_map(|item| async move { item })
        .boxed();

        ParUnfoldUnordered { stream }
    }

    /// Creates a stream elements produced by multiple concurrent workers. It is a blocking analogous to
    /// [par_unfold_unordered()].
    pub fn par_unfold_blocking_unordered<P, IF, UF, State, Item>(
        config: P,
        init_f: IF,
        unfold_f: UF,
    ) -> ParUnfoldUnordered<Item>
    where
        IF: 'static + FnMut(usize) -> State + Send + Clone,
        UF: 'static + FnMut(usize, State) -> Option<(Item, State)> + Send + Clone,
        Item: 'static + Send,
        P: IntoParStreamParams,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (output_tx, output_rx) = flume::bounded(buf_size);

        let worker_futs = (0..num_workers).map(|worker_index| {
            let mut init_f = init_f.clone();
            let mut unfold_f = unfold_f.clone();
            let output_tx = output_tx.clone();

            rt::spawn_blocking(move || {
                let mut state = init_f(worker_index);

                loop {
                    match unfold_f(worker_index, state) {
                        Some((item, new_state)) => {
                            let result = output_tx.send(item);
                            if result.is_err() {
                                break;
                            }
                            state = new_state;
                        }
                        None => {
                            break;
                        }
                    }
                }
            })
        });

        let join_future = future::try_join_all(worker_futs);

        let stream = stream::select(
            output_rx.into_stream().map(Some),
            join_future.into_stream().map(|result| {
                result.unwrap();
                None
            }),
        )
        .filter_map(|item| async move { item })
        .boxed();

        ParUnfoldUnordered { stream }
    }

    /// A stream combinator returned from [par_unfold_unordered()](super::par_unfold_unordered())
    /// and  [par_unfold_blocking_unordered()](super::par_unfold_blocking_unordered()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ParUnfoldUnordered<T> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, T>,
    }

    impl<T> Stream for ParUnfoldUnordered<T> {
        type Item = T;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
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
    ) -> Sync<S::Item>
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
                return Sync {
                    stream: stream::empty().boxed(),
                };
            }
            1 => {
                return Sync {
                    stream: streams.into_iter().next().unwrap().map(Ok).boxed(),
                };
            }
            _ => {}
        }

        let mut select_stream = stream::select_all(streams);
        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);

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
        })
        .map(|result| result.unwrap());

        let join_future = future::join(input_future, sync_future);

        let stream = utils::join_future_stream(join_future, output_rx.into_stream()).boxed();

        Sync { stream }
    }

    /// A stream combinator returned from [sync_by_key()](super::sync_by_key()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct Sync<T> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, Result<(usize, T), (usize, T)>>,
    }

    impl<T> Stream for Sync<T> {
        type Item = Result<(usize, T), (usize, T)>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
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
    ) -> TrySync<T, E>
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
                return TrySync {
                    stream: stream::empty().boxed(),
                };
            }
            1 => {
                return TrySync {
                    stream: streams
                        .into_iter()
                        .next()
                        .unwrap()
                        .and_then(|item| async move { Ok(Ok(item)) })
                        .boxed(),
                };
            }
            _ => {}
        }

        let mut select_stream = stream::select_all(streams);
        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);

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
        })
        .map(|result| result.unwrap());

        let join_future = future::join(input_future, sync_future);

        let stream = stream::select(
            join_future.into_stream().map(|_| None),
            output_rx.into_stream().map(|item| Some(item)),
        )
        .filter_map(|item| async move { item })
        .boxed();

        TrySync { stream }
    }

    /// A stream combinator returned from [try_sync_by_key()](super::try_sync_by_key()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TrySync<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, Result<Result<(usize, T), (usize, T)>, E>>,
    }

    impl<T, E> Stream for TrySync<T, E> {
        type Item = Result<Result<(usize, T), (usize, T)>, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_unfold_blocking

pub use try_unfold_blocking::*;

mod try_unfold_blocking {
    use super::*;

    /// A fallible analogue to [unfold_blocking](crate::stream::unfold_blocking).
    pub fn try_unfold_blocking<IF, UF, State, Item, Error>(
        buf_size: impl Into<Option<usize>>,
        mut init_f: IF,
        mut unfold_f: UF,
    ) -> TryUnfoldBlocking<Item, Error>
    where
        IF: 'static + FnMut() -> Result<State, Error> + Send,
        UF: 'static + FnMut(State) -> Result<Option<(Item, State)>, Error> + Send,
        Item: 'static + Send,
        Error: 'static + Send,
    {
        let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
        let (data_tx, data_rx) = flume::bounded(buf_size);

        let producer_fut = rt::spawn_blocking(move || {
            let mut state = match init_f() {
                Ok(state) => state,
                Err(err) => {
                    let _ = data_tx.send(Err(err));
                    return;
                }
            };

            loop {
                match unfold_f(state) {
                    Ok(Some((item, new_state))) => {
                        let result = data_tx.send(Ok(item));
                        if result.is_err() {
                            break;
                        }
                        state = new_state;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = data_tx.send(Err(err));
                        break;
                    }
                }
            }
        });

        let stream = stream::select(
            producer_fut
                .into_stream()
                .map(|result| {
                    if let Err(err) = result {
                        panic!("unable to spawn a worker: {:?}", err);
                    }
                    None
                })
                .fuse(),
            data_rx
                .into_stream()
                .map(|item: Result<Item, Error>| Some(item)),
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryUnfoldBlocking { stream }
    }

    /// A fallible stream combinator returned from [try_unfold_blocking()](super::try_unfold_blocking()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryUnfoldBlocking<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, Result<T, E>>,
    }

    impl<T, E> Stream for TryUnfoldBlocking<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_unfold_unordered

pub use try_par_unfold_unordered::*;

mod try_par_unfold_unordered {
    use super::*;

    /// A fallible analogue to [par_unfold_unordered](crate::stream::par_unfold_unordered).
    pub fn try_par_unfold_unordered<P, IF, UF, IFut, UFut, State, Item, Error>(
        config: P,
        mut init_f: IF,
        unfold_f: UF,
    ) -> TryParUnfoldUnordered<Item, Error>
    where
        IF: 'static + FnMut(usize) -> IFut,
        UF: 'static + FnMut(usize, State) -> UFut + Send + Clone,
        IFut: 'static + Future<Output = Result<State, Error>> + Send,
        UFut: 'static + Future<Output = Result<Option<(Item, State)>, Error>> + Send,
        State: Send,
        Item: 'static + Send,
        Error: 'static + Send,
        P: IntoParStreamParams,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let terminate = Arc::new(AtomicBool::new(false));

        let worker_futs = (0..num_workers).map(move |worker_index| {
            let init_fut = init_f(worker_index);
            let mut unfold_f = unfold_f.clone();
            let output_tx = output_tx.clone();
            let terminate = terminate.clone();

            rt::spawn(async move {
                let mut state = match init_fut.await {
                    Ok(state) => state,
                    Err(err) => {
                        let _ = output_tx.send_async(Err(err)).await;
                        terminate.store(true, Release);
                        return;
                    }
                };

                loop {
                    if terminate.load(Acquire) {
                        break;
                    }

                    match unfold_f(worker_index, state).await {
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
            .map(|result| result.unwrap())
        });

        let join_future = future::join_all(worker_futs);

        let stream = stream::select(
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
        .boxed();

        TryParUnfoldUnordered { stream }
    }

    /// A fallible analogue to [par_unfold_blocking_unordered](crate::stream::par_unfold_blocking_unordered).
    pub fn try_par_unfold_blocking_unordered<P, IF, UF, State, Item, Error>(
        config: P,
        init_f: IF,
        unfold_f: UF,
    ) -> TryParUnfoldUnordered<Item, Error>
    where
        IF: 'static + FnMut(usize) -> Result<State, Error> + Send + Clone,
        UF: 'static + FnMut(usize, State) -> Result<Option<(Item, State)>, Error> + Send + Clone,
        Item: 'static + Send,
        Error: 'static + Send,
        P: IntoParStreamParams,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let terminate = Arc::new(AtomicBool::new(false));

        let worker_futs = (0..num_workers).map(|worker_index| {
            let mut init_f = init_f.clone();
            let mut unfold_f = unfold_f.clone();
            let output_tx = output_tx.clone();
            let terminate = terminate.clone();

            rt::spawn_blocking(move || {
                let mut state = match init_f(worker_index) {
                    Ok(state) => state,
                    Err(err) => {
                        let _ = output_tx.send(Err(err));
                        terminate.store(true, Release);
                        return;
                    }
                };

                loop {
                    if terminate.load(Acquire) {
                        break;
                    }

                    match unfold_f(worker_index, state) {
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
                }
            })
        });

        let join_future = future::try_join_all(worker_futs);

        let stream = stream::select(
            output_rx.into_stream().map(Some),
            join_future.into_stream().map(|result| {
                result.unwrap();
                None
            }),
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
        .boxed();

        TryParUnfoldUnordered { stream }
    }

    /// A stream combinator returned from [try_par_unfold_unordered()](super::try_par_unfold_unordered())
    /// and  [try_par_unfold_blocking_unordered()](super::try_par_unfold_blocking_unordered()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParUnfoldUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxStream<'static, Result<T, E>>,
    }

    impl<T, E> Stream for TryParUnfoldUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

#[cfg(tests)]
mod tests {
    use super::*;

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
        let numbers: Vec<_> = super::par_unfold_unordered(
            4,
            |index| async move { (index + 1) * 100 },
            |index, quota| async move {
                (quota > 0).then(|| {
                    let mut rng = rand::thread_rng();
                    let val = rng.gen_range(0..10) + index * 100;
                    (val, quota - 1)
                })
            },
        )
        .collect()
        .await;

        let counts = numbers
            .iter()
            .map(|val| {
                let worker_index = val / 100;
                let number = val - worker_index * 100;
                assert!(number < 10);
                (worker_index, 1)
            })
            .into_grouping_map()
            .sum();

        assert_eq!(counts.len(), 4);
        assert!((0..4).all(|worker_index| counts[&worker_index] == (worker_index + 1) * 100));
    }
    #[tokio::test]
    async fn par_unfold_blocking_test() {
        let numbers: Vec<_> = super::par_unfold_blocking_unordered(
            4,
            |index| {
                let rng = rand::thread_rng();
                let quota = (index + 1) * 100;
                (rng, quota)
            },
            |index, (mut rng, quota)| {
                (quota > 0).then(|| {
                    let val = rng.gen_range(0..10) + index * 100;
                    (val, (rng, quota - 1))
                })
            },
        )
        .collect()
        .await;

        let counts = numbers
            .iter()
            .map(|val| {
                let worker_index = val / 100;
                let number = val - worker_index * 100;
                assert!(number < 10);
                (worker_index, 1)
            })
            .into_grouping_map()
            .sum();

        assert_eq!(counts.len(), 4);
        assert!((0..4).all(|worker_index| counts[&worker_index] == (worker_index + 1) * 100));
    }

    #[tokio::test]
    async fn unfold_blocking_test() {
        {
            let numbers: Vec<_> = super::unfold_blocking(
                None,
                || 0,
                move |count| {
                    let output = count;
                    if output < 1000 {
                        Some((output, count + 1))
                    } else {
                        None
                    }
                },
            )
            .collect()
            .await;

            let expect: Vec<_> = (0..1000).collect();

            assert_eq!(numbers, expect);
        }

        {
            let numbers: Vec<_> = super::unfold_blocking(
                None,
                || (0, rand::thread_rng()),
                move |(acc, mut rng)| {
                    let val = rng.gen_range(1..=10);
                    let acc = acc + val;
                    if acc < 100 {
                        Some((acc, (acc, rng)))
                    } else {
                        None
                    }
                },
            )
            .collect()
            .await;

            assert!(numbers.iter().all(|&val| val < 100));
            assert!(izip!(&numbers, numbers.iter().skip(1)).all(|(&prev, &next)| prev < next));
        }
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
    async fn try_unfold_blocking_test() {
        {
            let mut stream =
                super::try_unfold_blocking(None, || Err("init error"), |()| Ok(Some(((), ()))));
            assert_eq!(stream.next().await, Some(Err("init error")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = super::try_unfold_blocking(
                None,
                || Ok(0),
                |count| {
                    if count < 3 {
                        Ok(Some(((), count + 1)))
                    } else {
                        Err("exceed")
                    }
                },
            );
            assert_eq!(stream.next().await, Some(Ok(())));
            assert_eq!(stream.next().await, Some(Ok(())));
            assert_eq!(stream.next().await, Some(Ok(())));
            assert_eq!(stream.next().await, Some(Err("exceed")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = super::try_unfold_blocking(
                None,
                || Result::<_, ()>::Ok(0),
                |count| {
                    if count < 3 {
                        Ok(Some(((), count + 1)))
                    } else {
                        Ok(None)
                    }
                },
            );
            assert_eq!(stream.next().await, Some(Ok(())));
            assert_eq!(stream.next().await, Some(Ok(())));
            assert_eq!(stream.next().await, Some(Ok(())));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_par_unfold_test() {
        let mut stream = super::try_par_unfold_unordered(
            4,
            |_index| async move { Ok(5) },
            |index, quota| async move {
                if quota > 0 {
                    Ok(Some((index, quota - 1)))
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
        assert!(counts.values().all(|&count| count <= 5));
    }

    #[tokio::test]
    async fn try_par_unfold_blocking_test() {
        let mut stream = super::try_par_unfold_blocking_unordered(
            4,
            |_index| Ok(5),
            |index, quota| {
                if quota > 0 {
                    Ok(Some((index, quota - 1)))
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
        assert!(counts.values().all(|&count| count <= 5));
    }
}
