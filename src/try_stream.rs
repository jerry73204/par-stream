use crate::{
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
    rt,
    stream::{batching_channel, BatchingReceiver, BatchingSender},
    utils::{BoxedFuture, BoxedStream},
};
use tokio::sync::{broadcast, Mutex};

/// An extension trait that controls ordering of items of fallible streams.
pub trait FallibleIndexedStreamExt
where
    Self: TryStream,
{
    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_enumerate<T, E>(self) -> TryEnumerate<Self, T, E>
    where
        Self: Stream<Item = Result<T, E>>;

    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_wrapping_enumerate<T, E>(self) -> TryWrappingEnumerate<Self, T, E>
    where
        Self: Stream<Item = Result<T, E>>;

    /// Creates a fallible stream that reorders the items according to the iteration count.
    ///
    /// It is usually combined with [try_wrapping_enumerate](FallibleIndexedStreamExt::try_wrapping_enumerate).
    fn try_reorder_enumerated<T, E>(self) -> TryReorderEnumerated<Self, T, E>
    where
        Self: Stream<Item = Result<(usize, T), E>>;
}

impl<S> FallibleIndexedStreamExt for S
where
    S: TryStream,
{
    fn try_enumerate<T, E>(self) -> TryEnumerate<Self, T, E>
    where
        Self: Stream<Item = Result<T, E>>,
    {
        TryEnumerate {
            stream: self,
            counter: 0,
            fused: false,
            _phantom: PhantomData,
        }
    }

    fn try_wrapping_enumerate<T, E>(self) -> TryWrappingEnumerate<Self, T, E>
    where
        Self: Stream<Item = Result<T, E>>,
    {
        TryWrappingEnumerate {
            stream: self,
            counter: 0,
            fused: false,
            _phantom: PhantomData,
        }
    }

    fn try_reorder_enumerated<T, E>(self) -> TryReorderEnumerated<Self, T, E>
    where
        Self: Stream<Item = Result<(usize, T), E>>,
    {
        TryReorderEnumerated {
            stream: self,
            commit: 0,
            fused: false,
            buffer: HashMap::new(),
            _phantom: PhantomData,
        }
    }
}

/// An extension trait that provides fallible combinators for parallel processing on streams.
pub trait FallibleParStreamExt
where
    Self: 'static + Send + TryStream + FallibleIndexedStreamExt,
{
    /// A fallible analogue to [then_spawned](crate::ParStreamExt::then_spawned).
    fn try_then_spawned<T, U, E, F, Fut>(
        self,
        buf_size: impl Into<Option<usize>>,
        f: F,
    ) -> TryThenSpawned<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: Future<Output = Result<U, E>> + Send;

    /// A fallible analogue to [map_spawned](crate::ParStreamExt::map_spawned).
    fn try_map_spawned<T, U, E, F>(
        self,
        buf_size: impl Into<Option<usize>>,
        f: F,
    ) -> TryMapSpawned<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Result<U, E> + Send;

    /// A fallible analogue to [batching](crate::ParStreamExt::batching) that consumes
    /// as many elements as it likes for each next output element.
    fn try_batching<T, U, E, F, Fut>(self, f: F) -> TryBatching<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        Self: Stream<Item = Result<T, E>>,
        F: FnOnce(BatchingReceiver<T>, BatchingSender<U>) -> Fut,
        Fut: 'static + Future<Output = Result<(), E>> + Send;

    /// A fallible analogue to [par_batching_unordered](crate::ParStreamExt::par_batching_unordered).
    fn try_par_batching_unordered<T, U, E, P, F, Fut>(
        self,
        config: P,
        f: F,
    ) -> TryParBatchingUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        F: FnMut(usize, flume::Receiver<T>, flume::Sender<U>) -> Fut,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        P: IntoParStreamParams;

    /// A fallible analogue to [tee](crate::ParStreamExt::tee) that stops sending items when
    /// receiving an error.
    fn try_tee<T, E>(self, buf_size: impl Into<Option<usize>>) -> TryTee<T, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send + Clone,
        E: 'static + Send + Clone;

    /// A fallible analogue to [par_then](crate::ParStreamExt::par_then).
    fn try_par_then<P, T, U, E, F, Fut>(self, config: P, f: F) -> TryParThen<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send;

    /// A fallible analogue to [par_then_init](crate::ParStreamExt::par_then_init).
    fn try_par_then_init<P, T, U, E, B, InitF, ThenF, Fut>(
        self,
        config: P,
        init_f: InitF,
        map_f: ThenF,
    ) -> TryParThen<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        ThenF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send;

    /// A fallible analogue to [par_then_unordered](crate::ParStreamExt::par_then_unordered).
    fn try_par_then_unordered<P, T, U, E, F, Fut>(
        self,
        config: P,
        f: F,
    ) -> TryParThenUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        U: 'static + Send,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        P: IntoParStreamParams;

    /// A fallible analogue to [par_then_init_unordered](crate::ParStreamExt::par_then_init_unordered).
    fn try_par_then_init_unordered<P, T, U, E, B, InitF, ThenF, Fut>(
        self,
        config: P,
        init_f: InitF,
        map_f: ThenF,
    ) -> TryParThenUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        ThenF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send;

    /// A fallible analogue to [par_map](crate::ParStreamExt::par_map).
    fn try_par_map<P, T, U, E, F, Func>(self, config: P, f: F) -> TryParMap<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send;

    /// A fallible analogue to [par_map_init](crate::ParStreamExt::par_map_init).
    fn try_par_map_init<P, T, U, E, B, InitF, MapF, Func>(
        self,
        config: P,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMap<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send;

    /// A fallible analogue to [par_map_unordered](crate::ParStreamExt::par_map_unordered).
    fn try_par_map_unordered<P, T, U, E, F, Func>(
        self,
        config: P,
        f: F,
    ) -> TryParMapUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send;

    /// A fallible analogue to [par_map_init_unordered](crate::ParStreamExt::par_map_init_unordered).
    fn try_par_map_init_unordered<P, T, U, E, B, InitF, MapF, Func>(
        self,
        config: P,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParMapUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send;

    /// Runs this stream to completion, executing asynchronous closure for each element on the stream
    /// in parallel.
    fn try_par_for_each<P, T, E, F, Fut>(self, config: P, f: F) -> TryParForEach<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), E>> + Send;

    /// A fallible analogue to [par_for_each_init](crate::ParStreamExt::par_for_each_init).
    fn try_par_for_each_init<P, T, E, B, InitF, MapF, Fut>(
        self,
        config: P,
        init_f: InitF,
        map_f: MapF,
    ) -> TryParForEach<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), E>> + Send;

    /// A fallible analogue to [par_for_each_blocking](crate::ParStreamExt::par_for_each_blocking).
    fn try_par_for_each_blocking<P, T, E, F, Func>(
        self,
        config: P,
        f: F,
    ) -> TryParForEachBlocking<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), E> + Send;

    /// A fallible analogue to [par_for_each_blocking_init](crate::ParStreamExt::par_for_each_blocking_init).
    fn try_par_for_each_blocking_init<P, T, E, B, InitF, MapF, Func>(
        self,
        config: P,
        init_f: InitF,
        f: MapF,
    ) -> TryParForEachBlocking<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), E> + Send;
}

impl<S> FallibleParStreamExt for S
where
    S: 'static + Send + TryStream + FallibleIndexedStreamExt,
{
    fn try_then_spawned<T, U, E, F, Fut>(
        self,
        buf_size: impl Into<Option<usize>>,
        mut f: F,
    ) -> TryThenSpawned<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: Future<Output = Result<U, E>> + Send,
    {
        let buf_size = buf_size.into().unwrap_or(2);
        let (tx, rx) = flume::bounded(buf_size);

        let future = rt::spawn(async move {
            let mut stream = self.boxed();

            loop {
                match stream.next().await {
                    Some(Ok(input)) => {
                        let output = f(input).await;
                        let is_err = output.is_err();

                        if tx.send_async(output).await.is_err() {
                            break;
                        }
                        if is_err {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        let _ = tx.send_async(Err(err)).await;
                        break;
                    }
                    None => break,
                }
            }
        })
        .map(|result| result.unwrap());

        let stream = futures::stream::select(
            rx.into_stream().map(Some),
            future.map(|()| None).into_stream(),
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryThenSpawned { stream }
    }

    fn try_map_spawned<T, U, E, F>(
        self,
        buf_size: impl Into<Option<usize>>,
        mut f: F,
    ) -> TryMapSpawned<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Result<U, E> + Send,
    {
        let buf_size = buf_size.into().unwrap_or(2);
        let (tx, rx) = flume::bounded(buf_size);

        let future = rt::spawn_blocking(move || {
            let mut stream = self.boxed();

            loop {
                match rt::block_on(stream.next()) {
                    Some(Ok(input)) => {
                        let output = f(input);
                        let is_err = output.is_err();

                        if tx.send(output).is_err() {
                            break;
                        }
                        if is_err {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        let _ = tx.send(Err(err));
                        break;
                    }
                    None => break,
                }
            }
        })
        .map(|result| result.unwrap());

        let stream = futures::stream::select(
            rx.into_stream().map(Some),
            future.map(|()| None).into_stream(),
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryMapSpawned { stream }
    }

    fn try_batching<T, U, E, F, Fut>(self, f: F) -> TryBatching<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        Self: Stream<Item = Result<T, E>>,
        F: FnOnce(BatchingReceiver<T>, BatchingSender<U>) -> Fut,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
    {
        let mut stream = self.boxed();

        let (mut input_tx, input_rx) = batching_channel();
        let (output_tx, output_rx) = batching_channel();

        let input_future = async move {
            while let Some(item) = stream.try_next().await? {
                let result = input_tx.send(item).await;
                if result.is_err() {
                    break;
                }
            }
            Ok(())
        };
        let batching_future = f(input_rx, output_tx);
        let join_future = futures::future::try_join(input_future, batching_future);

        let output_stream = futures::stream::unfold(output_rx, move |mut output_rx| async move {
            output_rx.recv().await.map(|output| (output, output_rx))
        });
        let select_stream = futures::stream::select(
            output_stream.map(|item| Ok(Some(item))),
            join_future.into_stream().map(|result| result.map(|_| None)),
        )
        .boxed();

        let stream = futures::stream::try_unfold(
            (Some(select_stream), None),
            move |(mut stream, error)| async move {
                if let Some(stream_) = &mut stream {
                    match stream_.next().await {
                        Some(Ok(Some(output))) => return Ok(Some((Some(output), (stream, error)))),
                        Some(Ok(None)) => {
                            return Ok(Some((None, (stream, error))));
                        }
                        Some(Err(err)) => {
                            return Ok(Some((None, (stream, Some(err)))));
                        }
                        None => {
                            // stream = None;
                        }
                    }
                }

                if let Some(error) = error {
                    return Err(error);
                }

                Ok(None)
            },
        )
        .try_filter_map(|item| async move { Ok(item) })
        .boxed();

        TryBatching { stream }
    }

    fn try_par_batching_unordered<T, U, E, P, F, Fut>(
        self,
        config: P,
        mut f: F,
    ) -> TryParBatchingUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: FnMut(usize, flume::Receiver<T>, flume::Sender<U>) -> Fut,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();

        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);

        let input_fut = rt::spawn(async move {
            let mut stream = self.boxed();

            while let Some(item) = stream.next().await {
                let result = input_tx.send_async(item?).await;
                if result.is_err() {
                    break;
                }
            }
            Ok(())
        })
        .map(|result| result.unwrap());

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|worker_index| {
                let fut = f(worker_index, input_rx.clone(), output_tx.clone());
                rt::spawn(fut).map(|result| result.unwrap())
            })
            .collect();

        let join_fut =
            futures::future::try_join(input_fut, futures::future::try_join_all(worker_futs))
                .map(|result| result.map(|_| ()));

        let select_stream = futures::stream::select(
            output_rx.into_stream().map(|item| Ok(Some(item))),
            join_fut.into_stream().map(|result| result.map(|()| None)),
        )
        .boxed();

        let stream = futures::stream::try_unfold(
            (Some(select_stream), None),
            |(mut stream, error)| async move {
                if let Some(stream_) = &mut stream {
                    match stream_.next().await {
                        Some(Ok(Some(item))) => {
                            return Ok(Some((Some(item), (stream, error))));
                        }
                        Some(Ok(None)) => {
                            return Ok(Some((None, (stream, error))));
                        }
                        Some(Err(err)) => {
                            return Ok(Some((None, (stream, Some(err)))));
                        }
                        None => {}
                    }
                }

                if let Some(error) = error {
                    return Err(error);
                }

                Ok(None)
            },
        )
        .try_filter_map(|item| async move { Ok(item) })
        .boxed();

        TryParBatchingUnordered { stream }
    }

    fn try_tee<T, E>(self, buf_size: impl Into<Option<usize>>) -> TryTee<T, E>
    where
        Self: Stream<Item = Result<T, E>>,
        T: 'static + Send + Clone,
        E: 'static + Send + Clone,
    {
        let buf_size = buf_size.into();
        let (tx, rx) = match buf_size {
            Some(buf_size) => flume::bounded(buf_size),
            None => flume::unbounded(),
        };
        let sender_set = Arc::new(flurry::HashSet::new());
        let guard = sender_set.guard();
        sender_set.insert(ByAddress(Arc::new(tx)), &guard);

        let future = {
            let sender_set = sender_set.clone();

            let future = rt::spawn(async move {
                let mut stream = self.boxed();

                while let Some(item) = stream.next().await {
                    let futures: Vec<_> = sender_set
                        .pin()
                        .iter()
                        .map(|tx| {
                            let tx = tx.clone();
                            let item = item.clone();
                            async move {
                                let result = tx.send_async(item).await;
                                (result, tx)
                            }
                        })
                        .collect();

                    let results = futures::future::join_all(futures).await;
                    let success_count = results
                        .iter()
                        .filter(|(result, tx)| {
                            let ok = result.is_ok();
                            if !ok {
                                sender_set.pin().remove(tx);
                            }
                            ok
                        })
                        .count();

                    if item.is_err() || success_count == 0 {
                        break;
                    }
                }
            });

            Arc::new(Mutex::new(Some(future)))
        };

        TryTee {
            future,
            sender_set: Arc::downgrade(&sender_set),
            receiver: rx,
            buf_size,
        }
    }

    fn try_par_then<P, T, U, E, F, Fut>(self, config: P, mut f: F) -> TryParThen<U, E>
    where
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        Self: Stream<Item = Result<T, E>>,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();

        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (reorder_tx, reorder_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);

        let input_future = {
            rt::spawn(async move {
                let mut stream = self.boxed();
                let mut index = 0;

                loop {
                    let item = tokio::select! {
                        item = stream.try_next() => item.map_err(|err| (index, err))?,
                        _ = terminate_rx.recv() => break,
                    };

                    match item {
                        Some(item) => {
                            let future = f(item);
                            if input_tx.send_async((index, future)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }

                    index += 1;
                }

                Ok(())
            })
            .map(|result| result.unwrap())
        };

        let mut worker_futures: Vec<_> = (0..num_workers)
            .map(|_| {
                let input_rx = input_rx.clone();
                let reorder_tx = reorder_tx.clone();
                let terminate_tx = terminate_tx.clone();

                rt::spawn(async move {
                    loop {
                        let (index, future) = match input_rx.recv_async().await {
                            Ok(item) => item,
                            Err(_) => {
                                break;
                            }
                        };
                        match future.await {
                            Ok(item) => {
                                if reorder_tx.send_async((index, item)).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                let _ = terminate_tx.send(());
                                return Err((index, err));
                            }
                        }
                    }

                    Ok(())
                })
                .map(|result| result.unwrap())
                .boxed()
            })
            .collect();

        let select_worker_future = async move {
            let mut errors = vec![];

            while !worker_futures.is_empty() {
                let (result, index, _) = futures::future::select_all(&mut worker_futures).await;
                worker_futures.remove(index);

                if let Err((index, error)) = result {
                    errors.push((index, error));
                }
            }

            errors
        };

        let reorder_future = rt::spawn(async move {
            let mut map = HashMap::new();
            let mut commit = 0;

            'outer: loop {
                let (index, item) = match reorder_rx.recv_async().await {
                    Ok(tuple) => tuple,
                    Err(_) => break,
                };

                match commit.cmp(&index) {
                    Less => {
                        map.insert(index, item);
                    }
                    Equal => {
                        if output_tx.send_async(item).await.is_err() {
                            break 'outer;
                        }
                        commit += 1;

                        'inner: loop {
                            match map.remove(&commit) {
                                Some(item) => {
                                    if output_tx.send_async(item).await.is_err() {
                                        break 'outer;
                                    };
                                    commit += 1;
                                }
                                None => break 'inner,
                            }
                        }
                    }
                    Greater => panic!("duplicated index number {}", index),
                }
            }
        })
        .map(|result| result.unwrap());

        let join_all_future = async move {
            let (input_result, mut worker_results, ()) =
                futures::future::join3(input_future, select_worker_future, reorder_future).await;

            if let Err((_, err)) = input_result {
                return Err(err);
            }

            worker_results.sort_by_cached_key(|&(index, _)| index);
            if let Some((_, err)) = worker_results.into_iter().next() {
                return Err(err);
            }

            Ok(())
        };

        let select_stream = futures::stream::select(
            output_rx.into_stream().map(|item| Ok(Some(item))),
            join_all_future
                .map(|result| result.map(|()| None))
                .into_stream(),
        )
        .boxed();

        let stream = futures::stream::unfold(
            (Some(select_stream), None),
            |(mut select_stream, mut error)| async move {
                if let Some(stream) = &mut select_stream {
                    match stream.next().await {
                        Some(Ok(Some(item))) => {
                            let output = Ok(item);
                            let state = (select_stream, error);
                            return Some((Some(output), state));
                        }
                        Some(Ok(None)) => {
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        Some(Err(err)) => {
                            error = Some(err);
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        None => {
                            // select_stream = None;
                        }
                    }
                }

                if let Some(err) = error {
                    let output = Err(err);
                    let state = (None, None);
                    return Some((Some(output), state));
                }

                None
            },
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryParThen { stream }
    }

    fn try_par_then_init<P, T, U, E, B, InitF, MapF, Fut>(
        self,
        config: P,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParThen<U, E>
    where
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        Self: Stream<Item = Result<T, E>>,
    {
        let init = init_f();
        self.try_par_then(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_then_unordered<P, T, U, E, F, Fut>(
        self,
        config: P,
        mut f: F,
    ) -> TryParThenUnordered<U, E>
    where
        U: 'static + Send,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);

        let input_future = {
            async move {
                let mut stream = self.boxed();

                loop {
                    let item = tokio::select! {
                        item = stream.try_next() => item?,
                        _ = terminate_rx.recv() => break
                    };

                    match item {
                        Some(item) => {
                            let fut = f(item);
                            let result = input_tx.send_async(fut).await;
                            if result.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }

                Ok(())
            }
        };

        let mut worker_futures: Vec<_> = (0..num_workers)
            .map(|_| {
                let input_rx = input_rx.clone();
                let output_tx = output_tx.clone();
                let terminate_tx = terminate_tx.clone();

                rt::spawn(async move {
                    loop {
                        let output = match input_rx.recv_async().await {
                            Ok(fut) => fut.await,
                            Err(_) => break,
                        };
                        match output {
                            Ok(output) => {
                                if output_tx.send_async(output).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                let _ = terminate_tx.send(());
                                return Err(err);
                            }
                        }
                    }

                    Ok(())
                })
                .map(|result| result.unwrap())
                .boxed()
            })
            .collect();

        let select_worker_future = async move {
            while !worker_futures.is_empty() {
                let (result, index, _) = futures::future::select_all(&mut worker_futures).await;
                worker_futures.remove(index);

                if let Err(error) = result {
                    let _ = futures::future::join_all(worker_futures).await;
                    return Err(error);
                }
            }

            Ok(())
        };

        let join_all_future = async move {
            let (input_result, worker_result) =
                futures::future::join(input_future, select_worker_future).await;

            match (input_result, worker_result) {
                (Err(err), _) => Err(err),
                (Ok(_), Err(err)) => Err(err),
                _ => Ok(()),
            }
        };

        let select_stream = futures::stream::select(
            output_rx.into_stream().map(|item| Ok(Some(item))),
            join_all_future
                .map(|result| result.map(|()| None))
                .into_stream(),
        )
        .boxed();

        let stream = futures::stream::unfold(
            (Some(select_stream), None),
            |(mut select_stream, mut error)| async move {
                if let Some(stream) = &mut select_stream {
                    match stream.next().await {
                        Some(Ok(Some(item))) => {
                            let output = Ok(item);
                            let state = (select_stream, error);
                            return Some((Some(output), state));
                        }
                        Some(Ok(None)) => {
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        Some(Err(err)) => {
                            error = Some(err);
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        None => {
                            // select_stream = None;
                        }
                    }
                }

                if let Some(err) = error {
                    let output = Err(err);
                    let state = (None, None);
                    return Some((Some(output), state));
                }

                None
            },
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryParThenUnordered { stream }
    }

    fn try_par_then_init_unordered<P, T, U, E, B, InitF, MapF, Fut>(
        self,
        config: P,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParThenUnordered<U, E>
    where
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        Self: Stream<Item = Result<T, E>>,
    {
        let init = init_f();
        self.try_par_then_unordered(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_map<P, T, U, E, F, Func>(self, config: P, mut f: F) -> TryParMap<U, E>
    where
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
        Self: Stream<Item = Result<T, E>>,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();

        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (reorder_tx, reorder_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);

        let input_future = {
            rt::spawn(async move {
                let mut stream = self.boxed();
                let mut index = 0;

                loop {
                    let item = tokio::select! {
                        item = stream.try_next() => item.map_err(|err| (index, err))?,
                        _ = terminate_rx.recv() => break,
                    };

                    match item {
                        Some(item) => {
                            let future = f(item);
                            if input_tx.send_async((index, future)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }

                    index += 1;
                }

                Ok(())
            })
            .map(|result| result.unwrap())
        };

        let mut worker_futures: Vec<_> = (0..num_workers)
            .map(|_| {
                let input_rx = input_rx.clone();
                let reorder_tx = reorder_tx.clone();
                let terminate_tx = terminate_tx.clone();

                rt::spawn_blocking(move || {
                    loop {
                        let (index, job) = match input_rx.recv() {
                            Ok(item) => item,
                            Err(_) => {
                                break;
                            }
                        };
                        match job() {
                            Ok(item) => {
                                if reorder_tx.send((index, item)).is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                let _ = terminate_tx.send(());
                                return Err((index, err));
                            }
                        }
                    }

                    Ok(())
                })
                .map(|result| result.unwrap())
                .boxed()
            })
            .collect();

        let select_worker_future = async move {
            let mut errors = vec![];

            while !worker_futures.is_empty() {
                let (result, index, _) = futures::future::select_all(&mut worker_futures).await;
                worker_futures.remove(index);

                if let Err((index, error)) = result {
                    errors.push((index, error));
                }
            }

            errors
        };

        let reorder_future = rt::spawn(async move {
            let mut map = HashMap::new();
            let mut commit = 0;

            'outer: loop {
                let (index, item) = match reorder_rx.recv_async().await {
                    Ok(tuple) => tuple,
                    Err(_) => break,
                };

                match commit.cmp(&index) {
                    Less => {
                        map.insert(index, item);
                    }
                    Equal => {
                        if output_tx.send_async(item).await.is_err() {
                            break 'outer;
                        }
                        commit += 1;

                        'inner: loop {
                            match map.remove(&commit) {
                                Some(item) => {
                                    if output_tx.send_async(item).await.is_err() {
                                        break 'outer;
                                    };
                                    commit += 1;
                                }
                                None => break 'inner,
                            }
                        }
                    }
                    Greater => panic!("duplicated index number {}", index),
                }
            }
        })
        .map(|result| result.unwrap());

        let join_all_future = async move {
            let (input_result, mut worker_results, ()) =
                futures::future::join3(input_future, select_worker_future, reorder_future).await;

            if let Err((_, err)) = input_result {
                return Err(err);
            }

            worker_results.sort_by_cached_key(|&(index, _)| index);
            if let Some((_, err)) = worker_results.into_iter().next() {
                return Err(err);
            }

            Ok(())
        };

        let select_stream = futures::stream::select(
            output_rx.into_stream().map(|item| Ok(Some(item))),
            join_all_future
                .map(|result| result.map(|()| None))
                .into_stream(),
        )
        .boxed();

        let stream = futures::stream::unfold(
            (Some(select_stream), None),
            |(mut select_stream, mut error)| async move {
                if let Some(stream) = &mut select_stream {
                    match stream.next().await {
                        Some(Ok(Some(item))) => {
                            let output = Ok(item);
                            let state = (select_stream, error);
                            return Some((Some(output), state));
                        }
                        Some(Ok(None)) => {
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        Some(Err(err)) => {
                            error = Some(err);
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        None => {
                            // select_stream = None;
                        }
                    }
                }

                if let Some(err) = error {
                    let output = Err(err);
                    let state = (None, None);
                    return Some((Some(output), state));
                }

                None
            },
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryParMap { stream }
    }

    fn try_par_map_init<P, T, U, E, B, InitF, MapF, Func>(
        self,
        config: P,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMap<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
    {
        let init = init_f();
        self.try_par_map(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_map_unordered<P, T, U, E, F, Func>(
        self,
        config: P,
        mut f: F,
    ) -> TryParMapUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (input_tx, input_rx) = flume::bounded(buf_size);
        let (output_tx, output_rx) = flume::bounded(buf_size);
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);

        let input_future = {
            async move {
                let mut stream = self.boxed();

                loop {
                    let item = tokio::select! {
                        item = stream.try_next() => item?,
                        _ = terminate_rx.recv() => break
                    };

                    match item {
                        Some(item) => {
                            let fut = f(item);
                            let result = input_tx.send_async(fut).await;
                            if result.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }

                Ok(())
            }
        };

        let mut worker_futures: Vec<_> = (0..num_workers)
            .map(|_| {
                let input_rx = input_rx.clone();
                let output_tx = output_tx.clone();
                let terminate_tx = terminate_tx.clone();

                rt::spawn_blocking(move || {
                    loop {
                        let output = match input_rx.recv() {
                            Ok(job) => job(),
                            Err(_) => break,
                        };
                        match output {
                            Ok(output) => {
                                if output_tx.send(output).is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                let _ = terminate_tx.send(());
                                return Err(err);
                            }
                        }
                    }

                    Ok(())
                })
                .map(|result| result.unwrap())
                .boxed()
            })
            .collect();

        let select_worker_future = async move {
            while !worker_futures.is_empty() {
                let (result, index, _) = futures::future::select_all(&mut worker_futures).await;
                worker_futures.remove(index);

                if let Err(error) = result {
                    let _ = futures::future::join_all(worker_futures).await;
                    return Err(error);
                }
            }

            Ok(())
        };

        let join_all_future = async move {
            let (input_result, worker_result) =
                futures::future::join(input_future, select_worker_future).await;

            match (input_result, worker_result) {
                (Err(err), _) => Err(err),
                (Ok(_), Err(err)) => Err(err),
                _ => Ok(()),
            }
        };

        let select_stream = futures::stream::select(
            output_rx.into_stream().map(|item| Ok(Some(item))),
            join_all_future
                .map(|result| result.map(|()| None))
                .into_stream(),
        )
        .boxed();

        let stream = futures::stream::unfold(
            (Some(select_stream), None),
            |(mut select_stream, mut error)| async move {
                if let Some(stream) = &mut select_stream {
                    match stream.next().await {
                        Some(Ok(Some(item))) => {
                            let output = Ok(item);
                            let state = (select_stream, error);
                            return Some((Some(output), state));
                        }
                        Some(Ok(None)) => {
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        Some(Err(err)) => {
                            error = Some(err);
                            let state = (select_stream, error);
                            return Some((None, state));
                        }
                        None => {
                            // select_stream = None;
                        }
                    }
                }

                if let Some(err) = error {
                    let output = Err(err);
                    let state = (None, None);
                    return Some((Some(output), state));
                }

                None
            },
        )
        .filter_map(|item| async move { item })
        .boxed();

        TryParMapUnordered { stream }
    }

    fn try_par_map_init_unordered<P, T, U, E, B, InitF, MapF, Func>(
        self,
        config: P,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMapUnordered<U, E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        U: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
    {
        let init = init_f();
        self.try_par_map_unordered(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_for_each<P, T, E, F, Fut>(self, config: P, mut f: F) -> TryParForEach<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (map_tx, map_rx) = flume::bounded(buf_size);
        let (terminate_tx, _terminate_rx) = broadcast::channel(1);

        let map_fut = {
            let terminate_tx = terminate_tx.clone();

            async move {
                let mut stream = self.boxed();

                loop {
                    match stream.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            if map_tx.send_async(fut).await.is_err() {
                                break Ok(());
                            }
                        }
                        Ok(None) => break Ok(()),
                        Err(err) => {
                            let _result = terminate_tx.send(()); // shutdown workers
                            break Err(err); // output error
                        }
                    }
                }
            }
        };

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let terminate_tx = terminate_tx.clone();
                let mut terminate_rx = terminate_tx.subscribe();

                let worker_fut = async move {
                    loop {
                        tokio::select! {
                            result = map_rx.recv_async() => {
                                let fut = match result {
                                    Ok(fut) => fut,
                                    Err(_) => break Ok(()),
                                };

                                if let Err(err) = fut.await {
                                    let _result = terminate_tx.send(()); // shutdown workers
                                    break Err(err); // return error
                                }
                            }
                            _ = terminate_rx.recv() => break Ok(()),
                        }
                    }
                };
                rt::spawn(worker_fut).map(|result| result.unwrap())
            })
            .collect();

        let output_fut = async move {
            let (map_result, worker_results) =
                futures::join!(map_fut, futures::future::join_all(worker_futs));

            worker_results
                .into_iter()
                .fold(map_result, |folded, result| {
                    // the order takes the latest error
                    result.and(folded)
                })
        }
        .boxed();

        TryParForEach {
            future: output_fut.boxed(),
        }
    }

    fn try_par_for_each_init<P, T, E, B, InitF, MapF, Fut>(
        self,
        config: P,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParForEach<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
    {
        let init = init_f();
        self.try_par_for_each(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_for_each_blocking<P, T, E, F, Func>(
        self,
        config: P,
        mut f: F,
    ) -> TryParForEachBlocking<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), E> + Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (map_tx, map_rx) = flume::bounded(buf_size);
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);

        let input_fut = {
            let terminate_tx = terminate_tx.clone();

            async move {
                let mut stream = self.boxed();

                loop {
                    tokio::select! {
                        item = stream.try_next() => {
                            match item {
                                Ok(Some(item)) => {
                                    let fut = f(item);
                                    if map_tx.send_async(fut).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(err) => {
                                    let _ = terminate_tx.send(()); // shutdown workers
                                    return Err(err); // output error
                                }
                            }
                        }
                        _ = terminate_rx.recv() => {
                            break
                        }
                    }
                }

                Ok(())
            }
        };

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let terminate_tx = terminate_tx.clone();

                rt::spawn_blocking(move || {
                    loop {
                        match map_rx.recv() {
                            Ok(job) => {
                                let result = job();
                                if let Err(err) = result {
                                    let _result = terminate_tx.send(()); // shutdown workers
                                    return Err(err); // return error
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    Ok(())
                })
                .map(|result| result.unwrap())
            })
            .collect();

        let output_fut = async move {
            let (input_result, worker_results) =
                futures::join!(input_fut, futures::future::join_all(worker_futs));

            worker_results
                .into_iter()
                .fold(input_result, |folded, result| {
                    // the order takes the latest error
                    result.and(folded)
                })
        }
        .boxed();

        TryParForEachBlocking {
            future: output_fut.boxed(),
        }
    }

    fn try_par_for_each_blocking_init<P, T, E, B, InitF, MapF, Func>(
        self,
        config: P,
        mut init_f: InitF,
        mut f: MapF,
    ) -> TryParForEachBlocking<E>
    where
        Self: Stream<Item = Result<T, E>>,
        P: IntoParStreamParams,
        T: 'static + Send,
        E: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), E> + Send,
    {
        let init = init_f();
        self.try_par_for_each_blocking(config, move |item| f(init.clone(), item))
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
                    stream: futures::stream::empty().boxed(),
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

        let mut select_stream = futures::stream::select_all(streams);
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

        let join_future = futures::future::join(input_future, sync_future);

        let stream = futures::stream::select(
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
        pub(super) stream: BoxedStream<Result<Result<(usize, T), (usize, T)>, E>>,
    }

    impl<T, E> Stream for TrySync<T, E> {
        type Item = Result<Result<(usize, T), (usize, T)>, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_tee

pub use try_tee::*;

mod try_tee {
    use super::*;

    /// A fallible stream combinator returned from [try_tee()](FallibleParStreamExt::try_tee).
    #[derive(Debug)]
    pub struct TryTee<T, E> {
        pub(super) buf_size: Option<usize>,
        pub(super) future: Arc<Mutex<Option<rt::JoinHandle<()>>>>,
        pub(super) sender_set: Weak<flurry::HashSet<ByAddress<Arc<flume::Sender<Result<T, E>>>>>>,
        pub(super) receiver: flume::Receiver<Result<T, E>>,
    }

    impl<T, E> Clone for TryTee<T, E>
    where
        T: 'static + Send,
        E: 'static + Send,
    {
        fn clone(&self) -> Self {
            let buf_size = self.buf_size;
            let (tx, rx) = match buf_size {
                Some(buf_size) => flume::bounded(buf_size),
                None => flume::unbounded(),
            };
            let sender_set = self.sender_set.clone();

            if let Some(sender_set) = sender_set.upgrade() {
                let guard = sender_set.guard();
                sender_set.insert(ByAddress(Arc::new(tx)), &guard);
            }

            Self {
                future: self.future.clone(),
                sender_set,
                receiver: rx,
                buf_size,
            }
        }
    }

    impl<T, E> Stream for TryTee<T, E> {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Ok(mut future_opt) = self.future.try_lock() {
                if let Some(future) = &mut *future_opt {
                    if Pin::new(future).poll(cx).is_ready() {
                        *future_opt = None;
                    }
                }
            }

            match Pin::new(&mut self.receiver.recv_async()).poll(cx) {
                Ready(Ok(output)) => {
                    cx.waker().clone().wake();
                    Ready(Some(output))
                }
                Ready(Err(_)) => Ready(None),
                Pending => {
                    cx.waker().clone().wake();
                    Pending
                }
            }
        }
    }
}

// try_par_map

pub use try_par_map::*;

mod try_par_map {
    use super::*;

    /// A fallible stream combinator returned from [try_par_map()](FallibleParStreamExt::try_par_map) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParMap<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParMap<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_then_unordered

pub use try_par_map_unordered::*;

mod try_par_map_unordered {
    use super::*;

    /// A fallible stream combinator returned from [try_par_map_unordered()](FallibleParStreamExt::try_par_map_unordered) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParMapUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParMapUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_map

pub use try_par_then::*;

mod try_par_then {
    use super::*;

    /// A fallible stream combinator returned from [try_par_then()](FallibleParStreamExt::try_par_then) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParThen<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParThen<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_then_unordered

pub use try_par_then_unordered::*;

mod try_par_then_unordered {
    use super::*;

    /// A fallible stream combinator returned from [try_par_then_unordered()](FallibleParStreamExt::try_par_then_unordered) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParThenUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParThenUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_for_each

pub use try_par_for_each::*;

mod try_par_for_each {
    use super::*;

    /// A fallible stream combinator returned from [try_par_for_each()](FallibleParStreamExt::try_par_for_each) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParForEach<E> {
        #[derivative(Debug = "ignore")]
        pub(super) future: BoxedFuture<Result<(), E>>,
    }

    impl<E> Future for TryParForEach<E> {
        type Output = Result<(), E>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            Pin::new(&mut self.future).poll(cx)
        }
    }
}

// try_par_for_each

pub use try_par_for_each_blocking::*;

mod try_par_for_each_blocking {
    use super::*;

    /// A fallible stream combinator returned from [try_par_for_each()](FallibleParStreamExt::try_par_for_each) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParForEachBlocking<E> {
        #[derivative(Debug = "ignore")]
        pub(super) future: BoxedFuture<Result<(), E>>,
    }

    impl<E> Future for TryParForEachBlocking<E> {
        type Output = Result<(), E>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            Pin::new(&mut self.future).poll(cx)
        }
    }
}

// try_enumerate

pub use try_enumerate::*;

mod try_enumerate {
    use super::*;

    /// A fallible stream combinator returned from [try_wrapping_enumerate()](FallibleIndexedStreamExt::try_wrapping_enumerate).
    #[pin_project(project = TryEnumerateProj)]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryEnumerate<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) counter: usize,
        pub(super) fused: bool,
        pub(super) _phantom: PhantomData<(T, E)>,
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: S,
    }

    impl<S, T, E> Stream for TryEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        type Item = Result<(usize, T), E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let TryEnumerateProj {
                stream,
                fused,
                counter,
                ..
            } = self.project();

            if *fused {
                return Ready(None);
            }

            let poll = stream.poll_next(cx);
            match poll {
                Ready(Some(Ok(item))) => {
                    let index = *counter;
                    *counter += 1;
                    Ready(Some(Ok((index, item))))
                }
                Ready(Some(Err(err))) => {
                    *fused = true;
                    Ready(Some(Err(err)))
                }
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }

    impl<S, T, E> FusedStream for TryEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        fn is_terminated(&self) -> bool {
            self.fused
        }
    }
}

// try_wrapping_enumerate

pub use try_wrapping_enumerate::*;

mod try_wrapping_enumerate {
    use super::*;

    /// A fallible stream combinator returned from [try_wrapping_enumerate()](FallibleIndexedStreamExt::try_wrapping_enumerate).
    #[pin_project(project = TryWrappingEnumerateProj)]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryWrappingEnumerate<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) counter: usize,
        pub(super) fused: bool,
        pub(super) _phantom: PhantomData<(T, E)>,
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: S,
    }

    impl<S, T, E> Stream for TryWrappingEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        type Item = Result<(usize, T), E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let TryWrappingEnumerateProj {
                stream,
                fused,
                counter,
                ..
            } = self.project();

            if *fused {
                return Ready(None);
            }

            let poll = stream.poll_next(cx);
            match poll {
                Ready(Some(Ok(item))) => {
                    let index = *counter;
                    *counter = counter.wrapping_add(1);
                    Ready(Some(Ok((index, item))))
                }
                Ready(Some(Err(err))) => {
                    *fused = true;
                    Ready(Some(Err(err)))
                }
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }

    impl<S, T, E> FusedStream for TryWrappingEnumerate<S, T, E>
    where
        S: Stream<Item = Result<T, E>>,
    {
        fn is_terminated(&self) -> bool {
            self.fused
        }
    }
}

// reorder_enumerated

pub use try_reorder_enumerated::*;

mod try_reorder_enumerated {
    use super::*;

    /// A fallible stream combinator returned from [try_reorder_enumerated()](FallibleIndexedStreamExt::try_reorder_enumerated).
    #[pin_project(project = TryReorderEnumeratedProj)]
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryReorderEnumerated<S, T, E>
    where
        S: ?Sized,
    {
        pub(super) commit: usize,
        pub(super) fused: bool,
        pub(super) buffer: HashMap<usize, T>,
        pub(super) _phantom: PhantomData<E>,
        #[pin]
        #[derivative(Debug = "ignore")]
        pub(super) stream: S,
    }

    impl<S, T, E> Stream for TryReorderEnumerated<S, T, E>
    where
        S: Stream<Item = Result<(usize, T), E>>,
    {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let TryReorderEnumeratedProj {
                fused,
                stream,
                commit,
                buffer,
                ..
            } = self.project();

            if *fused {
                return Ready(None);
            }

            if let Some(item) = buffer.remove(commit) {
                *commit += 1;
                cx.waker().clone().wake();
                return Ready(Some(Ok(item)));
            }

            match stream.poll_next(cx) {
                Ready(Some(Ok((index, item)))) => match (*commit).cmp(&index) {
                    Less => match buffer.entry(index) {
                        hash_map::Entry::Occupied(_) => {
                            panic!("the index number {} appears more than once", index);
                        }
                        hash_map::Entry::Vacant(entry) => {
                            entry.insert(item);
                            cx.waker().clone().wake();
                            Pending
                        }
                    },
                    Equal => {
                        *commit += 1;
                        cx.waker().clone().wake();
                        Ready(Some(Ok(item)))
                    }
                    Greater => {
                        panic!("the index number {} appears more than once", index);
                    }
                },
                Ready(Some(Err(err))) => {
                    *fused = true;
                    Ready(Some(Err(err)))
                }
                Ready(None) => {
                    assert!(buffer.is_empty(), "the index numbers are not contiguous");
                    Ready(None)
                }
                Pending => Pending,
            }
        }
    }

    impl<S, T, E> FusedStream for TryReorderEnumerated<S, T, E>
    where
        Self: Stream,
    {
        fn is_terminated(&self) -> bool {
            self.fused
        }
    }
}

// try_batching

pub use try_batching::*;

mod try_batching {
    use super::*;

    /// A fallible stream combinator returned from [try_batching()](FallibleParStreamExt::try_batching()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryBatching<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryBatching<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_par_batching_unordered

pub use try_par_batching_unordered::*;

mod try_par_batching_unordered {
    use super::*;

    /// A fallible stream combinator returned from [try_par_batching_unordered()](FallibleParStreamExt::try_par_batching_unordered()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParBatchingUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParBatchingUnordered<T, E> {
        type Item = Result<T, E>;

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

        let stream = futures::stream::select(
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
        pub(super) stream: BoxedStream<Result<T, E>>,
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

        let join_future = futures::future::join_all(worker_futs);

        let stream = futures::stream::select(
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

        let join_future = futures::future::try_join_all(worker_futs);

        let stream = futures::stream::select(
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
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryParUnfoldUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_then_spawned

pub use try_then_spawned::*;

mod try_then_spawned {
    use super::*;

    /// A stream combinator returned from [try_then_spawned()](FallibleParStreamExt::try_then_spawned).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryThenSpawned<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryThenSpawned<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// try_map_spawned

pub use try_map_spawned::*;

mod try_map_spawned {
    use super::*;

    /// A stream combinator returned from [try_map_spawned()](FallibleParStreamExt::try_map_spawned).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryMapSpawned<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: BoxedStream<Result<T, E>>,
    }

    impl<T, E> Stream for TryMapSpawned<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.stream).poll_next(cx)
        }
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[tokio::test]
    async fn try_then_spawned_test() {
        {
            let values: Result<Vec<_>, ()> = futures::stream::iter(0..1000)
                .map(Ok)
                .try_then_spawned(None, |val| async move { Ok(val * 2) })
                .try_collect()
                .await;

            let expect: Vec<_> = (0..1000).map(|val| val * 2).collect();
            assert_eq!(values, Ok(expect));
        }

        {
            let mut stream =
                futures::stream::iter(0..1000)
                    .map(Ok)
                    .try_then_spawned(None, |val| async move {
                        if val < 3 {
                            Ok(val)
                        } else {
                            Err(val)
                        }
                    });

            assert_eq!(stream.next().await, Some(Ok(0)));
            assert_eq!(stream.next().await, Some(Ok(1)));
            assert_eq!(stream.next().await, Some(Ok(2)));
            assert_eq!(stream.next().await, Some(Err(3)));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_map_spawned_test() {
        {
            let values: Result<Vec<_>, ()> = futures::stream::iter(0..1000)
                .map(Ok)
                .try_map_spawned(None, |val| Ok(val * 2))
                .try_collect()
                .await;

            let expect: Vec<_> = (0..1000).map(|val| val * 2).collect();
            assert_eq!(values, Ok(expect));
        }

        {
            let mut stream = futures::stream::iter(0..1000)
                .map(Ok)
                .try_map_spawned(None, |val| if val < 3 { Ok(val) } else { Err(val) });

            assert_eq!(stream.next().await, Some(Ok(0)));
            assert_eq!(stream.next().await, Some(Ok(1)));
            assert_eq!(stream.next().await, Some(Ok(2)));
            assert_eq!(stream.next().await, Some(Err(3)));
            assert!(stream.next().await.is_none());
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

    #[tokio::test]
    async fn try_par_batching_unordered_test() {
        {
            let mut stream = futures::stream::iter(iter::repeat(1).take(10))
                .map(Ok)
                .try_par_batching_unordered::<_, (), _, _, _, _>(None, |_, _, _| async move {
                    Result::<(), _>::Err("init error")
                });

            assert_eq!(stream.next().await, Some(Err("init error")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(iter::repeat(1).take(10))
                .map(Ok)
                .try_par_batching_unordered(None, |_, input, output| async move {
                    let mut sum = 0;

                    while let Ok(val) = input.recv_async().await {
                        let new_sum = sum + val;
                        if new_sum >= 3 {
                            sum = 0;
                            let result = output.send_async(new_sum).await;
                            if result.is_err() {
                                break;
                            }
                        } else {
                            sum = new_sum;
                        }
                    }

                    if sum > 0 {
                        let _ = output.send_async(sum).await;
                    }

                    Result::<_, ()>::Ok(())
                });

            let mut total = 0;
            while total < 10 {
                let sum = stream.next().await.unwrap().unwrap();
                assert!(sum <= 3);
                total += sum;
            }
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(iter::repeat(1).take(10))
                .map(Ok)
                .try_par_batching_unordered(None, |_, input, output| async move {
                    let mut sum = 0;

                    while let Ok(val) = input.recv_async().await {
                        let new_sum = sum + val;
                        if new_sum >= 3 {
                            sum = 0;
                            let result = output.send_async(new_sum).await;
                            if result.is_err() {
                                break;
                            }
                        } else {
                            sum = new_sum;
                        }
                    }

                    if sum == 0 {
                        Ok(())
                    } else {
                        Err(sum)
                    }
                });

            let mut total = 0;
            while total < 10 {
                let result = stream.next().await.unwrap();
                match result {
                    Ok(sum) => {
                        assert!(sum == 3);
                        total += sum;
                    }
                    Err(sum) => {
                        assert!(sum < 3);
                        break;
                    }
                }
            }
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_batching_test() {
        {
            let mut stream = futures::stream::iter(0..10)
                .map(Ok)
                .try_batching::<_, usize, _, _, _>(|_, _| async move { Err("init error") });

            assert_eq!(stream.next().await, Some(Err("init error")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(0..10).map(Ok).try_batching(
                |mut input, mut output| async move {
                    let mut sum = 0;

                    while let Some(val) = input.recv().await {
                        let new_sum = val + sum;

                        if new_sum >= 10 {
                            sum = 0;
                            let result = output.send(new_sum).await;
                            if result.is_err() {
                                break;
                            }
                        } else {
                            sum = new_sum;
                        }
                    }

                    if sum == 0 {
                        Ok(())
                    } else {
                        dbg!();
                        Err("some elements are left behind")
                    }
                },
            );

            assert_eq!(stream.next().await, Some(Ok(10)));
            assert_eq!(stream.next().await, Some(Ok(11)));
            assert_eq!(stream.next().await, Some(Ok(15)));
            assert!(matches!(stream.next().await, Some(Err(_))));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(0..10).map(Ok).try_batching(
                |mut input, mut output| async move {
                    let mut sum = 0;

                    while let Some(val) = input.recv().await {
                        let new_sum = val + sum;

                        if new_sum >= 15 {
                            return Err("too large");
                        } else if new_sum >= 10 {
                            sum = 0;
                            let result = output.send(new_sum).await;
                            if result.is_err() {
                                break;
                            }
                        } else {
                            sum = new_sum;
                        }
                    }

                    if input.recv().await.is_none() {
                        Ok(())
                    } else {
                        Err("some elements are left behind")
                    }
                },
            );

            assert_eq!(stream.next().await, Some(Ok(10)));
            assert_eq!(stream.next().await, Some(Ok(11)));
            assert_eq!(stream.next().await, Some(Err("too large")));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_par_for_each_test() {
        {
            let result = futures::stream::iter(vec![Ok(1usize), Ok(2), Ok(6), Ok(4)].into_iter())
                .try_par_for_each(None, |_| async move { Result::<_, ()>::Ok(()) })
                .await;

            assert_eq!(result, Ok(()));
        }

        {
            let result =
                futures::stream::iter(vec![Ok(1usize), Ok(2), Err(-3isize), Ok(4)].into_iter())
                    .try_par_for_each(None, |_| async move { Ok(()) })
                    .await;

            assert_eq!(result, Err(-3));
        }
    }

    #[tokio::test]
    async fn try_par_for_each_blocking_test() {
        {
            let result = futures::stream::iter(vec![Ok(1usize), Ok(2), Ok(6), Ok(4)])
                .try_par_for_each_blocking(None, |_| || Result::<_, ()>::Ok(()))
                .await;

            assert_eq!(result, Ok(()));
        }

        {
            let result = futures::stream::iter(0..)
                .then(|val| async move {
                    if val == 3 {
                        Err(val)
                    } else {
                        Ok(val)
                    }
                })
                .try_par_for_each_blocking(8, |_| || Ok(()))
                .await;

            assert_eq!(result, Err(3));
        }

        {
            let result = futures::stream::iter(0..)
                .map(Ok)
                .try_par_for_each_blocking(None, |val| {
                    move || {
                        if val == 3 {
                            std::thread::sleep(Duration::from_millis(100));
                            Err(val)
                        } else {
                            Ok(())
                        }
                    }
                })
                .await;

            assert_eq!(result, Err(3));
        }
    }

    #[tokio::test]
    async fn try_par_then_test() {
        {
            let mut stream =
                futures::stream::iter(vec![Ok(1usize), Ok(2), Err(-3isize), Ok(4)].into_iter())
                    .try_par_then(None, |value| async move { Ok(value) });

            assert_eq!(stream.try_next().await, Ok(Some(1usize)));
            assert_eq!(stream.try_next().await, Ok(Some(2usize)));
            assert_eq!(stream.try_next().await, Err(-3isize));
            assert_eq!(stream.try_next().await, Ok(None));
        }

        {
            let vec: Result<Vec<()>, ()> = futures::stream::iter(vec![])
                .try_par_then(None, |()| async move { Ok(()) })
                .try_collect()
                .await;

            assert!(matches!(vec, Ok(vec) if vec.is_empty()));
        }

        {
            let mut stream = futures::stream::repeat(())
                .enumerate()
                .map(Ok)
                .try_par_then(3, |(index, ())| async move {
                    match index {
                        3 | 6 => Err(index),
                        index => Ok(index),
                    }
                });

            assert_eq!(stream.next().await, Some(Ok(0)));
            assert_eq!(stream.next().await, Some(Ok(1)));
            assert_eq!(stream.next().await, Some(Ok(2)));
            assert_eq!(stream.next().await, Some(Err(3)));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_reorder_enumerated_test() {
        let len: usize = 1000;
        let mut rng = rand::thread_rng();

        for _ in 0..10 {
            let err_index_1 = rng.gen_range(0..len);
            let err_index_2 = rng.gen_range(0..len);
            let min_err_index = err_index_1.min(err_index_2);

            let results: Vec<_> = futures::stream::iter(0..len)
                .map(move |value| {
                    if value == err_index_1 || value == err_index_2 {
                        Err(-(value as isize))
                    } else {
                        Ok(value)
                    }
                })
                .try_wrapping_enumerate()
                .try_par_then_unordered(None, |(index, value)| async move {
                    rt::sleep(Duration::from_millis(value as u64 % 10)).await;
                    Ok((index, value))
                })
                .try_reorder_enumerated()
                .collect()
                .await;
            assert!(results.len() <= min_err_index + 1);

            let (is_fused_at_error, _, _) = results.iter().cloned().fold(
                (true, false, 0),
                |(is_correct, found_err, expect), result| {
                    if !is_correct {
                        return (false, found_err, expect);
                    }

                    match result {
                        Ok(value) => {
                            let is_correct = value < min_err_index && value == expect && !found_err;
                            (is_correct, found_err, expect + 1)
                        }
                        Err(value) => {
                            let is_correct = (-value) as usize == min_err_index && !found_err;
                            let found_err = true;
                            (is_correct, found_err, expect + 1)
                        }
                    }
                },
            );
            assert!(is_fused_at_error);
        }
    }

    #[tokio::test]
    async fn try_sync_test() {
        {
            let stream1 = futures::stream::iter(vec![Ok(3), Ok(1), Ok(5), Ok(7)]);
            let stream2 = futures::stream::iter(vec![Ok(2), Ok(4), Ok(6), Err("error")]);

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
}
