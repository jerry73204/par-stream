use super::error::NullResult;
use crate::{
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
    rt,
};
use tokio::sync::{Mutex, Notify};
use tokio_stream::wrappers::ReceiverStream;

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
    use tokio::sync::mpsc;

    let buf_size = buf_size.into().unwrap_or_else(num_cpus::get);
    let (data_tx, data_rx) = mpsc::channel(buf_size);

    let producer_fut = rt::spawn_blocking(move || {
        let mut state = match init_f() {
            Ok(state) => state,
            Err(err) => {
                let _ = data_tx.blocking_send(Err(err));
                return;
            }
        };

        loop {
            match unfold_f(state) {
                Ok(Some((item, new_state))) => {
                    let result = data_tx.blocking_send(Ok(item));
                    if result.is_err() {
                        break;
                    }
                    state = new_state;
                }
                Ok(None) => break,
                Err(err) => {
                    let _ = data_tx.blocking_send(Err(err));
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
        ReceiverStream::new(data_rx).map(|item: Result<Item, Error>| Some(item)),
    )
    .filter_map(|item| async move { item });

    TryUnfoldBlocking {
        stream: Box::pin(stream),
    }
}

/// A fallible analogue to [par_unfold_unordered](crate::stream::par_unfold_unordered).
pub fn try_par_unfold_unordered<IF, UF, IFut, UFut, State, Item, Error>(
    config: impl IntoParStreamParams,
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
{
    use tokio::sync::mpsc;

    let ParStreamParams {
        num_workers,
        buf_size,
    } = config.into_par_stream_params();
    let (output_tx, output_rx) = mpsc::channel(buf_size);

    let worker_futs = (0..num_workers).map(|worker_index| {
        let init_fut = init_f(worker_index);
        let mut unfold_f = unfold_f.clone();
        let output_tx = output_tx.clone();

        rt::spawn(async move {
            let mut state = match init_fut.await {
                Ok(state) => state,
                Err(err) => {
                    let _ = output_tx.send(Err(err)).await;
                    return;
                }
            };

            loop {
                match unfold_f(worker_index, state).await {
                    Ok(Some((item, new_state))) => {
                        let result = output_tx.send(Ok(item)).await;
                        if result.is_err() {
                            break;
                        }
                        state = new_state;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(err) => {
                        let _ = output_tx.send(Err(err)).await;
                        break;
                    }
                }
            }
        })
    });

    let join_future = futures::future::try_join_all(worker_futs);

    let stream = futures::stream::select(
        ReceiverStream::new(output_rx).map(Some),
        join_future.into_stream().map(|result| {
            result.unwrap();
            None
        }),
    )
    .filter_map(|item| async move { item });

    TryParUnfoldUnordered {
        stream: Some(Box::pin(stream)),
    }
}

/// A fallible analogue to [par_unfold_blocking_unordered](crate::stream::par_unfold_blocking_unordered).
pub fn try_par_unfold_blocking_unordered<IF, UF, State, Item, Error>(
    config: impl IntoParStreamParams,
    init_f: IF,
    unfold_f: UF,
) -> TryParUnfoldUnordered<Item, Error>
where
    IF: 'static + FnMut(usize) -> Result<State, Error> + Send + Clone,
    UF: 'static + FnMut(usize, State) -> Result<Option<(Item, State)>, Error> + Send + Clone,
    Item: 'static + Send,
    Error: 'static + Send,
{
    use tokio::sync::mpsc;

    let ParStreamParams {
        num_workers,
        buf_size,
    } = config.into_par_stream_params();
    let (output_tx, output_rx) = mpsc::channel(buf_size);

    let worker_futs = (0..num_workers).map(|worker_index| {
        let mut init_f = init_f.clone();
        let mut unfold_f = unfold_f.clone();
        let output_tx = output_tx.clone();

        rt::spawn_blocking(move || {
            let mut state = match init_f(worker_index) {
                Ok(state) => state,
                Err(err) => {
                    let _ = output_tx.blocking_send(Err(err));
                    return;
                }
            };

            loop {
                match unfold_f(worker_index, state) {
                    Ok(Some((item, new_state))) => {
                        let result = output_tx.blocking_send(Ok(item));
                        if result.is_err() {
                            break;
                        }
                        state = new_state;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(err) => {
                        let _ = output_tx.blocking_send(Err(err));
                        break;
                    }
                }
            }
        })
    });

    let join_future = futures::future::try_join_all(worker_futs);

    let stream = futures::stream::select(
        ReceiverStream::new(output_rx).map(Some),
        join_future.into_stream().map(|result| {
            result.unwrap();
            None
        }),
    )
    .filter_map(|item| async move { item });

    TryParUnfoldUnordered {
        stream: Some(Box::pin(stream)),
    }
}

/// An extension trait for streams providing fallible combinators for parallel processing.
pub trait TryParStreamExt {
    /// A fallible analogue to [batching](crate::ParStreamExt::batching) that consumes
    /// as many elements as it likes for each next output element.
    fn try_batching<T, B, C, E, IF, BF, FF, IFut, BFut, FFut>(
        mut self,
        mut init_fn: IF,
        mut batching_fn: BF,
        mut finalize_fn: FF,
    ) -> TryBatching<B, E>
    where
        Self: 'static + Stream<Item = Result<T, E>> + Sized + Unpin + Send,
        IF: 'static + FnMut() -> IFut + Send,
        BF: 'static + FnMut(T, C) -> BFut + Send,
        FF: 'static + FnMut(C) -> FFut + Send,
        IFut: 'static + Future<Output = Result<C, E>> + Send,
        BFut: 'static + Future<Output = Result<ControlFlow<B, C>, E>> + Send,
        FFut: 'static + Future<Output = Result<Option<B>, E>> + Send,
        T: 'static + Send,
        B: 'static + Send,
        C: 'static + Send,
        E: 'static + Send,
    {
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(1);
        let request = Arc::new(Notify::new());
        let terminate = Arc::new(Notify::new());

        let input_fut = {
            let request = request.clone();
            let terminate = terminate.clone();

            async move {
                'outer: loop {
                    tokio::select! {
                        _ = request.notified() => {}
                        _ = terminate.notified() => {
                            break 'outer;
                        }
                    }

                    let mut state = match init_fn().await {
                        Ok(state) => state,
                        Err(err) => {
                            let _ = output_tx.send(Err(err)).await;
                            break 'outer;
                        }
                    };

                    'inner: loop {
                        let item = match self.next().await {
                            Some(Ok(item)) => item,
                            None => {
                                let output = finalize_fn(state).await;

                                match output {
                                    Ok(Some(output)) => {
                                        let _ = output_tx.send(Ok(output)).await;
                                    }
                                    Ok(None) => {}
                                    Err(err) => {
                                        let _ = output_tx.send(Err(err)).await;
                                    }
                                }

                                break 'outer;
                            }
                            Some(Err(err)) => {
                                let _ = output_tx.send(Err(err)).await;
                                break 'outer;
                            }
                        };
                        let control = batching_fn(item, state).await;

                        match control {
                            Ok(ControlFlow::Continue(new_state)) => {
                                state = new_state;
                            }
                            Ok(ControlFlow::Break(output)) => {
                                let result = output_tx.send(Ok(output)).await;
                                if result.is_err() {
                                    break 'outer;
                                }
                                break 'inner;
                            }
                            Err(err) => {
                                let _ = output_tx.send(Err(err)).await;
                                break 'outer;
                            }
                        }
                    }
                }
            }
        };

        let stream = futures::stream::select(
            ReceiverStream::new(output_rx).map(|output| Some(output)),
            input_fut.into_stream().map(|_| None),
        )
        .filter_map(|output| async move { output });

        TryBatching {
            stream: Box::pin(stream),
            request,
            terminate,
            requested: false,
        }
    }

    /// A fallible analogue to [par_batching_unordered](crate::ParStreamExt::par_batching_unordered).
    fn try_par_batching_unordered<T, B, C, E, IF, BF, FF, IFut, BFut, FFut>(
        mut self,
        config: impl IntoParStreamParams,
        init_fn: IF,
        batching_fn: BF,
        finalize_fn: FF,
    ) -> TryParBatchingUnordered<B, E>
    where
        Self: 'static + Stream<Item = Result<T, E>> + Sized + Unpin + Send,
        IF: 'static + FnMut() -> IFut + Send + Clone,
        BF: 'static + FnMut(T, C) -> BFut + Send + Clone,
        FF: 'static + FnMut(C) -> FFut + Send + Clone,
        IFut: 'static + Future<Output = Result<C, E>> + Send,
        BFut: 'static + Future<Output = Result<ControlFlow<B, C>, E>> + Send,
        FFut: 'static + Future<Output = Result<Option<B>, E>> + Send,
        T: 'static + Send,
        B: 'static + Send,
        C: 'static + Send,
        E: 'static + Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();

        let (input_tx, input_rx) = async_channel::bounded(buf_size);
        let (output_tx, output_rx) = async_channel::bounded(buf_size);

        let input_fut = rt::spawn(async move {
            while let Some(item) = self.next().await {
                let result = input_tx.send(item).await;
                if result.is_err() {
                    break;
                }
            }
        });

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let mut init_fn = init_fn.clone();
                let mut batching_fn = batching_fn.clone();
                let mut finalize_fn = finalize_fn.clone();
                let input_rx = input_rx.clone();
                let output_tx = output_tx.clone();

                rt::spawn(async move {
                    'outer: loop {
                        let mut state = None;

                        'inner: loop {
                            let item = match input_rx.recv().await {
                                Ok(Ok(item)) => item,
                                Ok(Err(err)) => {
                                    let _ = output_tx.send(Err(err)).await;
                                    break 'outer;
                                }
                                Err(_) => {
                                    if let Some(state) = state.take() {
                                        let result = finalize_fn(state).await.transpose();
                                        if let Some(result) = result {
                                            let _ = output_tx.send(result).await;
                                        }
                                    }
                                    break 'outer;
                                }
                            };

                            let state_ = match state.take() {
                                Some(state) => state,
                                None => match init_fn().await {
                                    Ok(state) => state,
                                    Err(err) => {
                                        let _ = output_tx.send(Err(err)).await;
                                        break 'outer;
                                    }
                                },
                            };
                            let control = batching_fn(item, state_).await;

                            match control {
                                Ok(ControlFlow::Continue(state_)) => state = Some(state_),
                                Ok(ControlFlow::Break(output)) => {
                                    let result = output_tx.send(Ok(output)).await;
                                    if result.is_err() {
                                        break 'outer;
                                    }
                                    break 'inner;
                                }
                                Err(err) => {
                                    let _ = output_tx.send(Err(err)).await;
                                    break 'outer;
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        let join_fut =
            futures::future::try_join(input_fut, futures::future::try_join_all(worker_futs))
                .map(|result| result.map(|_| ()));

        TryParBatchingUnordered {
            future: Some(Box::pin(join_fut)),
            output_rx: Some(output_rx),
        }
    }

    /// A fallible analogue to [tee](crate::ParStreamExt::tee) that stops sending items when
    /// receiving an error.
    fn try_tee<T, E>(mut self, buf_size: impl Into<Option<usize>>) -> TryTee<T, E>
    where
        Self: 'static + Stream<Item = Result<T, E>> + Sized + Unpin + Send,
        T: 'static + Send + Clone,
        E: 'static + Send + Clone,
    {
        let buf_size = buf_size.into();
        let (tx, rx) = match buf_size {
            Some(buf_size) => async_channel::bounded(buf_size),
            None => async_channel::unbounded(),
        };
        let sender_set = Arc::new(flurry::HashSet::new());
        let guard = sender_set.guard();
        sender_set.insert(ByAddress(Arc::new(tx)), &guard);

        let future = {
            let sender_set = sender_set.clone();

            let future = rt::spawn(async move {
                while let Some(item) = self.next().await {
                    let futures: Vec<_> = sender_set
                        .pin()
                        .iter()
                        .map(|tx| {
                            let tx = tx.clone();
                            let item = item.clone();
                            async move {
                                let result = tx.send(item).await;
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

    /// Create a fallible stream that gives the current iteration count.
    ///
    /// The count wraps to zero if the count overflows.
    fn try_wrapping_enumerate<T, E>(self) -> TryWrappingEnumerate<T, E, Self>
    where
        Self: Stream<Item = Result<T, E>> + Sized + Unpin + Send,
    {
        TryWrappingEnumerate {
            stream: self,
            counter: 0,
            fused: false,
        }
    }

    /// Creates a fallible stream that reorders the items according to the iteration count.
    ///
    /// It is usually combined with [try_wrapping_enumerate](TryParStreamExt::try_wrapping_enumerate).
    fn try_reorder_enumerated<T, E>(self) -> TryReorderEnumerated<T, E, Self>
    where
        Self: Stream<Item = Result<(usize, T), E>> + Sized + Unpin + Send,
    {
        TryReorderEnumerated {
            stream: self,
            counter: 0,
            fused: false,
            buffer: HashMap::new(),
        }
    }

    /// Fallible parallel stream.
    fn try_par_then<T, F, Fut>(
        mut self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (map_tx, map_rx) = async_channel::bounded(buf_size);
        let (reorder_tx, reorder_rx) = async_channel::bounded(buf_size);
        let (output_tx, output_rx) = async_channel::bounded(buf_size);

        let map_fut = {
            let reorder_tx = reorder_tx.clone();
            async move {
                let mut counter = 0u64;

                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            map_tx.send((counter, fut)).await?;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            reorder_tx.send((counter, Err(err))).await?;
                        }
                    }
                    counter = counter.wrapping_add(1);
                }

                Ok(())
            }
        };

        let reorder_fut = async move {
            let mut counter = 0u64;
            let mut pool = HashMap::new();

            while let Ok((index, output)) = reorder_rx.recv().await {
                if index != counter {
                    pool.insert(index, output);
                    continue;
                }

                output_tx.send(output).await?;
                counter = counter.wrapping_add(1);

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await?;
                    counter = counter.wrapping_add(1);
                }
            }

            Ok(())
        };

        let worker_futs: Vec<_> = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let reorder_tx = reorder_tx.clone();

                let worker_fut = async move {
                    while let Ok((index, fut)) = map_rx.recv().await {
                        let output = fut.await;
                        reorder_tx.send((index, output)).await?;
                    }
                    Ok(())
                };
                rt::spawn(worker_fut).map(|result| result.unwrap())
            })
            .collect();

        let par_then_fut = futures::future::try_join3(
            map_fut,
            reorder_fut,
            futures::future::try_join_all(worker_futs),
        );

        TryParMap {
            fut: Some(Box::pin(par_then_fut)),
            output_rx,
        }
    }

    /// Fallible parallel stream with in-local thread initializer.
    fn try_par_then_init<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let init = init_f();
        self.try_par_then(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_then_unordered<T, F, Fut>(
        mut self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (map_tx, map_rx) = async_channel::bounded(buf_size);
        let (output_tx, output_rx) = async_channel::bounded(buf_size);

        let map_fut = {
            let output_tx = output_tx.clone();
            async move {
                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            map_tx.send(fut).await?;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            output_tx.send(Err(err)).await?;
                        }
                    }
                }
                Ok(())
            }
        };

        let worker_futs = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let output_tx = output_tx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        let result = fut.await;
                        output_tx.send(result).await?;
                    }
                    Ok(())
                };
                rt::spawn(worker_fut).map(|result| result.unwrap())
            })
            .collect::<Vec<_>>();

        let par_then_fut =
            futures::future::try_join(map_fut, futures::future::try_join_all(worker_futs));

        TryParMapUnordered {
            fut: Some(Box::pin(par_then_fut)),
            output_rx,
        }
    }

    /// An parallel stream analogous to [try_par_then_unordered](TryParStreamExt::try_par_then_unordered) with
    /// in-local thread initializer
    fn try_par_then_init_unordered<T, B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<T, Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let init = init_f();
        self.try_par_then_unordered(config, move |item| map_f(init.clone(), item))
    }

    /// Fallible parallel stream that runs blocking workers.
    fn try_par_map<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        self.try_par_then(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Fallible parallel stream that runs blocking workers with in-local thread initializer.
    fn try_par_map_init<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMap<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let init = init_f();
        self.try_par_then(config, move |item| {
            let func = map_f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// A parallel stream that analogous to [try_par_map](TryParStreamExt::try_par_map) without respecting
    /// the order of input items.
    fn try_par_map_unordered<T, F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        self.try_par_then_unordered(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// A parallel stream that analogous to [try_par_map_unordered](TryParStreamExt::try_par_map_unordered) with
    /// in-local thread initializer.
    fn try_par_map_init_unordered<T, B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParMapUnordered<T, Self::Error>
    where
        T: 'static + Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<T, Self::Error> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let init = init_f();
        self.try_par_then_unordered(config, move |item| {
            let func = map_f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Runs this stream to completion, executing asynchronous closure for each element on the stream
    /// in parallel.
    fn try_par_for_each<F, Fut>(
        mut self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParForEach<Self::Error>
    where
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), Self::Error>> + Send,
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
    {
        let ParStreamParams {
            num_workers,
            buf_size,
        } = config.into_par_stream_params();
        let (map_tx, map_rx) = async_channel::bounded(buf_size);
        let (terminate_tx, _terminate_rx22) = tokio::sync::broadcast::channel(1);

        let map_fut = {
            let terminate_tx = terminate_tx.clone();

            async move {
                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            if map_tx.send(fut).await.is_err() {
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
                            result = map_rx.recv() => {
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
        };

        TryParForEach {
            fut: Some(Box::pin(output_fut)),
        }
    }

    /// Runs an fallible blocking task on each element of an stream in parallel.
    fn try_par_for_each_init<B, InitF, MapF, Fut>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut map_f: MapF,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), Self::Error>> + Send,
    {
        let init = init_f();
        self.try_par_for_each(config, move |item| map_f(init.clone(), item))
    }

    fn try_par_for_each_blocking<F, Func>(
        self,
        config: impl IntoParStreamParams,
        mut f: F,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), Self::Error> + Send,
    {
        self.try_par_for_each(config, move |item| {
            let func = f(item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }

    /// Creates a fallible parallel stream analogous to [try_par_for_each_blocking](TryParStreamExt::try_par_for_each_blocking)
    /// with a in-local thread initializer.
    fn try_par_for_each_blocking_init<B, InitF, MapF, Func>(
        self,
        config: impl IntoParStreamParams,
        mut init_f: InitF,
        mut f: MapF,
    ) -> TryParForEach<Self::Error>
    where
        Self: 'static + TryStreamExt + Sized + Unpin + Send,
        Self::Ok: Send,
        Self::Error: Send,
        B: 'static + Send + Clone,
        InitF: FnMut() -> B,
        MapF: 'static + FnMut(B, Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), Self::Error> + Send,
    {
        let init = init_f();

        self.try_par_for_each(config, move |item| {
            let func = f(init.clone(), item);
            rt::spawn_blocking(func).map(|result| result.unwrap())
        })
    }
}

impl<S> TryParStreamExt for S where S: TryStream {}

// try_tee

pub use try_tee::*;

mod try_tee {
    use super::*;

    /// A fallible stream combinator returned from [try_tee()](TryParStreamExt::try_tee).
    #[derive(Debug)]
    pub struct TryTee<T, E> {
        pub(super) buf_size: Option<usize>,
        pub(super) future: Arc<Mutex<Option<rt::JoinHandle<()>>>>,
        pub(super) sender_set:
            Weak<flurry::HashSet<ByAddress<Arc<async_channel::Sender<Result<T, E>>>>>>,
        pub(super) receiver: async_channel::Receiver<Result<T, E>>,
    }

    impl<T, E> Clone for TryTee<T, E>
    where
        T: 'static + Send,
        E: 'static + Send,
    {
        fn clone(&self) -> Self {
            let buf_size = self.buf_size;
            let (tx, rx) = match buf_size {
                Some(buf_size) => async_channel::bounded(buf_size),
                None => async_channel::unbounded(),
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

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Ok(mut future_opt) = self.future.try_lock() {
                if let Some(future) = &mut *future_opt {
                    if Pin::new(future).poll(cx).is_ready() {
                        *future_opt = None;
                    }
                }
            }

            Pin::new(&mut self.receiver).poll_next(cx)
        }
    }
}

// try_par_then

pub use try_par_then::*;

mod try_par_then {
    use super::*;

    /// A fallible stream combinator returned from [try_par_map()](TryParStreamExt::try_par_map) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParMap<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<((), (), Vec<()>)>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<Result<T, E>>,
    }

    impl<T, E> Stream for TryParMap<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// try_par_then_unordered

pub use try_par_map_unordered::*;

mod try_par_map_unordered {
    use super::*;

    /// A fallible stream combinator returned from [try_par_map_unordered()](TryParStreamExt::try_par_map_unordered) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParMapUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = NullResult<((), Vec<()>)>> + Send>>>,
        #[derivative(Debug = "ignore")]
        pub(super) output_rx: async_channel::Receiver<Result<T, E>>,
    }

    impl<T, E> Stream for TryParMapUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut should_wake = match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => true,
                    Poll::Ready(_) => {
                        self.fut = None;
                        false
                    }
                },
                None => false,
            };

            let poll = Pin::new(&mut self.output_rx).poll_next(cx);
            should_wake |= !self.output_rx.is_empty();

            if should_wake {
                cx.waker().wake_by_ref();
            }

            poll
        }
    }
}

// try_par_for_each

pub use try_par_for_each::*;

mod try_par_for_each {
    use super::*;

    /// A fallible stream combinator returned from [try_par_for_each()](TryParStreamExt::try_par_for_each) and its siblings.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParForEach<E> {
        #[derivative(Debug = "ignore")]
        pub(super) fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>>,
    }

    impl<E> Future for TryParForEach<E> {
        type Output = Result<(), E>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            match self.fut.as_mut() {
                Some(fut) => match Pin::new(fut).poll(cx) {
                    Poll::Pending => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(result) => {
                        self.fut = None;
                        Poll::Ready(result)
                    }
                },
                None => unreachable!(),
            }
        }
    }
}

// try_wrapping_enumerate

pub use try_wrapping_enumerate::*;

mod try_wrapping_enumerate {
    use super::*;

    /// A fallible stream combinator returned from [try_wrapping_enumerate()](TryParStreamExt::try_wrapping_enumerate).
    #[derive(Debug)]
    pub struct TryWrappingEnumerate<T, E, S>
    where
        S: Stream<Item = Result<T, E>> + Send,
    {
        pub(super) stream: S,
        pub(super) counter: usize,
        pub(super) fused: bool,
    }

    impl<T, E, S> Stream for TryWrappingEnumerate<T, E, S>
    where
        S: Stream<Item = Result<T, E>> + Unpin + Send,
    {
        type Item = Result<(usize, T), E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            if self.fused {
                return Poll::Ready(None);
            }

            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    let index = self.counter;
                    self.counter = self.counter.wrapping_add(1);
                    Poll::Ready(Some(Ok((index, item))))
                }
                Poll::Ready(Some(Err(err))) => {
                    self.fused = true;
                    Poll::Ready(Some(Err(err)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<T, E, S> FusedStream for TryWrappingEnumerate<T, E, S>
    where
        S: Stream<Item = Result<T, E>> + Unpin + Send,
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

    /// A fallible stream combinator returned from [try_reorder_enumerated()](TryParStreamExt::try_reorder_enumerated).
    #[pin_project(project = TryReorderEnumeratedProj)]
    #[derive(Debug)]
    pub struct TryReorderEnumerated<T, E, S>
    where
        S: Stream<Item = Result<(usize, T), E>> + Send,
    {
        #[pin]
        pub(super) stream: S,
        pub(super) counter: usize,
        pub(super) fused: bool,
        pub(super) buffer: HashMap<usize, T>,
    }

    impl<T, E, S> Stream for TryReorderEnumerated<T, E, S>
    where
        S: Stream<Item = Result<(usize, T), E>> + Unpin + Send,
    {
        type Item = Result<T, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let TryReorderEnumeratedProj {
                stream,
                counter,
                fused,
                buffer,
            } = self.project();

            if *fused {
                return Poll::Ready(None);
            }

            // get item from buffer
            let buffered_item_opt = buffer.remove(counter);

            if buffered_item_opt.is_some() {
                *counter = counter.wrapping_add(1);
            }

            match (stream.poll_next(cx), buffered_item_opt) {
                (Poll::Ready(Some(Ok((index, item)))), Some(buffered_item)) => {
                    assert!(
                        *counter <= index,
                        "the enumerated index {} appears more than once",
                        index
                    );

                    buffer.insert(index, item);
                    Poll::Ready(Some(Ok(buffered_item)))
                }
                (Poll::Ready(Some(Ok((index, item)))), None) => match (*counter).cmp(&index) {
                    Ordering::Less => {
                        buffer.insert(index, item);
                        Poll::Pending
                    }
                    Ordering::Equal => {
                        *counter = counter.wrapping_add(1);
                        Poll::Ready(Some(Ok(item)))
                    }
                    Ordering::Greater => {
                        panic!("the enumerated index {} appears more than once", index)
                    }
                },
                (Poll::Ready(Some(Err(err))), _) => {
                    *fused = true;
                    Poll::Ready(Some(Err(err)))
                }
                (_, Some(buffered_item)) => Poll::Ready(Some(Ok(buffered_item))),
                (Poll::Ready(None), None) => {
                    if buffer.is_empty() {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
                (Poll::Pending, None) => Poll::Pending,
            }
        }
    }

    impl<T, E, S> FusedStream for TryReorderEnumerated<T, E, S>
    where
        S: Stream<Item = Result<(usize, T), E>> + Unpin + Send,
        T: Unpin,
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

    /// A fallible stream combinator returned from [try_batching()](TryParStreamExt::try_batching()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryBatching<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: Pin<Box<dyn Stream<Item = Result<T, E>> + Send>>,
        pub(super) request: Arc<Notify>,
        pub(super) terminate: Arc<Notify>,
        pub(super) requested: bool,
    }

    impl<T, E> Stream for TryBatching<T, E> {
        type Item = Result<T, E>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if !self.requested {
                self.requested = true;
                self.request.notify_one();
            }

            let poll = Pin::new(&mut self.stream).poll_next(context);

            match poll {
                Poll::Pending => Poll::Pending,
                Poll::Ready(output) => {
                    self.requested = false;
                    Poll::Ready(output)
                }
            }
        }
    }

    impl<T, E> Drop for TryBatching<T, E> {
        fn drop(&mut self) {
            self.terminate.notify_one();
        }
    }
}

// try_par_batching_unordered

pub use try_par_batching_unordered::*;

mod try_par_batching_unordered {
    use super::*;

    /// A fallible stream combinator returned from [try_par_batching_unordered()](TryParStreamExt::try_par_batching_unordered()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParBatchingUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) future: Option<Pin<Box<dyn Future<Output = Result<(), rt::JoinError>> + Send>>>,
        pub(super) output_rx: Option<async_channel::Receiver<Result<T, E>>>,
    }

    impl<T, E> Stream for TryParBatchingUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut should_wake = false;

            if let Some(future) = &mut self.future {
                if let Poll::Ready(result) = Pin::new(future).poll(cx) {
                    result.unwrap();
                    self.future = None;
                } else {
                    should_wake = true;
                }
            }

            let output = if let Some(output_rx) = &mut self.output_rx {
                should_wake = true;
                let poll = Pin::new(&mut output_rx.recv()).poll(cx);

                match poll {
                    Poll::Ready(Ok(Ok(output))) => Poll::Ready(Some(Ok(output))),
                    Poll::Ready(Ok(Err(err))) => {
                        self.output_rx = None;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(Err(_)) => {
                        self.output_rx = None;
                        Poll::Ready(None)
                    }
                    Poll::Pending => Poll::Pending,
                }
            } else {
                Poll::Ready(None)
            };

            if should_wake {
                cx.waker().wake_by_ref();
            }

            output
        }
    }
}

// try_unfold_blocking

pub use try_unfold_blocking::*;

mod try_unfold_blocking {
    use super::*;

    /// A fallible stream combinator returned from [try_unfold_blocking()](super::try_unfold_blocking()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryUnfoldBlocking<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: Pin<Box<dyn Stream<Item = Result<T, E>> + Send>>,
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

    /// A stream combinator returned from [try_par_unfold_unordered()](super::try_par_unfold_unordered())
    /// and  [try_par_unfold_blocking_unordered()](super::try_par_unfold_blocking_unordered()).
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct TryParUnfoldUnordered<T, E> {
        #[derivative(Debug = "ignore")]
        pub(super) stream: Option<Pin<Box<dyn Stream<Item = Result<T, E>> + Send>>>,
    }

    impl<T, E> Stream for TryParUnfoldUnordered<T, E> {
        type Item = Result<T, E>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(stream) = &mut self.stream {
                let poll = Pin::new(stream).poll_next(cx);

                if let Poll::Ready(Some(Err(_)) | None) = &poll {
                    self.stream = None;
                }
                poll
            } else {
                Poll::Ready(None)
            }
        }
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;
    use std::time::Duration;

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
                .try_par_batching_unordered(
                    None,
                    || async move { Err("init error") },
                    |val, sum: usize| async move {
                        let sum = sum + val;

                        if sum >= 3 {
                            Ok(ControlFlow::Break(sum))
                        } else {
                            Ok(ControlFlow::Continue(sum))
                        }
                    },
                    |sum| async move { Ok(Some(sum)) },
                );

            assert_eq!(stream.next().await, Some(Err("init error")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(iter::repeat(1).take(10))
                .map(Ok)
                .try_par_batching_unordered(
                    None,
                    || async move { Result::<_, ()>::Ok(0) },
                    |val, sum| async move {
                        let sum = sum + val;

                        if sum >= 3 {
                            Ok(ControlFlow::Break(sum))
                        } else {
                            Ok(ControlFlow::Continue(sum))
                        }
                    },
                    |sum| async move { Ok(Some(sum)) },
                );

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
                .try_par_batching_unordered(
                    None,
                    || async move { Ok(0) },
                    |val, sum| async move {
                        let sum = sum + val;

                        if sum >= 3 {
                            Ok(ControlFlow::Break(sum))
                        } else {
                            Ok(ControlFlow::Continue(sum))
                        }
                    },
                    |sum| async move { Err(sum) },
                );

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
            let mut stream = futures::stream::iter(0..10).map(Ok).try_batching(
                || async move { Err("init error") },
                |val, sum: usize| async move {
                    let sum = sum + val;

                    if sum >= 10 {
                        Ok(ControlFlow::Break(sum))
                    } else {
                        Ok(ControlFlow::Continue(sum))
                    }
                },
                |_sum| async move { Err("some elements are left behind") },
            );

            assert_eq!(stream.next().await, Some(Err("init error")));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(0..10).map(Ok).try_batching(
                || async move { Ok(0) },
                |val, sum| async move {
                    let sum = sum + val;

                    if sum >= 10 {
                        Ok(ControlFlow::Break(sum))
                    } else {
                        Ok(ControlFlow::Continue(sum))
                    }
                },
                |_sum| async move { Err("some elements are left behind") },
            );

            assert_eq!(stream.next().await, Some(Ok(10)));
            assert_eq!(stream.next().await, Some(Ok(11)));
            assert_eq!(stream.next().await, Some(Ok(15)));
            assert!(matches!(stream.next().await, Some(Err(_))));
            assert!(stream.next().await.is_none());
        }

        {
            let mut stream = futures::stream::iter(0..10).map(Ok).try_batching(
                || async move { Ok(0) },
                |val, sum| async move {
                    let sum = sum + val;

                    if sum >= 15 {
                        Err("too large")
                    } else if sum >= 10 {
                        Ok(ControlFlow::Break(sum))
                    } else {
                        Ok(ControlFlow::Continue(sum))
                    }
                },
                |_sum| async move { Err("some elements are left behind") },
            );

            assert_eq!(stream.next().await, Some(Ok(10)));
            assert_eq!(stream.next().await, Some(Ok(11)));
            assert_eq!(stream.next().await, Some(Err("too large")));
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn try_batching_on_demand_test() {
        let count = Arc::new(AtomicUsize::new(0));

        let mut stream = {
            let count = count.clone();
            futures::stream::iter(0..).map(Ok).try_batching(
                move || {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Release);
                        Result::<_, ()>::Ok(0)
                    }
                },
                |val, sum| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let sum = sum + val;

                    if sum >= 10 {
                        Ok(ControlFlow::Break(sum))
                    } else {
                        Ok(ControlFlow::Continue(sum))
                    }
                },
                |_sum| async move { Ok(None) },
            )
        };

        // request 3 times
        assert_eq!(stream.next().await, Some(Ok(10)));
        assert_eq!(stream.next().await, Some(Ok(11)));
        assert_eq!(stream.next().await, Some(Ok(15)));
        drop(stream);

        // the init_fn must be called exactly 3 times
        assert_eq!(count.load(Acquire), 3);
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
    async fn try_par_then_test() {
        let mut stream =
            futures::stream::iter(vec![Ok(1usize), Ok(2), Err(-3isize), Ok(4)].into_iter())
                .try_par_then(None, |value| async move { Ok(value) });

        assert_eq!(stream.try_next().await, Ok(Some(1usize)));
        assert_eq!(stream.try_next().await, Ok(Some(2usize)));
        assert_eq!(stream.try_next().await, Err(-3isize));
        assert_eq!(stream.try_next().await, Ok(Some(4usize)));
        assert_eq!(stream.try_next().await, Ok(None));
    }

    #[tokio::test]
    async fn try_reorder_enumerated_test() {
        let len: usize = 1000;
        let mut rng = rand::thread_rng();

        for _ in 0..10 {
            let err_index_1 = rng.gen_range(0..len);
            let err_index_2 = rng.gen_range(0..len);
            let min_err_index = err_index_1.min(err_index_2);

            let results = futures::stream::iter(0..len)
                .map(move |value| {
                    if value == err_index_1 || value == err_index_2 {
                        Err(-(value as isize))
                    } else {
                        Ok(value)
                    }
                })
                .try_wrapping_enumerate()
                .try_par_then_unordered(None, |(index, value)| async move {
                    async_std::task::sleep(std::time::Duration::from_millis(value as u64 % 20))
                        .await;
                    Ok((index, value))
                })
                .try_reorder_enumerated()
                .collect::<Vec<_>>()
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
}
