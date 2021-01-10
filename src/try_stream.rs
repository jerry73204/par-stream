use crate::{
    common::*,
    config::{IntoParStreamParams, ParStreamParams},
};

/// An extension trait for [TryStream](TryStream) that provides parallel combinator functions.
pub trait TryParStreamExt {
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
        let (map_tx, map_rx) = async_std::sync::channel(buf_size);
        let (reorder_tx, reorder_rx) = async_std::sync::channel(buf_size);
        let (output_tx, output_rx) = async_std::sync::channel(buf_size);

        let map_fut = {
            let reorder_tx = reorder_tx.clone();
            async move {
                let mut counter = 0u64;

                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            map_tx.send((counter, fut)).await;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            reorder_tx.send((counter, Err(err))).await;
                        }
                    }
                    counter = counter.wrapping_add(1);
                }
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

                output_tx.send(output).await;
                counter = counter.wrapping_add(1);

                while let Some(output) = pool.remove(&counter) {
                    output_tx.send(output).await;
                    counter = counter.wrapping_add(1);
                }
            }
        };

        let worker_futs = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let reorder_tx = reorder_tx.clone();

                let worker_fut = async move {
                    while let Ok((index, fut)) = map_rx.recv().await {
                        let output = fut.await;
                        reorder_tx.send((index, output)).await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect::<Vec<_>>();

        let par_then_fut =
            futures::future::join3(map_fut, reorder_fut, futures::future::join_all(worker_futs));

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

    /// Fallible parallel stream that does not respect the ordering of input items.
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
        let (map_tx, map_rx) = async_std::sync::channel(buf_size);
        let (output_tx, output_rx) = async_std::sync::channel(buf_size);

        let map_fut = {
            let output_tx = output_tx.clone();
            async move {
                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            map_tx.send(fut).await;
                        }
                        Ok(None) => break,
                        Err(err) => {
                            output_tx.send(Err(err)).await;
                        }
                    }
                }
            }
        };

        let worker_futs = (0..num_workers)
            .map(|_| {
                let map_rx = map_rx.clone();
                let output_tx = output_tx.clone();

                let worker_fut = async move {
                    while let Ok(fut) = map_rx.recv().await {
                        let result = fut.await;
                        output_tx.send(result).await;
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
            })
            .collect::<Vec<_>>();

        let par_then_fut = futures::future::join(map_fut, futures::future::join_all(worker_futs));

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
            async_std::task::spawn_blocking(func)
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
            async_std::task::spawn_blocking(func)
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
            async_std::task::spawn_blocking(func)
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
            async_std::task::spawn_blocking(func)
        })
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
        let (map_tx, map_rx) = async_std::sync::channel(buf_size);
        let (terminate_tx, _terminate_rx22) = tokio::sync::broadcast::channel(1);

        let map_fut = {
            let terminate_tx = terminate_tx.clone();

            async move {
                loop {
                    match self.try_next().await {
                        Ok(Some(item)) => {
                            let fut = f(item);
                            map_tx.send(fut).await;
                        }
                        Ok(None) => break Ok(()),
                        Err(err) => {
                            // shutdown workers
                            let _ = terminate_tx.send(());

                            // output error
                            break Err(err);
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
                                match result {
                                    Ok(fut) => {
                                        if let Err(err) = fut.await {
                                            // shutdown workers
                                            let _ = terminate_tx.send(());

                                            // output error
                                            break Err(err);
                                        }
                                    }
                                    Err(_) => break Ok(()),
                                }
                            }
                            _ = terminate_rx.recv() => break Ok(()),
                        }
                    }
                };
                let worker_fut = async_std::task::spawn(worker_fut);
                worker_fut
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
            async_std::task::spawn_blocking(func)
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
            async_std::task::spawn_blocking(func)
        })
    }
}

impl<S> TryParStreamExt for S where S: TryStream {}

// try_par_then

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TryParMap<T, E> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), (), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::sync::Receiver<Result<T, E>>,
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

// try_par_then_unordered

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TryParMapUnordered<T, E> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = ((), Vec<()>)> + Send>>>,
    #[derivative(Debug = "ignore")]
    output_rx: async_std::sync::Receiver<Result<T, E>>,
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

// try_par_for_each

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TryParForEach<E> {
    #[derivative(Debug = "ignore")]
    fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send>>>,
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

// try_wrapping_enumerate

#[derive(Debug)]
pub struct TryWrappingEnumerate<T, E, S>
where
    S: Stream<Item = Result<T, E>> + Send,
{
    stream: S,
    counter: usize,
    fused: bool,
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

// reorder_enumerated

#[pin_project(project = TryReorderEnumeratedProj)]
#[derive(Debug)]
pub struct TryReorderEnumerated<T, E, S>
where
    S: Stream<Item = Result<(usize, T), E>> + Send,
{
    #[pin]
    stream: S,
    counter: usize,
    fused: bool,
    buffer: HashMap<usize, T>,
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

        if let Some(_) = buffered_item_opt {
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

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[async_std::test]
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

    #[async_std::test]
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

    #[async_std::test]
    async fn try_reorder_enumerated_test() {
        let len: usize = 1000;
        let mut rng = rand::thread_rng();

        for _ in 0..10 {
            let err_index_1 = rng.gen_range(0, len);
            let err_index_2 = rng.gen_range(0, len);
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
