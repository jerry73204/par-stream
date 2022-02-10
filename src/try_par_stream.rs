use crate::{
    common::*,
    config::{BufSize, ParParams},
    par_stream::ParStreamExt as _,
    rt,
    stream::StreamExt as _,
    try_index_stream::TryIndexStreamExt as _,
    try_stream::{TakeUntilError, TryStreamExt as _},
    utils,
};
use flume::r#async::RecvStream;
use tokio::sync::broadcast;

/// Stream for the [try_par_batching()](TryParStreamExt::try_par_batching) method.
pub type TryParBatching<T, E> = TakeUntilError<RecvStream<'static, Result<T, E>>, T, E>;

/// The trait extends [TryStream](futures::stream::TryStream) types with parallel processing combinators.
pub trait TryParStreamExt
where
    Self: 'static + Send + TryStream,
    Self::Ok: 'static + Send,
    Self::Error: 'static + Send,
{
    /// Fallible stream combinator for [map_blocking](crate::ParStreamExt::map_blocking).
    fn try_map_blocking<B, T, F>(
        self,
        buf_size: B,
        f: F,
    ) -> RecvStream<'static, Result<T, Self::Error>>
    where
        B: Into<BufSize>,
        T: Send,
        F: 'static + Send + FnMut(Self::Ok) -> Result<T, Self::Error>;

    /// Fallible stream combinator for [par_batching](crate::ParStreamExt::par_batching).
    fn try_par_batching<U, P, F, Fut>(self, params: P, f: F) -> TryParBatching<U, Self::Error>
    where
        Self: Sized,
        P: Into<ParParams>,
        F: 'static
            + Clone
            + Send
            + FnMut(usize, flume::Receiver<Result<Self::Ok, Self::Error>>) -> Fut,
        Fut: 'static
            + Future<
                Output = Result<
                    Option<(U, flume::Receiver<Result<Self::Ok, Self::Error>>)>,
                    Self::Error,
                >,
            >
            + Send,
        U: 'static + Send;

    /// Fallible stream combinator for [par_then](crate::ParStreamExt::par_then).
    fn try_par_then<U, P, F, Fut>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, Self::Error>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, Self::Error>> + Send;

    /// Fallible stream combinator for [par_then_unordered](crate::ParStreamExt::par_then_unordered).
    fn try_par_then_unordered<U, P, F, Fut>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, Self::Error>>
    where
        U: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, Self::Error>> + Send,
        P: Into<ParParams>;

    /// Fallible stream combinator for [par_map](crate::ParStreamExt::par_map).
    fn try_par_map<U, P, F, Func>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, Self::Error>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, Self::Error> + Send;

    /// Fallible stream combinator for [par_map_unordered](crate::ParStreamExt::par_map_unordered).
    fn try_par_map_unordered<U, P, F, Func>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, Self::Error>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, Self::Error> + Send;

    /// Fallible stream combinator for [par_for_each](crate::par_stream::ParStreamExt::par_for_each).
    fn try_par_for_each<P, F, Fut>(
        self,
        params: P,
        f: F,
    ) -> BoxFuture<'static, Result<(), Self::Error>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(Self::Ok) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), Self::Error>> + Send;

    /// Fallible stream combinator for [par_for_each_blocking](crate::par_stream::ParStreamExt::par_for_each_blocking).
    fn try_par_for_each_blocking<P, F, Func>(
        self,
        params: P,
        f: F,
    ) -> BoxFuture<'static, Result<(), Self::Error>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(Self::Ok) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), Self::Error> + Send;
}

impl<S, T, E> TryParStreamExt for S
where
    Self: 'static + Send + Stream<Item = Result<T, E>>,
    T: 'static + Send,
    E: 'static + Send,
{
    fn try_map_blocking<B, U, F>(self, buf_size: B, mut f: F) -> RecvStream<'static, Result<U, E>>
    where
        B: Into<BufSize>,
        U: Send,
        F: 'static + Send + FnMut(T) -> Result<U, E>,
    {
        let buf_size = buf_size.into().get();
        let mut stream = self.boxed();
        let (output_tx, output_rx) = utils::channel(buf_size);

        rt::spawn_blocking(move || loop {
            match rt::block_on(stream.next()) {
                Some(Ok(input)) => {
                    let result = f(input);
                    let is_err = result.is_err();

                    if output_tx.send(result).is_err() {
                        break;
                    }

                    if is_err {
                        break;
                    }
                }
                Some(Err(err)) => {
                    let _ = output_tx.send(Err(err));
                    break;
                }
                None => break,
            }
        });

        output_rx.into_stream()
    }

    fn try_par_batching<U, P, F, Fut>(self, params: P, f: F) -> TryParBatching<U, E>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static
            + Clone
            + Send
            + FnMut(usize, flume::Receiver<Result<Self::Ok, Self::Error>>) -> Fut,
        Fut: 'static
            + Future<
                Output = Result<
                    Option<(U, flume::Receiver<Result<Self::Ok, Self::Error>>)>,
                    Self::Error,
                >,
            >
            + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();

        let (input_tx, input_rx) = utils::channel(buf_size);
        let (output_tx, output_rx) = utils::channel(buf_size);
        let (terminate_tx, _) = broadcast::channel(1);

        rt::spawn(async move {
            let _ = self.map(Ok).forward(input_tx.into_sink()).await;
        });

        (0..num_workers).for_each(move |worker_index| {
            let input_rx = input_rx.clone();
            let output_tx = output_tx.clone();
            let mut terminate_rx = terminate_tx.subscribe();
            let terminate_tx = terminate_tx.clone();
            let f = f.clone();

            rt::spawn(async move {
                let _ = stream::repeat(())
                    .take_until(async move {
                        let _ = terminate_rx.recv().await;
                    })
                    .stateful_then(
                        Some((f, terminate_tx, input_rx)),
                        move |state, ()| async move {
                            let (mut f, terminate_tx, input_rx) = state.unwrap();
                            let result = f(worker_index, input_rx).await;

                            if result.is_err() {
                                let _ = terminate_tx.send(());
                            }

                            match result {
                                Ok(Some((item, input_rx))) => {
                                    Some((Some((f, terminate_tx, input_rx)), Ok(item)))
                                }
                                Ok(None) => None,
                                Err(err) => Some((None, Err(err))),
                            }
                        },
                    )
                    .take_until_error()
                    .map(Ok)
                    .forward(output_tx.into_sink())
                    .await;
            });
        });

        output_rx.into_stream().take_until_error()
    }

    fn try_par_then<U, P, F, Fut>(self, params: P, mut f: F) -> BoxStream<'static, Result<U, E>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
    {
        self.take_until_error()
            .enumerate()
            .par_then_unordered(params, move |(index, input)| {
                let fut = input.map(|input| f(input));

                async move {
                    let output = fut?.await?;
                    Ok((index, output))
                }
            })
            .try_reorder_enumerated()
            .boxed()
    }

    fn try_par_then_unordered<U, P, F, Fut>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, E>>
    where
        U: 'static + Send,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<U, E>> + Send,
        P: Into<ParParams>,
    {
        let (input_error, input_stream) = self.catch_error();
        let output_stream = input_stream.par_then_unordered(params, f);

        stream::select(
            input_error
                .map(|result| result.map(|()| None))
                .into_stream(),
            output_stream.map(|result| result.map(Some)),
        )
        .try_filter_map(|item| future::ok(item))
        .take_until_error()
        .boxed()
    }

    fn try_par_map<U, P, F, Func>(self, params: P, mut f: F) -> BoxStream<'static, Result<U, E>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
    {
        self.take_until_error()
            .enumerate()
            .par_map_unordered(params, move |(index, input)| {
                let func = input.map(|input| f(input));

                move || {
                    let output = (func?)()?;
                    Ok((index, output))
                }
            })
            .try_reorder_enumerated()
            .boxed()
    }

    fn try_par_map_unordered<U, P, F, Func>(
        self,
        params: P,
        f: F,
    ) -> BoxStream<'static, Result<U, E>>
    where
        P: Into<ParParams>,
        U: 'static + Send,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<U, E> + Send,
    {
        let (input_error, input_stream) = self.catch_error();
        let output_stream = input_stream.par_map_unordered(params, f);

        stream::select(
            input_error
                .map(|result| result.map(|()| None))
                .into_stream(),
            output_stream.map(|result| result.map(Some)),
        )
        .try_filter_map(|item| future::ok(item))
        .take_until_error()
        .boxed()
    }

    fn try_par_for_each<P, F, Fut>(self, params: P, f: F) -> BoxFuture<'static, Result<(), E>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(T) -> Fut + Send,
        Fut: 'static + Future<Output = Result<(), E>> + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);
        let input_stream = self
            .take_until_error()
            .take_until(async move {
                let _ = terminate_rx.recv().await;
            })
            .stateful_map(f, |mut f, item| {
                let fut = item.map(|item| f(item));
                Some((f, fut))
            })
            .spawned(buf_size);

        let worker_futures = (0..num_workers).map(move |_| {
            let terminate_tx = terminate_tx.clone();

            rt::spawn(
                input_stream
                    .clone()
                    .stateful_then(terminate_tx, |terminate_tx, fut| async move {
                        let result = async move {
                            fut?.await?;
                            Ok(())
                        }
                        .await;

                        if result.is_err() {
                            let _ = terminate_tx.send(());
                        }

                        Some((terminate_tx, result))
                    })
                    .try_for_each(|()| future::ok(())),
            )
        });

        future::try_join_all(worker_futures)
            .map(|result| result.map(|_| ()))
            .boxed()
    }

    fn try_par_for_each_blocking<P, F, Func>(
        self,
        params: P,
        f: F,
    ) -> BoxFuture<'static, Result<(), E>>
    where
        P: Into<ParParams>,
        F: 'static + FnMut(T) -> Func + Send,
        Func: 'static + FnOnce() -> Result<(), E> + Send,
    {
        let ParParams {
            num_workers,
            buf_size,
        } = params.into();
        let (terminate_tx, mut terminate_rx) = broadcast::channel(1);
        let stream = self
            .take_until_error()
            .take_until(async move {
                let _ = terminate_rx.recv().await;
            })
            .stateful_map(f, |mut f, item| {
                let fut = item.map(|item| f(item));
                Some((f, fut))
            })
            .spawned(buf_size);

        let worker_futures = (0..num_workers).map(|_| {
            let mut stream = stream.clone();
            let terminate_tx = terminate_tx.clone();

            rt::spawn_blocking(move || {
                while let Some(func) = rt::block_on(stream.next()) {
                    let result = (move || {
                        (func?)()?;
                        Ok(())
                    })();
                    if let Err(err) = result {
                        let _result = terminate_tx.send(()); // shutdown workers
                        return Err(err); // return error
                    }
                }

                Ok(())
            })
        });

        future::try_join_all(worker_futures)
            .map(|result| result.map(|_| ()))
            .boxed()
    }
}

// tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::async_test;
    use rand::prelude::*;

    async_test! {
        async fn try_par_batching_test() {
            {
                let mut stream = stream::iter(iter::repeat(1).take(10))
                    .map(Ok)
                    .try_par_batching(None, |_, _| async move {
                        Result::<Option<((), _)>, _>::Err("init error")
                    });

                assert_eq!(stream.next().await, Some(Err("init error")));
                assert!(stream.next().await.is_none());
            }

            {
                let mut stream = stream::repeat(1)
                    .take(10)
                    .map(Result::<_, ()>::Ok)
                    .try_par_batching(None, |_, rx| async move {
                        let mut sum = 0;

                        while let Ok(val) = rx.recv_async().await {
                            sum += val?;
                            if sum >= 3 {
                                return Ok(Some((sum, rx)));
                            }
                        }

                        if sum > 0 {
                            return Ok(Some((sum, rx)));
                        }

                        Ok(None)
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
                let mut stream = stream::repeat(1).take(10).map(Ok).try_par_batching(
                    None,
                    |_, rx| async move {
                        let mut sum = 0;

                        while let Ok(val) = rx.recv_async().await {
                            sum += val?;
                            if sum >= 3 {
                                return Ok(Some((sum, rx)));
                            }
                        }

                        if sum == 0 {
                            Ok(None)
                        } else {
                            Err(sum)
                        }
                    },
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


        async fn try_par_for_each_test() {
            {
                let result = stream::iter(vec![Ok(1usize), Ok(2), Ok(6), Ok(4)].into_iter())
                    .try_par_for_each(None, |_| async move { Result::<_, ()>::Ok(()) })
                    .await;

                assert_eq!(result, Ok(()));
            }

            {
                let result = stream::iter(vec![Ok(1usize), Ok(2), Err(-3isize), Ok(4)].into_iter())
                    .try_par_for_each(None, |_| async move { Ok(()) })
                    .await;

                assert_eq!(result, Err(-3));
            }
        }


        async fn try_par_for_each_blocking_test() {
            {
                let result = stream::iter(vec![Ok(1usize), Ok(2), Ok(6), Ok(4)])
                    .try_par_for_each_blocking(None, |_| || Result::<_, ()>::Ok(()))
                    .await;

                assert_eq!(result, Ok(()));
            }

            {
                let result = stream::iter(0..)
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
                let result = stream::iter(0..)
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


        async fn try_par_then_test() {
            {
                let vec: Vec<Result<_, _>> =
                    stream::iter(vec![Ok(1usize), Ok(2), Err(-3isize), Ok(4)].into_iter())
                    .try_par_then(None, |value| future::ok(value))
                    .collect()
                    .await;

                assert!(matches!(
                    *vec,
                    [Err(-3)] | [Ok(1), Err(-3)] | [Ok(2), Err(-3)] | [Ok(1), Ok(2), Err(-3)],
                ));
            }

            {
                let vec: Result<Vec<()>, ()> = stream::iter(vec![])
                    .try_par_then(None, |()| async move { Ok(()) })
                    .try_collect()
                    .await;

                assert!(matches!(vec, Ok(vec) if vec.is_empty()));
            }

            {
                let vec: Vec<Result<_, _>> = stream::iter(1..)
                    .map(Ok)
                    .try_par_then(3, |index| async move {
                        match index {
                            3 | 6 => Err(index),
                            index => Ok(index),
                        }
                    })
                    .collect()
                    .await;

                assert!(matches!(
                    *vec,
                    [Err(3)] | [Ok(1), Err(3)] | [Ok(2), Err(3)] | [Ok(1), Ok(2), Err(3)],
                ));
            }
        }


        async fn try_reorder_enumerated_test() {
            let len: usize = 1000;
            let mut rng = rand::thread_rng();

            for _ in 0..10 {
                let err_index_1 = rng.gen_range(0..len);
                let err_index_2 = rng.gen_range(0..len);
                let min_err_index = err_index_1.min(err_index_2);

                let results: Vec<_> = stream::iter(0..len)
                    .map(move |value| {
                        if value == err_index_1 || value == err_index_2 {
                            Err(-(value as isize))
                        } else {
                            Ok(value)
                        }
                    })
                    .try_enumerate()
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


        async fn try_map_blocking_test() {
            {
                let vec: Vec<_> = stream::iter(vec![Ok(1u64), Ok(2), Err(-3i64), Ok(4)])
                    .try_map_blocking(None, |val| Ok(val.pow(10)))
                    .collect()
                    .await;

                assert_eq!(vec, [Ok(1), Ok(1024), Err(-3)]);
            }

            {
                let vec: Vec<_> = stream::iter(vec![Ok(1i64), Ok(2), Err(-3i64), Ok(4)])
                    .try_map_blocking(None, |val| if val >= 2 { Err(-val) } else { Ok(val) })
                    .collect()
                    .await;

                assert_eq!(vec, [Ok(1), Err(-2)]);
            }
        }
    }
}
