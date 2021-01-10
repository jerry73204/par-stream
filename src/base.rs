use crate::common::*;

// ParStreamExt trait

pub trait ParStreamExt {
    fn wrapping_enumerate<T>(self) -> WrappingEnumerate<T, Self>
    where
        Self: Stream<Item = T> + Sized + Unpin,
    {
        WrappingEnumerate {
            stream: self,
            counter: 0,
        }
    }

    fn reorder_enumerated<T>(self) -> ReorderEnumerated<T, Self>
    where
        Self: Stream<Item = (usize, T)> + Unpin + Sized,
    {
        ReorderEnumerated {
            stream: self,
            counter: 0,
            buffer: HashMap::new(),
        }
    }
}

impl<S> ParStreamExt for S where S: Stream {}

// wrapping_enumerate

#[pin_project(project = WrappingEnumerateProj)]
#[derive(Debug)]
pub struct WrappingEnumerate<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    #[pin]
    stream: S,
    counter: usize,
}

impl<T, S> Stream for WrappingEnumerate<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    type Item = (usize, T);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let WrappingEnumerateProj { stream, counter } = self.project();

        match stream.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let index = *counter;
                *counter = counter.wrapping_add(1);
                Poll::Ready(Some((index, item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// reorder_enumerated

#[pin_project]
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ReorderEnumerated<T, S>
where
    S: Stream<Item = (usize, T)> + Unpin,
{
    #[pin]
    stream: S,
    counter: usize,
    buffer: HashMap<usize, T>,
}

impl<T, S> Stream for ReorderEnumerated<T, S>
where
    S: Stream<Item = (usize, T)> + Unpin,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let stream = this.stream;
        let counter = this.counter;
        let buffer = this.buffer;

        let buffered_item_opt = buffer.remove(counter);
        if let Some(_) = buffered_item_opt {
            *counter = counter.wrapping_add(1);
        }

        match (stream.poll_next(cx), buffered_item_opt) {
            (Poll::Ready(Some((index, item))), Some(buffered_item)) => {
                assert!(
                    *counter <= index,
                    "the enumerated index {} appears more than once",
                    index
                );
                buffer.insert(index, item);
                Poll::Ready(Some(buffered_item))
            }
            (Poll::Ready(Some((index, item))), None) => match (*counter).cmp(&index) {
                Ordering::Less => {
                    buffer.insert(index, item);
                    Poll::Pending
                }
                Ordering::Equal => {
                    *counter = counter.wrapping_add(1);
                    Poll::Ready(Some(item))
                }
                Ordering::Greater => {
                    panic!("the enumerated index {} appears more than once", index)
                }
            },
            (_, Some(buffered_item)) => Poll::Ready(Some(buffered_item)),
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

// TryParStreamExt trait

pub trait TryParStreamExt {
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
}

impl<S> TryParStreamExt for S where S: Stream {}

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

// try_reorder_enumerated

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
