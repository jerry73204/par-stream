use crate::common::*;
use tokio::sync::oneshot;

pub fn new<T>(init: T) -> StateStream<T> {
    StateStream::new(init)
}

#[pin_project]
pub struct StateStream<T> {
    #[pin]
    receiver: Option<oneshot::Receiver<T>>,
    value: Option<T>,
}

impl<T> StateStream<T> {
    pub fn new(init: T) -> Self {
        Self {
            value: Some(init),
            receiver: None,
        }
    }
}

impl<T> Stream for StateStream<T> {
    type Item = Handle<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        Ready(loop {
            if let Some(value) = this.value.take() {
                let (tx, rx) = oneshot::channel();
                this.receiver.set(Some(rx));
                break Some(Handle {
                    inner: Some(Inner { value, sender: tx }),
                });
            } else if let Some(receiver) = this.receiver.as_mut().as_pin_mut() {
                match ready!(receiver.poll(cx)) {
                    Ok(value) => {
                        *this.value = Some(value);
                        this.receiver.set(None);
                    }
                    Err(_) => {
                        this.receiver.set(None);
                        break None;
                    }
                }
            } else {
                break None;
            }
        })
    }
}

pub struct Handle<T> {
    inner: Option<Inner<T>>,
}

struct Inner<T> {
    value: T,
    sender: oneshot::Sender<T>,
}

impl<T> Handle<T> {
    fn inner(&self) -> &Inner<T> {
        self.inner.as_ref().unwrap()
    }

    pub fn send(mut self) -> Result<(), T> {
        let Inner { value, sender } = self.inner.take().unwrap();
        sender.send(value)
    }

    pub fn take(mut self) -> T {
        self.inner.take().unwrap().value
    }

    pub fn close(mut self) {
        let _ = self.inner.take();
    }
}

impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        if let Some(Inner { value, sender }) = self.inner.take() {
            let _ = sender.send(value);
        }
    }
}

impl<T> Debug for Handle<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.inner().value.fmt(f)
    }
}

impl<T> Display for Handle<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.inner().value.fmt(f)
    }
}

impl<T> PartialEq<T> for Handle<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        self.inner().value.eq(other)
    }
}

impl<T> PartialOrd<T> for Handle<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &T) -> Option<cmp::Ordering> {
        self.inner().value.partial_cmp(other)
    }
}

impl<T> Hash for Handle<T>
where
    T: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.inner().value.hash(state);
    }
}

impl<T> Deref for Handle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner().value
    }
}

impl<T> DerefMut for Handle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner.as_mut().unwrap().value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn state_stream_test() {
        let quota = 100;

        let state_stream = super::new(0);

        let count: usize = stream::repeat(())
            .zip(state_stream)
            .filter_map(|((), mut cost)| async move {
                if *cost < quota {
                    *cost += 1;
                    cost.send().unwrap();
                    Some(())
                } else {
                    cost.close();
                    None
                }
            })
            .count()
            .await;

        assert_eq!(count, quota);
    }

    #[tokio::test]
    async fn state_stream_simple_test() {
        {
            let mut state_stream = super::new(0);

            let handle = state_stream.next().await.unwrap();
            handle.send().unwrap();

            let handle = state_stream.next().await.unwrap();
            drop(handle);

            let handle = state_stream.next().await.unwrap();
            handle.take();

            assert!(state_stream.next().await.is_none());
        }

        {
            let mut state_stream = super::new(0);
            let handle = state_stream.next().await.unwrap();
            drop(state_stream);
            assert!(handle.send().is_err());
        }

        {
            let mut state_stream = super::new(0);
            let handle = state_stream.next().await.unwrap();
            handle.close();
            assert!(state_stream.next().await.is_none());
        }
    }
}
