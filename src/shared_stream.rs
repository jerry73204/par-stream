use crate::common::*;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use futures::task::{waker_ref, ArcWake};
use std::sync::Weak;

// constants

const IDLE: usize = 0;
const POLLING: usize = 1;
const COMPLETE: usize = 2;
const POISONED: usize = 3;

const NULL_WAKER_KEY: usize = usize::max_value();

/// Stream for the [`shared`](super::StreamExt::shared) method.
///
/// The stream is cloneable. Polling the stream will poll the internal
/// stream shared with the other owners. If there are multiple consumers
/// for the shared stream, the items are sent in first-come-first-serve manner.
#[must_use = "streams do nothing unless you consume or poll them"]
pub struct Shared<St>
where
    St: ?Sized + Stream,
{
    inner: Option<Arc<Inner<St>>>,
    waker_key: usize,
}

struct Inner<St>
where
    St: ?Sized + Stream,
{
    state: AtomicUsize,
    notifier: Arc<Notifier>,
    stream: UnsafeCell<St>,
}

struct Notifier {
    /// The number of times the stream is awaken.
    wake_count: AtomicUsize,
    /// The list of pending waker keys.
    pending_waker_keys: SegQueue<usize>,
    /// The pairs of a waker key and a waker.
    wakers: DashMap<usize, Waker>,
}

/// A weak reference to a [`Shared`] that can be upgraded much like an `Arc`.
pub struct WeakShared<St: Stream>(Weak<Inner<St>>);

impl<St: Stream> Clone for WeakShared<St> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

// The future itself is polled behind the `Arc`, so it won't be moved
// when `Shared` is moved.
impl<St: Stream> Unpin for Shared<St> {}

impl<St: Stream> fmt::Debug for Shared<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("inner", &self.inner)
            .field("waker_key", &self.waker_key)
            .finish()
    }
}

impl<St: Stream> fmt::Debug for Inner<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner").finish()
    }
}

impl<St: Stream> fmt::Debug for WeakShared<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakShared").finish()
    }
}

unsafe impl<St> Send for Inner<St>
where
    St: Stream + Send,
    St::Item: Send,
{
}

unsafe impl<St> Sync for Inner<St>
where
    St: Stream + Send,
    St::Item: Send,
{
}

impl<St: Stream> Shared<St> {
    pub fn new(stream: St) -> Self {
        let inner = Inner {
            stream: UnsafeCell::new(stream),
            state: AtomicUsize::new(IDLE),
            notifier: Arc::new(Notifier {
                wake_count: AtomicUsize::new(0),
                wakers: DashMap::new(),
                pending_waker_keys: SegQueue::new(),
            }),
        };

        Self {
            inner: Some(Arc::new(inner)),
            waker_key: NULL_WAKER_KEY,
        }
    }
}

impl<St> Shared<St>
where
    St: Stream,
{
    /// Creates a new [`WeakShared`] for this [`Shared`].
    ///
    /// Returns [`None`] if it has already been polled to completion.
    pub fn downgrade(&self) -> Option<WeakShared<St>> {
        if let Some(inner) = self.inner.as_ref() {
            return Some(WeakShared(Arc::downgrade(inner)));
        }
        None
    }

    /// Gets the number of strong pointers to this allocation.
    ///
    /// Returns [`None`] if it has already been polled to completion.
    ///
    /// # Safety
    ///
    /// This method by itself is safe, but using it correctly requires extra care. Another thread
    /// can change the strong count at any time, including potentially between calling this method
    /// and acting on the result.
    pub fn strong_count(&self) -> Option<usize> {
        self.inner.as_ref().map(Arc::strong_count)
    }

    /// Gets the number of weak pointers to this allocation.
    ///
    /// Returns [`None`] if it has already been polled to completion.
    ///
    /// # Safety
    ///
    /// This method by itself is safe, but using it correctly requires extra care. Another thread
    /// can change the weak count at any time, including potentially between calling this method
    /// and acting on the result.
    pub fn weak_count(&self) -> Option<usize> {
        self.inner.as_ref().map(Arc::weak_count)
    }
}

impl<St> Inner<St>
where
    St: Stream,
{
    /// Registers the current task to receive a wakeup when we are awoken.
    fn record_waker(&self, waker_key: &mut usize, cx: &mut Context<'_>) {
        let notifier = &self.notifier;
        let new_waker = cx.waker();

        if *waker_key == NULL_WAKER_KEY {
            *waker_key = next_waker_key();
            notifier.wakers.insert(*waker_key, new_waker.clone());
        } else {
            use dashmap::mapref::entry::Entry as E;

            match notifier.wakers.entry(*waker_key) {
                E::Occupied(entry) => {
                    let mut old_waker = entry.into_ref();

                    if !new_waker.will_wake(&*old_waker) {
                        *old_waker = new_waker.clone();
                    }
                }
                E::Vacant(entry) => {
                    entry.insert(new_waker.clone());
                }
            }
        }
        debug_assert!(*waker_key != NULL_WAKER_KEY);
    }
}

impl<St> FusedStream for Shared<St>
where
    St: Stream,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

impl<St> Stream for Shared<St>
where
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Return end of stream if polled again after completion
        let inner = match this.inner.take() {
            Some(inner) => inner,
            None => {
                return Ready(None);
            }
        };

        // Fast path for when the wrapped stream has already completed
        if inner.state.load(Acquire) == COMPLETE {
            return Ready(None);
        }

        // Make sure a waker key is registered for this waker.
        inner.record_waker(&mut this.waker_key, cx);

        // Transfer state: IDLE -> POLLING
        match inner
            .state
            .compare_exchange(IDLE, POLLING, SeqCst, SeqCst)
            .unwrap_or_else(|x| x)
        {
            IDLE => {
                // Lock acquired, fall through
            }
            POLLING => {
                // Another task is currently polling, at this point we just want
                // to ensure that the waker for this task is registered
                inner.notifier.register_pending(this.waker_key);
                this.inner = Some(inner);
                return Pending;
            }
            COMPLETE => {
                return Ready(None);
            }
            POISONED => panic!("inner stream panicked during poll"),
            _ => unreachable!(),
        }

        /* start of critical section (to the end of function) */

        // the guard marks poisoned state when dropping if panic happened
        let _reset = Reset(&inner.state);

        // create context for underlying stream
        let waker = waker_ref(&inner.notifier);
        let mut stream_cx = Context::from_waker(&waker);

        // get stream reference
        let stream = unsafe {
            let stream = &mut *inner.stream.get();
            Pin::new_unchecked(stream)
        };

        // remember the wake count before polling
        let wake_count = inner.notifier.wake_count();

        match stream.poll_next(&mut stream_cx) {
            Pending => {
                // Transfer state: POLLING -> IDLE
                inner.state.store(IDLE, SeqCst);

                // Register the waker key to pending list.
                let should_wake = inner
                    .notifier
                    .wake_or_register_pending(this.waker_key, wake_count);

                // If the wake_count changed, indicating the stream wakes earlier, wake itself.
                if should_wake {
                    cx.waker().wake_by_ref();
                }

                drop(_reset);
                this.inner = Some(inner);
                Pending
            }
            Ready(Some(item)) => {
                // Transfer state: POLLING -> IDLE
                inner.state.store(IDLE, SeqCst);

                // Wake pending tasks
                inner.notifier.notify();

                drop(_reset); // Make borrow checker happy
                this.inner = Some(inner);
                Ready(Some(item))
            }
            Ready(None) => {
                // Transfer state: POLLING -> COMPLETE
                inner.state.store(COMPLETE, SeqCst);

                // Wake all tasks
                inner.notifier.close(this.waker_key);
                drop(_reset); // Make borrow checker happy

                Ready(None)
            }
        }
    }
}

impl<St> Clone for Shared<St>
where
    St: Stream,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            waker_key: NULL_WAKER_KEY,
        }
    }
}

impl<St> Drop for Shared<St>
where
    St: ?Sized + Stream,
{
    fn drop(&mut self) {
        if self.waker_key != NULL_WAKER_KEY {
            if let Some(ref inner) = self.inner {
                inner.notifier.wakers.remove(&self.waker_key);
            }
        }
    }
}

impl ArcWake for Notifier {
    fn wake_by_ref(this: &Arc<Self>) {
        this.wake_count.fetch_add(1, SeqCst);
        this.notify();
    }
}

impl Notifier {
    fn wake_count(&self) -> usize {
        self.wake_count.load(Acquire)
    }

    /// Register the waker_key to pending list.
    fn register_pending(&self, waker_key: usize) {
        self.pending_waker_keys.push(waker_key);
    }

    /// Wake or register the waker_key to pending list according to expected wake count.
    ///
    /// The methods returns whether to wake or not.
    fn wake_or_register_pending(&self, waker_key: usize, expected_wake_count: usize) -> bool {
        debug_assert!(waker_key != NULL_WAKER_KEY);
        self.pending_waker_keys.push(waker_key);
        self.wake_count
            .compare_exchange(expected_wake_count, expected_wake_count, SeqCst, SeqCst)
            .is_err()
    }

    fn notify(&self) {
        while let Some(waker_key) = self.pending_waker_keys.pop() {
            if let Some(waker) = self.wakers.get(&waker_key) {
                waker.wake_by_ref();
            }
        }
    }

    fn close(&self, waker_key: usize) {
        debug_assert!(waker_key != NULL_WAKER_KEY);

        self.wakers.retain(|&key, waker| {
            if key != waker_key {
                waker.wake_by_ref();
            }
            false
        });
    }
}

impl<St: Stream> WeakShared<St> {
    /// Attempts to upgrade this [`WeakShared`] into a [`Shared`].
    ///
    /// Returns [`None`] if all clones of the [`Shared`] have been dropped or polled
    /// to completion.
    pub fn upgrade(&self) -> Option<Shared<St>> {
        Some(Shared {
            inner: Some(self.0.upgrade()?),
            waker_key: NULL_WAKER_KEY,
        })
    }
}

struct Reset<'a>(&'a AtomicUsize);

impl Drop for Reset<'_> {
    fn drop(&mut self) {
        use std::thread;

        if thread::panicking() {
            self.0.store(POISONED, SeqCst);
        }
    }
}

fn next_waker_key() -> usize {
    static KEY: AtomicUsize = AtomicUsize::new(0);
    KEY.fetch_add(1, SeqCst)
}
