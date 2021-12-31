use crate::common::*;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use futures::task::{waker_ref, ArcWake};
use std::sync::{RwLock, Weak};

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
    cache: RwLock<Option<WakerCache>>,
}

struct WakerCache {
    pending_waker_keys: SegQueue<usize>,
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
                cache: RwLock::new(Some(WakerCache {
                    wakers: DashMap::new(),
                    pending_waker_keys: SegQueue::new(),
                })),
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
        let guard = self.notifier.cache.read().unwrap();

        let cache = match guard.as_ref() {
            Some(cache) => cache,
            None => return,
        };

        let new_waker = cx.waker();

        if *waker_key == NULL_WAKER_KEY {
            *waker_key = next_waker_key();
            cache.wakers.insert(*waker_key, new_waker.clone());
        } else {
            use dashmap::mapref::entry::Entry as E;

            match cache.wakers.entry(*waker_key) {
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

        let inner = this
            .inner
            .take()
            .expect("Shared stream polled again after completion");

        // Fast path for when the wrapped future has already completed
        if inner.state.load(Acquire) == COMPLETE {
            return Ready(None);
        }

        inner.record_waker(&mut this.waker_key, cx);

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

        let waker = waker_ref(&inner.notifier);
        let mut cx = Context::from_waker(&waker);

        let _reset = Reset(&inner.state);

        let stream = unsafe {
            let stream = &mut *inner.stream.get();
            Pin::new_unchecked(stream)
        };

        match stream.poll_next(&mut cx) {
            Pending => {
                let ok = inner
                    .state
                    .compare_exchange(POLLING, IDLE, SeqCst, SeqCst)
                    .is_ok();
                debug_assert!(ok);

                drop(_reset);
                inner.notifier.register_pending(this.waker_key);
                this.inner = Some(inner);
                Pending
            }
            Ready(Some(item)) => {
                let ok = inner
                    .state
                    .compare_exchange(POLLING, IDLE, SeqCst, SeqCst)
                    .is_ok();
                debug_assert!(ok);

                // Wake another task
                inner.notifier.notify_one();
                drop(_reset); // Make borrow checker happy
                this.inner = Some(inner);

                Ready(Some(item))
            }
            Ready(None) => {
                inner.state.store(COMPLETE, SeqCst);

                // Wake all tasks and drop the cache
                inner.notifier.notify_all();
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
                if let Ok(cache) = inner.notifier.cache.read() {
                    if let Some(cache) = cache.as_ref() {
                        cache.wakers.remove(&self.waker_key);
                    }
                }
            }
        }
    }
}

impl ArcWake for Notifier {
    fn wake_by_ref(this: &Arc<Self>) {
        this.notify_one();
    }
}

impl Notifier {
    fn register_pending(&self, waker_key: usize) {
        debug_assert!(waker_key != NULL_WAKER_KEY);
        let guard = self.cache.read().unwrap();

        if let Some(cache) = &*guard {
            cache.pending_waker_keys.push(waker_key);
        }
    }

    fn notify_one(&self) {
        let guard = self.cache.read().unwrap();

        if let Some(cache) = &*guard {
            let WakerCache {
                pending_waker_keys,
                wakers,
            } = cache;

            while let Some(waker_key) = pending_waker_keys.pop() {
                if let Some(waker) = wakers.get(&waker_key) {
                    waker.wake_by_ref();
                }
            }
        }
    }

    fn notify_all(&self) {
        let mut guard = self.cache.write().unwrap();

        if let Some(cache) = guard.take() {
            cache.wakers.into_iter().for_each(|(_, waker)| {
                waker.wake();
            });
        }
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
