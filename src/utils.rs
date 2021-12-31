pub fn channel<T>(capacity: impl Into<Option<usize>>) -> (flume::Sender<T>, flume::Receiver<T>) {
    match capacity.into() {
        Some(capacity) => flume::bounded(capacity),
        None => flume::unbounded(),
    }
}
