pub struct NullError;

impl From<()> for NullError {
    fn from(_: ()) -> Self {
        Self
    }
}

impl<T> From<async_std::channel::SendError<T>> for NullError {
    fn from(_: async_std::channel::SendError<T>) -> Self {
        Self
    }
}

impl From<async_std::channel::RecvError> for NullError {
    fn from(_: async_std::channel::RecvError) -> Self {
        Self
    }
}

pub type NullResult<T> = Result<T, NullError>;
