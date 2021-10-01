pub struct NullError;

impl From<()> for NullError {
    fn from(_: ()) -> Self {
        Self
    }
}

impl<T> From<async_channel::SendError<T>> for NullError {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self
    }
}

impl From<async_channel::RecvError> for NullError {
    fn from(_: async_channel::RecvError) -> Self {
        Self
    }
}

pub type NullResult<T> = Result<T, NullError>;
