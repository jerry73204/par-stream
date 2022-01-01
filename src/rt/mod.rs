//! Asynchronous runtime methods.

use crate::utils::{has_async_std, has_tokio, no_rt};

no_rt! {
    mod no_rt;
    pub use no_rt::*;
}

has_tokio! {
    mod rt_tokio;
    pub use rt_tokio::*;
}

has_async_std! {
    mod rt_async_std;
    pub use rt_async_std::*;
}
