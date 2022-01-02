//! Asynchronous runtime methods.

use crate::utils::{has_async_std, has_tokio, no_rt};

mod runtime;
pub use runtime::*;

no_rt! {
    mod rt_custom;
    pub use rt_custom::*;
}

has_tokio! {
    mod rt_tokio;
    pub use rt_tokio::*;
}

has_async_std! {
    mod rt_async_std;
    pub use rt_async_std::*;
}
