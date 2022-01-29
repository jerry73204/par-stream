// macro_rules! has_rt {
//     ($($item:item)*) => {
//         $(
//             crate::utils::has_tokio! {
//                 $item
//             }

//             crate::utils::has_async_std! {
//                 $item
//             }
//         )*
//     };
// }
// pub(crate) use has_rt;

macro_rules! no_rt {
    ($($item:item)*) => {
        $(
            #[cfg(all(not(feature = "runtime-async-std"), not(feature = "runtime-tokio"),))]
            $item
        )*
    };
}
pub(crate) use no_rt;

macro_rules! has_tokio {
    ($($item:item)*) => {
        $(
            #[cfg(all(not(feature = "runtime-async-std"), feature = "runtime-tokio",))]
            $item
        )*
    };
}
pub(crate) use has_tokio;

macro_rules! has_async_std {
    ($($item:item)*) => {
        $(
            #[cfg(all(feature = "runtime-async-std", not(feature = "runtime-tokio"),))]
            $item
        )*
    };
}
pub(crate) use has_async_std;

#[allow(unused_macros)]
macro_rules! async_test {
    ($($item:item)*) => {
        crate::utils::has_tokio! {
            $(
                #[tokio::test]
                $item
            )*
        }

        crate::utils::has_async_std! {
            $(
                #[async_std::test]
                $item
            )*
        }
    };
}
#[allow(unused_imports)]
pub(crate) use async_test;

pub fn channel<T>(capacity: impl Into<Option<usize>>) -> (flume::Sender<T>, flume::Receiver<T>) {
    match capacity.into() {
        Some(capacity) => flume::bounded(capacity),
        None => flume::unbounded(),
    }
}
