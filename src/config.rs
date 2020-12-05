// use crate::common::*;

/// A helper trait that converts types to parallel stream configuration.
pub trait IntoParStreamConfig: Sized {
    fn into_par_stream_config(self) -> ParStreamConfig;
    fn into_par_stream_params(self) -> ParStreamParams {
        self.into_par_stream_config().into()
    }
}

impl IntoParStreamConfig for Option<usize> {
    fn into_par_stream_config(self) -> ParStreamConfig {
        match self {
            Some(size) => {
                assert!(
                    size >= 1,
                    "the number of workers must be equal to greater to 1"
                );
                ParStreamConfig::Absolute(size)
            }
            None => ParStreamConfig::Auto,
        }
    }
}

impl IntoParStreamConfig for usize {
    fn into_par_stream_config(self) -> ParStreamConfig {
        assert!(
            self >= 1,
            "the number of workers must be a number greater or equal to 1"
        );
        ParStreamConfig::Absolute(self)
    }
}

impl IntoParStreamConfig for f64 {
    fn into_par_stream_config(self) -> ParStreamConfig {
        assert!(
            self.is_finite() && self >= 1.0,
            "the scale number must be a number greater or equal to 1"
        );
        ParStreamConfig::Scale(self)
    }
}

impl IntoParStreamConfig for (usize, usize) {
    fn into_par_stream_config(self) -> ParStreamConfig {
        let (num_workers, buf_size) = self;
        assert!(
            num_workers >= 1,
            "the number of workers must be equal to greater to 1"
        );
        assert!(
            buf_size >= 1,
            "the buffer size must be equal to greater to 1"
        );
        ParStreamConfig::Custom {
            num_workers,
            buf_size,
        }
    }
}

/// Parallel stream configuration.
#[derive(Debug, Clone)]
pub enum ParStreamConfig {
    Auto,
    Absolute(usize),
    Scale(f64),
    Custom { num_workers: usize, buf_size: usize },
}

/// Parallel stream parameters.
#[derive(Debug, Clone)]
pub struct ParStreamParams {
    pub(crate) num_workers: usize,
    pub(crate) buf_size: usize,
}

impl From<ParStreamConfig> for ParStreamParams {
    fn from(from: ParStreamConfig) -> Self {
        use ParStreamConfig::*;

        let (num_workers, buf_size) = match from {
            Auto => {
                let num_workers = num_cpus::get();
                let buf_size = num_workers * 2;
                (num_workers, buf_size)
            }
            Absolute(num_workers) => {
                let buf_size = num_workers * 2;
                (num_workers, buf_size)
            }
            Scale(scale) => {
                let num_workers = (num_cpus::get() as f64 * scale).ceil() as usize;
                let buf_size = num_workers * 2;
                (num_workers, buf_size)
            }
            Custom {
                num_workers,
                buf_size,
            } => (num_workers, buf_size),
        };

        Self {
            num_workers,
            buf_size,
        }
    }
}
