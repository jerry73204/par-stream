use crate::common::*;

/// The default value returned by [get_buf_size_scale()].
pub const DEFAULT_BUF_SIZE_SCALE: f64 = 2.0;

static BUF_SIZE_SCALE: OnceCell<f64> = OnceCell::new();
static DEFAULT_NUM_WORKERS: Lazy<usize> = Lazy::new(|| {
    let value = num_cpus::get();
    assert!(value > 0);
    value
});

/// Sets the global scaling factor for buffer size.
///
/// The default buffer size will be determined by `scale * num_cpus`.
/// The method must be called at most once and before calling of any other
/// methods in this crate. Otherwise it returns an error with current value.
///
/// # Panics
/// The `scale` must be positive and finite.
pub fn set_buf_size_scale(scale: f64) -> Result<(), f64> {
    assert!(scale.is_finite() && scale > 0.0);
    BUF_SIZE_SCALE.set(scale)
}

/// Gets the global scaling factor for buffer size.
///
/// If [set_buf_size_scale] was not called before, it returns
/// [DEFAULT_BUF_SIZE_SCALE]. Otherwise it returns the value accordingly.
///
/// Note that calling this function causes future calls to [set_buf_size_scale]
/// to fail.
pub fn get_buf_size_scale() -> f64 {
    *BUF_SIZE_SCALE.get_or_init(|| DEFAULT_BUF_SIZE_SCALE)
}

fn default_buf_size() -> usize {
    scale_positive(*DEFAULT_NUM_WORKERS, get_buf_size_scale())
}

pub(crate) fn scale_positive(value: usize, scale: f64) -> usize {
    assert!(value > 0);
    assert!(scale.is_finite() && scale > 0.0);
    cmp::max((value as f64 * scale).round() as usize, 1)
}

pub use config_::*;
mod config_ {
    use super::*;

    /// The determination strategy for the number of workers and buffer size.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum ParParamsConfig {
        Default,
        FixedWorkers {
            num_workers: usize,
        },
        ScaleOfCpus {
            scale: f64,
        },
        Manual {
            num_workers: NumWorkers,
            buf_size: BufSize,
        },
    }

    impl ParParamsConfig {
        pub fn to_params(&self) -> ParParams {
            match *self {
                Self::Default => {
                    let num_workers = *DEFAULT_NUM_WORKERS;
                    let buf_size = Some(scale_positive(num_workers, get_buf_size_scale()));

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
                Self::FixedWorkers { num_workers } => {
                    let buf_size = Some(scale_positive(num_workers, get_buf_size_scale()));

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
                Self::ScaleOfCpus { scale } => {
                    let num_workers = scale_positive(*DEFAULT_NUM_WORKERS, scale);
                    let buf_size = Some(scale_positive(num_workers, get_buf_size_scale()));

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
                Self::Manual {
                    num_workers,
                    buf_size,
                } => {
                    let num_workers = num_workers.get();
                    let buf_size = buf_size.get();

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
            }
        }
    }

    /// The determination strategy for the number of workers.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum NumWorkers {
        Default,
        Fixed(usize),
        ScaleOfCpus(f64),
    }

    impl NumWorkers {
        pub fn get(&self) -> usize {
            match *self {
                Self::Default => *DEFAULT_NUM_WORKERS,
                Self::Fixed(val) => val,
                Self::ScaleOfCpus(scale) => scale_positive(*DEFAULT_NUM_WORKERS, scale),
            }
        }
    }

    impl Default for NumWorkers {
        fn default() -> Self {
            Self::Default
        }
    }

    impl From<Option<usize>> for NumWorkers {
        fn from(num_workers: Option<usize>) -> Self {
            num_workers.map(Self::Fixed).unwrap_or(Self::Default)
        }
    }

    impl From<usize> for NumWorkers {
        fn from(value: usize) -> Self {
            Self::Fixed(value)
        }
    }

    impl From<f64> for NumWorkers {
        fn from(scale: f64) -> Self {
            Self::ScaleOfCpus(scale)
        }
    }

    /// The buffer size determination strategy.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum BufSize {
        Default,
        Fixed(usize),
        ScaleOfCpus(f64),
        Unbounded,
    }

    impl BufSize {
        pub fn get(&self) -> Option<usize> {
            match *self {
                Self::Default => default_buf_size().into(),
                Self::Fixed(val) => val.into(),
                Self::ScaleOfCpus(scale) => scale_positive(*DEFAULT_NUM_WORKERS, scale).into(),
                Self::Unbounded => None,
            }
        }
    }

    impl Default for BufSize {
        fn default() -> Self {
            Self::Default
        }
    }

    impl From<Option<usize>> for BufSize {
        fn from(value: Option<usize>) -> Self {
            value.map(Self::Fixed).unwrap_or(Self::Default)
        }
    }

    impl From<usize> for BufSize {
        fn from(value: usize) -> Self {
            Self::Fixed(value)
        }
    }

    impl From<f64> for BufSize {
        fn from(scale: f64) -> Self {
            Self::ScaleOfCpus(scale)
        }
    }
}

pub use params::*;
mod params {
    use super::*;

    /// The parameters including `num_workers` and `buf_size`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ParParams {
        pub num_workers: usize,
        pub buf_size: Option<usize>,
    }

    impl Default for ParParams {
        fn default() -> Self {
            ParParamsConfig::Default.to_params()
        }
    }

    impl From<Option<ParParamsConfig>> for ParParams {
        fn from(config: Option<ParParamsConfig>) -> Self {
            config.map(|config| config.to_params()).unwrap_or_default()
        }
    }

    impl From<ParParamsConfig> for ParParams {
        fn from(config: ParParamsConfig) -> Self {
            config.to_params()
        }
    }

    impl From<usize> for ParParams {
        fn from(num_workers: usize) -> Self {
            ParParamsConfig::FixedWorkers { num_workers }.to_params()
        }
    }

    impl From<f64> for ParParams {
        fn from(scale: f64) -> Self {
            ParParamsConfig::ScaleOfCpus { scale }.to_params()
        }
    }
}
