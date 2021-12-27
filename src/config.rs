use crate::common::*;

pub const DEFAULT_BUF_SIZE_SCALE: f64 = 2.0;
static BUF_SIZE_SCALE: OnceCell<f64> = OnceCell::new();
static DEFAULT_NUM_WORKERS: Lazy<usize> = Lazy::new(|| {
    let value = num_cpus::get();
    assert!(value > 0);
    value
});

pub fn set_buf_size_scale(scale: f64) -> bool {
    assert!(scale.is_finite() && scale > 0.0);
    BUF_SIZE_SCALE.set(scale).is_ok()
}

pub fn get_buf_size_scale() -> f64 {
    *BUF_SIZE_SCALE.get_or_init(|| DEFAULT_BUF_SIZE_SCALE)
}

fn default_buf_size(num_workers: usize) -> usize {
    scale_positive(num_workers, get_buf_size_scale())
}

fn scale_positive(value: usize, scale: f64) -> usize {
    assert!(value > 0);
    assert!(scale.is_finite() && scale > 0.0);
    cmp::max((value as f64 * scale).round() as usize, 1)
}

pub use config::*;
mod config {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
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
                    let buf_size = Some(default_buf_size(num_workers));

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
                Self::FixedWorkers { num_workers } => {
                    let buf_size = Some(default_buf_size(num_workers));

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
                Self::ScaleOfCpus { scale } => {
                    assert!(scale.is_finite() && scale > 0.0);
                    let num_workers =
                        cmp::max((*DEFAULT_NUM_WORKERS as f64 * scale).round() as usize, 1);
                    let buf_size = Some(default_buf_size(num_workers));

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
                    let buf_size = buf_size.get(num_workers);

                    ParParams {
                        num_workers,
                        buf_size,
                    }
                }
            }
        }
    }

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

    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum BufSize {
        Default,
        Fixed(usize),
        ScaleOfCpus(f64),
        ScaleOfWorkers(f64),
        Unbounded,
    }

    impl BufSize {
        pub fn get(&self, num_workers: usize) -> Option<usize> {
            match *self {
                Self::Default => default_buf_size(num_workers).into(),
                Self::Fixed(val) => val.into(),
                Self::ScaleOfCpus(scale) => scale_positive(*DEFAULT_NUM_WORKERS, scale).into(),
                Self::ScaleOfWorkers(scale) => scale_positive(num_workers, scale).into(),
                Self::Unbounded => None,
            }
        }
    }
}

pub use params::*;
mod params {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
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
