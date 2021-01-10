pub trait IntoParStreamParams {
    fn into_par_stream_params(self) -> ParStreamParams;
}

impl<T> IntoParStreamParams for T
where
    ParStreamConfig: From<T>,
{
    fn into_par_stream_params(self) -> ParStreamParams {
        let config: ParStreamConfig = self.into();
        let params: ParStreamParams = config.into();
        params
    }
}

/// Parallel stream configuration.
#[derive(Debug, Clone)]
pub struct ParStreamConfig {
    pub num_workers: Value,
    pub buf_size: Value,
}

impl From<Option<usize>> for ParStreamConfig {
    fn from(size: Option<usize>) -> Self {
        match size {
            Some(size) => ParStreamConfig {
                num_workers: Value::Absolute(size),
                buf_size: Value::Absolute(size),
            },
            None => ParStreamConfig {
                num_workers: Value::Auto,
                buf_size: Value::Auto,
            },
        }
    }
}

impl From<usize> for ParStreamConfig {
    fn from(size: usize) -> Self {
        ParStreamConfig {
            num_workers: Value::Absolute(size),
            buf_size: Value::Absolute(size),
        }
    }
}

impl From<f64> for ParStreamConfig {
    fn from(scale: f64) -> Self {
        ParStreamConfig {
            num_workers: Value::Scale(scale),
            buf_size: Value::Scale(scale),
        }
    }
}

impl From<(usize, usize)> for ParStreamConfig {
    fn from((num_workers, buf_size): (usize, usize)) -> Self {
        ParStreamConfig {
            num_workers: Value::Absolute(num_workers),
            buf_size: Value::Absolute(buf_size),
        }
    }
}

impl From<(f64, usize)> for ParStreamConfig {
    fn from((num_workers, buf_size): (f64, usize)) -> Self {
        ParStreamConfig {
            num_workers: Value::Scale(num_workers),
            buf_size: Value::Absolute(buf_size),
        }
    }
}

impl From<(usize, f64)> for ParStreamConfig {
    fn from((num_workers, buf_size): (usize, f64)) -> Self {
        ParStreamConfig {
            num_workers: Value::Absolute(num_workers),
            buf_size: Value::Scale(buf_size),
        }
    }
}

impl From<(f64, f64)> for ParStreamConfig {
    fn from((num_workers, buf_size): (f64, f64)) -> Self {
        ParStreamConfig {
            num_workers: Value::Scale(num_workers),
            buf_size: Value::Scale(buf_size),
        }
    }
}

/// Sum type of absolute value and scaling value.
#[derive(Debug, Clone)]
pub enum Value {
    Auto,
    Absolute(usize),
    Scale(f64),
}

impl Value {
    pub fn to_absolute(&self) -> usize {
        match *self {
            Self::Auto => num_cpus::get(),
            Self::Absolute(val) => {
                assert!(val > 0, "absolute value must be positive");
                val
            }
            Self::Scale(scale) => {
                assert!(
                    scale.is_finite() && scale.is_sign_positive(),
                    "scaling value must be positive finite"
                );
                (num_cpus::get() as f64 * scale).ceil() as usize
            }
        }
    }
}

/// Parallel stream parameters.
#[derive(Debug, Clone)]
pub struct ParStreamParams {
    pub(crate) num_workers: usize,
    pub(crate) buf_size: usize,
}

impl From<ParStreamConfig> for ParStreamParams {
    fn from(from: ParStreamConfig) -> Self {
        let ParStreamConfig {
            num_workers,
            buf_size,
        } = from;

        let num_workers = num_workers.to_absolute();
        let buf_size = buf_size.to_absolute();

        Self {
            num_workers,
            buf_size,
        }
    }
}
