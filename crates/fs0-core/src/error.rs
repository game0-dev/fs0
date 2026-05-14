pub type Result<T> = std::result::Result<T, Fs0Error>;

#[derive(Debug, thiserror::Error)]
pub enum Fs0Error {
    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("frame length {actual} exceeds maximum {max}")]
    FrameTooLarge { actual: usize, max: usize },

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("postcard error")]
    Postcard(#[from] postcard::Error),

    #[error("zstd error")]
    Zstd(String),
}
