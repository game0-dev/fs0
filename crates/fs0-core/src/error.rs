use crate::HashId;
use serde::{Deserialize, Serialize};

pub type Fs0Result<T> = std::result::Result<T, Fs0Error>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum Fs0Error {
    #[error("invalid frame: {message}")]
    InvalidFrame { message: String },
    #[error("frame length {actual} exceeds maximum {max}")]
    FrameTooLarge { actual: usize, max: usize },
    #[error("io error: {message}")]
    Io { message: String },
    #[error("postcard error: {message}")]
    Postcard { message: String },
    #[error("zstd error: {message}")]
    Zstd { message: String },
    #[error("sqlite error: {message}")]
    Sqlite { message: String },
    #[error("invalid config: {message}")]
    InvalidConfig { message: String },
    #[error("invalid data: {message}")]
    InvalidData { message: String },
    #[error("invalid path: {path}")]
    InvalidPath { path: String },
    #[error("integer conversion failed: {message}")]
    IntegerConversion { message: String },
    #[error("path already exists: {path}")]
    AlreadyExists { path: String },
    #[error("volume does not exist: {path}")]
    VolumeNotFound { path: String },
    #[error("chunk {chunk_id:?} was not found")]
    ChunkNotFound { chunk_id: HashId },
    #[error("bundle {bundle_id:?} was not found")]
    BundleNotFound { bundle_id: HashId },
    #[error("volume capacity exceeded: required end {required_end}, max {max_bytes}")]
    CapacityExceeded { required_end: u64, max_bytes: u64 },
    #[error("hash mismatch at volume offset {volume_offset}")]
    HashMismatch { volume_offset: u64 },
    #[error("unsupported")]
    Unsupported,
    #[error("not found")]
    NotFound,
    #[error("volume already mounted")]
    VolumeAlreadyMounted,
    #[error("version conflict")]
    VersionConflict,
    #[error("chunk not ready")]
    ChunkNotReady,
    #[error("invalid request")]
    InvalidRequest,
    #[error("unauthorized")]
    Unauthorized,
    #[error("unknown volume")]
    UnknownVolume,
    #[error("internal: {message}")]
    Internal { message: String },
}

impl From<std::io::Error> for Fs0Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            message: err.to_string(),
        }
    }
}

impl From<postcard::Error> for Fs0Error {
    fn from(err: postcard::Error) -> Self {
        Self::Postcard {
            message: err.to_string(),
        }
    }
}

impl From<rusqlite::Error> for Fs0Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sqlite {
            message: err.to_string(),
        }
    }
}

impl From<toml::de::Error> for Fs0Error {
    fn from(err: toml::de::Error) -> Self {
        Self::InvalidConfig {
            message: err.to_string(),
        }
    }
}
