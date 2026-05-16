use fs0_core::ChunkId;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, VolumeError>;

#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
    #[error("invalid volume config: {0}")]
    InvalidConfig(String),

    #[error("invalid chunk: {0}")]
    InvalidChunk(String),

    #[error("volume already exists: {}", .0.display())]
    AlreadyExists(PathBuf),

    #[error("volume does not exist: {}", .0.display())]
    NotFound(PathBuf),

    #[error("chunk {0:?} was not found in volume")]
    ChunkNotFound(ChunkId),

    #[error("volume capacity exceeded: required end {required_end}, max {max_bytes}")]
    CapacityExceeded { required_end: u64, max_bytes: u64 },

    #[error("chunk hash mismatch at volume offset {volume_offset}")]
    HashMismatch { volume_offset: u64 },

    #[error("integer conversion failed: {0}")]
    IntegerConversion(String),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("sqlite error")]
    Sqlite(#[from] rusqlite::Error),
}
