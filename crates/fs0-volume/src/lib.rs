mod db;
mod volume;

pub type Result<T> = std::result::Result<T, fs0_core::Fs0Error>;
pub use fs0_core::Fs0Error;
pub use volume::{BundleMeta, ChunkMeta, DATA_FILE_SIZE, RAW_CHUNK_SIZE, Volume, VolumeMeta};
