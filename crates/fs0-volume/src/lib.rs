mod data_file_cache;
mod db;
mod io_platform;
mod volume;

pub type Result<T> = std::result::Result<T, fs0_core::Fs0Error>;
pub use fs0_core::Fs0Error;
pub use fs0_core::{DATA_FILE_SIZE, RAW_CHUNK_SIZE};
pub use volume::{BundleMeta, ChunkMeta, Volume, VolumeMeta, VolumeOptions};
