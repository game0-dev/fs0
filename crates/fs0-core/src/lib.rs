pub mod codec;
pub mod compression;
pub mod hash;
pub mod protocol;

pub use codec::*;
pub use compression::*;
pub use hash::*;
pub use protocol::*;

pub const DEFAULT_ZSTD_LEVEL: i32 = 9;
pub const CONTROL_ALPN: &[u8] = b"/fs0/control/1";
pub const DATA_ALPN: &[u8] = b"/fs0/data/1";
pub const RAW_CHUNK_SIZE: u64 = 512 * 1024;
pub const DATA_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024;
pub const VOLUME_FORMAT_VERSION: u64 = 1;
pub const VOLUME_READ_CONCURRENCY: usize = 4;
pub const VOLUME_WRITE_CONCURRENCY: usize = 1;
pub const MAX_OPEN_DATA_FILES: usize = 2048;
pub const DATA_FILE_IDLE_TTL_MS: u64 = 60_000;
