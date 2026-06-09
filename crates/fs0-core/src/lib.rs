pub mod compression;
pub mod error;
pub mod hash;
pub mod protocol;
pub mod sqlite;
pub mod utils;

pub use compression::*;
pub use error::*;
pub use hash::*;
pub use sqlite::*;

/// Default zstd compression level used by fs0 clients.
pub const DEFAULT_ZSTD_LEVEL: i32 = 9;
/// fs0 wire compatibility version required during client and storage registration.
pub const FS0_VERSION: &str = "0.1.0";
/// ALPN protocol identifier for central control-plane connections.
pub const TRANSPORT_CONTROL_ALPN: &[u8] = b"/fs0/control/1";
/// ALPN protocol identifier for storage data-plane connections.
pub const TRANSPORT_DATA_ALPN: &[u8] = b"/fs0/data/1";
/// ALPN protocol identifier for storage-to-storage replication connections.
pub const TRANSPORT_STORAGE_ALPN: &[u8] = b"/fs0/storage/1";
/// Number of bytes used to prefix each postcard frame body length.
pub const TRANSPORT_FRAME_LEN_BYTES: usize = 4;
/// Maximum encoded frame body size accepted by the transport layer. 2MB
pub const TRANSPORT_MAX_FRAME_BODY_LEN: usize = 2 * 1024 * 1024;
/// Raw bytes targeted for each committed bundle.
pub const VOLUME_BUNDLE_RAW_SIZE: u64 = 128 * 1024 * 1024; // 128M
/// Raw chunk target size before compression.
pub const VOLUME_RAW_CHUNK_SIZE: u64 = 1024 * 1024; // 1M
/// Size of each backing data file within a volume.
pub const VOLUME_DEFAULT_DATA_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4G
/// SQLite metadata filename stored inside each volume root.
pub const VOLUME_DB_FILE: &str = ".fs0.sqlite";
/// Prefix for each backing data file within a volume.
pub const VOLUME_DATA_FILE_PREFIX: &str = ".fs0.data.";
/// Current volume metadata format version.
pub const VOLUME_FORMAT_VERSION: u64 = 1;
/// Maximum number of concurrent chunk reads from a volume.
pub const VOLUME_READ_CONCURRENCY: usize = 4;
/// Maximum number of concurrent chunk writes to a volume.
pub const VOLUME_WRITE_CONCURRENCY: usize = 1;
/// Maximum number of active backing data files kept open per volume.
pub const VOLUME_MAX_OPEN_DATA_FILES: usize = 2048;
/// Idle time before an unused backing data file handle is closed.
pub const VOLUME_DATA_FILE_IDLE_TTL_MS: u64 = 60_000; // 60s
/// Lifetime for update leases issued by central.
pub const UPDATE_LEASE_TTL_MS: u64 = 60_000;
/// Maximum volume usage ratio accepted for new update placement.
pub const UPDATE_VOLUME_USAGE_THRESHOLD: f64 = 0.95;
/// Default number of concurrent chunk uploads from the client.
pub const DEFAULT_CLIENT_DATA_CONCURRENCY: usize = 32;
/// Default desired replica count for newly written data.
pub const DEFAULT_REPLICATION_FACTOR: u16 = 2;
