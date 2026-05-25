use crate::HashId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionMessage {
    Ping,
    Pong,
    Error(Fs0Error),
    RegisterClient {
        name: Option<String>,
    },
    ClientRegistered {
        client_id: u64,
        storages: Vec<StoragePeerInfo>,
    },
    RegisterStorage {
        request: RegisterStorageRequest,
    },
    StorageRegistered {
        storage_id: u64,
        storages: Vec<StoragePeerInfo>,
    },
    StorageChanged(StoragePeerInfo),
    StorageRemoved {
        storage_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRequest {
    CreateVolume {
        name: String,
        max_bytes: u64,
    },
    GrantUploadLease(UploadLease),
    RevokeUploadLease {
        lease_id: u64,
    },
    CentralStatus,
    ListDirectory {
        dir: String,
        limit: u32,
        cursor: Option<u64>,
    },
    GetFileReadPlan {
        path: String,
    },
    GetFileReadPlanById {
        file_id: u64,
    },
    DeleteFile {
        path: String,
    },
    DeleteFileById {
        file_id: u64,
    },
    CopyFile {
        source_path: String,
        target_path: String,
    },
    CopyFileById {
        source_file_id: u64,
        target_path: String,
    },
    RenameFile {
        source_path: String,
        target_path: String,
    },
    RenameFileById {
        file_id: u64,
        target_path: String,
    },
    GetFileChangeLogs {
        after_event_id: u64,
        limit: u32,
    },
    BeginAppend(BeginAppendRequest),
    CommitAppend(CommitAppendRequest),
    AbortAppend {
        lease_id: u64,
    },
    ReportBundleReplica(BundleReplicaReport),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    CreateVolume(u64),
    UploadLeaseGranted { lease_id: u64 },
    UploadLeaseRevoked { lease_id: u64 },
    CentralStatus(CentralStatus),
    ListDirectory(DirectoryEntries),
    GetFileChangeLogs(FileChangeLogs),
    BeginAppend(AppendLease),
    CommitAppend(FileReadPlan),
    AbortAppend,
    GetFileReadPlan(FileReadPlan),
    GetFileReadPlanById(FileReadPlan),
    DeleteFile,
    DeleteFileById,
    CopyFile(FileRecord),
    CopyFileById(FileRecord),
    RenameFile(FileRecord),
    RenameFileById(FileRecord),
    ReportBundleReplica,
    Error(Fs0Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataRequest {
    HasChunk {
        volume_id: u64,
        chunk_id: HashId,
    },
    UploadChunk {
        volume_id: u64,
        chunk_id: HashId,
        compressed_hash: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    },
    DownloadChunk {
        volume_id: u64,
        chunk_id: HashId,
    },
    HasBundle {
        volume_id: u64,
        bundle_id: HashId,
    },
    CommitBundle {
        volume_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    },
    ListBundleChunks {
        volume_id: u64,
        bundle_id: HashId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataResponse {
    HasChunk {
        exists: bool,
        raw_len: Option<u64>,
        compressed_len: Option<u64>,
    },
    UploadChunk {
        chunk_id: HashId,
        raw_len: u64,
        compressed_len: u64,
    },
    DownloadChunk {
        compressed_bytes: Vec<u8>,
    },
    HasBundle {
        exists: bool,
        raw_len: Option<u64>,
        compressed_len: Option<u64>,
    },
    CommitBundle {
        bundle_id: HashId,
        raw_len: u64,
        compressed_len: u64,
    },
    ListBundleChunks {
        chunks: Vec<BundleChunkRef>,
    },
    Error(Fs0Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleChunkRef {
    pub chunk_index: u64,
    pub chunk_id: HashId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CentralStatus {
    pub storages: Vec<CentralStorageStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CentralStorageStatus {
    pub storage_id: u64,
    pub name: String,
    pub volumes: Vec<CentralVolumeStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CentralVolumeStatus {
    pub volume_id: u64,
    pub name: String,
    pub max_bytes: u64,
    pub used_bytes: u64,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileReadPlan {
    pub file_id: u64,
    pub path: String,
    pub size: u64,
    pub bundles: Vec<FileBundleRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileBundleRef {
    pub bundle_index: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
    pub bundle_id: HashId,
    pub replicas: Vec<ReplicaLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaLocation {
    pub storage_id: u64,
    pub volume_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterStorageRequest {
    pub storage_id: u64,
    pub name: String,
    pub volumes: Vec<StorageVolumeInfo>,
    pub iroh_endpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageVolumeInfo {
    pub volume_id: u64,
    pub name: String,
    pub max_bytes: u64,
    pub max_volume_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePeerInfo {
    pub storage_id: u64,
    pub name: String,
    pub volumes: Vec<StorageVolumeInfo>,
    pub iroh_endpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
    pub file_id: u64,
    pub path: String,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub file_id: u64,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntries {
    pub entries: Vec<DirectoryEntry>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginAppendRequest {
    pub path: String,
    pub expected_size: u64,
    pub create: bool,
    pub prefer_volume_name: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendLease {
    pub lease_id: u64,
    pub file_id: u64,
    pub volume_id: u64,
    pub base_size: u64,
    pub expires_at_ms: u64,
    pub prefer_volume_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadLease {
    pub lease_id: u64,
    pub client_id: u64,
    pub file_id: u64,
    pub volume_id: u64,
    pub base_size: u64,
    pub expires_at_ms: u64,
    pub prefer_volume_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadTarget {
    pub storage_id: u64,
    pub volume_id: u64,
    pub iroh_endpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAppendRequest {
    pub lease_id: u64,
    pub base_size: u64,
    pub new_size: u64,
    pub bundles: Vec<CommittedBundle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBundle {
    pub bundle_index: u64,
    pub bundle_id: HashId,
    pub raw_len: u64,
    pub compressed_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleReplicaReport {
    pub events: Vec<BundleReplicaEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleReplicaEvent {
    pub event_id: u64,
    pub kind: BundleReplicaEventKind,
    pub volume_id: u64,
    pub bundle_id: HashId,
    pub raw_len: Option<u64>,
    pub compressed_len: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BundleReplicaEventKind {
    Stored,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChangeLogs {
    pub operations: Vec<FileChangeLog>,
    pub next_event_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChangeLog {
    pub event_id: u64,
    pub kind: FileChangeLogKind,
    pub file_id: Option<u64>,
    pub old_path: Option<String>,
    pub new_path: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileChangeLogKind {
    Created,
    Updated,
    Moved,
    Deleted,
}
