use crate::id::ChunkId;
use crate::manifest::{FileManifest, ReplicaLocation};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub storage_id: u64,
    pub endpoint_id: Vec<u8>,
    pub relay_url: String,
    pub direct_addrs: Vec<String>,
    pub supported_alpns: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRequest {
    Ping,
    CreateVolume(CreateVolumeRequest),
    RegisterStorage(RegisterStorageRequest),
    ListStoragePeers,
    ListFiles,
    ListDirectory(ListDirectoryRequest),
    LookupPath { path: String },
    GetFileRecord { path: String },
    ListFileEvents(ListFileEventsRequest),
    BeginAppend(BeginAppendRequest),
    PlanChunks(PlanChunksRequest),
    CommitAppend(CommitAppendRequest),
    AbortAppend(AbortAppendRequest),
    GetFileManifest { path: String },
    RecordChunkEvents(StorageChunkEvents),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ping,
    CreateVolume(u64),
    RegisterStorage(u64),
    ListStoragePeers(Vec<StoragePeerInfo>),
    ListFiles(Vec<FileRecord>),
    ListDirectory(DirectoryEntries),
    LookupPath(Option<FileRecord>),
    GetFileRecord(Option<FileRecord>),
    ListFileEvents(FileEvents),
    Error(Fs0ProtocolError),
    BeginAppend(AppendLease),
    PlanChunks(ChunkPlans),
    CommitAppend(FileManifest),
    AbortAppend,
    GetFileManifest(FileManifest),
    RecordChunkEvents,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionMessage {
    RegisterClient { name: Option<String> },
    RegisterStorage { request: RegisterStorageRequest },
    ClientRegistered { client_id: u64 },
    StorageRegistered { storage_id: u64 },
    Ping,
    Pong,
    UploadLease(UploadLease),
    RevokeUploadLease { lease_id: u64 },
    Error(Fs0ProtocolError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fs0ProtocolError {
    Unsupported,
    NotFound,
    AlreadyExists,
    VolumeAlreadyMounted,
    VersionConflict,
    ChunkNotReady,
    InvalidRequest,
    Unauthorized,
    UnknownVolume,
    HashMismatch,
    CapacityExceeded,
    Internal,
}

impl fmt::Display for Fs0ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Fs0ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateVolumeRequest {
    pub name: Option<String>,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterStorageRequest {
    pub storage_id: u64,
    pub name: String,
    pub volumes: Vec<StorageVolumeInfo>,
    pub data_endpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageVolumeInfo {
    pub volume_id: u64,
    pub name: Option<String>,
    pub max_bytes: u64,
    pub active_volume_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePeerInfo {
    pub storage_id: u64,
    pub name: String,
    pub volumes: Vec<StorageVolumeInfo>,
    pub data_endpoint: Vec<u8>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntries {
    pub entries: Vec<DirectoryEntry>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListDirectoryRequest {
    pub dir: String,
    pub limit: u32,
    pub cursor: Option<u64>,
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
pub struct PlanChunksRequest {
    pub lease_id: u64,
    pub chunks: Vec<ChunkPlanInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPlanInput {
    pub chunk_id: ChunkId,
    pub raw_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPlans {
    pub chunks: Vec<ChunkPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPlan {
    pub chunk_id: ChunkId,
    pub action: ChunkPlanAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkPlanAction {
    Reuse {
        replicas: Vec<ReplicaLocation>,
    },
    Upload {
        targets: Vec<UploadTarget>,
    },
    AddReplica {
        existing_replicas: Vec<ReplicaLocation>,
        targets: Vec<UploadTarget>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadTarget {
    pub storage_id: u64,
    pub volume_id: u64,
    pub data_endpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAppendRequest {
    pub lease_id: u64,
    pub base_size: u64,
    pub new_size: u64,
    pub chunks: Vec<CommittedChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedChunk {
    pub chunk_index: u64,
    pub chunk_id: ChunkId,
    pub raw_len: u64,
    pub compressed_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageChunkEvents {
    pub events: Vec<StorageChunkEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageChunkEvent {
    pub event_id: u64,
    pub kind: StorageChunkEventKind,
    pub volume_id: u64,
    pub chunk_id: ChunkId,
    pub raw_len: Option<u64>,
    pub compressed_len: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageChunkEventKind {
    Stored,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortAppendRequest {
    pub lease_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListFileEventsRequest {
    pub after_event_id: u64,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEvents {
    pub events: Vec<FileEvent>,
    pub next_event_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEvent {
    pub event_id: u64,
    pub kind: FileEventKind,
    pub file_id: Option<u64>,
    pub old_path: Option<String>,
    pub new_path: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileEventKind {
    Created,
    Updated,
    Moved,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataRequest {
    Ping,
    HasChunk {
        volume_id: u64,
        chunk_id: ChunkId,
    },
    UploadChunk {
        volume_id: u64,
        chunk_id: ChunkId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    },
    GetChunk {
        volume_id: u64,
        chunk_id: ChunkId,
    },
    GetRange {
        volume_id: u64,
        chunk_id: ChunkId,
        offset: u64,
        len: u64,
    },
    RepairCopy {
        job_id: u64,
        source_volume_id: u64,
        chunk_id: ChunkId,
        target: ReplicaLocation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataResponse {
    Pong,
    Error(Fs0ProtocolError),
    ChunkPresence {
        exists: bool,
        raw_len: Option<u64>,
        compressed_len: Option<u64>,
    },
    ChunkStored {
        chunk_id: ChunkId,
        raw_len: u64,
        compressed_len: u64,
    },
    Bytes(Vec<u8>),
    RepairStarted,
}
