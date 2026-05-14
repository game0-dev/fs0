use crate::manifest::{FileManifest, Fs0Path, ObjectManifest, ReplicaLocation};
use serde::{Deserialize, Serialize};

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
    RegisterClient { name: Option<String> },
    CreateStorage(CreateStorageRequest),
    CreateVolume(CreateVolumeRequest),
    RegisterStorage(RegisterStorageRequest),
    ListStoragePeers,
    ListFiles,
    ListDirectory(ListDirectoryRequest),
    LookupPath { path: Fs0Path },
    GetFileRecord { path: Fs0Path },
    Ping,
    BeginAppend(BeginAppendRequest),
    PrepareUpload(PrepareUploadRequest),
    CommitAppend(CommitAppendRequest),
    GetFileManifest { path: Fs0Path },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    ClientRegistered { client_id: u64 },
    StorageCreated { storage_id: u64 },
    VolumeCreated { volume_id: u64 },
    StorageRegistered { storage_id: u64 },
    StoragePeers(Vec<StoragePeerInfo>),
    Files(Vec<FileRecord>),
    DirectoryEntries(DirectoryEntries),
    FileRecord(Option<FileRecord>),
    Pong,
    Error(ControlError),
    AppendLease(AppendLease),
    UploadPrepared { upload_id: u64 },
    AppendCommitted { file_manifest: FileManifest },
    FileManifest(FileManifest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlError {
    pub code: ControlErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlErrorCode {
    Unsupported,
    NotFound,
    AlreadyExists,
    VolumeAlreadyMounted,
    VersionConflict,
    InvalidRequest,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStorageRequest {
    pub name: String,
}

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
    pub path: Fs0Path,
    pub version: u64,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub volume_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: Fs0Path,
    pub file_id: u64,
    pub version: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntries {
    pub entries: Vec<DirectoryEntry>,
    pub next_cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListDirectoryRequest {
    pub parent_path: Fs0Path,
    pub limit: u32,
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginAppendRequest {
    pub path: Fs0Path,
    pub expected_version: u64,
    pub expected_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendLease {
    pub lease_id: u64,
    pub file_id: u64,
    pub base_version: u64,
    pub base_size: u64,
    pub fencing_token: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareUploadRequest {
    pub lease_id: u64,
    pub object_manifest: ObjectManifest,
    pub target_volume_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitAppendRequest {
    pub lease_id: u64,
    pub base_version: u64,
    pub base_size: u64,
    pub objects: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataRequest {
    Ping,
    UploadObject {
        upload_id: u64,
        manifest: ObjectManifest,
    },
    GetChunk {
        object_id: u64,
        chunk_index: u32,
    },
    GetRange {
        object_id: u64,
        offset: u64,
        len: u64,
    },
    RepairCopy {
        job_id: u64,
        object_id: u64,
        target: ReplicaLocation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataResponse {
    Pong,
    UploadAccepted,
    Bytes(Vec<u8>),
    RepairStarted,
}
