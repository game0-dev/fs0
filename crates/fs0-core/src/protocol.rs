use crate::{Fs0Error, HashId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolRequest {
    Control(ControlRequest),
    Data(DataRequest),
    Event(ProtocolEvent),
    CentralAdmin(CentralAdminRequest),
    StorageAdmin(StorageAdminRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolResponse {
    Error(Fs0Error),
    Control(ControlResponse),
    Data(DataResponse),
    CentralAdmin(CentralAdminResponse),
    StorageAdmin(StorageAdminResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolEvent {
    StorageChanged(StoragePeerInfo),
    StorageRemoved { storage_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CentralAdminRequest {
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CentralAdminResponse {
    Status(CentralAdminStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CentralAdminStatus {
    pub clients_count: u32,
    pub storages: Vec<StoragePeerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageAdminRequest {
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageAdminResponse {
    Status(StorageAdminStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAdminStatus {
    pub storage_id: u64,
    pub volumes: Vec<StorageVolumeInfo>,
    pub connected_storages: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlRequest {
    RegisterClient {
        name: Option<String>,
        token: String,
        version: String,
    },
    RegisterStorage {
        name: String,
        token: String,
        version: String,
        volumes: Vec<StorageVolumeInfo>,
        iroh_endpoint: Vec<u8>,
    },
    CreateVolume {
        name: String,
        max_bytes: u64,
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
    BeginUpdate(BeginUpdateRequest),
    CommitUpdate(CommitUpdateRequest),
    AbortUpdate {
        lease_id: u64,
        file_id: u64,
    },
    GrantUploadLease(GrantUploadLeaseRequest),
    RevokeUploadLease {
        lease_id: u64,
    },
    ReportBundleReplica {
        events: Vec<BundleReplicaEvent>,
    },
    UpdateStorageVolumeOffset {
        volume_id: u64,
        max_volume_offset: u64,
    },
    ValidateClientAuth {
        client_id: u64,
        client_token: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    RegisterClient {
        client_id: u64,
        storages: Vec<StoragePeerInfo>,
    },
    RegisterStorage {
        storage_id: u64,
        storages: Vec<StoragePeerInfo>,
    },
    CreateVolume {
        volume_id: u64,
    },
    CentralStatus {
        clients_count: u32,
        storages: Vec<StoragePeerInfo>,
    },
    ListDirectory(DirectoryEntries),
    GetFileReadPlan(FileReadPlan),
    GetFileReadPlanById(FileReadPlan),
    DeleteFile,
    DeleteFileById,
    CopyFile(FileRecord),
    CopyFileById(FileRecord),
    RenameFile(FileRecord),
    RenameFileById(FileRecord),
    GetFileChangeLogs(FileChangeLogs),
    BeginUpdate(UpdateLease),
    CommitUpdate(FileRecord),
    AbortUpdate,
    GrantUploadLease {
        lease_id: u64,
    },
    RevokeUploadLease,
    ReportBundleReplica,
    UpdateStorageVolumeOffset,
    ValidateClientAuth {
        client_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataRequest {
    Authenticate {
        client_id: u64,
        client_token: String,
    },
    HasChunk {
        volume_id: u64,
        chunk_id: HashId,
    },
    UploadChunk {
        lease_id: u64,
        file_id: u64,
        volume_id: u64,
        chunk_id: HashId,
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
        lease_id: u64,
        file_id: u64,
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
    Authenticate {
        client_id: u64,
    },
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleChunkRef {
    pub chunk_index: u64,
    pub chunk_id: HashId,
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
pub struct StorageVolumeInfo {
    pub volume_id: u64,
    pub name: String,
    pub max_bytes: u64,
    pub max_volume_offset: u64,
    pub read_only: bool,
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
pub struct BeginUpdateRequest {
    pub path: String,
    pub offset: u64,
    pub prefer_volume_name: Option<String>,
    pub update_size_hint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantUploadLeaseRequest {
    pub lease_id: u64,
    pub file_id: u64,
    pub volume_id: u64,
    pub base_size: u64,
    pub expires_at_ms: u64,
    pub prefer_volume_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateLease {
    pub lease_id: u64,
    pub file_id: u64,
    pub volume_id: u64,
    pub base_size: u64,
    pub offset: u64,
    pub expires_at_ms: u64,
    pub prefer_volume_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitUpdateRequest {
    pub lease_id: u64,
    pub file_id: u64,
    pub base_size: u64,
    pub new_size: u64,
    pub bundles: Vec<CommittedBundle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBundle {
    pub bundle_id: HashId,
    pub raw_len: u64,
    pub compressed_len: u64,
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
