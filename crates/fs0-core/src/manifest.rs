use crate::id::ChunkId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    pub file_id: u64,
    pub path: String,
    pub size: u64,
    pub chunks: Vec<FileChunkRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChunkRef {
    pub chunk_index: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
    pub chunk_id: ChunkId,
    pub replicas: Vec<ReplicaLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaLocation {
    pub storage_id: u64,
    pub volume_id: u64,
}
