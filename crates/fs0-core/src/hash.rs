use crate::id::ChunkId;

#[must_use]
pub fn blake3_hash(bytes: &[u8]) -> ChunkId {
    ChunkId(*blake3::hash(bytes).as_bytes())
}
