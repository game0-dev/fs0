use serde::{Deserialize, Serialize};

use crate::protocol::BundleChunkRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HashId(pub [u8; 32]);

impl HashId {
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for HashId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

#[must_use]
pub fn blake3_hash(bytes: &[u8]) -> HashId {
    HashId(*blake3::hash(bytes).as_bytes())
}

#[must_use]
pub fn bundle_hash_from_chunks(chunks: &[BundleChunkRef]) -> HashId {
    let mut hasher = blake3::Hasher::new();
    for chunk in chunks {
        hasher.update(chunk.chunk_id.as_bytes());
    }
    HashId(*hasher.finalize().as_bytes())
}
