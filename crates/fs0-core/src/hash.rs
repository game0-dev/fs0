use serde::{Deserialize, Serialize};
use std::fmt;

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

    #[must_use]
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.as_bytes() {
            use std::fmt::Write as _;
            let _ = write!(output, "{byte:02x}");
        }
        output
    }
}

impl From<[u8; 32]> for HashId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl fmt::Display for HashId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.as_bytes() {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
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
