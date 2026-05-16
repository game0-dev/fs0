use crate::error::{Fs0Error, Result};
use crate::id::ChunkId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fs0Path(String);

impl Fs0Path {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_path(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Fs0Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Fs0Path {
    type Err = Fs0Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl TryFrom<String> for Fs0Path {
    type Error = Fs0Error;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl Serialize for Fs0Path {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Fs0Path {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

fn validate_path(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Fs0Error::InvalidPath("path is empty".to_owned()));
    }
    if !value.starts_with('/') {
        return Err(Fs0Error::InvalidPath(format!(
            "path must be absolute: {value}"
        )));
    }
    if value.split('/').any(|component| component == "..") {
        return Err(Fs0Error::InvalidPath(format!(
            "path cannot contain '..': {value}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    pub file_id: u64,
    pub path: Fs0Path,
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
