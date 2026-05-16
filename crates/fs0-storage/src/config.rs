use crate::error::Result;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub storage_id: u64,
    pub name: String,
    pub central_endpoint: Vec<u8>,
    pub cert: PathBuf,
    pub p2p_relay: StorageP2pRelayConfig,
    pub volumes: Vec<StorageVolumeConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageP2pRelayConfig {
    pub port: u16,
    pub quic_port: u16,
    pub public_url: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageVolumeConfig {
    pub path: PathBuf,
    pub volume_id: u64,
}

impl StorageConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&contents)?)
    }
}
