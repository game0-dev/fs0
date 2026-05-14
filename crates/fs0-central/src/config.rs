use crate::Result;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralConfig {
    pub tcp_port: u16,
    pub db_path: PathBuf,
    pub p2p_relay: CentralP2pRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralP2pRelayConfig {
    pub port: u16,
    pub quic_port: u16,
    pub public_url: String,
}

impl CentralConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&contents)?)
    }
}
