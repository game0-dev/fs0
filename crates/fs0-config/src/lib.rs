use fs0_core::DEFAULT_REPLICATION_FACTOR;
use fs0_core::Fs0Error;
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Fs0Error>;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Fs0Config {
    pub central: Option<CentralConfig>,
    pub storage: Option<StorageConfig>,
    pub client: Option<ClientConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralConfig {
    pub db_path: PathBuf,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,
    pub p2p_relay: CentralP2pRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralP2pRelayConfig {
    pub port: u16,
    pub quic_port: u16,
    pub public_url: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub storage_id: u64,
    pub name: String,
    pub central_endpoint: Vec<u8>,
    pub p2p_relay: StorageP2pRelayConfig,
    #[serde(default)]
    pub volume_io: StorageVolumeIoConfig,
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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageVolumeIoConfig {
    #[serde(default = "default_volume_read_concurrency")]
    pub read_concurrency: usize,
    #[serde(default = "default_volume_write_concurrency")]
    pub write_concurrency: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientConfig {
    pub central_endpoint: Vec<u8>,
    pub p2p_relay: ClientP2pRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientP2pRelayConfig {
    pub port: u16,
    pub quic_port: u16,
    pub public_url: String,
}

impl Default for StorageVolumeIoConfig {
    fn default() -> Self {
        Self {
            read_concurrency: default_volume_read_concurrency(),
            write_concurrency: default_volume_write_concurrency(),
        }
    }
}

impl Fs0Config {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        load_toml(path)
    }

    pub fn central(self) -> Result<CentralConfig> {
        self.central.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [central] config section".to_owned(),
        })
    }

    pub fn storage(self) -> Result<StorageConfig> {
        self.storage.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [storage] config section".to_owned(),
        })
    }

    pub fn client(self) -> Result<ClientConfig> {
        self.client.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [client] config section".to_owned(),
        })
    }
}

impl CentralConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        load_toml(path)
    }
}

impl StorageConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        load_toml(path)
    }
}

impl ClientConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        load_toml(path)
    }
}

fn load_toml<T>(path: impl AsRef<Path>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let contents = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&contents)?)
}

fn default_volume_read_concurrency() -> usize {
    fs0_core::VOLUME_READ_CONCURRENCY
}

fn default_volume_write_concurrency() -> usize {
    fs0_core::VOLUME_WRITE_CONCURRENCY
}

fn default_replication_factor() -> u16 {
    DEFAULT_REPLICATION_FACTOR
}
