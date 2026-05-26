use fs0_core::{
    DEFAULT_REPLICATION_FACTOR, Fs0Error, Fs0Result, VOLUME_READ_CONCURRENCY,
    VOLUME_WRITE_CONCURRENCY,
};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Full config example:
///
/// ```toml
/// [central]
/// db_path = ".local/central.sqlite"
/// replication_factor = 2
/// auth_tokens = ["dev-token"]
///
/// [central.relay]
/// port = 3340
/// quic_port = 7824
/// public_url = "http://127.0.0.1:3340"
///
/// [client]
/// token = "dev-token"
/// central_endpoint = "127.0.0.1:3340"
///
/// [storage]
/// name = "local-storage-1"
/// token = "dev-token"
/// central_endpoint = "127.0.0.1:3340"
///
/// [[storage.volumes]]
/// path = ".local/volume-1"
/// volume_id = 1
/// name = "local-volume-1"
/// read_only = false
///
/// [storage.volumes.volume_io]
/// read_concurrency = 4
/// write_concurrency = 1
/// ```
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
    #[serde(default)]
    pub auth_tokens: Vec<String>,
    pub relay: CentralRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralRelayConfig {
    pub port: u16,
    pub quic_port: u16,
    pub public_url: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub name: String,
    pub token: String,
    pub central_endpoint: String,
    pub volumes: Vec<StorageVolumeConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageVolumeConfig {
    pub path: PathBuf,
    pub volume_id: u64,
    pub name: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub volume_io: StorageVolumeIoConfig,
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
    pub token: String,
    pub central_endpoint: String,
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
    pub fn load_from(path: impl AsRef<Path>) -> Fs0Result<Self> {
        load_toml(path)
    }

    pub fn central(self) -> Fs0Result<CentralConfig> {
        self.central.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [central] config section".to_owned(),
        })
    }

    pub fn storage(self) -> Fs0Result<StorageConfig> {
        self.storage.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [storage] config section".to_owned(),
        })
    }

    pub fn client(self) -> Fs0Result<ClientConfig> {
        self.client.ok_or_else(|| Fs0Error::InvalidConfig {
            message: "missing [client] config section".to_owned(),
        })
    }
}

impl CentralConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Fs0Result<Self> {
        load_toml(path)
    }
}

impl StorageConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Fs0Result<Self> {
        load_toml(path)
    }
}

impl ClientConfig {
    pub fn load_from(path: impl AsRef<Path>) -> Fs0Result<Self> {
        load_toml(path)
    }
}

fn load_toml<T>(path: impl AsRef<Path>) -> Fs0Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let contents = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&contents)?)
}

fn default_volume_read_concurrency() -> usize {
    VOLUME_READ_CONCURRENCY
}

fn default_volume_write_concurrency() -> usize {
    VOLUME_WRITE_CONCURRENCY
}

fn default_replication_factor() -> u16 {
    DEFAULT_REPLICATION_FACTOR
}
