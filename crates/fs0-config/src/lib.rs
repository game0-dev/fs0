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
/// secret_key = "central-secret-key"
/// bind_port = 3340
/// public_addr = "127.0.0.1:3340"
/// replication_factor = 2
/// auth_tokens = ["dev-token"]
///
/// [central.relay]
/// bind_port = 443
/// public_url = "http://127.0.0.1:443"
///
/// [client]
/// token = "dev-token"
/// central_endpoint_id = "central-endpoint-id"
/// central_addr = "127.0.0.1:3340"
///
/// [storage]
/// name = "local-storage-1"
/// token = "dev-token"
/// secret_key = "storage-secret-key"
/// bind_port = 3341
/// central_endpoint_id = "central-endpoint-id"
/// central_addr = "127.0.0.1:3340"
/// check_hash_before_write = false
///
/// [storage.relay]
/// url = "http://127.0.0.1:443"
///
/// [[storage.volumes]]
/// path = ".local/volume-1"
/// volume_id = 1
/// name = "local-volume-1"
/// read_only = false
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
    pub secret_key: String,
    pub bind_port: u16,
    #[serde(default)]
    pub public_addr: Option<String>,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,
    #[serde(default)]
    pub auth_tokens: Vec<String>,
    pub relay: CentralRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralRelayConfig {
    pub bind_port: u16,
    pub public_url: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub name: String,
    pub secret_key: String,
    pub token: String,
    pub central_endpoint_id: String,
    pub central_addr: String,
    #[serde(default)]
    pub bind_port: Option<u16>,
    #[serde(default)]
    pub relay: Option<RelayClientConfig>,
    pub volumes: Vec<StorageVolumeConfig>,
    #[serde(default)]
    pub check_hash_before_write: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageVolumeConfig {
    pub path: PathBuf,
    pub volume_id: u64,
    pub name: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default = "default_volume_read_concurrency")]
    pub read_concurrency: usize,
    #[serde(default = "default_volume_write_concurrency")]
    pub write_concurrency: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientConfig {
    pub token: String,
    pub central_endpoint_id: String,
    pub central_addr: String,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub bind_port: Option<u16>,
    #[serde(default)]
    pub relay: Option<RelayClientConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RelayClientConfig {
    pub url: String,
    #[serde(default)]
    pub quic_port: Option<u16>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_current_config_shape() {
        let config: Fs0Config = toml::from_str(
            r#"
            [central]
            db_path = ".local/central.sqlite"
            secret_key = "central-secret-key"
            bind_port = 3340
            public_addr = "127.0.0.1:3340"
            auth_tokens = ["dev-token"]

            [central.relay]
            bind_port = 443
            public_url = "http://127.0.0.1:443"

            [client]
            token = "dev-token"
            central_endpoint_id = "central-endpoint-id"
            central_addr = "127.0.0.1:3340"

            [storage]
            name = "local-storage-1"
            secret_key = "storage-secret-key"
            token = "dev-token"
            central_endpoint_id = "central-endpoint-id"
            central_addr = "127.0.0.1:3340"
            bind_port = 3341

            [storage.relay]
            url = "http://127.0.0.1:443"

            [[storage.volumes]]
            path = ".local/volume-1"
            volume_id = 1
            name = "local-volume-1"
            "#,
        )
        .unwrap();

        let central = config.central.unwrap();
        assert_eq!(central.bind_port, 3340);
        assert_eq!(central.public_addr.as_deref(), Some("127.0.0.1:3340"));
        assert_eq!(central.relay.bind_port, 443);

        let storage = config.storage.unwrap();
        assert_eq!(storage.central_endpoint_id, "central-endpoint-id");
        assert_eq!(storage.bind_port, Some(3341));
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.url.as_str()),
            Some("http://127.0.0.1:443")
        );
        assert!(!storage.check_hash_before_write);

        let client = config.client.unwrap();
        assert_eq!(client.central_addr, "127.0.0.1:3340");
        assert!(client.secret_key.is_none());
    }
}
