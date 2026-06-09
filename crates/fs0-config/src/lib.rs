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
/// bind_port = 7800
/// replication_factor = 2
/// auth_tokens = ["dev-token"]
///
/// [central.relay]
/// public_url = "https://1.2.3.4:7801"
/// token = "relay-token"
/// https_bind_port = 7801
/// quic_bind_port = 7802
/// cert_path = ".local/relay-cert.pem"
/// key_path = ".local/relay-key.pem"
///
/// [client]
/// token = "dev-token"
/// central_endpoint_id = "central-endpoint-id"
/// central_addr = "1.2.3.4:7800"
///
/// [client.relay]
/// url = "https://1.2.3.4:7801"
/// token = "relay-token"
/// quic_port = 7802
/// ca_cert = """
/// -----BEGIN CERTIFICATE-----
/// ...
/// -----END CERTIFICATE-----
/// """
///
/// [storage]
/// name = "local-storage-1"
/// token = "dev-token"
/// bind_port = 3341
/// central_endpoint_id = "central-endpoint-id"
/// central_addr = "1.2.3.4:7800"
/// check_hash_before_write = false
///
/// [storage.relay]
/// url = "https://1.2.3.4:7801"
/// token = "relay-token"
/// quic_port = 7802
/// ca_cert = """
/// -----BEGIN CERTIFICATE-----
/// ...
/// -----END CERTIFICATE-----
/// """
///
/// [[storage.volumes]]
/// path = ".local/volume-1"
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
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,
    #[serde(default)]
    pub auth_tokens: Vec<String>,
    pub relay: CentralRelayConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CentralRelayConfig {
    pub public_url: String,
    pub token: String,
    pub https_bind_port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub quic_bind_port: u16,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    pub name: String,
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
    pub relay: Option<RelayClientConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RelayClientConfig {
    pub url: String,
    pub token: String,
    pub quic_port: u16,
    #[serde(default)]
    pub ca_cert: Option<String>,
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
            bind_port = 7800
            replication_factor = 2
            auth_tokens = ["dev-token"]

            [central.relay]
            public_url = "https://1.2.3.4:7801"
            token = "relay-token"
            https_bind_port = 7801
            cert_path = ".local/relay-cert.pem"
            key_path = ".local/relay-key.pem"
            quic_bind_port = 7802

            [client]
            token = "dev-token"
            central_endpoint_id = "central-endpoint-id"
            central_addr = "1.2.3.4:7800"

            [client.relay]
            url = "https://1.2.3.4:7801"
            token = "relay-token"
            quic_port = 7802
            ca_cert = """
            -----BEGIN CERTIFICATE-----
            test-client-ca
            -----END CERTIFICATE-----
            """

            [storage]
            name = "local-storage-1"
            token = "dev-token"
            central_endpoint_id = "central-endpoint-id"
            central_addr = "1.2.3.4:7800"
            bind_port = 3341
            check_hash_before_write = false

            [storage.relay]
            url = "https://1.2.3.4:7801"
            token = "relay-token"
            quic_port = 7802
            ca_cert = """
            -----BEGIN CERTIFICATE-----
            test-storage-ca
            -----END CERTIFICATE-----
            """

            [[storage.volumes]]
            path = ".local/volume-1"
            name = "local-volume-1"
            "#,
        )
        .unwrap();

        let central = config.central.unwrap();
        assert_eq!(central.bind_port, 7800);
        assert_eq!(central.replication_factor, 2);
        assert_eq!(central.relay.public_url, "https://1.2.3.4:7801");
        assert_eq!(central.relay.token, "relay-token");
        assert_eq!(central.relay.https_bind_port, 7801);
        assert_eq!(central.relay.quic_bind_port, 7802);

        let storage = config.storage.unwrap();
        assert_eq!(storage.central_endpoint_id, "central-endpoint-id");
        assert_eq!(storage.central_addr, "1.2.3.4:7800");
        assert_eq!(storage.bind_port, Some(3341));
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.url.as_str()),
            Some("https://1.2.3.4:7801")
        );
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.token.as_str()),
            Some("relay-token")
        );
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.quic_port),
            Some(7802)
        );
        assert!(
            storage
                .relay
                .as_ref()
                .and_then(|relay| relay.ca_cert.as_deref())
                .is_some_and(|ca_cert| ca_cert.contains("test-storage-ca"))
        );
        assert!(!storage.check_hash_before_write);

        let client = config.client.unwrap();
        assert_eq!(client.central_addr, "1.2.3.4:7800");
        assert_eq!(
            client.relay.as_ref().map(|relay| relay.url.as_str()),
            Some("https://1.2.3.4:7801")
        );
        assert_eq!(
            client.relay.as_ref().map(|relay| relay.token.as_str()),
            Some("relay-token")
        );
        assert!(
            client
                .relay
                .as_ref()
                .and_then(|relay| relay.ca_cert.as_deref())
                .is_some_and(|ca_cert| ca_cert.contains("test-client-ca"))
        );
    }
}
