use fs0_core::{
    DEFAULT_CLIENT_DATA_CONCURRENCY, DEFAULT_REPLICATION_FACTOR, Fs0Error, Fs0Result,
    VOLUME_READ_CONCURRENCY, VOLUME_WRITE_CONCURRENCY,
};
use iroh::{EndpointAddr, EndpointId};
use serde::Deserialize;
use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// Full config example:
///
/// ```toml
/// [central]
/// db_path = ".local/central.sqlite"
/// secret_key = "REPLACE_WITH_CENTRAL_SECRET_KEY"
/// bind_port = 7800
/// replication_factor = 2
/// auth_tokens = ["REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"]
///
/// [central.relay]
/// public_url = "https://1.2.3.4:7801"
/// token = "REPLACE_WITH_RELAY_TOKEN"
/// https_bind_port = 7801
/// quic_bind_port = 7802
/// cert_path = ".local/relay-cert.pem"
/// key_path = ".local/relay-key.pem"
///
/// [client]
/// name = "local-client-1"
/// token = "REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"
/// central_endpoint_id = "1992d53c02cdc04566e5c0edb1ce83305cd550297953a047a445ea3264b54b18"
/// central_addr = "1.2.3.4:7800"
/// upload_concurrency = 32
/// download_concurrency = 32
/// download_cache_dir = ".local/client-cache"
///
/// [client.relay]
/// url = "https://1.2.3.4:7801"
/// token = "REPLACE_WITH_RELAY_TOKEN"
/// quic_port = 7802
/// ca_cert = """
/// -----BEGIN CERTIFICATE-----
/// ...
/// -----END CERTIFICATE-----
/// """
///
/// [storage]
/// name = "local-storage-1"
/// token = "REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"
/// bind_port = 3341
/// central_endpoint_id = "1992d53c02cdc04566e5c0edb1ce83305cd550297953a047a445ea3264b54b18"
/// central_addr = "1.2.3.4:7800"
/// check_hash_before_write = false
///
/// [storage.relay]
/// url = "https://1.2.3.4:7801"
/// token = "REPLACE_WITH_RELAY_TOKEN"
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

#[derive(Debug, Deserialize)]
struct CentralRoot {
    central: Option<CentralConfig>,
}

#[derive(Debug, Deserialize)]
struct StorageRoot {
    storage: Option<StorageConfig>,
}

#[derive(Debug, Deserialize)]
struct ClientRoot {
    client: Option<ClientConfig>,
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
    #[serde(flatten)]
    pub central_endpoint: EndpointConfig,
    #[serde(default)]
    pub bind_port: Option<u16>,
    #[serde(default)]
    pub relay: Option<RelayClientConfig>,
    #[serde(default)]
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
    pub read_concurrency: u32,
    #[serde(default = "default_volume_write_concurrency")]
    pub write_concurrency: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClientConfig {
    #[serde(default)]
    pub name: Option<String>,
    pub token: String,
    #[serde(flatten)]
    pub central_endpoint: EndpointConfig,
    #[serde(default)]
    pub relay: Option<RelayClientConfig>,
    #[serde(default = "default_client_data_concurrency")]
    pub upload_concurrency: usize,
    #[serde(default = "default_client_data_concurrency")]
    pub download_concurrency: usize,
    #[serde(default = "default_client_download_cache_dir")]
    pub download_cache_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub struct EndpointConfig {
    #[serde(rename = "central_endpoint_id")]
    pub id: EndpointId,
    #[serde(rename = "central_addr")]
    pub addr: SocketAddr,
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

    pub fn load_central_from(path: impl AsRef<Path>) -> Fs0Result<CentralConfig> {
        CentralRoot::load_from(path)
    }

    pub fn load_storage_from(path: impl AsRef<Path>) -> Fs0Result<StorageConfig> {
        StorageRoot::load_from(path)
    }

    pub fn load_client_from(path: impl AsRef<Path>) -> Fs0Result<ClientConfig> {
        ClientRoot::load_from(path)
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

impl CentralRoot {
    fn load_from(path: impl AsRef<Path>) -> Fs0Result<CentralConfig> {
        load_toml::<Self>(path)?
            .central
            .ok_or_else(|| Fs0Error::InvalidConfig {
                message: "missing [central] config section".to_owned(),
            })
    }
}

impl StorageRoot {
    fn load_from(path: impl AsRef<Path>) -> Fs0Result<StorageConfig> {
        load_toml::<Self>(path)?
            .storage
            .ok_or_else(|| Fs0Error::InvalidConfig {
                message: "missing [storage] config section".to_owned(),
            })
    }
}

impl ClientRoot {
    fn load_from(path: impl AsRef<Path>) -> Fs0Result<ClientConfig> {
        load_toml::<Self>(path)?
            .client
            .ok_or_else(|| Fs0Error::InvalidConfig {
                message: "missing [client] config section".to_owned(),
            })
    }
}

impl ClientConfig {
    pub fn new(
        token: String,
        central_endpoint: EndpointConfig,
        relay: Option<RelayClientConfig>,
    ) -> Self {
        Self {
            name: None,
            token,
            central_endpoint,
            relay,
            upload_concurrency: default_client_data_concurrency(),
            download_concurrency: default_client_data_concurrency(),
            download_cache_dir: default_client_download_cache_dir(),
        }
    }
}

impl From<EndpointConfig> for EndpointAddr {
    fn from(config: EndpointConfig) -> Self {
        EndpointAddr::new(config.id).with_ip_addr(config.addr)
    }
}

fn load_toml<T>(path: impl AsRef<Path>) -> Fs0Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let contents = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&contents)?)
}

fn default_volume_read_concurrency() -> u32 {
    VOLUME_READ_CONCURRENCY as u32
}

fn default_volume_write_concurrency() -> u32 {
    VOLUME_WRITE_CONCURRENCY as u32
}

fn default_replication_factor() -> u16 {
    DEFAULT_REPLICATION_FACTOR
}

fn default_client_data_concurrency() -> usize {
    DEFAULT_CLIENT_DATA_CONCURRENCY
}

fn default_client_download_cache_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .map(|home| home.join(".fs0").join("cache"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CENTRAL_ENDPOINT_ID: &str =
        "1992d53c02cdc04566e5c0edb1ce83305cd550297953a047a445ea3264b54b18";

    #[test]
    fn parses_current_config_shape() {
        let config_toml = format!(
            r#"
            [central]
            db_path = ".local/central.sqlite"
            secret_key = "REPLACE_WITH_CENTRAL_SECRET_KEY"
            bind_port = 7800
            replication_factor = 2
            auth_tokens = ["REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"]

            [central.relay]
            public_url = "https://1.2.3.4:7801"
            token = "REPLACE_WITH_RELAY_TOKEN"
            https_bind_port = 7801
            cert_path = ".local/relay-cert.pem"
            key_path = ".local/relay-key.pem"
            quic_bind_port = 7802

            [client]
            token = "REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"
            central_endpoint_id = "{CENTRAL_ENDPOINT_ID}"
            central_addr = "1.2.3.4:7800"

            [client.relay]
            url = "https://1.2.3.4:7801"
            token = "REPLACE_WITH_RELAY_TOKEN"
            quic_port = 7802
            ca_cert = """
            -----BEGIN CERTIFICATE-----
            test-client-ca
            -----END CERTIFICATE-----
            """

            [storage]
            name = "local-storage-1"
            token = "REPLACE_WITH_CLIENT_OR_STORAGE_TOKEN"
            central_endpoint_id = "{CENTRAL_ENDPOINT_ID}"
            central_addr = "1.2.3.4:7800"
            bind_port = 3341
            check_hash_before_write = false

            [storage.relay]
            url = "https://1.2.3.4:7801"
            token = "REPLACE_WITH_RELAY_TOKEN"
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
        );
        let config: Fs0Config = toml::from_str(&config_toml).unwrap();

        let central = config.central.unwrap();
        assert_eq!(central.bind_port, 7800);
        assert_eq!(central.replication_factor, 2);
        assert_eq!(central.relay.public_url, "https://1.2.3.4:7801");
        assert_eq!(central.relay.token, "REPLACE_WITH_RELAY_TOKEN");
        assert_eq!(central.relay.https_bind_port, 7801);
        assert_eq!(central.relay.quic_bind_port, 7802);

        let storage = config.storage.unwrap();
        assert_eq!(storage.central_endpoint.id.to_string(), CENTRAL_ENDPOINT_ID);
        assert_eq!(storage.central_endpoint.addr.to_string(), "1.2.3.4:7800");
        assert_eq!(storage.bind_port, Some(3341));
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.url.as_str()),
            Some("https://1.2.3.4:7801")
        );
        assert_eq!(
            storage.relay.as_ref().map(|relay| relay.token.as_str()),
            Some("REPLACE_WITH_RELAY_TOKEN")
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
        let storage_endpoint = EndpointAddr::from(storage.central_endpoint);
        assert_eq!(storage_endpoint.id, storage.central_endpoint.id);

        let client = config.client.unwrap();
        assert_eq!(client.name, None);
        assert_eq!(client.central_endpoint.id.to_string(), CENTRAL_ENDPOINT_ID);
        assert_eq!(client.central_endpoint.addr.to_string(), "1.2.3.4:7800");
        assert_eq!(client.upload_concurrency, DEFAULT_CLIENT_DATA_CONCURRENCY);
        assert_eq!(client.download_concurrency, DEFAULT_CLIENT_DATA_CONCURRENCY);
        assert_eq!(
            client.download_cache_dir,
            default_client_download_cache_dir()
        );
        let client_endpoint = EndpointAddr::from(client.central_endpoint);
        assert_eq!(client_endpoint.id, client.central_endpoint.id);
        assert_eq!(
            client.relay.as_ref().map(|relay| relay.url.as_str()),
            Some("https://1.2.3.4:7801")
        );
        assert_eq!(
            client.relay.as_ref().map(|relay| relay.token.as_str()),
            Some("REPLACE_WITH_RELAY_TOKEN")
        );
        assert!(
            client
                .relay
                .as_ref()
                .and_then(|relay| relay.ca_cert.as_deref())
                .is_some_and(|ca_cert| ca_cert.contains("test-client-ca"))
        );
    }

    #[test]
    fn central_loader_ignores_invalid_client_and_storage_sections() {
        let config_toml = r#"
            [central]
            db_path = ".local/central.sqlite"
            secret_key = "REPLACE_WITH_CENTRAL_SECRET_KEY"
            bind_port = 7800
            replication_factor = 1

            [central.relay]
            public_url = "https://127.0.0.1:7801"
            token = "REPLACE_WITH_RELAY_TOKEN"
            https_bind_port = 7801
            cert_path = ".local/relay-cert.pem"
            key_path = ".local/relay-key.pem"
            quic_bind_port = 7802

            [client]
            token = "REPLACE_WITH_CLIENT_TOKEN"
            central_endpoint_id = "REPLACE_WITH_CENTRAL_ENDPOINT_ID_FROM_CENTRAL_RUN"
            central_addr = "127.0.0.1:7800"

            [storage]
            name = "local-storage-1"
            token = "REPLACE_WITH_STORAGE_TOKEN"
            central_endpoint_id = "REPLACE_WITH_CENTRAL_ENDPOINT_ID_FROM_CENTRAL_RUN"
            central_addr = "127.0.0.1:7800"
        "#;
        let path = env::temp_dir().join(format!(
            "fs0-config-central-loader-{}.toml",
            std::process::id()
        ));
        std::fs::write(&path, config_toml).unwrap();

        let central = Fs0Config::load_central_from(&path).unwrap();

        let _ = std::fs::remove_file(path);
        assert_eq!(central.bind_port, 7800);
        assert_eq!(central.replication_factor, 1);
    }

    #[test]
    fn storage_volumes_default_to_empty() {
        let config_toml = format!(
            r#"
            [storage]
            name = "local-storage-1"
            token = "REPLACE_WITH_STORAGE_TOKEN"
            central_endpoint_id = "{CENTRAL_ENDPOINT_ID}"
            central_addr = "127.0.0.1:7800"
            bind_port = 3341
        "#
        );
        let config: StorageRoot = toml::from_str(&config_toml).unwrap();
        let storage = config.storage.unwrap();

        assert!(storage.volumes.is_empty());
    }
}
