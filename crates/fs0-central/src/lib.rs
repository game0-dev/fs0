mod config;
mod db;
mod error;

pub use config::{CentralConfig, CentralP2pRelayConfig};
pub use db::{CommitFileLocation, VolumeRecord};
pub use error::{CentralError, Result};

use db::CentralDb;
use fs0_core::{
    ControlError, ControlErrorCode, ControlRequest, ControlResponse, CreateStorageRequest,
    CreateVolumeRequest, DirectoryEntries, FileRecord, Fs0Path, ListDirectoryRequest,
    RegisterStorageRequest, StoragePeerInfo,
};
use fs0_transport::{read_frame, write_frame};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct CentralServer {
    state: Arc<CentralState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRecord {
    pub storage_id: u64,
    pub name: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

struct CentralState {
    config: Arc<CentralConfig>,
    next_client_id: AtomicU64,
    next_storage_id: AtomicU64,
    storage_records: RwLock<HashMap<u64, StorageRecord>>,
    storages: RwLock<HashMap<u64, StoragePeerInfo>>,
    online_volumes: RwLock<HashMap<u64, u64>>,
    db: Mutex<CentralDb>,
}

impl CentralServer {
    pub fn new(config: CentralConfig) -> Result<Self> {
        validate_config(&config)?;
        let db = CentralDb::open(&config.db_path)?;
        Ok(Self {
            state: Arc::new(CentralState {
                config: Arc::new(config),
                next_client_id: AtomicU64::new(1),
                next_storage_id: AtomicU64::new(1),
                storage_records: RwLock::new(HashMap::new()),
                storages: RwLock::new(HashMap::new()),
                online_volumes: RwLock::new(HashMap::new()),
                db: Mutex::new(db),
            }),
        })
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.state.config
    }

    pub async fn run(&self) -> Result<()> {
        let _relay = self.start_relay().await?;
        let listener = TcpListener::bind(localhost_addr(self.state.config.tcp_port)).await?;

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                let _ = server.handle_connection(stream, peer_addr).await;
            });
        }
    }

    pub fn alloc_client_id(&self) -> u64 {
        self.state.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn create_storage(&self, request: CreateStorageRequest) -> Result<StorageRecord> {
        let now = now_ms();
        let record = StorageRecord {
            storage_id: self.state.next_storage_id.fetch_add(1, Ordering::Relaxed),
            name: request.name,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.state
            .storage_records
            .write()
            .await
            .insert(record.storage_id, record.clone());
        Ok(record)
    }

    pub async fn create_volume(&self, request: CreateVolumeRequest) -> Result<VolumeRecord> {
        self.state
            .db
            .lock()
            .await
            .create_volume(request.name.as_deref(), request.max_bytes)
    }

    pub async fn register_storage(
        &self,
        request: RegisterStorageRequest,
    ) -> Result<StoragePeerInfo> {
        {
            let storage_records = self.state.storage_records.read().await;
            let storage = storage_records.get(&request.storage_id).ok_or_else(|| {
                CentralError::not_found(format!(
                    "storage {} was not found in central state",
                    request.storage_id
                ))
            })?;
            if storage.name != request.name {
                return Err(CentralError::invalid_request(format!(
                    "storage {} is named {}, not {}",
                    request.storage_id, storage.name, request.name
                )));
            }
        }

        {
            let db = self.state.db.lock().await;
            for volume in &request.volumes {
                let registered = db.get_volume(volume.volume_id)?.ok_or_else(|| {
                    CentralError::not_found(format!(
                        "volume {} was not found in central metadata",
                        volume.volume_id
                    ))
                })?;
                if registered.max_bytes != volume.max_bytes {
                    return Err(CentralError::invalid_request(format!(
                        "volume {} max_bytes mismatch: central={}, storage={}",
                        volume.volume_id, registered.max_bytes, volume.max_bytes
                    )));
                }
            }
        }

        let mut storages = self.state.storages.write().await;
        if storages.contains_key(&request.storage_id) {
            return Err(CentralError::invalid_request(format!(
                "storage {} is already registered",
                request.storage_id
            )));
        }
        let mut online_volumes = self.state.online_volumes.write().await;
        for volume in &request.volumes {
            if let Some(existing_storage_id) = online_volumes.get(&volume.volume_id)
                && *existing_storage_id != request.storage_id
            {
                return Err(CentralError::volume_already_mounted(
                    volume.volume_id,
                    existing_storage_id,
                ));
            }
        }

        let peer = StoragePeerInfo {
            storage_id: request.storage_id,
            name: request.name,
            volumes: request.volumes,
            data_endpoint: request.data_endpoint,
        };
        for volume in &peer.volumes {
            online_volumes.insert(volume.volume_id, peer.storage_id);
        }
        storages.insert(peer.storage_id, peer.clone());
        Ok(peer)
    }

    pub async fn unregister_storage(&self, storage_id: u64) -> Result<()> {
        self.state.storages.write().await.remove(&storage_id);
        self.state
            .online_volumes
            .write()
            .await
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
        Ok(())
    }

    pub async fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self
            .state
            .storages
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    pub async fn commit_file_location(&self, request: CommitFileLocation) -> Result<FileRecord> {
        self.state.db.lock().await.commit_file_location(request)
    }

    pub async fn get_file_by_path(&self, path: &Fs0Path) -> Result<Option<FileRecord>> {
        self.state.db.lock().await.get_file_by_path(path)
    }

    pub async fn list_files(&self) -> Result<Vec<FileRecord>> {
        self.state.db.lock().await.list_files()
    }

    pub async fn list_directory(&self, request: ListDirectoryRequest) -> Result<DirectoryEntries> {
        self.state.db.lock().await.list_directory(
            &request.parent_path,
            request.limit,
            request.cursor,
        )
    }

    async fn handle_connection(&self, mut stream: TcpStream, _peer_addr: SocketAddr) -> Result<()> {
        let mut storage_id = None;

        loop {
            let request = match read_frame::<ControlRequest, _>(&mut stream).await {
                Ok(request) => request,
                Err(err) => {
                    if let Some(storage_id) = storage_id {
                        self.unregister_storage(storage_id).await?;
                    }
                    return Err(err.into());
                }
            };
            let (response, registered_storage_id) = self.handle_control_request(request).await;
            if let Some(id) = registered_storage_id {
                storage_id = Some(id);
            }
            if let Err(err) = write_frame(&mut stream, &response).await {
                if let Some(storage_id) = storage_id {
                    self.unregister_storage(storage_id).await?;
                }
                return Err(err.into());
            }
        }
    }

    async fn handle_control_request(
        &self,
        request: ControlRequest,
    ) -> (ControlResponse, Option<u64>) {
        match request {
            ControlRequest::RegisterClient { .. } => (
                ControlResponse::ClientRegistered {
                    client_id: self.alloc_client_id(),
                },
                None,
            ),
            ControlRequest::CreateStorage(request) => match self.create_storage(request).await {
                Ok(storage) => (
                    ControlResponse::StorageCreated {
                        storage_id: storage.storage_id,
                    },
                    None,
                ),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::CreateVolume(request) => match self.create_volume(request).await {
                Ok(volume) => (
                    ControlResponse::VolumeCreated {
                        volume_id: volume.volume_id,
                    },
                    None,
                ),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::RegisterStorage(request) => {
                let storage_id = request.storage_id;
                match self.register_storage(request).await {
                    Ok(_) => (
                        ControlResponse::StorageRegistered { storage_id },
                        Some(storage_id),
                    ),
                    Err(err) => (ControlResponse::Error(err.into()), None),
                }
            }
            ControlRequest::ListStoragePeers => (
                ControlResponse::StoragePeers(self.storage_peers().await),
                None,
            ),
            ControlRequest::ListFiles => match self.list_files().await {
                Ok(files) => (ControlResponse::Files(files), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::ListDirectory(request) => match self.list_directory(request).await {
                Ok(entries) => (ControlResponse::DirectoryEntries(entries), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::LookupPath { path } | ControlRequest::GetFileRecord { path } => {
                match self.get_file_by_path(&path).await {
                    Ok(file) => (ControlResponse::FileRecord(file), None),
                    Err(err) => (ControlResponse::Error(err.into()), None),
                }
            }
            ControlRequest::Ping => (ControlResponse::Pong, None),
            ControlRequest::BeginAppend(_)
            | ControlRequest::PrepareUpload(_)
            | ControlRequest::CommitAppend(_)
            | ControlRequest::GetFileManifest { .. } => (
                ControlResponse::Error(ControlError {
                    code: ControlErrorCode::Unsupported,
                    message: "control method is not implemented yet".to_owned(),
                }),
                None,
            ),
        }
    }

    async fn start_relay(&self) -> Result<iroh_relay::server::Server> {
        let relay_config = &self.state.config.p2p_relay;

        let http_addr = localhost_addr(relay_config.port);
        let quic_addr = localhost_addr(relay_config.quic_port);
        let mut server_config = iroh_relay::server::ServerConfig::default();
        server_config.relay = Some(iroh_relay::server::RelayConfig::new(http_addr));
        let mut quic_config = iroh_relay::server::QuicConfig::new(quic_addr);
        quic_config.server_config = Some(self_signed_quic_server_config()?);
        server_config.quic = Some(quic_config);
        let relay = iroh_relay::server::Server::spawn(server_config)
            .await
            .map_err(|err| CentralError::Relay(err.to_string()))?;
        Ok(relay)
    }
}

fn validate_config(config: &CentralConfig) -> Result<()> {
    if config.p2p_relay.public_url.trim().is_empty() {
        return Err(CentralError::Config(
            "p2p_relay.public_url must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn localhost_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_millis() as u64
}

fn self_signed_quic_server_config() -> Result<rustls::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ])
    .map_err(|err| CentralError::Relay(err.to_string()))?;
    let rustls_cert = cert.cert.der().clone();
    let private_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let private_key = rustls::pki_types::PrivateKeyDer::from(private_key);
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|err| CentralError::Relay(err.to_string()))?
        .with_no_client_auth()
        .with_single_cert(vec![rustls_cert], private_key)
        .map_err(|err| CentralError::Relay(err.to_string()))
}
