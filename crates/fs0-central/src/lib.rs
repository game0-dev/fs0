mod config;
mod db;
mod error;

pub use config::{CentralConfig, CentralP2pRelayConfig};
pub use db::VolumeRecord;
pub use error::{CentralError, Result};

use db::CentralDb;
use fs0_core::{
    AbortAppendRequest, AppendLease, BeginAppendRequest, ChunkPlan, ChunkPlanAction, ChunkPlans,
    CommitAppendRequest, ControlRequest, ControlResponse, CreateVolumeRequest, DirectoryEntries,
    FileEvents, FileManifest, FileRecord, Fs0Path, ListDirectoryRequest, ListFileEventsRequest,
    PlanChunksRequest, RegisterStorageRequest, ReplicaLocation, SessionMessage, StoragePeerInfo,
    UploadTarget,
};
use fs0_transport::{bind_endpoint, encode_endpoint_addr, read_frame, write_frame};
use iroh::endpoint::Connection;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, Notify, RwLock};

#[derive(Clone)]
pub struct CentralServer {
    state: Arc<CentralState>,
}

struct CentralState {
    config: Arc<CentralConfig>,
    next_client_id: AtomicU64,
    storages: RwLock<HashMap<u64, StoragePeerInfo>>,
    online_volumes: RwLock<HashMap<u64, u64>>,
    db: Mutex<CentralDb>,
    control_endpoint: RwLock<Option<Vec<u8>>>,
    ready: Notify,
}

impl CentralServer {
    pub fn new(config: CentralConfig) -> Result<Self> {
        validate_config(&config)?;
        let db = CentralDb::open(&config.db_path)?;
        Ok(Self {
            state: Arc::new(CentralState {
                config: Arc::new(config),
                next_client_id: AtomicU64::new(1),
                storages: RwLock::new(HashMap::new()),
                online_volumes: RwLock::new(HashMap::new()),
                db: Mutex::new(db),
                control_endpoint: RwLock::new(None),
                ready: Notify::new(),
            }),
        })
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.state.config
    }

    pub async fn run(&self) -> Result<()> {
        let _relay = self.start_relay().await?;
        let endpoint = bind_endpoint(
            &self.state.config.p2p_relay.public_url,
            self.state.config.p2p_relay.quic_port,
            vec![fs0_core::CONTROL_ALPN.to_vec()],
        )
        .await?;
        {
            let mut control_endpoint = self.state.control_endpoint.write().await;
            *control_endpoint = Some(encode_endpoint_addr(&endpoint)?);
        }
        self.state.ready.notify_waiters();

        while let Some(incoming) = endpoint.accept().await {
            let server = self.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.await else {
                    return;
                };
                let _ = server.handle_control_connection(connection).await;
            });
        }
        Ok(())
    }

    pub async fn wait_control_endpoint(&self) -> Result<Vec<u8>> {
        loop {
            if let Some(endpoint) = self.state.control_endpoint.read().await.clone() {
                return Ok(endpoint);
            }
            self.state.ready.notified().await;
        }
    }

    pub fn alloc_client_id(&self) -> u64 {
        self.state.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn create_volume(&self, request: CreateVolumeRequest) -> Result<VolumeRecord> {
        self.state.db.lock().await.create_volume(request)
    }

    pub async fn register_storage(
        &self,
        mut request: RegisterStorageRequest,
    ) -> Result<StoragePeerInfo> {
        {
            let db = self.state.db.lock().await;
            for volume in &mut request.volumes {
                let registered = db.get_volume(volume.volume_id)?.ok_or_else(|| {
                    CentralError::not_found(format!(
                        "volume {} was not found in central metadata",
                        volume.volume_id
                    ))
                })?;
                volume.name = registered.name.clone();
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

    pub async fn get_file_by_path(&self, path: &Fs0Path) -> Result<Option<FileRecord>> {
        self.state.db.lock().await.get_file_by_path(path)
    }

    pub async fn list_files(&self) -> Result<Vec<FileRecord>> {
        self.state.db.lock().await.list_files()
    }

    pub async fn list_directory(&self, request: ListDirectoryRequest) -> Result<DirectoryEntries> {
        self.state
            .db
            .lock()
            .await
            .list_directory(&request.dir, request.limit, request.cursor)
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        self.state.db.lock().await.begin_append(request, 0)
    }

    pub async fn plan_chunks(&self, request: PlanChunksRequest) -> Result<ChunkPlans> {
        let peers = self.storage_peers().await;
        let mut plans = Vec::with_capacity(request.chunks.len());
        let prefer_volume_name = self.preferred_volume_name(request.lease_id).await?;
        let db = self.state.db.lock().await;
        for chunk in request.chunks {
            let replicas = self
                .hydrate_replicas(db.chunk_replicas(chunk.chunk_id)?)
                .await;
            let targets = upload_targets(&peers, &replicas, prefer_volume_name.as_deref());
            let action = if replicas.is_empty() {
                ChunkPlanAction::Upload { targets }
            } else if targets.is_empty() {
                ChunkPlanAction::Reuse { replicas }
            } else {
                ChunkPlanAction::AddReplica {
                    existing_replicas: replicas,
                    targets,
                }
            };
            plans.push(ChunkPlan {
                chunk_id: chunk.chunk_id,
                action,
            });
        }
        Ok(ChunkPlans { chunks: plans })
    }

    async fn hydrate_replicas(&self, replicas: Vec<ReplicaLocation>) -> Vec<ReplicaLocation> {
        let online_volumes = self.state.online_volumes.read().await;
        replicas
            .into_iter()
            .filter_map(|replica| {
                online_volumes
                    .get(&replica.volume_id)
                    .map(|storage_id| ReplicaLocation {
                        storage_id: *storage_id,
                        volume_id: replica.volume_id,
                    })
            })
            .collect()
    }

    async fn preferred_volume_name(&self, lease_id: u64) -> Result<Option<String>> {
        self.state
            .db
            .lock()
            .await
            .lease_prefer_volume_name(lease_id)
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileManifest> {
        let manifest = self.state.db.lock().await.commit_append(request)?;
        Ok(self.hydrate_manifest_replicas(manifest).await)
    }

    pub async fn abort_append(&self, request: AbortAppendRequest) -> Result<()> {
        self.state.db.lock().await.abort_append(request)
    }

    pub async fn list_file_events(&self, request: ListFileEventsRequest) -> Result<FileEvents> {
        self.state.db.lock().await.list_file_events(request)
    }

    pub async fn get_file_manifest(&self, path: &Fs0Path) -> Result<FileManifest> {
        let manifest = self.state.db.lock().await.get_file_manifest(path)?;
        Ok(self.hydrate_manifest_replicas(manifest).await)
    }

    async fn hydrate_manifest_replicas(&self, mut manifest: FileManifest) -> FileManifest {
        for chunk in &mut manifest.chunks {
            chunk.replicas = self
                .hydrate_replicas(std::mem::take(&mut chunk.replicas))
                .await;
        }
        manifest
    }

    async fn handle_control_connection(&self, connection: Connection) -> Result<()> {
        let mut storage_id = None;
        let (mut session_send, mut session_recv) = connection.accept_bi().await.map_err(|err| {
            CentralError::Transport(fs0_transport::TransportError::Iroh(err.to_string()))
        })?;
        match read_frame::<SessionMessage, _>(&mut session_recv).await? {
            SessionMessage::RegisterClient { .. } => {
                write_frame(
                    &mut session_send,
                    &SessionMessage::ClientRegistered {
                        client_id: self.alloc_client_id(),
                    },
                )
                .await?;
            }
            SessionMessage::RegisterStorage(request) => {
                let id = request.storage_id;
                match self.register_storage(request).await {
                    Ok(_) => {
                        storage_id = Some(id);
                        write_frame(
                            &mut session_send,
                            &SessionMessage::StorageRegistered { storage_id: id },
                        )
                        .await?;
                    }
                    Err(err) => {
                        write_frame(
                            &mut session_send,
                            &SessionMessage::Error(err.to_control_error()),
                        )
                        .await?;
                    }
                }
            }
            SessionMessage::Heartbeat => {
                write_frame(&mut session_send, &SessionMessage::Pong).await?;
            }
            _ => {
                write_frame(
                    &mut session_send,
                    &SessionMessage::Error(
                        CentralError::invalid_request(
                            "first session message must register a client or storage",
                        )
                        .to_control_error(),
                    ),
                )
                .await?;
                return Ok(());
            }
        }

        loop {
            tokio::select! {
                request = read_frame::<SessionMessage, _>(&mut session_recv) => {
                    match request {
                        Ok(SessionMessage::Heartbeat) => {
                            write_frame(&mut session_send, &SessionMessage::Pong).await?;
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                stream = connection.accept_bi() => {
                    let Ok((mut send, mut recv)) = stream else {
                        break;
                    };
                    let server = self.clone();
                    tokio::spawn(async move {
                        let Ok(request) = read_frame::<ControlRequest, _>(&mut recv).await else {
                            return;
                        };
                        let (response, _) = server.handle_control_request(request).await;
                        let _ = write_frame(&mut send, &response).await;
                        let _ = send.finish();
                    });
                }
            }
        }

        if let Some(storage_id) = storage_id {
            self.unregister_storage(storage_id).await?;
        }
        Ok(())
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
            ControlRequest::ListFileEvents(request) => match self.list_file_events(request).await {
                Ok(events) => (ControlResponse::FileEvents(events), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::Ping => (ControlResponse::Pong, None),
            ControlRequest::BeginAppend(request) => match self.begin_append(request).await {
                Ok(lease) => (ControlResponse::AppendLease(lease), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::PlanChunks(request) => match self.plan_chunks(request).await {
                Ok(plans) => (ControlResponse::ChunkPlans(plans), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::CommitAppend(request) => match self.commit_append(request).await {
                Ok(file_manifest) => (ControlResponse::AppendCommitted { file_manifest }, None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::AbortAppend(request) => match self.abort_append(request).await {
                Ok(()) => (ControlResponse::AppendAborted, None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::GetFileManifest { path } => match self.get_file_manifest(&path).await {
                Ok(manifest) => (ControlResponse::FileManifest(manifest), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
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

fn upload_targets(
    peers: &[StoragePeerInfo],
    existing_replicas: &[ReplicaLocation],
    prefer_volume_name: Option<&str>,
) -> Vec<UploadTarget> {
    let target_count = 2usize.saturating_sub(existing_replicas.len());
    if target_count == 0 {
        return Vec::new();
    }
    let mut targets = Vec::new();
    for (peer, volume) in preferred_peer_volumes(peers, prefer_volume_name) {
        if existing_replicas
            .iter()
            .any(|replica| replica.storage_id == peer.storage_id)
        {
            continue;
        }
        targets.push(UploadTarget {
            storage_id: peer.storage_id,
            volume_id: volume.volume_id,
            data_endpoint: peer.data_endpoint.clone(),
        });
        if targets.len() >= target_count {
            break;
        }
    }
    targets
}

fn preferred_peer_volumes<'a>(
    peers: &'a [StoragePeerInfo],
    prefer_volume_name: Option<&str>,
) -> Vec<(&'a StoragePeerInfo, &'a fs0_core::StorageVolumeInfo)> {
    let mut preferred = Vec::new();
    let mut fallback = Vec::new();
    for peer in peers {
        for volume in &peer.volumes {
            if prefer_volume_name.is_some_and(|name| volume.name.as_deref() == Some(name)) {
                preferred.push((peer, volume));
            } else {
                fallback.push((peer, volume));
            }
        }
    }
    preferred.extend(fallback);
    preferred
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
