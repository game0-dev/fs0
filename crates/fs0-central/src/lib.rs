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
    FileEvents, FileManifest, FileRecord, ListDirectoryRequest, ListFileEventsRequest,
    PlanChunksRequest, RegisterStorageRequest, ReplicaLocation, SessionMessage, StorageChunkEvents,
    StoragePeerInfo, UploadTarget,
};
use fs0_transport::{bind_endpoint, encode_endpoint_addr, read_frame, write_frame};
use iroh::Endpoint;
use iroh::endpoint::Connection;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

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
    control_endpoint: Vec<u8>,
    accept_task: StdMutex<Option<JoinHandle<()>>>,
    relay: iroh_relay::server::Server,
}

impl CentralServer {
    pub async fn run(config: CentralConfig) -> Result<Self> {
        validate_config(&config)?;
        let relay = start_relay(&config).await?;
        let endpoint = bind_endpoint(
            &config.p2p_relay.public_url,
            config.p2p_relay.quic_port,
            vec![fs0_core::CONTROL_ALPN.to_vec()],
        )
        .await?;
        let control_endpoint = encode_endpoint_addr(&endpoint)?;
        let db = CentralDb::open(&config.db_path)?;

        let server = Self {
            state: Arc::new(CentralState {
                config: Arc::new(config),
                next_client_id: AtomicU64::new(1),
                storages: RwLock::new(HashMap::new()),
                online_volumes: RwLock::new(HashMap::new()),
                db: Mutex::new(db),
                control_endpoint,
                accept_task: StdMutex::new(None),
                relay,
            }),
        };
        let accept_server = server.clone();
        let accept_task = tokio::spawn(async move {
            accept_loop(endpoint, accept_server).await;
        });
        *server
            .state
            .accept_task
            .lock()
            .expect("central accept task lock poisoned") = Some(accept_task);
        Ok(server)
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.state.config
    }

    #[must_use]
    pub fn control_endpoint(&self) -> &[u8] {
        &self.state.control_endpoint
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

    pub async fn get_file_by_path(&self, path: &str) -> Result<Option<FileRecord>> {
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
        let chunks = {
            let db = self.state.db.lock().await;
            request
                .chunks
                .into_iter()
                .map(|chunk| {
                    let volume_ids = db.chunk_replica_volumes(chunk.chunk_id)?;
                    Ok((chunk, volume_ids))
                })
                .collect::<Result<Vec<_>>>()?
        };
        for (chunk, volume_ids) in chunks {
            let replicas = self.hydrate_replica_volumes(volume_ids).await;
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

    async fn hydrate_replica_volumes(&self, volume_ids: Vec<u64>) -> Vec<ReplicaLocation> {
        let online_volumes = self.state.online_volumes.read().await;
        volume_ids
            .into_iter()
            .filter_map(|volume_id| {
                online_volumes
                    .get(&volume_id)
                    .map(|storage_id| ReplicaLocation {
                        storage_id: *storage_id,
                        volume_id,
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
        self.hydrate_manifest_replicas(manifest).await
    }

    pub async fn abort_append(&self, request: AbortAppendRequest) -> Result<()> {
        self.state.db.lock().await.abort_append(request)
    }

    pub async fn list_file_events(&self, request: ListFileEventsRequest) -> Result<FileEvents> {
        self.state.db.lock().await.list_file_events(request)
    }

    pub async fn record_chunk_events(
        &self,
        storage_id: u64,
        events: StorageChunkEvents,
    ) -> Result<()> {
        {
            let online_volumes = self.state.online_volumes.read().await;
            for event in &events.events {
                if online_volumes.get(&event.volume_id) != Some(&storage_id) {
                    return Err(CentralError::invalid_request(format!(
                        "volume {} is not mounted by storage {}",
                        event.volume_id, storage_id
                    )));
                }
            }
        }
        self.state.db.lock().await.record_chunk_events(events)
    }

    pub async fn get_file_manifest(&self, path: &str) -> Result<FileManifest> {
        let manifest = self.state.db.lock().await.get_file_manifest(path)?;
        self.hydrate_manifest_replicas(manifest).await
    }

    async fn hydrate_manifest_replicas(&self, mut manifest: FileManifest) -> Result<FileManifest> {
        let chunk_replica_volumes = {
            let db = self.state.db.lock().await;
            manifest
                .chunks
                .iter()
                .map(|chunk| {
                    let volume_ids = db.chunk_replica_volumes(chunk.chunk_id)?;
                    Ok((chunk.chunk_id, volume_ids))
                })
                .collect::<Result<HashMap<_, _>>>()?
        };
        for chunk in &mut manifest.chunks {
            let volume_ids = chunk_replica_volumes
                .get(&chunk.chunk_id)
                .cloned()
                .unwrap_or_default();
            chunk.replicas = self.hydrate_replica_volumes(volume_ids).await;
        }
        Ok(manifest)
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
            SessionMessage::RegisterStorage { request } => {
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
                            &SessionMessage::Error(err.to_protocol_error()),
                        )
                        .await?;
                    }
                }
            }
            SessionMessage::Ping => {
                write_frame(&mut session_send, &SessionMessage::Pong).await?;
            }
            _ => {
                write_frame(
                    &mut session_send,
                    &SessionMessage::Error(
                        CentralError::invalid_request(
                            "first session message must register a client or storage",
                        )
                        .to_protocol_error(),
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
                        Ok(SessionMessage::Ping) => {
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
                        let (response, _) = server.handle_control_request(request, storage_id).await;
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
        actor_storage_id: Option<u64>,
    ) -> (ControlResponse, Option<u64>) {
        match request {
            ControlRequest::Ping => (ControlResponse::Ping, None),
            ControlRequest::CreateVolume(request) => match self.create_volume(request).await {
                Ok(volume) => (ControlResponse::CreateVolume(volume.volume_id), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::RegisterStorage(request) => {
                let storage_id = request.storage_id;
                match self.register_storage(request).await {
                    Ok(_) => (
                        ControlResponse::RegisterStorage(storage_id),
                        Some(storage_id),
                    ),
                    Err(err) => (ControlResponse::Error(err.into()), None),
                }
            }
            ControlRequest::ListStoragePeers => (
                ControlResponse::ListStoragePeers(self.storage_peers().await),
                None,
            ),
            ControlRequest::ListFiles => match self.list_files().await {
                Ok(files) => (ControlResponse::ListFiles(files), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::ListDirectory(request) => match self.list_directory(request).await {
                Ok(entries) => (ControlResponse::ListDirectory(entries), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::LookupPath { path } => match self.get_file_by_path(&path).await {
                Ok(file) => (ControlResponse::LookupPath(file), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::GetFileRecord { path } => match self.get_file_by_path(&path).await {
                Ok(file) => (ControlResponse::GetFileRecord(file), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::ListFileEvents(request) => match self.list_file_events(request).await {
                Ok(events) => (ControlResponse::ListFileEvents(events), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::BeginAppend(request) => match self.begin_append(request).await {
                Ok(lease) => (ControlResponse::BeginAppend(lease), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::PlanChunks(request) => match self.plan_chunks(request).await {
                Ok(plans) => (ControlResponse::PlanChunks(plans), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::CommitAppend(request) => match self.commit_append(request).await {
                Ok(file_manifest) => (ControlResponse::CommitAppend(file_manifest), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::AbortAppend(request) => match self.abort_append(request).await {
                Ok(()) => (ControlResponse::AbortAppend, None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::GetFileManifest { path } => match self.get_file_manifest(&path).await {
                Ok(manifest) => (ControlResponse::GetFileManifest(manifest), None),
                Err(err) => (ControlResponse::Error(err.into()), None),
            },
            ControlRequest::RecordChunkEvents(events) => {
                let Some(storage_id) = actor_storage_id else {
                    return (
                        ControlResponse::Error(
                            CentralError::invalid_request(
                                "only a registered storage session can record chunk events",
                            )
                            .into(),
                        ),
                        None,
                    );
                };
                match self.record_chunk_events(storage_id, events).await {
                    Ok(()) => (ControlResponse::RecordChunkEvents, None),
                    Err(err) => (ControlResponse::Error(err.into()), None),
                }
            }
        }
    }
}

impl Drop for CentralState {
    fn drop(&mut self) {
        if let Some(task) = self
            .accept_task
            .get_mut()
            .expect("central accept task lock poisoned")
            .take()
        {
            task.abort();
        }
        let _ = &self.relay;
    }
}

async fn accept_loop(endpoint: Endpoint, server: CentralServer) {
    while let Some(incoming) = endpoint.accept().await {
        let server = server.clone();
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                return;
            };
            let _ = server.handle_control_connection(connection).await;
        });
    }
}

async fn start_relay(config: &CentralConfig) -> Result<iroh_relay::server::Server> {
    let relay_config = &config.p2p_relay;

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
