mod config;
mod db;

pub use config::{CentralConfig, CentralP2pRelayConfig};
pub use db::VolumeRecord;
pub use fs0_core::Fs0Error;

pub type Result<T> = std::result::Result<T, Fs0Error>;

use db::CentralDb;
use fs0_core::{
    AppendLease, BeginAppendRequest, BundleReplicaReport, CommitAppendRequest, ControlRequest,
    ControlResponse, DirectoryEntries, FileChangeLogs, FileReadPlan, FileRecord,
    RegisterStorageRequest, ReplicaLocation, SessionMessage, StoragePeerInfo,
};
use fs0_transport::{bind_endpoint, encode_endpoint_addr, read_frame, write_frame};
use iroh::Endpoint;
use iroh::endpoint::Connection;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
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
    accept_task: Mutex<Option<JoinHandle<()>>>,
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
        .await
        .map_err(internal_error)?;
        let control_endpoint = encode_endpoint_addr(&endpoint).map_err(internal_error)?;
        let db = CentralDb::open(&config.db_path)?;

        let server = Self {
            state: Arc::new(CentralState {
                config: Arc::new(config),
                next_client_id: AtomicU64::new(1),
                storages: RwLock::new(HashMap::new()),
                online_volumes: RwLock::new(HashMap::new()),
                db: Mutex::new(db),
                control_endpoint,
                accept_task: Mutex::new(None),
                relay,
            }),
        };
        let accept_server = server.clone();
        let accept_task = tokio::spawn(async move {
            accept_loop(endpoint, accept_server).await;
        });
        *server.state.accept_task.lock() = Some(accept_task);
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

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Result<VolumeRecord> {
        self.state.db.lock().create_volume(name, max_bytes)
    }

    pub async fn register_storage(
        &self,
        mut request: RegisterStorageRequest,
    ) -> Result<StoragePeerInfo> {
        {
            let db = self.state.db.lock();
            for volume in &mut request.volumes {
                let registered = db.get_volume(volume.volume_id)?.ok_or(Fs0Error::NotFound)?;
                volume.name = registered.name;
                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }

        let mut storages = self.state.storages.write();
        if storages.contains_key(&request.storage_id) {
            return Err(Fs0Error::AlreadyExists {
                path: format!("storage:{}", request.storage_id),
            });
        }

        let mut online_volumes = self.state.online_volumes.write();
        for volume in &request.volumes {
            if let Some(existing_storage_id) = online_volumes.get(&volume.volume_id)
                && *existing_storage_id != request.storage_id
            {
                return Err(Fs0Error::VolumeAlreadyMounted);
            }
        }

        let peer = StoragePeerInfo {
            storage_id: request.storage_id,
            name: request.name,
            volumes: request.volumes,
            iroh_endpoint: request.iroh_endpoint,
        };
        for volume in &peer.volumes {
            online_volumes.insert(volume.volume_id, peer.storage_id);
        }
        storages.insert(peer.storage_id, peer.clone());
        Ok(peer)
    }

    pub async fn unregister_storage(&self, storage_id: u64) -> Result<()> {
        self.state.storages.write().remove(&storage_id);
        self.state
            .online_volumes
            .write()
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
        Ok(())
    }

    pub async fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self
            .state
            .storages
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    pub async fn get_file_by_path(&self, path: &str) -> Result<Option<FileRecord>> {
        self.state.db.lock().get_file_by_path(path)
    }

    pub async fn list_files(&self) -> Result<Vec<FileRecord>> {
        self.state.db.lock().list_files()
    }

    pub async fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Result<DirectoryEntries> {
        self.state.db.lock().list_directory(dir, limit, cursor)
    }

    pub async fn get_file_read_plan(&self, path: &str) -> Result<FileReadPlan> {
        let plan = self.state.db.lock().get_file_read_plan(path)?;
        self.hydrate_read_plan_replicas(plan).await
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Result<FileReadPlan> {
        let plan = self.state.db.lock().get_file_read_plan_by_id(file_id)?;
        self.hydrate_read_plan_replicas(plan).await
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        self.state.db.lock().begin_append(request, 0)
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileReadPlan> {
        let plan = self.state.db.lock().commit_append(request)?;
        self.hydrate_read_plan_replicas(plan).await
    }

    pub async fn abort_append(&self, lease_id: u64) -> Result<()> {
        self.state.db.lock().abort_append(lease_id)
    }

    pub async fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Result<FileChangeLogs> {
        self.state
            .db
            .lock()
            .get_file_change_logs(after_event_id, limit)
    }

    pub async fn delete_file(&self, path: &str) -> Result<()> {
        self.state.db.lock().delete_file(path)
    }

    pub async fn delete_file_by_id(&self, file_id: u64) -> Result<()> {
        self.state.db.lock().delete_file_by_id(file_id)
    }

    pub async fn copy_file(&self, source_path: &str, target_path: &str) -> Result<FileRecord> {
        self.state.db.lock().copy_file(source_path, target_path)
    }

    pub async fn copy_file_by_id(
        &self,
        source_file_id: u64,
        target_path: &str,
    ) -> Result<FileRecord> {
        self.state
            .db
            .lock()
            .copy_file_by_id(source_file_id, target_path)
    }

    pub async fn rename_file(&self, source_path: &str, target_path: &str) -> Result<FileRecord> {
        self.state.db.lock().rename_file(source_path, target_path)
    }

    pub async fn rename_file_by_id(&self, file_id: u64, target_path: &str) -> Result<FileRecord> {
        self.state.db.lock().rename_file_by_id(file_id, target_path)
    }

    pub async fn report_bundle_replica(
        &self,
        storage_id: u64,
        report: BundleReplicaReport,
    ) -> Result<()> {
        {
            let online_volumes = self.state.online_volumes.read();
            for event in &report.events {
                if online_volumes.get(&event.volume_id) != Some(&storage_id) {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }
        self.state.db.lock().record_bundle_events(report)
    }

    async fn hydrate_read_plan_replicas(&self, mut plan: FileReadPlan) -> Result<FileReadPlan> {
        let bundle_replica_volumes = {
            let db = self.state.db.lock();
            plan.bundles
                .iter()
                .map(|bundle| {
                    let volume_ids = db.bundle_replica_volumes(bundle.bundle_id)?;
                    Ok((bundle.bundle_id, volume_ids))
                })
                .collect::<Result<HashMap<_, _>>>()?
        };
        for bundle in &mut plan.bundles {
            let volume_ids = bundle_replica_volumes
                .get(&bundle.bundle_id)
                .cloned()
                .unwrap_or_default();
            bundle.replicas = self.hydrate_replica_volumes(volume_ids).await;
        }
        Ok(plan)
    }

    async fn hydrate_replica_volumes(&self, volume_ids: Vec<u64>) -> Vec<ReplicaLocation> {
        let online_volumes = self.state.online_volumes.read();
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

    async fn handle_control_connection(&self, connection: Connection) -> Result<()> {
        let mut storage_id = None;
        let (mut session_send, mut session_recv) =
            connection
                .accept_bi()
                .await
                .map_err(|err| Fs0Error::Internal {
                    message: err.to_string(),
                })?;
        match read_frame::<SessionMessage, _>(&mut session_recv)
            .await
            .map_err(internal_error)?
        {
            SessionMessage::RegisterClient { .. } => {
                let storages = self.storage_peers().await;
                write_frame(
                    &mut session_send,
                    &SessionMessage::ClientRegistered {
                        client_id: self.alloc_client_id(),
                        storages,
                    },
                )
                .await
                .map_err(internal_error)?;
            }
            SessionMessage::RegisterStorage { request } => {
                let id = request.storage_id;
                match self.register_storage(request).await {
                    Ok(_) => {
                        storage_id = Some(id);
                        let storages = self.storage_peers().await;
                        write_frame(
                            &mut session_send,
                            &SessionMessage::StorageRegistered {
                                storage_id: id,
                                storages,
                            },
                        )
                        .await
                        .map_err(internal_error)?;
                    }
                    Err(err) => {
                        write_frame(&mut session_send, &SessionMessage::Error(err))
                            .await
                            .map_err(internal_error)?;
                    }
                }
            }
            SessionMessage::Ping => {
                write_frame(&mut session_send, &SessionMessage::Pong)
                    .await
                    .map_err(internal_error)?;
            }
            _ => {
                write_frame(
                    &mut session_send,
                    &SessionMessage::Error(fs0_core::Fs0Error::InvalidRequest),
                )
                .await
                .map_err(internal_error)?;
                return Ok(());
            }
        }

        loop {
            tokio::select! {
                request = read_frame::<SessionMessage, _>(&mut session_recv) => {
                    match request {
                        Ok(SessionMessage::Ping) => {
                            write_frame(&mut session_send, &SessionMessage::Pong)
                                .await
                                .map_err(internal_error)?;
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
                        let response = server.handle_control_request(request, storage_id).await;
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
    ) -> ControlResponse {
        match request {
            ControlRequest::CreateVolume { name, max_bytes } => {
                match self.create_volume(name, max_bytes).await {
                    Ok(volume) => ControlResponse::CreateVolume(volume.volume_id),
                    Err(err) => ControlResponse::Error(err),
                }
            }
            ControlRequest::GrantUploadLease(lease) => ControlResponse::UploadLeaseGranted {
                lease_id: lease.lease_id,
            },
            ControlRequest::RevokeUploadLease { lease_id } => {
                ControlResponse::UploadLeaseRevoked { lease_id }
            }
            ControlRequest::ListDirectory { dir, limit, cursor } => {
                match self.list_directory(&dir, limit, cursor).await {
                    Ok(entries) => ControlResponse::ListDirectory(entries),
                    Err(err) => ControlResponse::Error(err),
                }
            }
            ControlRequest::GetFileReadPlan { path } => {
                match self.get_file_read_plan(&path).await {
                    Ok(plan) => ControlResponse::GetFileReadPlan(plan),
                    Err(err) => ControlResponse::Error(err),
                }
            }
            ControlRequest::GetFileReadPlanById { file_id } => {
                match self.get_file_read_plan_by_id(file_id).await {
                    Ok(plan) => ControlResponse::GetFileReadPlanById(plan),
                    Err(err) => ControlResponse::Error(err),
                }
            }
            ControlRequest::DeleteFile { path } => match self.delete_file(&path).await {
                Ok(()) => ControlResponse::DeleteFile,
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::DeleteFileById { file_id } => {
                match self.delete_file_by_id(file_id).await {
                    Ok(()) => ControlResponse::DeleteFileById,
                    Err(err) => ControlResponse::Error(err),
                }
            }
            ControlRequest::CopyFile {
                source_path,
                target_path,
            } => match self.copy_file(&source_path, &target_path).await {
                Ok(file) => ControlResponse::CopyFile(file),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::CopyFileById {
                source_file_id,
                target_path,
            } => match self.copy_file_by_id(source_file_id, &target_path).await {
                Ok(file) => ControlResponse::CopyFileById(file),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::RenameFile {
                source_path,
                target_path,
            } => match self.rename_file(&source_path, &target_path).await {
                Ok(file) => ControlResponse::RenameFile(file),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::RenameFileById {
                file_id,
                target_path,
            } => match self.rename_file_by_id(file_id, &target_path).await {
                Ok(file) => ControlResponse::RenameFileById(file),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::GetFileChangeLogs {
                after_event_id,
                limit,
            } => match self.get_file_change_logs(after_event_id, limit).await {
                Ok(logs) => ControlResponse::GetFileChangeLogs(logs),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::BeginAppend(request) => match self.begin_append(request).await {
                Ok(lease) => ControlResponse::BeginAppend(lease),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::CommitAppend(request) => match self.commit_append(request).await {
                Ok(plan) => ControlResponse::CommitAppend(plan),
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::AbortAppend { lease_id } => match self.abort_append(lease_id).await {
                Ok(()) => ControlResponse::AbortAppend,
                Err(err) => ControlResponse::Error(err),
            },
            ControlRequest::ReportBundleReplica(report) => {
                let Some(storage_id) = actor_storage_id else {
                    return ControlResponse::Error(fs0_core::Fs0Error::Unauthorized);
                };
                match self.report_bundle_replica(storage_id, report).await {
                    Ok(()) => ControlResponse::ReportBundleReplica,
                    Err(err) => ControlResponse::Error(err),
                }
            }
        }
    }
}

impl Drop for CentralState {
    fn drop(&mut self) {
        if let Some(task) = self.accept_task.get_mut().take() {
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
        .map_err(internal_error)?;
    Ok(relay)
}

fn validate_config(config: &CentralConfig) -> Result<()> {
    if config.p2p_relay.public_url.trim().is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: "p2p_relay.public_url must not be empty".to_owned(),
        });
    }
    Ok(())
}

fn localhost_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn self_signed_quic_server_config() -> Result<rustls::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ])
    .map_err(internal_error)?;
    let rustls_cert = cert.cert.der().clone();
    let private_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let private_key = rustls::pki_types::PrivateKeyDer::from(private_key);
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(internal_error)?
        .with_no_client_auth()
        .with_single_cert(vec![rustls_cert], private_key)
        .map_err(internal_error)
}

fn internal_error(err: impl std::fmt::Display) -> Fs0Error {
    Fs0Error::Internal {
        message: err.to_string(),
    }
}
