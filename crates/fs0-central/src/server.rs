use crate::db::CentralDb;
use crate::{CentralConfig, Fs0Error, Result};
use fs0_core::{
    BundleReplicaReport, ControlRequest, ControlResponse, FileReadPlan, RegisterStorageRequest,
    ReplicaLocation, SessionMessage, StoragePeerInfo,
};
use fs0_transport::{bind_endpoint, encode_endpoint_addr, read_frame, write_frame};
use iroh::Endpoint;
use iroh::endpoint::Connection;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinHandle;

pub struct CentralServer {
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
    pub async fn run(config: CentralConfig) -> Result<Arc<Self>> {
        if config.p2p_relay.public_url.trim().is_empty() {
            return Err(Fs0Error::InvalidConfig {
                message: "p2p_relay.public_url must not be empty".to_owned(),
            });
        }

        let relay = start_relay(&config).await?;
        let endpoint = bind_endpoint(
            &config.p2p_relay.public_url,
            config.p2p_relay.quic_port,
            vec![fs0_core::CONTROL_ALPN.to_vec()],
        )
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
        let control_endpoint =
            encode_endpoint_addr(&endpoint).map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
        let db = CentralDb::open(&config.db_path)?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            next_client_id: AtomicU64::new(1),
            storages: RwLock::new(HashMap::new()),
            online_volumes: RwLock::new(HashMap::new()),
            db: Mutex::new(db),
            control_endpoint,
            accept_task: Mutex::new(None),
            relay,
        });
        let accept_server = Arc::downgrade(&server);
        let accept_task = tokio::spawn(async move {
            accept_loop(endpoint, accept_server).await;
        });
        *server.accept_task.lock() = Some(accept_task);
        Ok(server)
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.config
    }

    #[must_use]
    pub fn control_endpoint(&self) -> &[u8] {
        &self.control_endpoint
    }

    pub async fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storage_peers_snapshot()
    }

    fn alloc_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    fn storage_peers_snapshot(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self.storages.read().values().cloned().collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    fn select_append_volume(&self, prefer_volume_name: Option<&str>) -> Result<u64> {
        let storages = self.storages.read();
        if let Some(name) = prefer_volume_name {
            for peer in storages.values() {
                if let Some(volume) = peer.volumes.iter().find(|volume| volume.name == name) {
                    return Ok(volume.volume_id);
                }
            }
        }
        storages
            .values()
            .flat_map(|peer| peer.volumes.iter())
            .map(|volume| volume.volume_id)
            .next()
            .ok_or(Fs0Error::NotFound)
    }

    fn register_storage(&self, mut request: RegisterStorageRequest) -> Result<StoragePeerInfo> {
        {
            let db = self.db.lock();
            for volume in &mut request.volumes {
                let registered = db.get_volume(volume.volume_id)?.ok_or(Fs0Error::NotFound)?;
                volume.name = registered.name;
                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }

        let mut storages = self.storages.write();
        if storages.contains_key(&request.storage_id) {
            return Err(Fs0Error::AlreadyExists {
                path: format!("storage:{}", request.storage_id),
            });
        }

        let mut online_volumes = self.online_volumes.write();
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

    fn unregister_storage(&self, storage_id: u64) {
        self.storages.write().remove(&storage_id);
        self.online_volumes
            .write()
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
    }

    fn hydrate_read_plan_replicas(&self, mut plan: FileReadPlan) -> Result<FileReadPlan> {
        let bundle_replica_volumes = {
            let db = self.db.lock();
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
            bundle.replicas = self.hydrate_replica_volumes(volume_ids);
        }
        Ok(plan)
    }

    fn hydrate_replica_volumes(&self, volume_ids: Vec<u64>) -> Vec<ReplicaLocation> {
        let online_volumes = self.online_volumes.read();
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

    fn report_bundle_replica(&self, storage_id: u64, report: BundleReplicaReport) -> Result<()> {
        {
            let online_volumes = self.online_volumes.read();
            for event in &report.events {
                if online_volumes.get(&event.volume_id) != Some(&storage_id) {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }
        self.db.lock().record_bundle_events(report)
    }
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        if let Some(task) = self.accept_task.get_mut().take() {
            task.abort();
        }
        let _ = &self.relay;
    }
}

async fn accept_loop(endpoint: Endpoint, server: Weak<CentralServer>) {
    while let Some(incoming) = endpoint.accept().await {
        let Some(server) = server.upgrade() else {
            break;
        };
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                return;
            };
            let _ = handle_control_connection(server, connection).await;
        });
    }
}

async fn handle_control_connection(
    server: Arc<CentralServer>,
    connection: Connection,
) -> Result<()> {
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
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })? {
        SessionMessage::RegisterClient { .. } => {
            let storages = server.storage_peers_snapshot();
            write_frame(
                &mut session_send,
                &SessionMessage::ClientRegistered {
                    client_id: server.alloc_client_id(),
                    storages,
                },
            )
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
        }
        SessionMessage::RegisterStorage { request } => {
            let id = request.storage_id;
            match server.register_storage(request) {
                Ok(_) => {
                    storage_id = Some(id);
                    let storages = server.storage_peers_snapshot();
                    write_frame(
                        &mut session_send,
                        &SessionMessage::StorageRegistered {
                            storage_id: id,
                            storages,
                        },
                    )
                    .await
                    .map_err(|err| Fs0Error::Internal {
                        message: err.to_string(),
                    })?;
                }
                Err(err) => {
                    write_frame(&mut session_send, &SessionMessage::Error(err))
                        .await
                        .map_err(|err| Fs0Error::Internal {
                            message: err.to_string(),
                        })?;
                }
            }
        }
        SessionMessage::Ping => {
            write_frame(&mut session_send, &SessionMessage::Pong)
                .await
                .map_err(|err| Fs0Error::Internal {
                    message: err.to_string(),
                })?;
        }
        _ => {
            write_frame(
                &mut session_send,
                &SessionMessage::Error(fs0_core::Fs0Error::InvalidRequest),
            )
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
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
                            .map_err(|err| Fs0Error::Internal {
                                message: err.to_string(),
                            })?;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            stream = connection.accept_bi() => {
                let Ok((mut send, mut recv)) = stream else {
                    break;
                };
                let server = server.clone();
                tokio::spawn(async move {
                    let Ok(request) = read_frame::<ControlRequest, _>(&mut recv).await else {
                        return;
                    };
                    let response = handle_control_request(server, request, storage_id).await;
                    let _ = write_frame(&mut send, &response).await;
                    let _ = send.finish();
                });
            }
        }
    }

    if let Some(storage_id) = storage_id {
        server.unregister_storage(storage_id);
    }
    Ok(())
}

async fn handle_control_request(
    server: Arc<CentralServer>,
    request: ControlRequest,
    actor_storage_id: Option<u64>,
) -> ControlResponse {
    match request {
        ControlRequest::CreateVolume { name, max_bytes } => {
            match server.db.lock().create_volume(name, max_bytes) {
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
            match server.db.lock().list_directory(&dir, limit, cursor) {
                Ok(entries) => ControlResponse::ListDirectory(entries),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlan { path } => {
            let result = server
                .db
                .lock()
                .get_file_read_plan(&path)
                .and_then(|plan| server.hydrate_read_plan_replicas(plan));
            match result {
                Ok(plan) => ControlResponse::GetFileReadPlan(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlanById { file_id } => {
            let result = server
                .db
                .lock()
                .get_file_read_plan_by_id(file_id)
                .and_then(|plan| server.hydrate_read_plan_replicas(plan));
            match result {
                Ok(plan) => ControlResponse::GetFileReadPlanById(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFile { path } => match server.db.lock().delete_file(&path) {
            Ok(()) => ControlResponse::DeleteFile,
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::DeleteFileById { file_id } => {
            match server.db.lock().delete_file_by_id(file_id) {
                Ok(()) => ControlResponse::DeleteFileById,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CopyFile {
            source_path,
            target_path,
        } => match server.db.lock().copy_file(&source_path, &target_path) {
            Ok(file) => ControlResponse::CopyFile(file),
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::CopyFileById {
            source_file_id,
            target_path,
        } => match server
            .db
            .lock()
            .copy_file_by_id(source_file_id, &target_path)
        {
            Ok(file) => ControlResponse::CopyFileById(file),
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::RenameFile {
            source_path,
            target_path,
        } => match server.db.lock().rename_file(&source_path, &target_path) {
            Ok(file) => ControlResponse::RenameFile(file),
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::RenameFileById {
            file_id,
            target_path,
        } => match server.db.lock().rename_file_by_id(file_id, &target_path) {
            Ok(file) => ControlResponse::RenameFileById(file),
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::GetFileChangeLogs {
            after_event_id,
            limit,
        } => match server.db.lock().get_file_change_logs(after_event_id, limit) {
            Ok(logs) => ControlResponse::GetFileChangeLogs(logs),
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::BeginAppend(request) => {
            let volume_id = match server.select_append_volume(request.prefer_volume_name.as_deref())
            {
                Ok(volume_id) => volume_id,
                Err(err) => return ControlResponse::Error(err),
            };
            match server.db.lock().begin_append(request, 0, volume_id) {
                Ok(lease) => ControlResponse::BeginAppend(lease),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CommitAppend(request) => {
            let result = server
                .db
                .lock()
                .commit_append(request)
                .and_then(|plan| server.hydrate_read_plan_replicas(plan));
            match result {
                Ok(plan) => ControlResponse::CommitAppend(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::AbortAppend { lease_id } => match server.db.lock().abort_append(lease_id) {
            Ok(()) => ControlResponse::AbortAppend,
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::ReportBundleReplica(report) => {
            let Some(storage_id) = actor_storage_id else {
                return ControlResponse::Error(fs0_core::Fs0Error::Unauthorized);
            };
            match server.report_bundle_replica(storage_id, report) {
                Ok(()) => ControlResponse::ReportBundleReplica,
                Err(err) => ControlResponse::Error(err),
            }
        }
    }
}

async fn start_relay(config: &CentralConfig) -> Result<iroh_relay::server::Server> {
    let relay_config = &config.p2p_relay;

    let http_addr = SocketAddr::from(([127, 0, 0, 1], relay_config.port));
    let quic_addr = SocketAddr::from(([127, 0, 0, 1], relay_config.quic_port));
    let mut server_config = iroh_relay::server::ServerConfig::default();
    server_config.relay = Some(iroh_relay::server::RelayConfig::new(http_addr));
    let mut quic_config = iroh_relay::server::QuicConfig::new(quic_addr);
    quic_config.server_config = Some(self_signed_quic_server_config()?);
    server_config.quic = Some(quic_config);
    iroh_relay::server::Server::spawn(server_config)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

fn self_signed_quic_server_config() -> Result<rustls::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec![
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ])
    .map_err(|err| Fs0Error::Internal {
        message: err.to_string(),
    })?;
    let rustls_cert = cert.cert.der().clone();
    let private_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let private_key = rustls::pki_types::PrivateKeyDer::from(private_key);
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?
        .with_no_client_auth()
        .with_single_cert(vec![rustls_cert], private_key)
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}
