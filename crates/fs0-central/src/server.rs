use crate::db::CentralDb;
use crate::{CentralConfig, Fs0Result};
use fs0_core::{
    BeginAppendRequest, BundleReplicaEvent, ControlRequest, ControlResponse, Fs0Error,
    GrantUploadLeaseRequest, StoragePeerInfo, StorageVolumeInfo, TRANSPORT_CONTROL_ALPN,
};
use fs0_transport::{control_rpc, encode_endpoint_addr, read_frame, write_frame};
use iroh::{
    Endpoint,
    endpoint::{Connection, VarInt, presets},
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const APPEND_VOLUME_USAGE_THRESHOLD_NUMERATOR: u64 = 95;
const APPEND_VOLUME_USAGE_THRESHOLD_DENOMINATOR: u64 = 100;

#[derive(Debug)]
pub struct CentralServer {
    config: Arc<CentralConfig>,
    next_client_id: AtomicU64,
    next_storage_id: AtomicU64,
    clients: RwLock<HashMap<u64, String>>,
    storages: RwLock<HashMap<u64, StoragePeerInfo>>,
    storage_connections: RwLock<HashMap<u64, Connection>>,
    online_volumes: RwLock<HashMap<u64, u64>>,
    upload_lease_routes: RwLock<HashMap<u64, StorageUploadLease>>,
    db: Mutex<CentralDb>,
    control_endpoint: Vec<u8>,
    endpoint: Endpoint,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    accept_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageUploadLease {
    storage_id: u64,
    storage_lease_id: u64,
}

impl CentralServer {
    pub async fn run(config: CentralConfig) -> Fs0Result<Arc<Self>> {
        if config.replication_factor == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: "replication_factor must be greater than zero".to_owned(),
            });
        }

        let endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![TRANSPORT_CONTROL_ALPN.to_vec()])
            .bind()
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
        let control_endpoint = encode_endpoint_addr(&endpoint)?;
        let db = CentralDb::open(&config.db_path)?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            next_client_id: AtomicU64::new(1),
            next_storage_id: AtomicU64::new(1),
            clients: RwLock::new(HashMap::new()),
            storages: RwLock::new(HashMap::new()),
            storage_connections: RwLock::new(HashMap::new()),
            online_volumes: RwLock::new(HashMap::new()),
            upload_lease_routes: RwLock::new(HashMap::new()),
            db: Mutex::new(db),
            control_endpoint,
            endpoint,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            accept_task: Mutex::new(None),
        });

        *server.accept_task.lock() = Some(spawn_accept_loop(
            server.endpoint.clone(),
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));

        Ok(server)
    }

    pub async fn run_config(path: impl AsRef<Path>) -> Fs0Result<Arc<Self>> {
        Self::run(fs0_config::Fs0Config::load_from(path)?.central()?).await
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

    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.endpoint.close().await;

        let accept_task = self.accept_task.lock().take();
        if let Some(task) = accept_task {
            let _ = task.await;
        }
    }

    fn is_exiting(&self) -> bool {
        self.exit.load(Ordering::Acquire)
    }

    fn token_allowed(&self, token: &str) -> bool {
        self.config
            .auth_tokens
            .iter()
            .any(|allowed| allowed == token)
    }

    fn storage_peers_snapshot(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self.storages.read().values().cloned().collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    fn register_client(&self, token: String) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        if !self.token_allowed(&token) {
            return Err(Fs0Error::Unauthorized);
        }

        let client_id = self.next_client_id.fetch_add(1, Ordering::AcqRel);
        self.clients.write().insert(client_id, token);

        Ok((client_id, self.storage_peers_snapshot()))
    }

    fn validate_client_auth(&self, client_id: u64, client_token: String) -> Fs0Result<()> {
        let clients = self.clients.read();
        let Some(token) = clients.get(&client_id) else {
            return Err(Fs0Error::Unauthorized);
        };

        if token == &client_token {
            Ok(())
        } else {
            Err(Fs0Error::Unauthorized)
        }
    }

    fn unregister_client(&self, client_id: u64) {
        self.clients.write().remove(&client_id);
    }

    fn register_storage(
        &self,
        name: String,
        token: String,
        mut volumes: Vec<StorageVolumeInfo>,
        iroh_endpoint: Vec<u8>,
        connection: Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        if !self.token_allowed(&token) {
            return Err(Fs0Error::Unauthorized);
        }

        {
            let db = self.db.lock();
            for volume in &mut volumes {
                let registered = db.get_volume(volume.volume_id)?.ok_or(Fs0Error::NotFound)?;
                volume.name = registered.name;

                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }

        let storage_id = self.next_storage_id.fetch_add(1, Ordering::AcqRel);
        let peer = StoragePeerInfo {
            storage_id,
            name,
            volumes,
            iroh_endpoint,
        };

        {
            let mut online_volumes = self.online_volumes.write();
            for volume in &peer.volumes {
                if online_volumes.contains_key(&volume.volume_id) {
                    return Err(Fs0Error::VolumeAlreadyMounted);
                }
            }

            for volume in &peer.volumes {
                online_volumes.insert(volume.volume_id, storage_id);
            }
        }

        self.storage_connections
            .write()
            .insert(storage_id, connection);
        self.storages.write().insert(storage_id, peer);

        Ok((storage_id, self.storage_peers_snapshot()))
    }

    fn unregister_storage(&self, storage_id: u64) {
        self.storages.write().remove(&storage_id);
        self.storage_connections.write().remove(&storage_id);
        self.online_volumes
            .write()
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
        self.upload_lease_routes
            .write()
            .retain(|_, route| route.storage_id != storage_id);
    }

    fn central_status(&self) -> Fs0Result<(u32, Vec<StoragePeerInfo>)> {
        Ok((
            self.clients.read().len() as u32,
            self.storage_peers_snapshot(),
        ))
    }

    fn select_append_volume(
        &self,
        prefer_volume_name: Option<&str>,
        append_size_hint: Option<u64>,
    ) -> Fs0Result<u64> {
        let storages = self.storages.read();

        if let Some(name) = prefer_volume_name {
            for peer in storages.values() {
                if let Some(volume) = peer.volumes.iter().find(|volume| volume.name == name)
                    && Self::volume_accepts_append(volume, append_size_hint)
                {
                    return Ok(volume.volume_id);
                }
            }

            return Err(Fs0Error::InvalidRequest);
        }

        storages
            .values()
            .flat_map(|peer| peer.volumes.iter())
            .filter(|volume| Self::volume_accepts_append(volume, append_size_hint))
            .min_by(|left, right| {
                let left_used = u128::from(left.max_volume_offset) * u128::from(right.max_bytes);
                let right_used = u128::from(right.max_volume_offset) * u128::from(left.max_bytes);

                left_used
                    .cmp(&right_used)
                    .then_with(|| left.volume_id.cmp(&right.volume_id))
            })
            .map(|volume| volume.volume_id)
            .ok_or(Fs0Error::NotFound)
    }

    fn volume_accepts_append(volume: &StorageVolumeInfo, append_size_hint: Option<u64>) -> bool {
        if volume.read_only || volume.max_bytes == 0 {
            return false;
        }

        if volume
            .max_volume_offset
            .saturating_mul(APPEND_VOLUME_USAGE_THRESHOLD_DENOMINATOR)
            >= volume
                .max_bytes
                .saturating_mul(APPEND_VOLUME_USAGE_THRESHOLD_NUMERATOR)
        {
            return false;
        }

        if let Some(size) = append_size_hint
            && volume.max_bytes.saturating_sub(volume.max_volume_offset) < size
        {
            return false;
        }

        true
    }

    fn hydrate_read_plan_replicas(
        &self,
        mut plan: fs0_core::FileReadPlan,
    ) -> Fs0Result<fs0_core::FileReadPlan> {
        let bundle_replica_volumes = {
            let db = self.db.lock();
            plan.bundles
                .iter()
                .map(|bundle| {
                    let volume_ids = db.bundle_replica_volumes(bundle.bundle_id)?;
                    Ok((bundle.bundle_id, volume_ids))
                })
                .collect::<Fs0Result<HashMap<_, _>>>()?
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

    fn hydrate_replica_volumes(&self, volume_ids: Vec<u64>) -> Vec<fs0_core::ReplicaLocation> {
        let online_volumes = self.online_volumes.read();

        volume_ids
            .into_iter()
            .filter_map(|volume_id| {
                online_volumes
                    .get(&volume_id)
                    .map(|storage_id| fs0_core::ReplicaLocation {
                        storage_id: *storage_id,
                        volume_id,
                    })
            })
            .collect()
    }

    async fn begin_append(
        &self,
        request: BeginAppendRequest,
        client_id: u64,
    ) -> Fs0Result<fs0_core::AppendLease> {
        let volume_id = self.select_append_volume(
            request.prefer_volume_name.as_deref(),
            request.append_size_hint,
        )?;
        let lease = self.db.lock().begin_append(request, client_id, volume_id)?;

        match self.grant_upload_lease_to_storage(&lease, client_id).await {
            Ok(storage_lease_id) => {
                let storage_id = self.storage_id_for_volume(volume_id)?;
                self.upload_lease_routes.write().insert(
                    lease.lease_id,
                    StorageUploadLease {
                        storage_id,
                        storage_lease_id,
                    },
                );

                Ok(lease)
            }
            Err(err) => {
                let _ = self.db.lock().abort_append(lease.lease_id, client_id);
                Err(err)
            }
        }
    }

    async fn commit_append(
        &self,
        request: fs0_core::CommitAppendRequest,
        client_id: u64,
    ) -> Fs0Result<fs0_core::FileReadPlan> {
        let lease_id = request.lease_id;
        let plan = self
            .db
            .lock()
            .commit_append(request, client_id)
            .and_then(|plan| self.hydrate_read_plan_replicas(plan))?;

        self.revoke_storage_upload_lease(lease_id).await;

        Ok(plan)
    }

    async fn abort_append(&self, lease_id: u64, client_id: u64) -> Fs0Result<()> {
        self.db.lock().abort_append(lease_id, client_id)?;
        self.revoke_storage_upload_lease(lease_id).await;

        Ok(())
    }

    async fn grant_upload_lease_to_storage(
        &self,
        lease: &fs0_core::AppendLease,
        client_id: u64,
    ) -> Fs0Result<u64> {
        let storage_id = self.storage_id_for_volume(lease.volume_id)?;
        let connection = self
            .storage_connections
            .read()
            .get(&storage_id)
            .cloned()
            .ok_or(Fs0Error::NotFound)?;
        let request = ControlRequest::GrantUploadLease(GrantUploadLeaseRequest {
            client_id,
            file_id: lease.file_id,
            volume_id: lease.volume_id,
            base_size: lease.base_size,
            prefer_volume_name: lease.prefer_volume_name.clone(),
        });

        match control_rpc(&connection, request).await? {
            ControlResponse::GrantUploadLease { lease_id } => Ok(lease_id),
            ControlResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected grant upload lease response: {response:?}"),
            }),
        }
    }

    async fn revoke_storage_upload_lease(&self, lease_id: u64) {
        let route = self.upload_lease_routes.write().remove(&lease_id);
        let Some(route) = route else {
            return;
        };

        let connection = self
            .storage_connections
            .read()
            .get(&route.storage_id)
            .cloned();
        let Some(connection) = connection else {
            return;
        };

        let _ = control_rpc(
            &connection,
            ControlRequest::RevokeUploadLease {
                lease_id: route.storage_lease_id,
            },
        )
        .await;
    }

    fn storage_id_for_volume(&self, volume_id: u64) -> Fs0Result<u64> {
        self.online_volumes
            .read()
            .get(&volume_id)
            .copied()
            .ok_or(Fs0Error::NotFound)
    }

    fn report_bundle_replica(
        &self,
        storage_id: u64,
        events: Vec<BundleReplicaEvent>,
    ) -> Fs0Result<()> {
        {
            let online_volumes = self.online_volumes.read();
            for event in &events {
                if online_volumes.get(&event.volume_id) != Some(&storage_id) {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }

        self.db.lock().record_bundle_events(events)
    }
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }
}

fn spawn_accept_loop(
    endpoint: Endpoint,
    server: Weak<CentralServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let shutdown_notify = shutdown_notify.clone();
                    tokio::spawn(async move {
                        let Ok(connection) = incoming.await else {
                            return;
                        };
                        handle_control_connection(server, connection, shutdown_notify).await;
                    });
                }
            }
        }
    })
}

async fn handle_control_connection(
    server: Arc<CentralServer>,
    connection: Connection,
    shutdown_notify: Arc<Notify>,
) {
    let mut client_id = None;
    let mut storage_id = None;

    loop {
        if server.is_exiting() {
            break;
        }

        tokio::select! {
            _ = shutdown_notify.notified() => break,
            stream = connection.accept_bi() => {
                let Ok((mut send, mut recv)) = stream else {
                    break;
                };

                let response = match read_frame::<ControlRequest, _>(&mut recv).await {
                    Ok(request) => {
                        handle_control_request(
                            &server,
                            &connection,
                            request,
                            &mut client_id,
                            &mut storage_id,
                        )
                        .await
                    }
                    Err(err) => ControlResponse::Error(err),
                };

                let _ = write_frame(&mut send, &response).await;
                let _ = send.finish();
            }
        }
    }

    if let Some(client_id) = client_id {
        server.unregister_client(client_id);
    }
    if let Some(storage_id) = storage_id {
        server.unregister_storage(storage_id);
    }

    connection.close(VarInt::from_u32(0), b"central control closed");
}

async fn handle_control_request(
    server: &CentralServer,
    connection: &Connection,
    request: ControlRequest,
    actor_client_id: &mut Option<u64>,
    actor_storage_id: &mut Option<u64>,
) -> ControlResponse {
    match request {
        ControlRequest::RegisterClient { name: _, token } => {
            if actor_client_id.is_some() || actor_storage_id.is_some() {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match server.register_client(token) {
                Ok((client_id, storages)) => {
                    *actor_client_id = Some(client_id);
                    ControlResponse::RegisterClient {
                        client_id,
                        storages,
                    }
                }
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RegisterStorage {
            name,
            token,
            volumes,
            iroh_endpoint,
        } => {
            if actor_client_id.is_some() || actor_storage_id.is_some() {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match server.register_storage(name, token, volumes, iroh_endpoint, connection.clone()) {
                Ok((storage_id, storages)) => {
                    *actor_storage_id = Some(storage_id);
                    ControlResponse::RegisterStorage {
                        storage_id,
                        storages,
                    }
                }
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::ValidateClientAuth {
            client_id,
            client_token,
        } => {
            if actor_storage_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.validate_client_auth(client_id, client_token) {
                Ok(()) => ControlResponse::ValidateClientAuth { client_id },
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CreateVolume { name, max_bytes } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().create_volume(name, max_bytes) {
                Ok(volume) => ControlResponse::CreateVolume {
                    volume_id: volume.volume_id,
                },
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CentralStatus => match server.central_status() {
            Ok((clients_count, storages)) => ControlResponse::CentralStatus {
                clients_count,
                storages,
            },
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::ListDirectory { dir, limit, cursor } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().list_directory(&dir, limit, cursor) {
                Ok(entries) => ControlResponse::ListDirectory(entries),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlan { path } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

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
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

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
        ControlRequest::DeleteFile { path } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().delete_file(&path) {
                Ok(()) => ControlResponse::DeleteFile,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFileById { file_id } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().delete_file_by_id(file_id) {
                Ok(()) => ControlResponse::DeleteFileById,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CopyFile {
            source_path,
            target_path,
        } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().copy_file(&source_path, &target_path) {
                Ok(file) => ControlResponse::CopyFile(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CopyFileById {
            source_file_id,
            target_path,
        } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server
                .db
                .lock()
                .copy_file_by_id(source_file_id, &target_path)
            {
                Ok(file) => ControlResponse::CopyFileById(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RenameFile {
            source_path,
            target_path,
        } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().rename_file(&source_path, &target_path) {
                Ok(file) => ControlResponse::RenameFile(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RenameFileById {
            file_id,
            target_path,
        } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().rename_file_by_id(file_id, &target_path) {
                Ok(file) => ControlResponse::RenameFileById(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileChangeLogs {
            after_event_id,
            limit,
        } => {
            if actor_client_id.is_none() {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().get_file_change_logs(after_event_id, limit) {
                Ok(logs) => ControlResponse::GetFileChangeLogs(logs),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::BeginAppend(request) => {
            let Some(client_id) = *actor_client_id else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.begin_append(request, client_id).await {
                Ok(lease) => ControlResponse::BeginAppend(lease),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CommitAppend(request) => {
            let Some(client_id) = *actor_client_id else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.commit_append(request, client_id).await {
                Ok(plan) => ControlResponse::CommitAppend(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::AbortAppend { lease_id } => {
            let Some(client_id) = *actor_client_id else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.abort_append(lease_id, client_id).await {
                Ok(()) => ControlResponse::AbortAppend,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::ReportBundleReplica { events } => {
            let Some(storage_id) = *actor_storage_id else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.report_bundle_replica(storage_id, events) {
                Ok(()) => ControlResponse::ReportBundleReplica,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GrantUploadLease(_) | ControlRequest::RevokeUploadLease { .. } => {
            ControlResponse::Error(Fs0Error::InvalidRequest)
        }
    }
}
