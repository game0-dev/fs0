use crate::{CentralConfig, Fs0Result, db::CentralDb};
use fs0_core::{
    APPEND_VOLUME_USAGE_THRESHOLD, Fs0Error, TRANSPORT_CONTROL_ALPN,
    protocol::{
        AppendLease, BeginAppendRequest, BundleReplicaEvent, CommitAppendRequest, ControlRequest,
        ControlResponse, FileReadPlan, GrantUploadLeaseRequest, ProtocolRequest, ProtocolResponse,
        ReplicaLocation, StoragePeerInfo, StorageVolumeInfo,
    },
};
use fs0_transport::{Connection, EndpointAddr, SecretKey, Transport};
use iroh_relay::server::{
    Access as RelayAccess, AccessConfig as RelayAccessConfig, CertConfig as RelayCertConfig,
    QuicConfig as RelayQuicConfig, RelayConfig as RelayServerConfig, Server as RelayServer,
    ServerConfig as RelayRootConfig, TlsConfig as RelayTlsConfig,
};
use parking_lot::{Mutex, RwLock};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};

#[derive(Debug)]
pub struct CentralServer {
    config: Arc<CentralConfig>,
    transport: Transport,
    next_id: AtomicU64,
    pub(crate) clients: RwLock<HashMap<u64, ClientControlConnection>>,
    pub(crate) storages: RwLock<HashMap<u64, StorageControlConnection>>,
    pub(crate) online_volumes: RwLock<HashMap<u64, u64>>,
    db: Mutex<CentralDb>,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    _join_handles: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClientControlConnection {
    pub(crate) token: String,
    pub(crate) connection: Connection,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageControlConnection {
    pub(crate) peer: StoragePeerInfo,
    pub(crate) connection: Connection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ControlConnectionIdentity {
    #[default]
    Anonymous,
    Client(u64),
    Storage(u64),
}

impl CentralServer {
    pub async fn run(config: CentralConfig) -> Fs0Result<Arc<Self>> {
        if config.replication_factor == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: "replication_factor must be greater than zero".to_owned(),
            });
        }

        let relay = spawn_relay(&config.relay).await?;
        let secret_key =
            config
                .secret_key
                .parse::<SecretKey>()
                .map_err(|err| Fs0Error::InvalidConfig {
                    message: format!("invalid central.secret_key: {err}"),
                })?;
        let transport = Transport::bind(
            vec![TRANSPORT_CONTROL_ALPN],
            Some(secret_key),
            Some(SocketAddr::from(([0, 0, 0, 0], config.bind_port))),
            None,
        )
        .await?;
        let db = CentralDb::open(&config.db_path)?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            transport,
            next_id: AtomicU64::new(1),
            clients: RwLock::new(HashMap::new()),
            storages: RwLock::new(HashMap::new()),
            online_volumes: RwLock::new(HashMap::new()),
            db: Mutex::new(db),
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            _join_handles: Mutex::new(None),
        });

        *server._join_handles.lock() = Some(spawn_central_tasks(
            server.transport.clone(),
            relay,
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));

        Ok(server)
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.config
    }

    #[must_use]
    pub fn control_endpoint(&self) -> EndpointAddr {
        self.transport.addr()
    }

    pub async fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storage_peers_snapshot()
    }

    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.transport.close().await;

        let join_handle = self._join_handles.lock().take();
        if let Some(task) = join_handle {
            let _ = task.await;
        }
    }

    fn is_exiting(&self) -> bool {
        self.exit.load(Ordering::Acquire)
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::AcqRel)
    }

    fn token_allowed(&self, token: &str) -> bool {
        self.config
            .auth_tokens
            .iter()
            .any(|allowed| allowed == token)
    }

    fn storage_peers_snapshot(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self
            .storages
            .read()
            .values()
            .map(|storage| storage.peer.clone())
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    fn register_client(
        &self,
        token: String,
        connection: Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        if !self.token_allowed(&token) {
            return Err(Fs0Error::Unauthorized);
        }

        let client_id = self.next_id();
        self.clients
            .write()
            .insert(client_id, ClientControlConnection { token, connection });

        Ok((client_id, self.storage_peers_snapshot()))
    }

    fn validate_client_auth(&self, client_id: u64, client_token: String) -> Fs0Result<()> {
        let clients = self.clients.read();
        let Some(client) = clients.get(&client_id) else {
            return Err(Fs0Error::Unauthorized);
        };

        if client.token == client_token {
            Ok(())
        } else {
            Err(Fs0Error::Unauthorized)
        }
    }

    fn unregister_client(&self, client_id: u64) {
        if let Some(client) = self.clients.write().remove(&client_id) {
            client.connection.close(b"central client unregistered");
        }
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
            let mut db = self.db.lock();
            for volume in &mut volumes {
                let registered = db.get_volume(volume.volume_id)?;
                volume.name = registered.name;
                volume.max_volume_offset =
                    registered.max_volume_offset.max(volume.max_volume_offset);
                if volume.max_volume_offset != registered.max_volume_offset {
                    db.update_volume_offset(volume.volume_id, volume.max_volume_offset)?;
                }

                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
        }

        let storage_id = self.next_id();
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

        self.storages.write().insert(
            peer.storage_id,
            StorageControlConnection { peer, connection },
        );

        Ok((storage_id, self.storage_peers_snapshot()))
    }

    fn unregister_storage(&self, storage_id: u64) {
        if let Some(storage) = self.storages.write().remove(&storage_id) {
            storage.connection.close(b"central storage unregistered");
        }
        self.online_volumes
            .write()
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
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
        let storages = self.storage_peers_snapshot();

        if let Some(name) = prefer_volume_name {
            for peer in &storages {
                if let Some(volume) = peer.volumes.iter().find(|volume| volume.name == name)
                    && Self::volume_accepts_append(volume, append_size_hint)
                {
                    return Ok(volume.volume_id);
                }
            }

            return Err(Fs0Error::InvalidRequest);
        }

        storages
            .iter()
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

        if volume.max_volume_offset as f64 / volume.max_bytes as f64
            >= APPEND_VOLUME_USAGE_THRESHOLD
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

    fn hydrate_read_plan_replicas(&self, mut plan: FileReadPlan) -> Fs0Result<FileReadPlan> {
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
            let online_volumes = self.online_volumes.read();
            bundle.replicas = volume_ids
                .into_iter()
                .filter_map(|volume_id| {
                    online_volumes
                        .get(&volume_id)
                        .map(|storage_id| ReplicaLocation {
                            storage_id: *storage_id,
                            volume_id,
                        })
                })
                .collect();
        }

        Ok(plan)
    }

    fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
        self.db
            .lock()
            .get_file_read_plan(path)
            .and_then(|plan| self.hydrate_read_plan_replicas(plan))
    }

    fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        self.db
            .lock()
            .get_file_read_plan_by_id(file_id)
            .and_then(|plan| self.hydrate_read_plan_replicas(plan))
    }

    async fn begin_append(&self, request: BeginAppendRequest) -> Fs0Result<AppendLease> {
        let volume_id = self.select_append_volume(
            request.prefer_volume_name.as_deref(),
            request.append_size_hint,
        )?;
        let storage_id = self
            .online_volumes
            .read()
            .get(&volume_id)
            .copied()
            .ok_or(Fs0Error::NotFound)?;
        let lease = self.db.lock().begin_append(request, volume_id)?;

        match self
            .grant_upload_lease_to_specific_storage(storage_id, &lease)
            .await
        {
            Ok(()) => Ok(lease),
            Err(err) => {
                let _ = self.db.lock().abort_append(lease.lease_id, lease.file_id);
                Err(err)
            }
        }
    }

    async fn commit_append(&self, request: CommitAppendRequest) -> Fs0Result<FileReadPlan> {
        let lease_id = request.lease_id;
        let file_id = request.file_id;
        let storage_id = self.storage_id_for_append_lease(lease_id, file_id).ok();
        let result = {
            self.db
                .lock()
                .commit_append(request)
                .and_then(|plan| self.hydrate_read_plan_replicas(plan))
        };

        if let Some(storage_id) = storage_id {
            self.revoke_storage_upload_lease(storage_id, lease_id).await;
        }

        result
    }

    async fn abort_append(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        let storage_id = self.storage_id_for_append_lease(lease_id, file_id).ok();
        self.db.lock().abort_append(lease_id, file_id)?;
        if let Some(storage_id) = storage_id {
            self.revoke_storage_upload_lease(storage_id, lease_id).await;
        }

        Ok(())
    }

    async fn grant_upload_lease_to_specific_storage(
        &self,
        storage_id: u64,
        lease: &AppendLease,
    ) -> Fs0Result<()> {
        let connection = self
            .storages
            .read()
            .get(&storage_id)
            .map(|storage| storage.connection.clone())
            .ok_or(Fs0Error::NotFound)?;
        let request = ControlRequest::GrantUploadLease(GrantUploadLeaseRequest {
            lease_id: lease.lease_id,
            file_id: lease.file_id,
            volume_id: lease.volume_id,
            base_size: lease.base_size,
            expires_at_ms: lease.expires_at_ms,
            prefer_volume_name: lease.prefer_volume_name.clone(),
        });

        match connection.rpc(ProtocolRequest::Control(request)).await? {
            ProtocolResponse::Control(ControlResponse::GrantUploadLease { lease_id })
                if lease_id == lease.lease_id =>
            {
                Ok(())
            }
            ProtocolResponse::Control(ControlResponse::Error(err))
            | ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected grant upload lease response: {response:?}"),
            }),
        }
    }

    async fn revoke_storage_upload_lease(&self, storage_id: u64, lease_id: u64) {
        let connection = self
            .storages
            .read()
            .get(&storage_id)
            .map(|storage| storage.connection.clone());
        let Some(connection) = connection else {
            return;
        };

        let _: Fs0Result<ProtocolResponse> = connection
            .rpc(ProtocolRequest::Control(
                ControlRequest::RevokeUploadLease { lease_id },
            ))
            .await;
    }

    fn storage_id_for_append_lease(&self, lease_id: u64, file_id: u64) -> Fs0Result<u64> {
        let volume_id = self
            .db
            .lock()
            .active_append_lease_volume(lease_id, file_id)?;
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
        let online_volumes = self.online_volumes.read();
        for event in &events {
            if online_volumes.get(&event.volume_id) != Some(&storage_id) {
                return Err(Fs0Error::InvalidRequest);
            }
        }

        self.db.lock().record_bundle_events(events)
    }

    fn update_storage_volume_offset(
        &self,
        storage_id: u64,
        volume_id: u64,
        max_volume_offset: u64,
    ) -> Fs0Result<()> {
        let registered = self
            .db
            .lock()
            .update_volume_offset(volume_id, max_volume_offset)?;
        let mut storages = self.storages.write();
        let storage = storages.get_mut(&storage_id).ok_or(Fs0Error::NotFound)?;
        let volume = storage
            .peer
            .volumes
            .iter_mut()
            .find(|volume| volume.volume_id == volume_id)
            .ok_or(Fs0Error::NotFound)?;

        volume.name = registered.name;
        volume.max_bytes = registered.max_bytes;
        volume.max_volume_offset = registered.max_volume_offset;

        Ok(())
    }
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }
}

async fn spawn_relay(config: &fs0_config::CentralRelayConfig) -> Fs0Result<RelayServer> {
    if config.token.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: "central.relay.token must not be empty".to_owned(),
        });
    }

    let mut relay_config =
        RelayServerConfig::new(SocketAddr::from(([0, 0, 0, 0], config.http_bind_port)));
    relay_config.access = relay_access_config(config.token.clone());
    relay_config.tls = Some(relay_tls_config(&config.tls)?);

    let mut root_config = RelayRootConfig::default();
    root_config.quic = Some(RelayQuicConfig::new(SocketAddr::from((
        [0, 0, 0, 0],
        config.quic.bind_port,
    ))));
    root_config.relay = Some(relay_config);

    let relay = RelayServer::spawn(root_config)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: format!("failed to start relay: {err}"),
        })?;

    Ok(relay)
}

fn relay_access_config(token: String) -> RelayAccessConfig {
    let token = Arc::new(token);
    RelayAccessConfig::Restricted(Box::new(move |request| {
        let token = token.clone();
        Box::pin(async move {
            if request.auth_token().as_deref() == Some(token.as_str()) {
                RelayAccess::Allow
            } else {
                RelayAccess::Deny
            }
        })
    }))
}

fn relay_tls_config(config: &fs0_config::CentralRelayTlsConfig) -> Fs0Result<RelayTlsConfig> {
    let certs = CertificateDer::pem_file_iter(&config.cert_path)
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to open central.relay.tls cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay.tls cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?;
    if certs.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: format!(
                "central.relay.tls cert_path {} contains no certificates",
                config.cert_path.display()
            ),
        });
    }

    let private_key =
        PrivateKeyDer::from_pem_file(&config.key_path).map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay.tls key_path {}: {err}",
                config.key_path.display()
            ),
        })?;
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("failed to configure central.relay.tls protocols: {err}"),
    })?
    .with_no_client_auth()
    .with_single_cert(certs, private_key)
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("invalid central.relay.tls certificate or key: {err}"),
    })?;

    Ok(RelayTlsConfig::new(
        SocketAddr::from(([0, 0, 0, 0], config.https_bind_port)),
        RelayCertConfig::Manual { server_config },
    ))
}

fn spawn_central_tasks(
    transport: Transport,
    relay: RelayServer,
    server: Weak<CentralServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let accept_task = spawn_accept_loop(transport, server, shutdown_notify);
        let _ = accept_task.await;
        let _ = relay.shutdown().await;
    })
}

fn spawn_accept_loop(
    endpoint: Transport,
    server: Weak<CentralServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                connection = endpoint.accept() => {
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let connection = match connection {
                        Ok(Some(connection)) => connection,
                        Ok(None) => break,
                        Err(_) => continue,
                    };
                    let shutdown_notify = shutdown_notify.clone();
                    tokio::spawn(async move {
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
    let identity = Arc::new(tokio::sync::Mutex::new(ControlConnectionIdentity::default()));
    if !server.is_exiting() {
        tokio::select! {
            _ = shutdown_notify.notified() => {}
            _ = connection.serve({
                let server = server.clone();
                let connection = connection.clone();
                let identity = identity.clone();
                move |request| {
                    let server = server.clone();
                    let connection = connection.clone();
                    let identity = identity.clone();
                    async move {
                        let mut identity = identity.lock().await;
                        let response = match request {
                            ProtocolRequest::Control(request) => {
                                ProtocolResponse::Control(handle_control_request(
                                    &server,
                                    &connection,
                                    request,
                                    &mut *identity,
                                )
                                .await)
                            }
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            }) => {}
        }
    }

    match *identity.lock().await {
        ControlConnectionIdentity::Anonymous => {}
        ControlConnectionIdentity::Client(client_id) => server.unregister_client(client_id),
        ControlConnectionIdentity::Storage(storage_id) => server.unregister_storage(storage_id),
    }

    connection.close(b"central control closed");
}

async fn handle_control_request(
    server: &CentralServer,
    connection: &Connection,
    request: ControlRequest,
    identity: &mut ControlConnectionIdentity,
) -> ControlResponse {
    match request {
        ControlRequest::RegisterClient { name: _, token } => {
            if !matches!(*identity, ControlConnectionIdentity::Anonymous) {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match server.register_client(token, connection.clone()) {
                Ok((client_id, storages)) => {
                    *identity = ControlConnectionIdentity::Client(client_id);
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
            if !matches!(*identity, ControlConnectionIdentity::Anonymous) {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match server.register_storage(name, token, volumes, iroh_endpoint, connection.clone()) {
                Ok((storage_id, storages)) => {
                    *identity = ControlConnectionIdentity::Storage(storage_id);
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
            if !matches!(*identity, ControlConnectionIdentity::Storage(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.validate_client_auth(client_id, client_token) {
                Ok(()) => ControlResponse::ValidateClientAuth { client_id },
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CreateVolume { name, max_bytes } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().list_directory(&dir, limit, cursor) {
                Ok(entries) => ControlResponse::ListDirectory(entries),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlan { path } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.get_file_read_plan(&path) {
                Ok(plan) => ControlResponse::GetFileReadPlan(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlanById { file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.get_file_read_plan_by_id(file_id) {
                Ok(plan) => ControlResponse::GetFileReadPlanById(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFile { path } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().delete_file(&path) {
                Ok(()) => ControlResponse::DeleteFile,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFileById { file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
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
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.db.lock().get_file_change_logs(after_event_id, limit) {
                Ok(logs) => ControlResponse::GetFileChangeLogs(logs),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::BeginAppend(request) => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.begin_append(request).await {
                Ok(lease) => ControlResponse::BeginAppend(lease),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CommitAppend(request) => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.commit_append(request).await {
                Ok(plan) => ControlResponse::CommitAppend(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::AbortAppend { lease_id, file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match server.abort_append(lease_id, file_id).await {
                Ok(()) => ControlResponse::AbortAppend,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::ReportBundleReplica { events } => {
            let ControlConnectionIdentity::Storage(storage_id) = *identity else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.report_bundle_replica(storage_id, events) {
                Ok(()) => ControlResponse::ReportBundleReplica,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::UpdateStorageVolumeOffset {
            volume_id,
            max_volume_offset,
        } => {
            let ControlConnectionIdentity::Storage(storage_id) = *identity else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match server.update_storage_volume_offset(storage_id, volume_id, max_volume_offset) {
                Ok(()) => ControlResponse::UpdateStorageVolumeOffset,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GrantUploadLease(_) | ControlRequest::RevokeUploadLease { .. } => {
            ControlResponse::Error(Fs0Error::InvalidRequest)
        }
    }
}
