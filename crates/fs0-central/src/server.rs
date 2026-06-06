use crate::{
    CentralConfig, Fs0Result,
    db::{CentralDb, CentralTx, CreateAppendLease},
};
use fs0_core::{
    APPEND_LEASE_TTL_MS, APPEND_VOLUME_USAGE_THRESHOLD, Fs0Error, TRANSPORT_CONTROL_ALPN,
    VOLUME_BUNDLE_RAW_SIZE,
    protocol::{
        AppendLease, BeginAppendRequest, BundleReplicaEvent, BundleReplicaEventKind,
        CommitAppendRequest, CommittedBundle, ControlRequest, ControlResponse, DirectoryEntries,
        FileBundleRef, FileChangeLogKind, FileChangeLogs, FileReadPlan, FileRecord,
        GrantUploadLeaseRequest, ProtocolRequest, ProtocolResponse, ReplicaLocation,
        StoragePeerInfo, StorageVolumeInfo,
    },
    utils::now_ms,
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
    collections::{HashMap, HashSet},
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
            let tx = db.tx()?;
            for volume in &mut volumes {
                let registered = tx.get_volume(volume.volume_id)?;
                volume.name = registered.name;
                volume.max_volume_offset =
                    registered.max_volume_offset.max(volume.max_volume_offset);
                if volume.max_volume_offset != registered.max_volume_offset {
                    tx.update_volume_offset(volume.volume_id, volume.max_volume_offset)?;
                }

                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
            tx.commit()?;
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
        let replica_volume_ids_by_bundle = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let mut seen = HashSet::new();
            let bundle_ids = plan
                .bundles
                .iter()
                .filter_map(|bundle| seen.insert(bundle.bundle_id).then_some(bundle.bundle_id))
                .collect::<Vec<_>>();
            let mut volumes: HashMap<_, Vec<u64>> = HashMap::new();
            for replica in tx.get_bundles_by_ids(&bundle_ids)? {
                volumes
                    .entry(replica.bundle_id)
                    .or_default()
                    .push(replica.volume_id);
            }
            tx.commit()?;
            volumes
        };

        for bundle in &mut plan.bundles {
            let volume_ids = replica_volume_ids_by_bundle
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
        let plan = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let file = tx.get_file_by_path(path)?;
            let plan = get_file_read_plan_tx(&tx, file.file_id)?;
            tx.commit()?;
            plan
        };

        self.hydrate_read_plan_replicas(plan)
    }

    fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        let plan = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let plan = get_file_read_plan_tx(&tx, file_id)?;
            tx.commit()?;
            plan
        };

        self.hydrate_read_plan_replicas(plan)
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
        let lease = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let now = now_ms();
            let expires_at_ms = now + APPEND_LEASE_TTL_MS;
            let (file, base_size) = match tx.get_file_by_path(&request.path) {
                Ok(file) => {
                    if request.offset > file.size_bytes {
                        return Err(Fs0Error::InvalidRequest);
                    }
                    let base_size = file.size_bytes;
                    (file, base_size)
                }
                Err(Fs0Error::NotFound) => {
                    if request.offset != 0 {
                        return Err(Fs0Error::NotFound);
                    }
                    let file = tx.create_file(&request.path, now)?;
                    (file, 0)
                }
                Err(err) => return Err(err),
            };
            tx.delete_expired_append_leases(now)?;
            if tx.file_has_active_append_lease(file.file_id)? {
                return Err(Fs0Error::AlreadyExists { path: request.path });
            }
            let lease = tx.create_append_lease(CreateAppendLease {
                file_id: file.file_id,
                volume_id,
                base_size_bytes: base_size,
                offset_bytes: request.offset,
                prefer_volume_name: request.prefer_volume_name,
                expires_at_ms,
                created_at_ms: now,
            })?;
            tx.commit()?;
            lease
        };

        match self
            .grant_upload_lease_to_specific_storage(storage_id, &lease)
            .await
        {
            Ok(()) => Ok(lease),
            Err(err) => {
                let _ = self.abort_append_db_only(lease.lease_id, lease.file_id);
                Err(err)
            }
        }
    }

    async fn commit_append(&self, request: CommitAppendRequest) -> Fs0Result<FileReadPlan> {
        let lease_id = request.lease_id;
        let file_id = request.file_id;
        let storage_id = self.storage_id_for_append_lease(lease_id, file_id).ok();
        let plan = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let plan = commit_append_tx(&tx, request)?;
            tx.commit()?;
            plan
        };
        let result = self.hydrate_read_plan_replicas(plan);

        if let Some(storage_id) = storage_id {
            self.revoke_storage_upload_lease(storage_id, lease_id).await;
        }

        result
    }

    async fn abort_append(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        let storage_id = self.storage_id_for_append_lease(lease_id, file_id).ok();
        self.abort_append_db_only(lease_id, file_id)?;
        if let Some(storage_id) = storage_id {
            self.revoke_storage_upload_lease(storage_id, lease_id).await;
        }

        Ok(())
    }

    fn abort_append_db_only(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        let mut db = self.db.lock();
        let tx = db.tx()?;
        tx.load_active_append_lease(lease_id, file_id)?;
        tx.delete_append_lease(lease_id)?;
        tx.commit()
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
        let volume_id = self.active_append_lease_volume_db(lease_id, file_id)?;
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

        let mut db = self.db.lock();
        let tx = db.tx()?;
        for event in events {
            match event.kind {
                BundleReplicaEventKind::Stored => {
                    tx.insert_bundle_replica(
                        event.bundle_id,
                        event.volume_id,
                        event.raw_len.ok_or(Fs0Error::InvalidRequest)?,
                        event.compressed_len.ok_or(Fs0Error::InvalidRequest)?,
                    )?;
                }
                BundleReplicaEventKind::Deleted => {
                    tx.delete_bundle_replica(event.bundle_id, event.volume_id)?;
                }
            }
        }
        tx.commit()
    }

    fn update_storage_volume_offset(
        &self,
        storage_id: u64,
        volume_id: u64,
        max_volume_offset: u64,
    ) -> Fs0Result<()> {
        let registered = {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            let registered = tx.update_volume_offset(volume_id, max_volume_offset)?;
            tx.commit()?;
            registered
        };
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

    fn active_append_lease_volume_db(&self, lease_id: u64, file_id: u64) -> Fs0Result<u64> {
        let mut db = self.db.lock();
        let tx = db.tx()?;
        let volume_id = tx.active_append_lease_volume(lease_id, file_id)?;
        tx.commit()?;
        Ok(volume_id)
    }
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }
}

fn commit_append_tx(tx: &CentralTx<'_>, request: CommitAppendRequest) -> Fs0Result<FileReadPlan> {
    let now = now_ms();
    let lease = tx.load_active_append_lease(request.lease_id, request.file_id)?;
    validate_append_base(&lease, request.base_size, request.new_size)?;

    let file = tx.get_file_by_id(lease.file_id)?;
    if file.size_bytes != request.base_size {
        return Err(Fs0Error::VersionConflict);
    }

    let first_bundle_index = lease.offset_bytes / VOLUME_BUNDLE_RAW_SIZE;
    let first_bundle_index_usize =
        usize::try_from(first_bundle_index).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("first_bundle_index {first_bundle_index} exceeds usize"),
        })?;
    let existing_file_bundles = tx.get_file_bundles_by_file_id(lease.file_id)?;
    let prefix_file_bundles = existing_file_bundles
        .get(..first_bundle_index_usize)
        .ok_or(Fs0Error::ChunkNotReady)?;
    let prefix_bundle_ids = prefix_file_bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let prefix_bundle_lengths = tx
        .get_uniq_bundles_by_ids(&prefix_bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    let mut prefix_bundles = Vec::with_capacity(prefix_file_bundles.len());
    for file_bundle in prefix_file_bundles {
        let bundle = prefix_bundle_lengths
            .get(&file_bundle.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        prefix_bundles.push(bundle.clone());
    }
    let (prefix_raw_size_bytes, prefix_compressed_size_bytes) =
        submitted_bundle_totals(&prefix_bundles)?;
    let (submitted_raw_size_bytes, _) = submitted_bundle_totals(&request.bundles)?;
    let bundles_to_insert = if submitted_raw_size_bytes == request.new_size {
        let submitted_prefix = request
            .bundles
            .get(..first_bundle_index_usize)
            .ok_or(Fs0Error::InvalidRequest)?;
        let (submitted_prefix_raw, submitted_prefix_compressed) =
            submitted_bundle_totals(submitted_prefix)?;
        if submitted_prefix_raw != prefix_raw_size_bytes
            || submitted_prefix_compressed != prefix_compressed_size_bytes
        {
            return Err(Fs0Error::InvalidRequest);
        }

        request
            .bundles
            .get(first_bundle_index_usize..)
            .ok_or(Fs0Error::InvalidRequest)?
    } else {
        let suffix_size_bytes = prefix_raw_size_bytes
            .checked_add(submitted_raw_size_bytes)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "committed bundle raw size overflow".to_owned(),
            })?;
        if suffix_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        request.bundles.as_slice()
    };

    validate_submitted_bundles_ready(tx, bundles_to_insert)?;
    let mut new_file_bundles = prefix_bundles;
    new_file_bundles.extend(bundles_to_insert.iter().cloned());
    tx.upsert_file_bundles_by_file_id(lease.file_id, &new_file_bundles)?;

    let (final_raw_size_bytes, final_compressed_size_bytes) =
        tx.calculate_size_by_file_id(lease.file_id)?;
    if final_raw_size_bytes != request.new_size {
        return Err(Fs0Error::InvalidRequest);
    }

    tx.update_file_after_append(
        lease.file_id,
        request.new_size,
        final_compressed_size_bytes,
        now,
    )?;
    tx.delete_append_lease(request.lease_id)?;
    let file_dir = tx.get_dir_path_by_id(file.dir_id)?;
    tx.insert_file_change_log(
        if file.size_bytes == 0 {
            FileChangeLogKind::Created
        } else {
            FileChangeLogKind::Updated
        },
        None,
        Some((file_dir.as_str(), file.name.as_str())),
        Some(lease.file_id),
        now,
    )?;
    get_file_read_plan_tx(tx, lease.file_id)
}

fn get_file_read_plan_tx(tx: &CentralTx<'_>, file_id: u64) -> Fs0Result<FileReadPlan> {
    let file = tx.get_file_by_id(file_id)?;
    let record = tx.file_record(&file)?;
    let file_bundles = tx.get_file_bundles_by_file_id(file_id)?;
    let bundle_ids = file_bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let ready_bundles = tx
        .get_uniq_bundles_by_ids(&bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    let mut bundles = Vec::with_capacity(file_bundles.len());
    for file_bundle in file_bundles {
        let bundle = ready_bundles
            .get(&file_bundle.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        bundles.push(FileBundleRef {
            bundle_index: file_bundle.bundle_index,
            raw_len: bundle.raw_len,
            compressed_len: bundle.compressed_len,
            bundle_id: file_bundle.bundle_id,
            replicas: Vec::new(),
        });
    }

    Ok(FileReadPlan {
        file_id: record.file_id,
        path: record.path,
        size: record.size_bytes,
        bundles,
    })
}

fn validate_submitted_bundles_ready(
    tx: &CentralTx<'_>,
    bundles: &[CommittedBundle],
) -> Fs0Result<()> {
    let bundle_ids = bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let ready_bundles = tx
        .get_uniq_bundles_by_ids(&bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    for submitted in bundles {
        let ready = ready_bundles
            .get(&submitted.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        if ready.raw_len != submitted.raw_len || ready.compressed_len != submitted.compressed_len {
            return Err(Fs0Error::InvalidRequest);
        }
    }

    Ok(())
}

fn submitted_bundle_totals(bundles: &[CommittedBundle]) -> Fs0Result<(u64, u64)> {
    let mut raw_size_bytes = 0u64;
    let mut compressed_size_bytes = 0u64;
    for bundle in bundles {
        raw_size_bytes = raw_size_bytes.checked_add(bundle.raw_len).ok_or_else(|| {
            Fs0Error::IntegerConversion {
                message: "submitted bundle raw size overflow".to_owned(),
            }
        })?;
        compressed_size_bytes = compressed_size_bytes
            .checked_add(bundle.compressed_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "submitted bundle compressed size overflow".to_owned(),
            })?;
    }

    Ok((raw_size_bytes, compressed_size_bytes))
}

fn validate_append_base(
    lease: &crate::db::LeaseRecord,
    base_size: u64,
    new_size: u64,
) -> Fs0Result<()> {
    if lease.base_size_bytes != base_size {
        return Err(Fs0Error::VersionConflict);
    }
    if new_size < lease.offset_bytes {
        return Err(Fs0Error::InvalidRequest);
    }

    Ok(())
}

async fn spawn_relay(config: &fs0_config::CentralRelayConfig) -> Fs0Result<RelayServer> {
    if config.token.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: "central.relay.token must not be empty".to_owned(),
        });
    }

    let mut relay_config = RelayServerConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)));
    relay_config.access = relay_access_config(config.token.clone());
    relay_config.tls = Some(relay_tls_config(config)?);

    let mut root_config = RelayRootConfig::default();
    root_config.quic = Some(RelayQuicConfig::new(SocketAddr::from((
        [0, 0, 0, 0],
        config.quic_bind_port,
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

fn relay_tls_config(config: &fs0_config::CentralRelayConfig) -> Fs0Result<RelayTlsConfig> {
    let certs = CertificateDer::pem_file_iter(&config.cert_path)
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to open central.relay cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?;
    if certs.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: format!(
                "central.relay cert_path {} contains no certificates",
                config.cert_path.display()
            ),
        });
    }

    let private_key =
        PrivateKeyDer::from_pem_file(&config.key_path).map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay key_path {}: {err}",
                config.key_path.display()
            ),
        })?;
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("failed to configure central.relay TLS protocols: {err}"),
    })?
    .with_no_client_auth()
    .with_single_cert(certs, private_key)
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("invalid central.relay certificate or key: {err}"),
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
                                    &mut identity,
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

fn create_volume(server: &CentralServer, name: String, max_bytes: u64) -> Fs0Result<u64> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let volume = tx.create_volume(name, max_bytes)?;
    tx.commit()?;
    Ok(volume.volume_id)
}

fn list_directory(
    server: &CentralServer,
    dir: &str,
    limit: u32,
    cursor: Option<u64>,
) -> Fs0Result<DirectoryEntries> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let entries = tx.list_directory(dir, limit, cursor)?;
    tx.commit()?;
    Ok(entries)
}

fn delete_file(server: &CentralServer, path: &str) -> Fs0Result<()> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let file = tx.get_file_by_path(path)?;
    let record = tx.file_record(&file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&record.path)?;
    tx.delete_file_bundles_by_file_id(file.file_id)?;
    tx.delete_file_by_id(file.file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Deleted,
        Some((old_dir.as_str(), old_name.as_str())),
        None,
        Some(file.file_id),
        now,
    )?;
    tx.commit()
}

fn delete_file_by_id(server: &CentralServer, file_id: u64) -> Fs0Result<()> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let file = tx.get_file_by_id(file_id)?;
    let record = tx.file_record(&file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&record.path)?;
    tx.delete_file_bundles_by_file_id(file_id)?;
    tx.delete_file_by_id(file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Deleted,
        Some((old_dir.as_str(), old_name.as_str())),
        None,
        Some(file_id),
        now,
    )?;
    tx.commit()
}

fn copy_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let source = tx.get_file_by_path(source_path)?;
    let file = copy_file_tx(&tx, source.file_id, target_path)?;
    tx.commit()?;
    Ok(file)
}

fn copy_file_by_id(
    server: &CentralServer,
    source_file_id: u64,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = copy_file_tx(&tx, source_file_id, target_path)?;
    tx.commit()?;
    Ok(file)
}

fn copy_file_tx(
    tx: &CentralTx<'_>,
    source_file_id: u64,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let now = now_ms();
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.copy_file_by_id(source_file_id, target_path, now)?;
    tx.copy_file_bundles(source_file_id, file.file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Created,
        None,
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file.file_id),
        now,
    )?;
    tx.file_record(&file)
}

fn rename_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let source = tx.get_file_by_path(source_path)?;
    let file = rename_file_tx(&tx, source.file_id, target_path)?;
    tx.commit()?;
    Ok(file)
}

fn rename_file_by_id(
    server: &CentralServer,
    file_id: u64,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = rename_file_tx(&tx, file_id, target_path)?;
    tx.commit()?;
    Ok(file)
}

fn rename_file_tx(tx: &CentralTx<'_>, file_id: u64, target_path: &str) -> Fs0Result<FileRecord> {
    let now = now_ms();
    let old_file = tx.get_file_by_id(file_id)?;
    let old_record = tx.file_record(&old_file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&old_record.path)?;
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.rename_file_by_id(file_id, target_path, now)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Moved,
        Some((old_dir.as_str(), old_name.as_str())),
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file_id),
        now,
    )?;
    tx.file_record(&file)
}

fn get_file_change_logs(
    server: &CentralServer,
    after_event_id: u64,
    limit: u32,
) -> Fs0Result<FileChangeLogs> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let logs = tx.get_file_change_logs(after_event_id, limit)?;
    tx.commit()?;
    Ok(logs)
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

            match create_volume(server, name, max_bytes) {
                Ok(volume_id) => ControlResponse::CreateVolume { volume_id },
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

            match list_directory(server, &dir, limit, cursor) {
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

            match delete_file(server, &path) {
                Ok(()) => ControlResponse::DeleteFile,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFileById { file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match delete_file_by_id(server, file_id) {
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

            match copy_file(server, &source_path, &target_path) {
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

            match copy_file_by_id(server, source_file_id, &target_path) {
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

            match rename_file(server, &source_path, &target_path) {
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

            match rename_file_by_id(server, file_id, &target_path) {
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

            match get_file_change_logs(server, after_event_id, limit) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use fs0_core::{HashId, protocol::CommittedBundle};

    fn open_test_db() -> CentralDb {
        CentralDb::open(":memory:").unwrap()
    }

    fn bundle_id(byte: u8) -> HashId {
        HashId::new([byte; 32])
    }

    fn committed_bundle(byte: u8, raw_len: u64, compressed_len: u64) -> CommittedBundle {
        CommittedBundle {
            bundle_id: bundle_id(byte),
            raw_len,
            compressed_len,
        }
    }

    fn create_volume(db: &mut CentralDb, name: &str) -> u64 {
        let tx = db.tx().unwrap();
        let volume_id = tx
            .create_volume(name.to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        tx.commit().unwrap();
        volume_id
    }

    fn begin_append(db: &mut CentralDb, volume_id: u64, path: &str, offset: u64) -> AppendLease {
        let tx = db.tx().unwrap();
        let now = now_ms();
        let expires_at_ms = now + APPEND_LEASE_TTL_MS;
        let (file, base_size) = match tx.get_file_by_path(path) {
            Ok(file) => {
                let base_size = file.size_bytes;
                (file, base_size)
            }
            Err(Fs0Error::NotFound) => {
                assert_eq!(offset, 0);
                (tx.create_file(path, now).unwrap(), 0)
            }
            Err(err) => panic!("unexpected begin append error {err:?}"),
        };
        tx.delete_expired_append_leases(now).unwrap();
        assert!(!tx.file_has_active_append_lease(file.file_id).unwrap());
        let lease = tx
            .create_append_lease(CreateAppendLease {
                file_id: file.file_id,
                volume_id,
                base_size_bytes: base_size,
                offset_bytes: offset,
                prefer_volume_name: None,
                expires_at_ms,
                created_at_ms: now,
            })
            .unwrap();
        tx.commit().unwrap();
        lease
    }

    fn commit_append(
        db: &mut CentralDb,
        lease: &AppendLease,
        new_size: u64,
        bundles: Vec<CommittedBundle>,
    ) -> Fs0Result<FileReadPlan> {
        let tx = db.tx()?;
        let plan = commit_append_tx(
            &tx,
            CommitAppendRequest {
                lease_id: lease.lease_id,
                file_id: lease.file_id,
                base_size: lease.base_size,
                new_size,
                bundles,
            },
        )?;
        tx.commit()?;
        Ok(plan)
    }

    fn record_bundle(
        db: &mut CentralDb,
        volume_id: u64,
        byte: u8,
        raw_len: u64,
        compressed_len: u64,
    ) {
        let tx = db.tx().unwrap();
        tx.insert_bundle_replica(bundle_id(byte), volume_id, raw_len, compressed_len)
            .unwrap();
        tx.commit().unwrap();
    }

    fn file_by_path(db: &mut CentralDb, path: &str) -> fs0_core::protocol::FileRecord {
        let tx = db.tx().unwrap();
        let file = tx.get_file_by_path(path).unwrap();
        let record = tx.file_record(&file).unwrap();
        tx.commit().unwrap();
        record
    }

    fn remove_bundle_replicas(db: &mut CentralDb, volume_id: u64, byte: u8) {
        let tx = db.tx().unwrap();
        tx.delete_bundle_replica(bundle_id(byte), volume_id)
            .unwrap();
        tx.commit().unwrap();
    }

    fn insert_conflicting_replica(db: &mut CentralDb, volume_id: u64, byte: u8) {
        let tx = db.tx().unwrap();
        tx.insert_bundle_replica(bundle_id(byte), volume_id, 11, 5)
            .unwrap();
        tx.commit().unwrap();
    }

    fn assert_error<T>(result: Fs0Result<T>, expected: Fs0Error) {
        match result {
            Ok(_) => panic!("expected error {expected:?}"),
            Err(err) => assert_eq!(err, expected),
        }
    }

    fn assert_plan_bundles(plan: &FileReadPlan, expected: &[(u64, u8, u64, u64)]) {
        let actual = plan
            .bundles
            .iter()
            .map(|bundle| {
                (
                    bundle.bundle_index,
                    bundle.bundle_id.as_bytes()[0],
                    bundle.raw_len,
                    bundle.compressed_len,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    fn seed_two_bundle_file(db: &mut CentralDb, volume_id: u64) -> FileReadPlan {
        record_bundle(db, volume_id, 1, VOLUME_BUNDLE_RAW_SIZE, 11);
        record_bundle(db, volume_id, 2, 40, 7);
        let lease = begin_append(db, volume_id, "/file.bin", 0);

        commit_append(
            db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 40,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(2, 40, 7),
            ],
        )
        .unwrap()
    }

    #[test]
    fn commit_append_accepts_suffix_bundles_from_first_bundle_index() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        let original = seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_append(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![committed_bundle(3, 50, 9)],
        )
        .unwrap();
        let file = file_by_path(&mut db, "/file.bin");

        assert_eq!(lease.base_size, original.size);
        assert_eq!(plan.size, VOLUME_BUNDLE_RAW_SIZE + 50);
        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_append_accepts_full_file_bundles_and_skips_existing_prefix() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_append(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(3, 50, 9),
            ],
        )
        .unwrap();
        let file = file_by_path(&mut db, "/file.bin");

        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_append_rejects_raw_total_that_does_not_match_new_size() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_append(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 11, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_append_rejects_compressed_len_that_does_not_match_replica_metadata() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_append(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 10, vec![committed_bundle(1, 10, 6)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_append_rejects_missing_replica_in_preserved_prefix() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        remove_bundle_replicas(&mut db, volume_id, 1);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        assert_error(
            commit_append(
                &mut db,
                &lease,
                VOLUME_BUNDLE_RAW_SIZE + 50,
                vec![committed_bundle(3, 50, 9)],
            ),
            Fs0Error::ChunkNotReady,
        );
    }

    #[test]
    fn commit_append_rejects_conflicting_replica_metadata() {
        let mut db = open_test_db();
        let primary_volume_id = create_volume(&mut db, "primary");
        let replica_volume_id = create_volume(&mut db, "replica");
        record_bundle(&mut db, primary_volume_id, 1, 10, 5);
        insert_conflicting_replica(&mut db, replica_volume_id, 1);
        let lease = begin_append(&mut db, primary_volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 10, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidData {
                message: "bundle replica metadata conflict".to_owned(),
            },
        );
    }
}
