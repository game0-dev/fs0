use crate::data_server::spawn_data_accept_loop;
use crate::{Fs0Result, StorageConfig};
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, ControlRequest, ControlResponse, Fs0Error,
    GrantUploadLeaseRequest, HashId, StorageVolumeInfo, TRANSPORT_DATA_ALPN,
    VOLUME_DATA_FILE_IDLE_TTL_MS, blake3_hash, decode_hex_bytes, now_ms,
};
use fs0_transport::{connect_control, control_rpc, encode_endpoint_addr, read_frame, write_frame};
use fs0_volume::{BundleMeta, ChunkMeta, Volume, VolumeMeta};
use iroh::{
    Endpoint,
    endpoint::{Connection, VarInt, presets},
};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

#[derive(Debug)]
pub struct StorageServer {
    config: Arc<StorageConfig>,
    storage_id: AtomicU64,
    next_upload_lease_id: AtomicU64,
    volumes: Arc<HashMap<u64, Arc<Volume>>>,
    upload_leases: RwLock<HashMap<u64, UploadLeaseState>>,
    central_connection: Connection,
    endpoint: Endpoint,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    data_task: Mutex<Option<JoinHandle<()>>>,
    control_task: Mutex<Option<JoinHandle<()>>>,
    reporter_task: Mutex<Option<JoinHandle<()>>>,
    file_reap_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UploadLeaseState {
    client_id: u64,
    volume_id: u64,
}

impl StorageServer {
    pub async fn run(config: StorageConfig) -> Fs0Result<Arc<Self>> {
        let volumes = Arc::new(open_volumes(&config)?);
        let endpoint = Endpoint::builder(presets::N0)
            .alpns(vec![TRANSPORT_DATA_ALPN.to_vec()])
            .bind()
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
        let data_endpoint = encode_endpoint_addr(&endpoint)?;
        let central_endpoint = decode_hex_bytes(&config.central_endpoint, "central_endpoint")?;
        let control = connect_control(&endpoint, &central_endpoint).await?;
        let storage_id = register_storage(&control, &config, &volumes, data_endpoint).await?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            storage_id: AtomicU64::new(storage_id),
            next_upload_lease_id: AtomicU64::new(1),
            volumes,
            upload_leases: RwLock::new(HashMap::new()),
            central_connection: control,
            endpoint,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            data_task: Mutex::new(None),
            control_task: Mutex::new(None),
            reporter_task: Mutex::new(None),
            file_reap_task: Mutex::new(None),
        });

        *server.control_task.lock() = Some(spawn_control_accept_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        *server.data_task.lock() = Some(spawn_data_accept_loop(
            server.endpoint.clone(),
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        *server.reporter_task.lock() = Some(spawn_bundle_reporter_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        *server.file_reap_task.lock() = Some(spawn_file_reap_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));

        Ok(server)
    }

    pub async fn run_config(path: impl AsRef<Path>) -> Fs0Result<Arc<Self>> {
        Self::run(fs0_config::Fs0Config::load_from(path)?.storage()?).await
    }

    #[must_use]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    #[must_use]
    pub fn storage_id(&self) -> u64 {
        self.storage_id.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    #[must_use]
    pub fn central_connection(&self) -> &Connection {
        &self.central_connection
    }

    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.central_connection
            .close(VarInt::from_u32(0), b"storage shutdown");
        self.endpoint.close().await;

        let data_task = self.data_task.lock().take();
        let control_task = self.control_task.lock().take();
        let reporter_task = self.reporter_task.lock().take();
        let file_reap_task = self.file_reap_task.lock().take();

        if let Some(task) = data_task {
            let _ = task.await;
        }
        if let Some(task) = control_task {
            let _ = task.await;
        }
        if let Some(task) = reporter_task {
            let _ = task.await;
        }
        if let Some(task) = file_reap_task {
            let _ = task.await;
        }
    }

    #[must_use]
    pub(crate) fn is_exiting(&self) -> bool {
        self.exit.load(Ordering::Acquire)
    }

    pub fn volumes(&self) -> Vec<VolumeMeta> {
        let mut volumes = self
            .volumes
            .values()
            .map(|volume| volume.meta())
            .collect::<Vec<_>>();
        volumes.sort_by_key(|volume| volume.volume_id);
        volumes
    }

    pub fn volume(&self, volume_id: u64) -> Fs0Result<Arc<Volume>> {
        self.volumes
            .get(&volume_id)
            .cloned()
            .ok_or(Fs0Error::UnknownVolume)
    }

    pub async fn validate_client_auth(
        &self,
        client_id: u64,
        client_token: String,
    ) -> Fs0Result<()> {
        match control_rpc(
            &self.central_connection,
            ControlRequest::ValidateClientAuth {
                client_id,
                client_token,
            },
        )
        .await?
        {
            ControlResponse::ValidateClientAuth { client_id: _ } => Ok(()),
            ControlResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected validate client auth response: {response:?}"),
            }),
        }
    }

    pub async fn put_chunk(
        &self,
        client_id: u64,
        volume_id: u64,
        chunk_id: HashId,
        compressed_hash: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Fs0Result<ChunkMeta> {
        if self.is_volume_read_only(volume_id) {
            return Err(Fs0Error::Unauthorized);
        }

        self.validate_upload_lease(client_id, volume_id)?;

        if blake3_hash(&compressed_bytes) != compressed_hash {
            return Err(Fs0Error::HashMismatch { volume_offset: 0 });
        }

        self.volume(volume_id)?
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, volume_id: u64, chunk_id: HashId) -> Fs0Result<Vec<u8>> {
        self.volume(volume_id)?.read_chunk(chunk_id).await
    }

    pub async fn chunk_meta(&self, volume_id: u64, chunk_id: HashId) -> Fs0Result<ChunkMeta> {
        self.volume(volume_id)?.chunk_meta(chunk_id).await
    }

    pub(crate) async fn has_chunk(
        &self,
        volume_id: u64,
        chunk_id: HashId,
    ) -> Fs0Result<Option<ChunkMeta>> {
        match self.chunk_meta(volume_id, chunk_id).await {
            Ok(meta) => Ok(Some(meta)),
            Err(Fs0Error::ChunkNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn commit_bundle(
        &self,
        client_id: u64,
        volume_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<BundleMeta> {
        if self.is_volume_read_only(volume_id) {
            return Err(Fs0Error::Unauthorized);
        }

        self.validate_upload_lease(client_id, volume_id)?;

        let bundle = self
            .volume(volume_id)?
            .commit_bundle(bundle_id, chunks)
            .await?;
        self.sync_pending_central_events_for_volume(volume_id).await?;

        Ok(bundle)
    }

    pub(crate) async fn list_bundle_chunks(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Fs0Result<Vec<BundleChunkRef>> {
        self.volume(volume_id)?.list_bundle_chunks(bundle_id).await
    }

    pub(crate) async fn bundle_meta(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Fs0Result<Option<(u64, u64)>> {
        let chunks = match self.list_bundle_chunks(volume_id, bundle_id).await {
            Ok(chunks) => chunks,
            Err(Fs0Error::BundleNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut raw_len = 0u64;
        let mut compressed_len = 0u64;
        for chunk in chunks {
            let meta = self.chunk_meta(volume_id, chunk.chunk_id).await?;
            raw_len =
                raw_len
                    .checked_add(meta.raw_len)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle raw_len overflow".to_owned(),
                    })?;
            compressed_len = compressed_len
                .checked_add(meta.compressed_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "bundle compressed_len overflow".to_owned(),
                })?;
        }

        Ok(Some((raw_len, compressed_len)))
    }

    fn is_volume_read_only(&self, volume_id: u64) -> bool {
        self.config
            .volumes
            .iter()
            .find(|volume| volume.volume_id == volume_id)
            .is_some_and(|volume| volume.read_only)
    }

    fn grant_upload_lease(&self, lease: GrantUploadLeaseRequest) -> Fs0Result<u64> {
        if !self.volumes.contains_key(&lease.volume_id) {
            return Err(Fs0Error::UnknownVolume);
        }

        let lease_id = self.next_upload_lease_id.fetch_add(1, Ordering::AcqRel);
        self.upload_leases.write().insert(
            lease_id,
            UploadLeaseState {
                client_id: lease.client_id,
                volume_id: lease.volume_id,
            },
        );

        Ok(lease_id)
    }

    fn revoke_upload_lease(&self, lease_id: u64) {
        self.upload_leases.write().remove(&lease_id);
    }

    fn validate_upload_lease(&self, client_id: u64, volume_id: u64) -> Fs0Result<()> {
        let leases = self.upload_leases.read();
        let allowed = leases
            .values()
            .any(|lease| lease.client_id == client_id && lease.volume_id == volume_id);

        if allowed {
            Ok(())
        } else {
            Err(Fs0Error::Unauthorized)
        }
    }

    async fn ack_pending_central_events(
        &self,
        events: &[BundleReplicaEvent],
    ) -> Fs0Result<()> {
        let mut by_volume = HashMap::<u64, u64>::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .and_modify(|max_event_id| *max_event_id = (*max_event_id).max(event.event_id))
                .or_insert(event.event_id);
        }

        for (volume_id, max_event_id) in by_volume {
            self.volume(volume_id)?
                .ack_pending_central_events(max_event_id)
                .await?;
        }

        Ok(())
    }

    async fn mark_pending_central_events_failed(
        &self,
        events: &[BundleReplicaEvent],
        failed_at_ms: u64,
    ) -> Fs0Result<()> {
        let mut by_volume = HashMap::<u64, u64>::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .and_modify(|max_event_id| *max_event_id = (*max_event_id).max(event.event_id))
                .or_insert(event.event_id);
        }

        for (volume_id, max_event_id) in by_volume {
            self.volume(volume_id)?
                .mark_pending_central_events_failed(max_event_id, failed_at_ms)
                .await?;
        }

        Ok(())
    }

    async fn sync_pending_central_events_for_volume(&self, volume_id: u64) -> Fs0Result<()> {
        loop {
            let mut events = self.volume(volume_id)?.pending_central_events(128).await?;
            if events.is_empty() {
                return Ok(());
            }

            events.sort_by_key(|event| event.event_id);
            self.report_pending_central_events(events).await?;
        }
    }

    async fn reconciliation_events(&self, limit: usize) -> Fs0Result<Vec<BundleReplicaEvent>> {
        let mut events = Vec::new();
        let mut per_volume = self.volumes.iter().collect::<Vec<_>>();
        per_volume.sort_by_key(|(volume_id, _)| **volume_id);

        for (_, volume) in per_volume {
            events.extend(volume.reconciliation_events(limit).await?);
            if events.len() >= limit {
                events.truncate(limit);
                break;
            }
        }

        Ok(events)
    }

    async fn reconcile_central_events(&self) -> Fs0Result<()> {
        let events = self.reconciliation_events(128).await?;
        if events.is_empty() {
            return Ok(());
        }

        self.report_bundle_events(events).await
    }

    async fn report_pending_central_events(
        &self,
        events: Vec<BundleReplicaEvent>,
    ) -> Fs0Result<()> {
        self.report_bundle_events(events.clone()).await?;
        self.ack_pending_central_events(&events).await
    }

    async fn report_bundle_events(&self, events: Vec<BundleReplicaEvent>) -> Fs0Result<()> {
        match control_rpc(
            &self.central_connection,
            ControlRequest::ReportBundleReplica {
                events: events.clone(),
            },
        )
        .await
        {
            Ok(ControlResponse::ReportBundleReplica) => Ok(()),
            Ok(response) => {
                self.mark_pending_central_events_failed(&events, now_ms())
                    .await?;
                Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected report bundle replica response: {response:?}"),
                })
            }
            Err(err) => {
                self.mark_pending_central_events_failed(&events, now_ms())
                    .await?;
                Err(err)
            }
        }
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.central_connection
            .close(VarInt::from_u32(0), b"storage dropped");
    }
}

fn open_volumes(config: &StorageConfig) -> Fs0Result<HashMap<u64, Arc<Volume>>> {
    let mut seen_ids = HashSet::with_capacity(config.volumes.len());
    let mut seen_names = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        if !seen_ids.insert(volume_config.volume_id) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume id {}", volume_config.volume_id),
            });
        }

        if !seen_names.insert(volume_config.name.clone()) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume name {}", volume_config.name),
            });
        }

        let read_concurrency = u32::try_from(volume_config.volume_io.read_concurrency).map_err(
            |_| Fs0Error::IntegerConversion {
                message: format!(
                    "read_concurrency {} exceeds u32",
                    volume_config.volume_io.read_concurrency
                ),
            },
        )?;
        let write_concurrency = u32::try_from(volume_config.volume_io.write_concurrency).map_err(
            |_| Fs0Error::IntegerConversion {
                message: format!(
                    "write_concurrency {} exceeds u32",
                    volume_config.volume_io.write_concurrency
                ),
            },
        )?;
        let volume = Volume::open(&volume_config.path, read_concurrency, write_concurrency)?;
        let meta = volume.meta();

        if meta.volume_id != volume_config.volume_id {
            return Err(Fs0Error::InvalidConfig {
                message: format!(
                    "configured volume id {} does not match volume metadata id {}: {}",
                    volume_config.volume_id,
                    meta.volume_id,
                    volume_config.path.display()
                ),
            });
        }

        volumes.insert(volume_config.volume_id, Arc::new(volume));
    }

    Ok(volumes)
}

async fn register_storage(
    control: &Connection,
    config: &StorageConfig,
    volumes: &HashMap<u64, Arc<Volume>>,
    data_endpoint: Vec<u8>,
) -> Fs0Result<u64> {
    let mut volume_infos = volumes
        .values()
        .map(|volume| volume.meta())
        .map(|volume| {
            let configured = config
                .volumes
                .iter()
                .find(|configured| configured.volume_id == volume.volume_id)
                .expect("opened volume must exist in config");

            StorageVolumeInfo {
                volume_id: volume.volume_id,
                name: configured.name.clone(),
                max_bytes: volume.max_bytes,
                max_volume_offset: volume.active_volume_offset,
                read_only: configured.read_only,
            }
        })
        .collect::<Vec<_>>();
    volume_infos.sort_by_key(|volume| volume.volume_id);

    match control_rpc(
        control,
        ControlRequest::RegisterStorage {
            name: config.name.clone(),
            token: config.token.clone(),
            volumes: volume_infos,
            iroh_endpoint: data_endpoint,
        },
    )
    .await?
    {
        ControlResponse::RegisterStorage { storage_id, .. } => Ok(storage_id),
        ControlResponse::Error(err) => Err(err),
        response => Err(Fs0Error::InvalidFrame {
            message: format!("unexpected storage registration response: {response:?}"),
        }),
    }
}

fn spawn_control_accept_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            tokio::select! {
                _ = shutdown_notify.notified() => break,
                stream = server.central_connection.accept_bi() => {
                    let Ok((mut send, mut recv)) = stream else {
                        break;
                    };
                    let response = match read_frame::<ControlRequest, _>(&mut recv).await {
                        Ok(request) => handle_control_request(&server, request),
                        Err(err) => ControlResponse::Error(err),
                    };

                    let _ = write_frame(&mut send, &response).await;
                    let _ = send.finish();
                }
            }
        }
    })
}

fn handle_control_request(server: &StorageServer, request: ControlRequest) -> ControlResponse {
    match request {
        ControlRequest::GrantUploadLease(lease) => match server.grant_upload_lease(lease) {
            Ok(lease_id) => ControlResponse::GrantUploadLease { lease_id },
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::RevokeUploadLease { lease_id } => {
            server.revoke_upload_lease(lease_id);
            ControlResponse::RevokeUploadLease
        }
        _ => ControlResponse::Error(Fs0Error::InvalidRequest),
    }
}

fn spawn_bundle_reporter_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            let _ = server.reconcile_central_events().await;
        }
    })
}

fn spawn_file_reap_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(VOLUME_DATA_FILE_IDLE_TTL_MS));
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            for volume in server.volumes.values() {
                volume.reap_idle_data_files();
            }
        }
    })
}
