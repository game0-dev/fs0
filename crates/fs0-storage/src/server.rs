use crate::data_server::spawn_data_accept_loop;
use crate::{Result, StorageConfig};
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, BundleReplicaReport, ControlRequest, ControlResponse,
    DATA_FILE_IDLE_TTL_MS, Fs0Error, HashId, RegisterStorageRequest, SessionMessage,
    StorageVolumeInfo, now_ms,
};
use fs0_transport::{
    bind_endpoint, connect_control, control_rpc, encode_endpoint_addr, read_frame, write_frame,
};
use fs0_volume::{BundleMeta, ChunkMeta, Volume, VolumeMeta, VolumeOptions};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream, VarInt},
};
use parking_lot::Mutex;
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
    volumes: Arc<HashMap<u64, Arc<Volume>>>,
    central_connection: Connection,
    _session: Arc<Mutex<SendStream>>,
    endpoint: Endpoint,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    data_task: Mutex<Option<JoinHandle<()>>>,
    event_task: Mutex<Option<JoinHandle<()>>>,
    file_reap_task: Mutex<Option<JoinHandle<()>>>,
}

impl StorageServer {
    pub async fn run(config: StorageConfig) -> Result<Arc<Self>> {
        let volumes = Arc::new(open_volumes(&config)?);
        let endpoint = bind_endpoint(
            &config.p2p_relay.public_url,
            config.p2p_relay.quic_port,
            vec![fs0_core::DATA_ALPN.to_vec()],
        )
        .await?;
        let data_endpoint = encode_endpoint_addr(&endpoint)?;
        let control = connect_control(&endpoint, &config.central_endpoint).await?;
        let (session_send, response) =
            register_storage(&control, &config, &volumes, data_endpoint).await?;
        let storage_id = match response {
            SessionMessage::StorageRegistered { storage_id, .. } => storage_id,
            SessionMessage::Error(err) => return Err(err),
            response => {
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected storage registration response: {response:?}"),
                });
            }
        };

        let server = Arc::new(Self {
            config: Arc::new(config),
            storage_id: AtomicU64::new(storage_id),
            volumes,
            central_connection: control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            data_task: Mutex::new(None),
            event_task: Mutex::new(None),
            file_reap_task: Mutex::new(None),
        });

        let data_task = spawn_data_accept_loop(
            server.endpoint.clone(),
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        );
        let event_task =
            spawn_central_event_sync(Arc::downgrade(&server), server.shutdown_notify.clone());
        let file_reap_task =
            spawn_file_reap_loop(Arc::downgrade(&server), server.shutdown_notify.clone());
        *server.data_task.lock() = Some(data_task);
        *server.event_task.lock() = Some(event_task);
        *server.file_reap_task.lock() = Some(file_reap_task);
        Ok(server)
    }

    pub async fn run_config(path: impl AsRef<Path>) -> Result<Arc<Self>> {
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
            .close(VarInt::from_u32(0), b"shutdown");
        self.endpoint.close().await;

        let data_task = self.data_task.lock().take();
        let event_task = self.event_task.lock().take();
        let file_reap_task = self.file_reap_task.lock().take();
        if let Some(task) = data_task {
            let _ = task.await;
        }
        if let Some(task) = event_task {
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

    pub fn volume(&self, volume_id: u64) -> Result<Arc<Volume>> {
        self.volumes
            .get(&volume_id)
            .cloned()
            .ok_or(Fs0Error::UnknownVolume)
    }

    pub async fn put_chunk(
        &self,
        volume_id: u64,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        self.volume(volume_id)?
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, volume_id: u64, chunk_id: HashId) -> Result<Vec<u8>> {
        self.volume(volume_id)?.read_chunk(chunk_id).await
    }

    pub async fn chunk_meta(&self, volume_id: u64, chunk_id: HashId) -> Result<ChunkMeta> {
        self.volume(volume_id)?.chunk_meta(chunk_id).await
    }

    pub(crate) async fn has_chunk(
        &self,
        volume_id: u64,
        chunk_id: HashId,
    ) -> Result<Option<ChunkMeta>> {
        match self.chunk_meta(volume_id, chunk_id).await {
            Ok(meta) => Ok(Some(meta)),
            Err(Fs0Error::ChunkNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn commit_bundle(
        &self,
        volume_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Result<BundleMeta> {
        let bundle = self
            .volume(volume_id)?
            .commit_bundle(bundle_id, chunks)
            .await?;
        self.sync_pending_central_events_for_volume(volume_id)
            .await?;
        Ok(bundle)
    }

    pub(crate) async fn list_bundle_chunks(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Result<Vec<BundleChunkRef>> {
        self.volume(volume_id)?.list_bundle_chunks(bundle_id).await
    }

    pub(crate) async fn bundle_meta(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Result<Option<(u64, u64)>> {
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

    async fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
        let mut per_volume = self.volumes.iter().collect::<Vec<(&u64, &Arc<Volume>)>>();
        per_volume.sort_by_key(|(volume_id, _)| **volume_id);

        let mut selected_volume_id = None;
        let mut events = Vec::new();
        for (volume_id, volume) in per_volume {
            let volume_events = volume.pending_central_events(limit).await?;
            if volume_events.is_empty() {
                continue;
            }
            selected_volume_id = Some(*volume_id);
            events = volume_events;
            break;
        }
        if let Some(volume_id) = selected_volume_id {
            events.sort_by_key(|event| event.event_id);
            for event in &events {
                debug_assert_eq!(event.volume_id, volume_id);
            }
        }
        Ok(events)
    }

    async fn ack_pending_central_events(&self, events: &[BundleReplicaEvent]) -> Result<()> {
        let mut by_volume: HashMap<u64, u64> = HashMap::new();
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
    ) -> Result<()> {
        let mut by_volume: HashMap<u64, u64> = HashMap::new();
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

    async fn sync_pending_central_events_for_volume(&self, volume_id: u64) -> Result<()> {
        loop {
            let mut events = self.volume(volume_id)?.pending_central_events(128).await?;
            if events.is_empty() {
                return Ok(());
            }
            events.sort_by_key(|event| event.event_id);
            self.report_pending_central_events(events).await?;
        }
    }

    async fn sync_next_pending_central_event_batch(&self) -> Result<()> {
        let events = self.pending_central_events(128).await?;
        if events.is_empty() {
            return Ok(());
        }
        self.report_pending_central_events(events).await
    }

    async fn report_pending_central_events(&self, events: Vec<BundleReplicaEvent>) -> Result<()> {
        let request = ControlRequest::ReportBundleReplica(BundleReplicaReport {
            events: events.clone(),
        });
        match control_rpc(&self.central_connection, request).await {
            Ok(ControlResponse::ReportBundleReplica) => {
                self.ack_pending_central_events(&events).await?;
                Ok(())
            }
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
            .close(VarInt::from_u32(0), b"shutdown");
    }
}

fn open_volumes(config: &StorageConfig) -> Result<HashMap<u64, Arc<Volume>>> {
    let mut seen = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        if !seen.insert(volume_config.volume_id) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume id {}", volume_config.volume_id),
            });
        }

        let volume = Volume::open_with_options(
            &volume_config.path,
            VolumeOptions {
                read_concurrency: config.volume_io.read_concurrency,
                write_concurrency: config.volume_io.write_concurrency,
            },
        )?;
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
) -> Result<(SendStream, SessionMessage)> {
    let mut volume_infos = volumes
        .values()
        .map(|volume| volume.meta())
        .map(|volume| StorageVolumeInfo {
            volume_id: volume.volume_id,
            name: format!("volume-{}", volume.volume_id),
            max_bytes: volume.max_bytes,
            max_volume_offset: volume.active_volume_offset,
        })
        .collect::<Vec<_>>();
    volume_infos.sort_by_key(|volume| volume.volume_id);

    let request = SessionMessage::RegisterStorage {
        request: RegisterStorageRequest {
            storage_id: config.storage_id,
            name: config.name.clone(),
            volumes: volume_infos,
            iroh_endpoint: data_endpoint,
        },
    };
    let (mut send, mut recv) = control.open_bi().await.map_err(|err| Fs0Error::Internal {
        message: err.to_string(),
    })?;
    write_frame(&mut send, &request).await?;
    let response = read_frame(&mut recv).await?;
    Ok((send, response))
}

fn spawn_central_event_sync(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(200));
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
            let _ = server.sync_next_pending_central_event_batch().await;
        }
    })
}

fn spawn_file_reap_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(DATA_FILE_IDLE_TTL_MS));
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
