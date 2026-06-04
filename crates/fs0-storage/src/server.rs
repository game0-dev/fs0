use crate::{
    Fs0Result, StorageConfig,
    client_server::spawn_client_connection_loop,
    tasks::{ServerTasks, spawn_bundle_reporter_loop, spawn_idle_file_close_loop},
};
use fs0_core::{
    Fs0Error, HashId, TRANSPORT_CONTROL_ALPN, TRANSPORT_DATA_ALPN, blake3_hash,
    protocol::{
        BundleChunkRef, BundleReplicaEvent, ControlRequest, ControlResponse,
        GrantUploadLeaseRequest, ProtocolRequest, ProtocolResponse, StoragePeerInfo,
        StorageVolumeInfo,
    },
    utils::now_ms,
    zstd_decompress,
};
use fs0_transport::{Connection, EndpointAddr, EndpointId, Transport};
use fs0_volume::{BundleMeta, ChunkMeta, Volume, VolumeMeta};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};

#[derive(Debug)]
pub struct StorageServer {
    config: Arc<StorageConfig>,
    storage_id: u64,
    volumes: Arc<HashMap<u64, Arc<Volume>>>,
    read_only_volume_ids: Arc<HashSet<u64>>,
    upload_leases: RwLock<HashMap<u64, UploadLeaseState>>,
    central_connection: Connection,
    endpoint: Transport,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    tasks: ServerTasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UploadLeaseState {
    file_id: u64,
    volume_id: u64,
    expires_at_ms: u64,
}

#[derive(Debug)]
struct OpenedVolumes {
    volumes: HashMap<u64, Arc<Volume>>,
    read_only_volume_ids: HashSet<u64>,
    infos: Vec<StorageVolumeInfo>,
}

impl StorageServer {
    pub async fn run(config: StorageConfig) -> Fs0Result<Arc<Self>> {
        let opened_volumes = open_volumes(&config)?;
        let volume_infos = opened_volumes.infos;
        let volumes = Arc::new(opened_volumes.volumes);
        let read_only_volume_ids = Arc::new(opened_volumes.read_only_volume_ids);
        let bind_addr = config
            .bind_port
            .map(|port| SocketAddr::from(([0, 0, 0, 0], port)));
        let endpoint = Transport::bind(
            vec![TRANSPORT_DATA_ALPN],
            None,
            bind_addr,
            config.relay.clone(),
        )
        .await?;
        let data_endpoint = postcard::to_allocvec(&endpoint.addr())?;
        let central_endpoint = central_endpoint_addr(&config)?;
        let central_connection = endpoint
            .connect(central_endpoint, TRANSPORT_CONTROL_ALPN)
            .await?;
        let (storage_id, _) =
            register_storage(&central_connection, &config, volume_infos, data_endpoint).await?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            storage_id,
            volumes,
            read_only_volume_ids,
            upload_leases: RwLock::new(HashMap::new()),
            central_connection,
            endpoint,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            tasks: ServerTasks::new(),
        });

        server.tasks.push(spawn_control_accept_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        server.tasks.push(spawn_connection_accept_loop(
            server.endpoint.clone(),
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        server.tasks.push(spawn_bundle_reporter_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        server.tasks.push(spawn_idle_file_close_loop(
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
        self.storage_id
    }

    #[must_use]
    pub fn endpoint(&self) -> &Transport {
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
        self.central_connection.close(b"storage shutdown");
        self.endpoint.close().await;

        self.tasks.join_all().await;
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

    pub(crate) fn close_idle_data_files(&self) {
        for volume in self.volumes.values() {
            volume.close_idle_data_files();
        }
    }

    pub async fn validate_client_auth(
        &self,
        client_id: u64,
        client_token: String,
    ) -> Fs0Result<()> {
        match self
            .central_connection
            .rpc(ProtocolRequest::Control(
                ControlRequest::ValidateClientAuth {
                    client_id,
                    client_token,
                },
            ))
            .await?
        {
            ProtocolResponse::Control(ControlResponse::ValidateClientAuth { client_id: _ }) => {
                Ok(())
            }
            ProtocolResponse::Control(ControlResponse::Error(err))
            | ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected validate client auth response: {response:?}"),
            }),
        }
    }

    pub async fn put_chunk(
        &self,
        lease_id: u64,
        file_id: u64,
        volume_id: u64,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Fs0Result<ChunkMeta> {
        if self.is_volume_read_only(volume_id) {
            return Err(Fs0Error::Unauthorized);
        }

        self.validate_upload_lease(lease_id, file_id, volume_id)?;

        self.check_raw_hash_before_write(chunk_id, raw_len, &compressed_bytes)?;

        self.volume(volume_id)?
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, volume_id: u64, chunk_id: HashId) -> Fs0Result<Vec<u8>> {
        let (_chunk, bytes) = self.volume(volume_id)?.read_chunk(chunk_id).await?;
        Ok(bytes)
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
        lease_id: u64,
        file_id: u64,
        volume_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<BundleMeta> {
        if self.is_volume_read_only(volume_id) {
            return Err(Fs0Error::Unauthorized);
        }

        self.validate_upload_lease(lease_id, file_id, volume_id)?;

        let bundle = self
            .volume(volume_id)?
            .commit_bundle(bundle_id, chunks)
            .await?;
        self.sync_bundle_change_records_for_volume(volume_id)
            .await?;
        self.report_volume_offset(volume_id).await?;

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
        self.read_only_volume_ids.contains(&volume_id)
    }

    fn check_raw_hash_before_write(
        &self,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: &[u8],
    ) -> Fs0Result<()> {
        if !self.config.check_hash_before_write {
            return Ok(());
        }

        let raw_len_usize = usize::try_from(raw_len).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("raw_len {raw_len} exceeds usize"),
        })?;
        let raw = zstd_decompress(compressed_bytes, raw_len_usize)?;
        if raw.len() as u64 != raw_len {
            return Err(Fs0Error::InvalidData {
                message: format!(
                    "decompressed chunk length {} does not match raw_len {raw_len}",
                    raw.len()
                ),
            });
        }
        if blake3_hash(&raw) != chunk_id {
            return Err(Fs0Error::HashMismatch { volume_offset: 0 });
        }

        Ok(())
    }

    fn grant_upload_lease(&self, lease: GrantUploadLeaseRequest) -> Fs0Result<u64> {
        if !self.volumes.contains_key(&lease.volume_id) {
            return Err(Fs0Error::UnknownVolume);
        }

        self.upload_leases.write().insert(
            lease.lease_id,
            UploadLeaseState {
                file_id: lease.file_id,
                volume_id: lease.volume_id,
                expires_at_ms: lease.expires_at_ms,
            },
        );

        Ok(lease.lease_id)
    }

    fn revoke_upload_lease(&self, lease_id: u64) {
        self.upload_leases.write().remove(&lease_id);
    }

    fn validate_upload_lease(&self, lease_id: u64, file_id: u64, volume_id: u64) -> Fs0Result<()> {
        let now = now_ms();
        self.upload_leases
            .write()
            .retain(|_, lease| lease.expires_at_ms > now);

        let allowed = self
            .upload_leases
            .read()
            .get(&lease_id)
            .is_some_and(|lease| lease.file_id == file_id && lease.volume_id == volume_id);

        if allowed {
            Ok(())
        } else {
            Err(Fs0Error::Unauthorized)
        }
    }

    async fn remove_bundle_change_records(&self, events: &[BundleReplicaEvent]) -> Fs0Result<()> {
        let mut by_volume = HashMap::<u64, u64>::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .and_modify(|max_event_id| *max_event_id = (*max_event_id).max(event.event_id))
                .or_insert(event.event_id);
        }

        for (volume_id, max_event_id) in by_volume {
            self.volume(volume_id)?
                .remove_bundle_change_records(max_event_id)
                .await?;
        }

        Ok(())
    }

    async fn sync_bundle_change_records_for_volume(&self, volume_id: u64) -> Fs0Result<()> {
        loop {
            let mut events = self
                .volume(volume_id)?
                .get_bundle_change_records(128)
                .await?;
            if events.is_empty() {
                return Ok(());
            }

            events.sort_by_key(|event| event.event_id);
            self.report_bundle_change_records(events).await?;
        }
    }

    pub(crate) async fn sync_bundle_change_records(&self) -> Fs0Result<()> {
        let mut per_volume = self.volumes.iter().collect::<Vec<_>>();
        per_volume.sort_by_key(|(volume_id, _)| **volume_id);

        for (volume_id, _) in per_volume {
            self.sync_bundle_change_records_for_volume(*volume_id)
                .await?;
        }

        Ok(())
    }

    async fn report_bundle_change_records(&self, events: Vec<BundleReplicaEvent>) -> Fs0Result<()> {
        self.report_bundle_events(events.clone()).await?;
        self.remove_bundle_change_records(&events).await
    }

    async fn report_bundle_events(&self, events: Vec<BundleReplicaEvent>) -> Fs0Result<()> {
        match self
            .central_connection
            .rpc(ProtocolRequest::Control(
                ControlRequest::ReportBundleReplica {
                    events: events.clone(),
                },
            ))
            .await
        {
            Ok(ProtocolResponse::Control(ControlResponse::ReportBundleReplica)) => Ok(()),
            Ok(ProtocolResponse::Control(ControlResponse::Error(err)))
            | Ok(ProtocolResponse::Error(err)) => Err(err),
            Ok(response) => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected report bundle replica response: {response:?}"),
            }),
            Err(err) => Err(err),
        }
    }

    async fn report_volume_offset(&self, volume_id: u64) -> Fs0Result<()> {
        let max_volume_offset = self.volume(volume_id)?.meta().active_volume_offset;
        match self
            .central_connection
            .rpc(ProtocolRequest::Control(
                ControlRequest::UpdateStorageVolumeOffset {
                    volume_id,
                    max_volume_offset,
                },
            ))
            .await
        {
            Ok(ProtocolResponse::Control(ControlResponse::UpdateStorageVolumeOffset)) => Ok(()),
            Ok(ProtocolResponse::Control(ControlResponse::Error(err)))
            | Ok(ProtocolResponse::Error(err)) => Err(err),
            Ok(response) => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected update storage volume offset response: {response:?}"),
            }),
            Err(err) => Err(err),
        }
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.central_connection.close(b"storage dropped");
    }
}

fn open_volumes(config: &StorageConfig) -> Fs0Result<OpenedVolumes> {
    let mut seen_ids = HashSet::with_capacity(config.volumes.len());
    let mut seen_names = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());
    let mut read_only_volume_ids = HashSet::new();
    let mut infos = Vec::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        if !seen_names.insert(volume_config.name.clone()) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume name {}", volume_config.name),
            });
        }

        let read_concurrency = u32::try_from(volume_config.read_concurrency).map_err(|_| {
            Fs0Error::IntegerConversion {
                message: format!(
                    "read_concurrency {} exceeds u32",
                    volume_config.read_concurrency
                ),
            }
        })?;
        let write_concurrency = u32::try_from(volume_config.write_concurrency).map_err(|_| {
            Fs0Error::IntegerConversion {
                message: format!(
                    "write_concurrency {} exceeds u32",
                    volume_config.write_concurrency
                ),
            }
        })?;
        let volume = Volume::open(&volume_config.path, read_concurrency, write_concurrency)?;
        let meta = volume.meta();
        if meta.volume_id == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: format!(
                    "volume {} has not been assigned a central volume id",
                    volume_config.path.display()
                ),
            });
        }
        if !seen_ids.insert(meta.volume_id) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume id {}", meta.volume_id),
            });
        }

        infos.push(StorageVolumeInfo {
            volume_id: meta.volume_id,
            name: volume_config.name.clone(),
            max_bytes: meta.max_bytes,
            max_volume_offset: meta.active_volume_offset,
            read_only: volume_config.read_only,
        });
        if volume_config.read_only {
            read_only_volume_ids.insert(meta.volume_id);
        }
        volumes.insert(meta.volume_id, Arc::new(volume));
    }
    infos.sort_by_key(|volume| volume.volume_id);

    Ok(OpenedVolumes {
        volumes,
        read_only_volume_ids,
        infos,
    })
}

fn central_endpoint_addr(config: &StorageConfig) -> Fs0Result<EndpointAddr> {
    let endpoint_id =
        parse_endpoint_id(&config.central_endpoint_id, "storage.central_endpoint_id")?;
    let socket_addr = parse_socket_addr(&config.central_addr, "storage.central_addr")?;

    Ok(EndpointAddr::new(endpoint_id).with_ip_addr(socket_addr))
}

fn parse_endpoint_id(value: &str, field: &str) -> Fs0Result<EndpointId> {
    value
        .parse::<EndpointId>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field}: {err}"),
        })
}

fn parse_socket_addr(value: &str, field: &str) -> Fs0Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field} {value}: {err}"),
        })
}

async fn register_storage(
    control: &Connection,
    config: &StorageConfig,
    volumes: Vec<StorageVolumeInfo>,
    data_endpoint: Vec<u8>,
) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
    match control
        .rpc(ProtocolRequest::Control(ControlRequest::RegisterStorage {
            name: config.name.clone(),
            token: config.token.clone(),
            volumes,
            iroh_endpoint: data_endpoint,
        }))
        .await?
    {
        ProtocolResponse::Control(ControlResponse::RegisterStorage {
            storage_id,
            storages,
        }) => Ok((storage_id, storages)),
        ProtocolResponse::Control(ControlResponse::Error(err)) | ProtocolResponse::Error(err) => {
            Err(err)
        }
        response => Err(Fs0Error::InvalidFrame {
            message: format!("unexpected storage registration response: {response:?}"),
        }),
    }
}

fn spawn_connection_accept_loop(
    endpoint: Transport,
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                accepted = endpoint.accept() => {
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let accepted = match accepted {
                        Ok(Some(accepted)) => accepted,
                        Ok(None) => break,
                        Err(_) => continue,
                    };
                    let alpn = accepted.alpn().to_vec();
                    let connection = accepted;

                    match alpn.as_slice() {
                        TRANSPORT_DATA_ALPN => {
                            server.tasks.push(spawn_client_connection_loop(
                                server.clone(),
                                connection,
                                shutdown_notify.clone(),
                            ));
                        }
                        _ => connection.close(b"unsupported storage alpn"),
                    }
                }
            }
        }
    })
}

fn spawn_control_accept_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(server) = server.upgrade() else {
            return;
        };
        if server.is_exiting() {
            return;
        }

        tokio::select! {
            _ = shutdown_notify.notified() => {}
            _ = server.central_connection.serve({
                let server = server.clone();
                move |request| {
                    let server = server.clone();
                    async move {
                        let response = match request {
                            ProtocolRequest::Control(request) => {
                                ProtocolResponse::Control(handle_control_request(&server, request))
                            }
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            }) => {}
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
