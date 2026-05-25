use fs0_config::Fs0Config;
pub use fs0_config::{ClientConfig, ClientP2pRelayConfig};
use fs0_core::{
    AppendLease, BUNDLE_TARGET_RAW_BYTES, BeginAppendRequest, BundleChunkRef, CentralStatus,
    CommitAppendRequest, CommittedBundle, ControlRequest, ControlResponse,
    DEFAULT_UPLOAD_CONCURRENCY, DEFAULT_ZSTD_LEVEL, DataRequest, DataResponse, DirectoryEntries,
    FileChangeLogs, FileReadPlan, Fs0Error, HashId, RAW_CHUNK_SIZE, SessionMessage,
    StoragePeerInfo, UploadTarget, blake3_hash, bundle_hash_from_chunks, zstd_compress,
    zstd_decompress,
};
use fs0_transport::{
    bind_endpoint, connect_control, connect_data, control_rpc, data_rpc, data_rpc_on_connection,
    ping_data_peer, read_frame, write_frame,
};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
};
use parking_lot::Mutex;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type Result<T> = std::result::Result<T, Fs0Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOptions {
    pub name: Option<String>,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            name: None,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
            download_concurrency: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOptions {
    pub limit: u32,
    pub cursor: Option<u64>,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteOptions {
    pub prefer_volume_name: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadRange {
    pub offset: u64,
    pub len: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub chunks: u64,
    pub bundles: u64,
}

#[derive(Debug, Clone)]
pub struct ChunkUpload {
    pub chunk_id: HashId,
    pub raw_len: u64,
    pub compressed_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkUploadResult {
    pub chunk_id: HashId,
    pub uploaded: bool,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    options: ClientOptions,
    client_id: u64,
    control: Connection,
    _session: Arc<Mutex<SendStream>>,
    endpoint: Endpoint,
    storages: Vec<StoragePeerInfo>,
}

impl Fs0Client {
    pub async fn connect(config: ClientConfig, options: ClientOptions) -> Result<Self> {
        Self::connect_parts(
            &config.central_endpoint,
            options,
            &config.p2p_relay.public_url,
            config.p2p_relay.quic_port,
        )
        .await
    }

    pub async fn connect_from_config(
        path: impl AsRef<Path>,
        options: ClientOptions,
    ) -> Result<Self> {
        Self::connect(Fs0Config::load_from(path)?.client()?, options).await
    }

    pub async fn connect_parts(
        central_endpoint: &[u8],
        options: ClientOptions,
        relay_url: &str,
        relay_quic_port: u16,
    ) -> Result<Self> {
        let endpoint = bind_endpoint(relay_url, relay_quic_port, Vec::new()).await?;
        let control = connect_control(&endpoint, central_endpoint).await?;
        let (mut session_send, mut session_recv) = control
            .open_bi()
            .await
            .map_err(|err| internal_error(err.to_string()))?;
        write_frame(
            &mut session_send,
            &SessionMessage::RegisterClient {
                name: options.name.clone(),
            },
        )
        .await?;
        let response = read_frame(&mut session_recv).await?;
        let (client_id, storages) = match response {
            SessionMessage::ClientRegistered {
                client_id,
                storages,
            } => (client_id, storages),
            SessionMessage::Error(err) => return Err(err),
            response => {
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected session response: {response:?}"),
                });
            }
        };

        Ok(Self {
            options,
            client_id,
            control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
            storages,
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.control.close(0u32.into(), b"fs0 client shutdown");
        self.endpoint.close().await;
        Ok(())
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storages.clone()
    }

    pub async fn central_status(&self) -> Result<CentralStatus> {
        match self.request(ControlRequest::CentralStatus).await? {
            ControlResponse::CentralStatus(status) => Ok(status),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Result<u64> {
        match self
            .request(ControlRequest::CreateVolume { name, max_bytes })
            .await?
        {
            ControlResponse::CreateVolume(volume_id) => Ok(volume_id),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn list_directory(
        &self,
        dir: &str,
        options: ListOptions,
    ) -> Result<DirectoryEntries> {
        match self
            .request(ControlRequest::ListDirectory {
                dir: dir.to_owned(),
                limit: options.limit,
                cursor: options.cursor,
            })
            .await?
        {
            ControlResponse::ListDirectory(entries) => Ok(entries),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan(&self, path: &str) -> Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlan {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::GetFileReadPlan(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn delete_file(&self, path: &str) -> Result<()> {
        match self
            .request(ControlRequest::DeleteFile {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::DeleteFile => Ok(()),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::BeginAppend(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileReadPlan> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::CommitAppend(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn abort_append(&self, lease_id: u64) -> Result<()> {
        match self
            .request(ControlRequest::AbortAppend { lease_id })
            .await?
        {
            ControlResponse::AbortAppend => Ok(()),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Result<FileChangeLogs> {
        match self
            .request(ControlRequest::GetFileChangeLogs {
                after_event_id,
                limit,
            })
            .await?
        {
            ControlResponse::GetFileChangeLogs(logs) => Ok(logs),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn read_to_vec(&self, remote_path: &str) -> Result<Vec<u8>> {
        self.read_range_to_vec(remote_path, ReadRange::default())
            .await
    }

    pub async fn read_range_to_vec(&self, remote_path: &str, range: ReadRange) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.download_to_writer(remote_path, &mut bytes, range)
            .await?;
        Ok(bytes)
    }

    pub async fn download_to_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        range: ReadRange,
    ) -> Result<TransferStats> {
        let file = tokio::fs::File::create(local_path).await?;
        self.download_to_writer(remote_path, file, range).await
    }

    pub async fn download_to_writer<W>(
        &self,
        remote_path: &str,
        mut writer: W,
        range: ReadRange,
    ) -> Result<TransferStats>
    where
        W: AsyncWrite + Unpin,
    {
        let plan = self.get_file_read_plan(remote_path).await?;
        let mut remaining = range.len.unwrap_or(u64::MAX);
        let mut current_offset = 0u64;
        let mut stats = TransferStats::default();

        for bundle in &plan.bundles {
            if remaining == 0 {
                break;
            }
            let bundle_start = current_offset;
            let bundle_end = bundle_start.saturating_add(bundle.raw_len);
            current_offset = bundle_end;
            if bundle_end <= range.offset {
                continue;
            }
            let target = self.read_target(bundle.replicas.as_slice())?;
            let chunks = self.list_bundle_chunks(&target, bundle.bundle_id).await?;
            for chunk in chunks {
                if remaining == 0 {
                    break;
                }
                let (raw_len, compressed_len) = self
                    .storage_has_chunk(&target, chunk.chunk_id)
                    .await?
                    .ok_or(Fs0Error::ChunkNotFound {
                        chunk_id: chunk.chunk_id,
                    })?;
                let chunk_start = bundle_start + chunk.chunk_index * RAW_CHUNK_SIZE;
                let chunk_end = chunk_start.saturating_add(raw_len);
                if chunk_end <= range.offset {
                    continue;
                }
                let compressed = self.download_chunk(&target, chunk.chunk_id).await?;
                let raw = zstd_decompress(&compressed, raw_len as usize)?;
                let start = range.offset.saturating_sub(chunk_start) as usize;
                let available = raw.len().saturating_sub(start);
                let take = available.min(remaining as usize);
                writer.write_all(&raw[start..start + take]).await?;
                remaining -= take as u64;
                stats.raw_bytes += take as u64;
                stats.compressed_bytes += compressed_len;
                stats.chunks += 1;
            }
            stats.bundles += 1;
        }
        writer.flush().await?;
        Ok(stats)
    }

    pub async fn put_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Result<FileReadPlan> {
        let file = tokio::fs::File::open(local_path).await?;
        self.put_from_reader(remote_path, file, options).await
    }

    pub async fn append_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Result<FileReadPlan> {
        let file = tokio::fs::File::open(local_path).await?;
        self.append_from_reader(remote_path, file, options).await
    }

    pub async fn put_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.write_from_reader(remote_path, reader, options, true, 0, 0)
            .await
    }

    pub async fn append_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let plan = self.get_file_read_plan(remote_path).await?;
        let next_bundle_index = plan
            .bundles
            .last()
            .map_or(0, |bundle| bundle.bundle_index + 1);
        self.write_from_reader(
            remote_path,
            reader,
            options,
            false,
            plan.size,
            next_bundle_index,
        )
        .await
    }

    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Result<()> {
        ping_data_peer(&self.endpoint, &peer.iroh_endpoint).await
    }

    pub async fn storage_has_chunk(
        &self,
        target: &UploadTarget,
        chunk_id: HashId,
    ) -> Result<Option<(u64, u64)>> {
        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::HasChunk {
                volume_id: target.volume_id,
                chunk_id,
            },
        )
        .await?
        {
            DataResponse::HasChunk {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            } => Ok(Some((raw_len, compressed_len))),
            DataResponse::HasChunk { exists: false, .. } => Ok(None),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunk_if_missing(
        &self,
        target: &UploadTarget,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<bool> {
        if self.storage_has_chunk(target, chunk_id).await?.is_some() {
            return Ok(false);
        }

        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::UploadChunk {
                volume_id: target.volume_id,
                chunk_id,
                raw_len,
                compressed_bytes,
            },
        )
        .await?
        {
            DataResponse::UploadChunk { .. } => Ok(true),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunks_if_missing(
        &self,
        target: &UploadTarget,
        chunks: Vec<ChunkUpload>,
    ) -> Result<Vec<ChunkUploadResult>> {
        self.upload_chunks_if_missing_with_concurrency(
            target,
            chunks,
            self.options.upload_concurrency,
        )
        .await
    }

    pub async fn upload_chunks_if_missing_with_concurrency(
        &self,
        target: &UploadTarget,
        chunks: Vec<ChunkUpload>,
        concurrency: usize,
    ) -> Result<Vec<ChunkUploadResult>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = concurrency.max(1);
        let connection = Arc::new(connect_data(&self.endpoint, &target.iroh_endpoint).await?);
        let mut chunk_iter = chunks.into_iter().enumerate();
        let mut upload_tasks = tokio::task::JoinSet::new();
        let mut results = Vec::new();

        loop {
            while upload_tasks.len() < concurrency {
                let Some((index, chunk)) = chunk_iter.next() else {
                    break;
                };
                let connection = connection.clone();
                let volume_id = target.volume_id;
                upload_tasks.spawn(async move {
                    upload_chunk_if_missing_on_connection(index, connection, volume_id, chunk).await
                });
            }

            if upload_tasks.is_empty() {
                break;
            }

            match upload_tasks.join_next().await {
                Some(Ok(Ok(result))) => results.push(result),
                Some(Ok(Err(err))) => {
                    upload_tasks.abort_all();
                    connection.close(0u32.into(), b"fs0 upload failed");
                    return Err(err);
                }
                Some(Err(err)) => {
                    upload_tasks.abort_all();
                    connection.close(0u32.into(), b"fs0 upload task failed");
                    return Err(internal_error(err.to_string()));
                }
                None => break,
            }
        }

        connection.close(0u32.into(), b"fs0 upload complete");
        results.sort_by_key(|(index, _)| *index);
        Ok(results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>())
    }

    pub async fn ping_first_storage_peer(&self) -> Result<StoragePeerInfo> {
        let mut peers = self.storage_peers();
        if peers.is_empty() {
            return Err(Fs0Error::NotFound);
        }
        let peer = peers.remove(0);
        self.ping_storage_peer(&peer).await?;
        Ok(peer)
    }

    async fn write_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        create: bool,
        expected_size: u64,
        first_bundle_index: u64,
    ) -> Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let lease = self
            .begin_append(BeginAppendRequest {
                path: remote_path.to_owned(),
                expected_size,
                create,
                prefer_volume_name: options.prefer_volume_name,
                idempotency_key: options.idempotency_key,
            })
            .await?;
        match self
            .write_lease_from_reader(lease.clone(), reader, first_bundle_index)
            .await
        {
            Ok((new_size, bundles)) => {
                self.commit_append(CommitAppendRequest {
                    lease_id: lease.lease_id,
                    base_size: lease.base_size,
                    new_size,
                    bundles,
                })
                .await
            }
            Err(err) => {
                let _ = self.abort_append(lease.lease_id).await;
                Err(err)
            }
        }
    }

    async fn write_lease_from_reader<R>(
        &self,
        lease: AppendLease,
        mut reader: R,
        first_bundle_index: u64,
    ) -> Result<(u64, Vec<CommittedBundle>)>
    where
        R: AsyncRead + Unpin,
    {
        let target = self.upload_target(lease.volume_id)?;
        let mut buffer = vec![0u8; RAW_CHUNK_SIZE as usize];
        let mut bundle_index = first_bundle_index;
        let mut next_size = lease.base_size;
        let mut current_bundle_raw = 0u64;
        let mut current_chunks = Vec::new();
        let mut current_uploads = Vec::new();
        let mut committed = Vec::new();

        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            let raw = &buffer[..read];
            let compressed = zstd_compress(raw, DEFAULT_ZSTD_LEVEL)?;
            let chunk_id = blake3_hash(raw);
            let chunk_index = current_chunks.len() as u64;
            current_chunks.push(BundleChunkRef {
                chunk_index,
                chunk_id,
            });
            current_bundle_raw += read as u64;
            next_size += read as u64;
            current_uploads.push(ChunkUpload {
                chunk_id,
                raw_len: read as u64,
                compressed_bytes: compressed,
            });

            if current_bundle_raw >= BUNDLE_TARGET_RAW_BYTES {
                let bundle_id = bundle_hash_from_chunks(&current_chunks);
                self.upload_chunks_if_missing(&target, std::mem::take(&mut current_uploads))
                    .await?;
                let bundle = self
                    .commit_bundle(&target, bundle_id, std::mem::take(&mut current_chunks))
                    .await?;
                committed.push(CommittedBundle {
                    bundle_index,
                    bundle_id,
                    raw_len: bundle.raw_len,
                    compressed_len: bundle.compressed_len,
                });
                bundle_index += 1;
                current_bundle_raw = 0;
            }
        }

        if !current_chunks.is_empty() {
            let bundle_id = bundle_hash_from_chunks(&current_chunks);
            self.upload_chunks_if_missing(&target, current_uploads)
                .await?;
            let bundle = self
                .commit_bundle(&target, bundle_id, current_chunks)
                .await?;
            committed.push(CommittedBundle {
                bundle_index,
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            });
        }

        Ok((next_size, committed))
    }

    async fn commit_bundle(
        &self,
        target: &UploadTarget,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Result<CommittedBundle> {
        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::CommitBundle {
                volume_id: target.volume_id,
                bundle_id,
                chunks,
            },
        )
        .await?
        {
            DataResponse::CommitBundle {
                raw_len,
                compressed_len,
                ..
            } => Ok(CommittedBundle {
                bundle_index: 0,
                bundle_id,
                raw_len,
                compressed_len,
            }),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    async fn list_bundle_chunks(
        &self,
        target: &UploadTarget,
        bundle_id: HashId,
    ) -> Result<Vec<BundleChunkRef>> {
        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::ListBundleChunks {
                volume_id: target.volume_id,
                bundle_id,
            },
        )
        .await?
        {
            DataResponse::ListBundleChunks { chunks } => Ok(chunks),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    async fn download_chunk(&self, target: &UploadTarget, chunk_id: HashId) -> Result<Vec<u8>> {
        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::DownloadChunk {
                volume_id: target.volume_id,
                chunk_id,
            },
        )
        .await?
        {
            DataResponse::DownloadChunk { compressed_bytes } => Ok(compressed_bytes),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    fn upload_target(&self, volume_id: u64) -> Result<UploadTarget> {
        self.storages
            .iter()
            .find(|peer| {
                peer.volumes
                    .iter()
                    .any(|volume| volume.volume_id == volume_id)
            })
            .map(|peer| UploadTarget {
                storage_id: peer.storage_id,
                volume_id,
                iroh_endpoint: peer.iroh_endpoint.clone(),
            })
            .ok_or(Fs0Error::NotFound)
    }

    fn read_target(&self, replicas: &[fs0_core::ReplicaLocation]) -> Result<UploadTarget> {
        for replica in replicas {
            if let Some(peer) = self
                .storages
                .iter()
                .find(|peer| peer.storage_id == replica.storage_id)
            {
                return Ok(UploadTarget {
                    storage_id: peer.storage_id,
                    volume_id: replica.volume_id,
                    iroh_endpoint: peer.iroh_endpoint.clone(),
                });
            }
        }
        Err(Fs0Error::NotFound)
    }

    async fn request(&self, request: ControlRequest) -> Result<ControlResponse> {
        control_rpc(&self.control, request).await
    }
}

async fn upload_chunk_if_missing_on_connection(
    index: usize,
    connection: Arc<Connection>,
    volume_id: u64,
    chunk: ChunkUpload,
) -> Result<(usize, ChunkUploadResult)> {
    match data_rpc_on_connection(
        &connection,
        DataRequest::HasChunk {
            volume_id,
            chunk_id: chunk.chunk_id,
        },
    )
    .await?
    {
        DataResponse::HasChunk { exists: true, .. } => {
            return Ok((
                index,
                ChunkUploadResult {
                    chunk_id: chunk.chunk_id,
                    uploaded: false,
                },
            ));
        }
        DataResponse::HasChunk { exists: false, .. } => {}
        DataResponse::Error(err) => return Err(err),
        response => return unexpected_data_response(response),
    }

    match data_rpc_on_connection(
        &connection,
        DataRequest::UploadChunk {
            volume_id,
            chunk_id: chunk.chunk_id,
            raw_len: chunk.raw_len,
            compressed_bytes: chunk.compressed_bytes,
        },
    )
    .await?
    {
        DataResponse::UploadChunk { .. } => Ok((
            index,
            ChunkUploadResult {
                chunk_id: chunk.chunk_id,
                uploaded: true,
            },
        )),
        DataResponse::Error(err) => Err(err),
        response => unexpected_data_response(response),
    }
}

fn unexpected_control_response<T>(response: ControlResponse) -> Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected control response: {response:?}"),
    })
}

fn unexpected_data_response<T>(response: DataResponse) -> Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected data response: {response:?}"),
    })
}

fn internal_error(message: String) -> Fs0Error {
    Fs0Error::Internal { message }
}
