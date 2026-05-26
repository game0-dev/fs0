pub use fs0_config::ClientConfig;
use fs0_config::Fs0Config;
use fs0_core::{
    AppendLease, BeginAppendRequest, BundleChunkRef, CommitAppendRequest, CommittedBundle,
    ControlRequest, ControlResponse, DEFAULT_CLIENT_DATA_CONCURRENCY, DEFAULT_ZSTD_LEVEL,
    DataRequest, DataResponse, DirectoryEntries, FileChangeLogs, FileReadPlan, Fs0Error, Fs0Result,
    HashId, StoragePeerInfo, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE, blake3_hash,
    bundle_hash_from_chunks, decode_hex_bytes, zstd_compress, zstd_decompress,
};
use fs0_transport::{
    connect_control, connect_data, control_rpc, data_rpc_on_connection, ping_data_peer,
};
use iroh::{
    Endpoint,
    endpoint::{Connection, presets},
};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
            upload_concurrency: DEFAULT_CLIENT_DATA_CONCURRENCY,
            download_concurrency: DEFAULT_CLIENT_DATA_CONCURRENCY,
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
    pub offset: Option<u64>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralStatus {
    pub clients_count: u32,
    pub storages: Vec<StoragePeerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTarget {
    pub storage_id: u64,
    pub volume_id: u64,
    pub iroh_endpoint: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ChunkUpload {
    pub chunk_id: HashId,
    pub compressed_hash: HashId,
    pub raw_len: u64,
    pub compressed_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkUploadResult {
    pub chunk_id: HashId,
    pub uploaded: bool,
}

#[derive(Debug)]
struct VerifiedChunk {
    chunk_index: u64,
    raw_len: u64,
    compressed_len: u64,
    raw: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    options: ClientOptions,
    token: String,
    client_id: u64,
    control: Connection,
    endpoint: Endpoint,
    storages: Arc<RwLock<Vec<StoragePeerInfo>>>,
}

impl Fs0Client {
    pub async fn connect(config: ClientConfig, options: ClientOptions) -> Fs0Result<Self> {
        let endpoint =
            Endpoint::builder(presets::N0)
                .bind()
                .await
                .map_err(|err| Fs0Error::Internal {
                    message: err.to_string(),
                })?;
        let central_endpoint = decode_hex_bytes(&config.central_endpoint, "central_endpoint")?;
        let control = connect_control(&endpoint, &central_endpoint).await?;
        let token = config.token;

        let response = control_rpc(
            &control,
            ControlRequest::RegisterClient {
                name: options.name.clone(),
                token: token.clone(),
            },
        )
        .await?;
        let (client_id, storages) = match response {
            ControlResponse::RegisterClient {
                client_id,
                storages,
            } => (client_id, storages),
            ControlResponse::Error(err) => return Err(err),
            response => return unexpected_control_response(response),
        };

        Ok(Self {
            options,
            token,
            client_id,
            control,
            endpoint,
            storages: Arc::new(RwLock::new(storages)),
        })
    }

    pub async fn connect_from_config(
        path: impl AsRef<Path>,
        options: ClientOptions,
    ) -> Fs0Result<Self> {
        Self::connect(Fs0Config::load_from(path)?.client()?, options).await
    }

    pub async fn shutdown(&self) -> Fs0Result<()> {
        self.control.close(0u32.into(), b"fs0 client shutdown");
        self.endpoint.close().await;

        Ok(())
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storages.read().clone()
    }

    pub async fn central_status(&self) -> Fs0Result<CentralStatus> {
        match self.request(ControlRequest::CentralStatus).await? {
            ControlResponse::CentralStatus {
                clients_count,
                storages,
            } => Ok(CentralStatus {
                clients_count,
                storages,
            }),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Fs0Result<u64> {
        match self
            .request(ControlRequest::CreateVolume { name, max_bytes })
            .await?
        {
            ControlResponse::CreateVolume { volume_id } => Ok(volume_id),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn list_directory(
        &self,
        dir: &str,
        options: ListOptions,
    ) -> Fs0Result<DirectoryEntries> {
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

    pub async fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
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

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn delete_file(&self, path: &str) -> Fs0Result<()> {
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

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Fs0Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::BeginAppend(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Fs0Result<FileReadPlan> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::CommitAppend(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn abort_append(&self, lease_id: u64) -> Fs0Result<()> {
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
    ) -> Fs0Result<FileChangeLogs> {
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

    pub async fn read_to_vec(&self, remote_path: &str) -> Fs0Result<Vec<u8>> {
        self.read_range_to_vec(remote_path, ReadRange::default())
            .await
    }

    pub async fn read_range_to_vec(
        &self,
        remote_path: &str,
        range: ReadRange,
    ) -> Fs0Result<Vec<u8>> {
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
    ) -> Fs0Result<TransferStats> {
        let file = tokio::fs::File::create(local_path).await?;
        self.download_to_writer(remote_path, file, range).await
    }

    pub async fn download_to_writer<W>(
        &self,
        remote_path: &str,
        mut writer: W,
        range: ReadRange,
    ) -> Fs0Result<TransferStats>
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

            let chunks = self.download_bundle_from_replicas(bundle).await?;
            for chunk in chunks {
                if remaining == 0 {
                    break;
                }

                let chunk_start = bundle_start + chunk.chunk_index * VOLUME_RAW_CHUNK_SIZE;
                let chunk_end = chunk_start.saturating_add(chunk.raw_len);
                if chunk_end <= range.offset {
                    continue;
                }

                let start = range.offset.saturating_sub(chunk_start) as usize;
                let available = chunk.raw.len().saturating_sub(start);
                let take = available.min(remaining as usize);
                writer.write_all(&chunk.raw[start..start + take]).await?;

                remaining -= take as u64;
                stats.raw_bytes += take as u64;
                stats.compressed_bytes += chunk.compressed_len;
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
    ) -> Fs0Result<FileReadPlan> {
        let local_path = local_path.as_ref();
        let append_size_hint = Some(tokio::fs::metadata(local_path).await?.len());
        let file = tokio::fs::File::open(local_path).await?;

        self.put_from_reader_with_size_hint(remote_path, file, options, append_size_hint)
            .await
    }

    pub async fn append_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan> {
        let local_path = local_path.as_ref();
        let append_size_hint = Some(tokio::fs::metadata(local_path).await?.len());
        let file = tokio::fs::File::open(local_path).await?;

        self.append_from_reader_with_size_hint(remote_path, file, options, append_size_hint)
            .await
    }

    pub async fn put_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.put_from_reader_with_size_hint(remote_path, reader, options, None)
            .await
    }

    pub async fn put_from_reader_with_size_hint<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        append_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.write_from_reader(remote_path, reader, options, true, 0, append_size_hint)
            .await
    }

    pub async fn append_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.append_from_reader_with_size_hint(remote_path, reader, options, None)
            .await
    }

    pub async fn append_from_reader_with_size_hint<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        append_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let offset = match options.offset {
            Some(offset) => offset,
            None => self.get_file_read_plan(remote_path).await?.size,
        };

        self.write_from_reader(
            remote_path,
            reader,
            options,
            false,
            offset,
            append_size_hint,
        )
        .await
    }

    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Fs0Result<()> {
        ping_data_peer(&self.endpoint, &peer.iroh_endpoint).await
    }

    pub async fn ping_first_storage_peer(&self) -> Fs0Result<StoragePeerInfo> {
        let mut peers = self.storage_peers();
        if peers.is_empty() {
            return Err(Fs0Error::NotFound);
        }

        let peer = peers.remove(0);
        self.ping_storage_peer(&peer).await?;

        Ok(peer)
    }

    pub async fn storage_has_chunk(
        &self,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<Option<(u64, u64)>> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::HasChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                },
            )
            .await?;

        match response {
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
        target: &StorageTarget,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Fs0Result<bool> {
        if self.storage_has_chunk(target, chunk_id).await?.is_some() {
            return Ok(false);
        }

        let compressed_hash = blake3_hash(&compressed_bytes);
        let response = self
            .storage_rpc(
                target,
                DataRequest::UploadChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                    compressed_hash,
                    raw_len,
                    compressed_bytes,
                },
            )
            .await?;

        match response {
            DataResponse::UploadChunk { .. } => Ok(true),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunks_if_missing(
        &self,
        target: &StorageTarget,
        chunks: Vec<ChunkUpload>,
    ) -> Fs0Result<Vec<ChunkUploadResult>> {
        self.upload_chunks_if_missing_with_concurrency(
            target,
            chunks,
            self.options.upload_concurrency,
        )
        .await
    }

    pub async fn upload_chunks_if_missing_with_concurrency(
        &self,
        target: &StorageTarget,
        chunks: Vec<ChunkUpload>,
        concurrency: usize,
    ) -> Fs0Result<Vec<ChunkUploadResult>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = concurrency.max(1);
        let connection = Arc::new(
            self.connect_authenticated_data(&target.iroh_endpoint)
                .await?,
        );
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
                    return Err(Fs0Error::Internal {
                        message: err.to_string(),
                    });
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

    async fn write_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        create: bool,
        offset: u64,
        append_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let lease = self
            .begin_append(BeginAppendRequest {
                path: remote_path.to_owned(),
                offset,
                create,
                prefer_volume_name: options.prefer_volume_name,
                append_size_hint,
            })
            .await?;
        let prefix = if lease.offset > lease.rewrite_offset {
            self.read_range_to_vec(
                remote_path,
                ReadRange {
                    offset: lease.rewrite_offset,
                    len: Some(lease.offset - lease.rewrite_offset),
                },
            )
            .await?
        } else {
            Vec::new()
        };
        let mut rewritten_reader = std::io::Cursor::new(prefix).chain(reader);

        match self
            .write_lease_from_reader(lease.clone(), &mut rewritten_reader)
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
    ) -> Fs0Result<(u64, Vec<CommittedBundle>)>
    where
        R: AsyncRead + Unpin,
    {
        let target = self.upload_target(lease.volume_id)?;
        let mut buffer = vec![0u8; VOLUME_RAW_CHUNK_SIZE as usize];
        let mut bundle_index = lease.first_bundle_index;
        let mut next_size = lease.rewrite_offset;
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
            let compressed_hash = blake3_hash(&compressed);
            let chunk_index = current_chunks.len() as u64;

            current_chunks.push(BundleChunkRef {
                chunk_index,
                chunk_id,
            });
            current_bundle_raw += read as u64;
            next_size += read as u64;
            current_uploads.push(ChunkUpload {
                chunk_id,
                compressed_hash,
                raw_len: read as u64,
                compressed_bytes: compressed,
            });

            if current_bundle_raw >= VOLUME_BUNDLE_RAW_SIZE {
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
        target: &StorageTarget,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<CommittedBundle> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::CommitBundle {
                    volume_id: target.volume_id,
                    bundle_id,
                    chunks,
                },
            )
            .await?;

        match response {
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
        target: &StorageTarget,
        bundle_id: HashId,
    ) -> Fs0Result<Vec<BundleChunkRef>> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::ListBundleChunks {
                    volume_id: target.volume_id,
                    bundle_id,
                },
            )
            .await?;

        match response {
            DataResponse::ListBundleChunks { chunks } => Ok(chunks),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    async fn download_chunk(&self, target: &StorageTarget, chunk_id: HashId) -> Fs0Result<Vec<u8>> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::DownloadChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                },
            )
            .await?;

        match response {
            DataResponse::DownloadChunk { compressed_bytes } => Ok(compressed_bytes),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    async fn download_bundle_from_replicas(
        &self,
        bundle: &fs0_core::FileBundleRef,
    ) -> Fs0Result<Vec<VerifiedChunk>> {
        let mut last_error = None;

        for target in self.read_targets(bundle.replicas.as_slice()) {
            match self.download_verified_bundle(&target, bundle).await {
                Ok(chunks) => return Ok(chunks),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or(Fs0Error::NotFound))
    }

    async fn download_verified_bundle(
        &self,
        target: &StorageTarget,
        bundle: &fs0_core::FileBundleRef,
    ) -> Fs0Result<Vec<VerifiedChunk>> {
        let chunks = self.list_bundle_chunks(target, bundle.bundle_id).await?;
        if bundle_hash_from_chunks(&chunks) != bundle.bundle_id {
            return Err(Fs0Error::InvalidData {
                message: "bundle id does not match listed chunk ids".to_owned(),
            });
        }

        let mut verified = Vec::with_capacity(chunks.len());
        let mut total_raw_len = 0u64;
        let mut total_compressed_len = 0u64;
        for chunk in chunks {
            let (raw_len, compressed_len) = self
                .storage_has_chunk(target, chunk.chunk_id)
                .await?
                .ok_or(Fs0Error::ChunkNotFound {
                chunk_id: chunk.chunk_id,
            })?;
            let compressed = self.download_chunk(target, chunk.chunk_id).await?;
            if compressed.len() as u64 != compressed_len {
                return Err(Fs0Error::InvalidData {
                    message: "downloaded compressed length does not match chunk metadata"
                        .to_owned(),
                });
            }

            let raw = zstd_decompress(&compressed, raw_len as usize)?;
            if raw.len() as u64 != raw_len || blake3_hash(&raw) != chunk.chunk_id {
                return Err(Fs0Error::HashMismatch { volume_offset: 0 });
            }

            total_raw_len =
                total_raw_len
                    .checked_add(raw_len)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle raw_len overflow".to_owned(),
                    })?;
            total_compressed_len = total_compressed_len
                .checked_add(compressed_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "bundle compressed_len overflow".to_owned(),
                })?;
            verified.push(VerifiedChunk {
                chunk_index: chunk.chunk_index,
                raw_len,
                compressed_len,
                raw,
            });
        }

        if total_raw_len != bundle.raw_len || total_compressed_len != bundle.compressed_len {
            return Err(Fs0Error::InvalidData {
                message: "downloaded bundle lengths do not match read plan".to_owned(),
            });
        }

        Ok(verified)
    }

    fn upload_target(&self, volume_id: u64) -> Fs0Result<StorageTarget> {
        self.storages
            .read()
            .iter()
            .find(|peer| {
                peer.volumes
                    .iter()
                    .any(|volume| volume.volume_id == volume_id)
            })
            .map(|peer| StorageTarget {
                storage_id: peer.storage_id,
                volume_id,
                iroh_endpoint: peer.iroh_endpoint.clone(),
            })
            .ok_or(Fs0Error::NotFound)
    }

    fn read_targets(&self, replicas: &[fs0_core::ReplicaLocation]) -> Vec<StorageTarget> {
        let storages = self.storages.read();

        replicas
            .iter()
            .filter_map(|replica| {
                storages
                    .iter()
                    .find(|peer| peer.storage_id == replica.storage_id)
                    .map(|peer| StorageTarget {
                        storage_id: peer.storage_id,
                        volume_id: replica.volume_id,
                        iroh_endpoint: peer.iroh_endpoint.clone(),
                    })
            })
            .collect()
    }

    async fn storage_rpc(
        &self,
        target: &StorageTarget,
        request: DataRequest,
    ) -> Fs0Result<DataResponse> {
        let connection = self
            .connect_authenticated_data(&target.iroh_endpoint)
            .await?;
        let response = data_rpc_on_connection(&connection, request).await;
        connection.close(0u32.into(), b"fs0 data rpc complete");

        response
    }

    async fn connect_authenticated_data(&self, data_endpoint: &[u8]) -> Fs0Result<Connection> {
        let connection = connect_data(&self.endpoint, data_endpoint).await?;
        match data_rpc_on_connection(
            &connection,
            DataRequest::Authenticate {
                client_id: self.client_id,
                client_token: self.token.clone(),
            },
        )
        .await?
        {
            DataResponse::Authenticate { client_id } if client_id == self.client_id => {
                Ok(connection)
            }
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    async fn request(&self, request: ControlRequest) -> Fs0Result<ControlResponse> {
        control_rpc(&self.control, request).await
    }
}

async fn upload_chunk_if_missing_on_connection(
    index: usize,
    connection: Arc<Connection>,
    volume_id: u64,
    chunk: ChunkUpload,
) -> Fs0Result<(usize, ChunkUploadResult)> {
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
            compressed_hash: chunk.compressed_hash,
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

fn unexpected_control_response<T>(response: ControlResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected control response: {response:?}"),
    })
}

fn unexpected_data_response<T>(response: DataResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected data response: {response:?}"),
    })
}
