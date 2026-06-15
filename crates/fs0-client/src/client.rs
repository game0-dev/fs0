use crate::{central_session::CentralSession, storage_session::StorageSession};
pub use fs0_config::ClientConfig;
use fs0_core::{
    DEFAULT_ZSTD_LEVEL, Fs0Error, Fs0Result, HashId, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE,
    blake3_hash, bundle_hash_from_chunks,
    protocol::{
        BeginUpdateRequest, BundleChunkRef, CommitBundleRequest, CommitUpdateRequest,
        CommittedBundle, DirectoryEntries, DownloadChunkRequest, FileChangeLogs, FileReadPlan,
        FileRecord, StoragePeerInfo, UploadChunkRequest,
    },
    zstd_compress, zstd_decompress,
};
use fs0_transport::Transport;
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub chunks: u64,
    pub bundles: u64,
    pub downloaded_compressed_bytes: u64,
    pub cached_compressed_bytes: u64,
    pub downloaded_chunks: u64,
    pub cached_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralStatus {
    pub clients_count: u32,
    pub storages: Vec<StoragePeerInfo>,
}

#[derive(Debug)]
struct PreparedBundle {
    index: u64,
    bundle_id: HashId,
    raw_len: u64,
    chunks: Vec<BundleChunkRef>,
    uploads: Vec<PreparedChunk>,
}

#[derive(Debug)]
struct PreparedChunk {
    chunk_id: HashId,
    raw_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    config: ClientConfig,
    central: Arc<CentralSession>,
    transport: Transport,
    storage_sessions: Arc<Mutex<HashMap<u64, Arc<StorageSession>>>>,
}

impl Fs0Client {
    pub async fn connect(config: ClientConfig) -> Fs0Result<Self> {
        let transport = Transport::bind(Vec::new(), None, None, config.relay.clone()).await?;
        let storage_sessions = Arc::new(Mutex::new(HashMap::new()));
        let central = Arc::new(CentralSession::new(
            config.clone(),
            transport.clone(),
            config.name.clone(),
        ));
        central.ensure_connected().await?;

        Ok(Self {
            config,
            central,
            transport,
            storage_sessions,
        })
    }

    pub async fn shutdown(&self) -> Fs0Result<()> {
        self.central.close(b"fs0 client shutdown").await;
        close_storage_sessions(&self.storage_sessions, b"fs0 client shutdown").await;
        self.transport.close().await;

        Ok(())
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.central.client_id()
    }

    pub fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.central.storage_peers()
    }

    pub async fn central_status(&self) -> Fs0Result<CentralStatus> {
        self.central.central_status().await
    }

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Fs0Result<u64> {
        self.central.create_volume(name, max_bytes).await
    }

    pub async fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Fs0Result<DirectoryEntries> {
        self.central.list_directory(dir, limit, cursor).await
    }

    pub async fn delete_file(&self, path: &str) -> Fs0Result<()> {
        self.central.delete_file(path).await
    }

    pub async fn delete_file_by_id(&self, file_id: u64) -> Fs0Result<()> {
        self.central.delete_file_by_id(file_id).await
    }

    pub async fn copy_file(&self, source_path: &str, target_path: &str) -> Fs0Result<FileRecord> {
        self.central.copy_file(source_path, target_path).await
    }

    pub async fn copy_file_by_id(
        &self,
        source_file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        self.central
            .copy_file_by_id(source_file_id, target_path)
            .await
    }

    pub async fn rename_file(&self, source_path: &str, target_path: &str) -> Fs0Result<FileRecord> {
        self.central.rename_file(source_path, target_path).await
    }

    pub async fn rename_file_by_id(
        &self,
        file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        self.central.rename_file_by_id(file_id, target_path).await
    }

    pub async fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Fs0Result<FileChangeLogs> {
        self.central
            .get_file_change_logs(after_event_id, limit)
            .await
    }

    pub async fn get_file_read_plan(&self, remote_path: &str) -> Fs0Result<FileReadPlan> {
        self.central.get_file_read_plan(remote_path).await
    }

    pub async fn upload<R>(
        &self,
        remote_path: &str,
        reader: R,
        prefer_volume_name: Option<String>,
    ) -> Fs0Result<FileRecord>
    where
        R: AsyncRead + Unpin,
    {
        self.upload_reader(remote_path, reader, prefer_volume_name, None)
            .await
    }

    pub async fn upload_file(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        prefer_volume_name: Option<String>,
    ) -> Fs0Result<FileRecord> {
        let local_path = local_path.as_ref();
        let update_size_hint = Some(tokio::fs::metadata(local_path).await?.len());
        let file = tokio::fs::File::open(local_path).await?;

        self.upload_reader(remote_path, file, prefer_volume_name, update_size_hint)
            .await
    }

    pub async fn download<W>(&self, remote_path: &str, mut writer: W) -> Fs0Result<TransferStats>
    where
        W: AsyncWrite + Unpin,
    {
        let plan = self.central.get_file_read_plan(remote_path).await?;
        let storages = self.storage_peers();
        let mut stats = TransferStats::default();

        for bundle in &plan.bundles {
            let mut last_error = None;
            let mut downloaded = None;

            for replica in &bundle.replicas {
                let Some(storage) = storages
                    .iter()
                    .find(|storage| storage.storage_id == replica.storage_id)
                else {
                    continue;
                };
                let session = self.storage_session(storage).await;
                let attempt = async {
                    let chunks = session
                        .inner
                        .list_bundle_chunks(replica.volume_id, bundle.bundle_id)
                        .await?;
                    if bundle_hash_from_chunks(&chunks) != bundle.bundle_id {
                        return Err(Fs0Error::InvalidData {
                            message: "bundle id does not match listed chunk ids".to_owned(),
                        });
                    }

                    let mut raw_chunks = Vec::with_capacity(chunks.len());
                    let mut raw_len = 0u64;
                    let mut compressed_len = 0u64;
                    for chunk in chunks {
                        let compressed = session
                            .download_chunk(DownloadChunkRequest {
                                volume_id: replica.volume_id,
                                chunk_id: chunk.chunk_id,
                            })
                            .await?;
                        let raw = decompress_and_verify_chunk(
                            chunk.chunk_id,
                            compressed.as_slice(),
                            VOLUME_RAW_CHUNK_SIZE,
                        )?;
                        raw_len = raw_len.checked_add(raw.len() as u64).ok_or_else(|| {
                            Fs0Error::IntegerConversion {
                                message: "bundle raw_len overflow".to_owned(),
                            }
                        })?;
                        compressed_len = compressed_len
                            .checked_add(compressed.len() as u64)
                            .ok_or_else(|| Fs0Error::IntegerConversion {
                                message: "bundle compressed_len overflow".to_owned(),
                            })?;
                        raw_chunks.push(raw);
                    }

                    if raw_len != bundle.raw_len || compressed_len != bundle.compressed_len {
                        return Err(Fs0Error::InvalidData {
                            message: "downloaded bundle lengths do not match read plan".to_owned(),
                        });
                    }

                    let chunk_count = raw_chunks.len() as u64;
                    Ok::<_, Fs0Error>((raw_chunks, raw_len, compressed_len, chunk_count))
                }
                .await;

                match attempt {
                    Ok(bundle_bytes) => {
                        downloaded = Some(bundle_bytes);
                        break;
                    }
                    Err(err) => last_error = Some(err),
                }
            }

            let (raw_chunks, raw_len, compressed_len, chunk_count) =
                downloaded.ok_or_else(|| last_error.unwrap_or(Fs0Error::NotFound))?;
            for raw in raw_chunks {
                writer.write_all(&raw).await?;
            }

            stats.raw_bytes += raw_len;
            stats.compressed_bytes += compressed_len;
            stats.downloaded_compressed_bytes += compressed_len;
            stats.chunks += chunk_count;
            stats.downloaded_chunks += chunk_count;
            stats.bundles += 1;
        }

        writer.flush().await?;

        Ok(stats)
    }

    pub async fn download_file(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
    ) -> Fs0Result<TransferStats> {
        let file = tokio::fs::File::create(local_path).await?;
        self.download(remote_path, file).await
    }

    async fn upload_reader<R>(
        &self,
        remote_path: &str,
        mut reader: R,
        prefer_volume_name: Option<String>,
        update_size_hint: Option<u64>,
    ) -> Fs0Result<FileRecord>
    where
        R: AsyncRead + Unpin,
    {
        let lease = self
            .central
            .begin_update(BeginUpdateRequest {
                path: remote_path.to_owned(),
                offset: 0,
                prefer_volume_name,
                update_size_hint,
            })
            .await?;

        let upload = async {
            let storages = self.storage_peers();
            let (storage, volume_id) = storages
                .iter()
                .find_map(|storage| {
                    storage
                        .volumes
                        .iter()
                        .find(|volume| volume.volume_id == lease.volume_id)
                        .map(|volume| (storage.clone(), volume.volume_id))
                })
                .ok_or(Fs0Error::NotFound)?;

            let mut new_size = 0u64;
            let mut suffix_bundles = BTreeMap::new();
            let session = self.storage_session(&storage).await;
            let mut bundle_index = 0u64;

            loop {
                let Some(bundle) = read_prepared_bundle(&mut reader, bundle_index).await? else {
                    break;
                };

                new_size = new_size.checked_add(bundle.raw_len).ok_or_else(|| {
                    Fs0Error::IntegerConversion {
                        message: "uploaded file size overflow".to_owned(),
                    }
                })?;

                if self.central.has_bundle(bundle.bundle_id, None).await? {
                    suffix_bundles.insert(
                        bundle.index,
                        CommittedBundle {
                            bundle_id: bundle.bundle_id,
                            raw_len: bundle.raw_len,
                            compressed_len: 0,
                        },
                    );
                } else {
                    if let Some((raw_len, compressed_len)) = session
                        .inner
                        .has_bundle(volume_id, bundle.bundle_id)
                        .await?
                    {
                        suffix_bundles.insert(
                            bundle.index,
                            CommittedBundle {
                                bundle_id: bundle.bundle_id,
                                raw_len,
                                compressed_len,
                            },
                        );
                    } else {
                        let mut uploads = tokio::task::JoinSet::new();
                        for chunk in bundle.uploads {
                            let session = Arc::clone(&session);
                            uploads.spawn(async move {
                                let raw_len = chunk.raw_bytes.len() as u64;
                                let request = UploadChunkRequest {
                                    lease_id: lease.lease_id,
                                    file_id: lease.file_id,
                                    volume_id,
                                    chunk_id: chunk.chunk_id,
                                    raw_len,
                                    compressed_bytes: zstd_compress(
                                        &chunk.raw_bytes,
                                        DEFAULT_ZSTD_LEVEL,
                                    )?,
                                };
                                let response = session.upload_chunk(request).await?;

                                if response.chunk_id != chunk.chunk_id
                                    || response.raw_len != raw_len
                                {
                                    return Err(Fs0Error::InvalidData {
                                        message: "uploaded chunk metadata does not match request"
                                            .to_owned(),
                                    });
                                }

                                Ok(())
                            });
                        }

                        while let Some(result) = uploads.join_next().await {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => {
                                    uploads.abort_all();
                                    return Err(err);
                                }
                                Err(err) => {
                                    uploads.abort_all();
                                    return Err(Fs0Error::Internal {
                                        message: err.to_string(),
                                    });
                                }
                            }
                        }

                        let bundle_index = bundle.index;
                        let committed = session
                            .inner
                            .commit_bundle(CommitBundleRequest {
                                volume_id,
                                lease_id: lease.lease_id,
                                file_id: lease.file_id,
                                bundle_id: bundle.bundle_id,
                                chunks: bundle.chunks,
                            })
                            .await?;
                        suffix_bundles.insert(bundle_index, committed);
                    }
                }

                bundle_index += 1;
            }

            Ok::<_, Fs0Error>((new_size, suffix_bundles.into_values().collect()))
        }
        .await;

        match upload {
            Ok((new_size, bundles)) => {
                let commit = self
                    .central
                    .commit_update(CommitUpdateRequest {
                        lease_id: lease.lease_id,
                        file_id: lease.file_id,
                        base_size: lease.base_size,
                        new_size,
                        bundles,
                    })
                    .await;
                if commit.is_err() {
                    let _ = self
                        .central
                        .abort_update(lease.lease_id, lease.file_id)
                        .await;
                }

                commit
            }
            Err(err) => {
                let _ = self
                    .central
                    .abort_update(lease.lease_id, lease.file_id)
                    .await;
                Err(err)
            }
        }
    }

    pub(crate) async fn storage_session(&self, storage: &StoragePeerInfo) -> Arc<StorageSession> {
        let mut sessions = self.storage_sessions.lock().await;
        sessions
            .entry(storage.storage_id)
            .or_insert_with(|| {
                Arc::new(StorageSession::new(
                    self.config.clone(),
                    self.transport.clone(),
                    self.client_id(),
                    storage.iroh_endpoint.clone(),
                ))
            })
            .clone()
    }
}

async fn close_storage_sessions(
    storage_sessions: &Arc<Mutex<HashMap<u64, Arc<StorageSession>>>>,
    reason: &[u8],
) {
    let sessions = storage_sessions
        .lock()
        .await
        .drain()
        .map(|(_, session)| session)
        .collect::<Vec<_>>();
    for session in sessions {
        session.close(reason).await;
    }
}

async fn read_prepared_bundle<R>(reader: &mut R, index: u64) -> Fs0Result<Option<PreparedBundle>>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; VOLUME_RAW_CHUNK_SIZE as usize];
    let mut raw_len = 0u64;
    let mut chunks = Vec::new();
    let mut uploads = Vec::new();

    while raw_len < VOLUME_BUNDLE_RAW_SIZE {
        let remaining = VOLUME_BUNDLE_RAW_SIZE - raw_len;
        let read_limit = buffer.len().min(remaining as usize);
        let read = reader.read(&mut buffer[..read_limit]).await?;
        if read == 0 {
            break;
        }

        let raw = &buffer[..read];
        let chunk_id = blake3_hash(raw);
        chunks.push(BundleChunkRef {
            chunk_index: chunks.len() as u64,
            chunk_id,
        });
        uploads.push(PreparedChunk {
            chunk_id,
            raw_bytes: raw.to_vec(),
        });
        raw_len += read as u64;
    }

    if chunks.is_empty() {
        return Ok(None);
    }

    Ok(Some(PreparedBundle {
        index,
        bundle_id: bundle_hash_from_chunks(&chunks),
        raw_len,
        chunks,
        uploads,
    }))
}

fn decompress_and_verify_chunk(
    chunk_id: HashId,
    compressed: &[u8],
    max_raw_len: u64,
) -> Fs0Result<Vec<u8>> {
    let max_raw_len = usize::try_from(max_raw_len).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("raw_len {max_raw_len} exceeds usize"),
    })?;
    let raw = zstd_decompress(compressed, max_raw_len)?;
    if raw.len() as u64 > VOLUME_RAW_CHUNK_SIZE || blake3_hash(&raw) != chunk_id {
        return Err(Fs0Error::HashMismatch { volume_offset: 0 });
    }

    Ok(raw)
}
