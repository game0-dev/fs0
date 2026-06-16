use crate::{
    central_session::CentralSession,
    storage_session::{StorageSession, UploadChunkJob},
};
pub use fs0_config::ClientConfig;
use fs0_core::{
    Fs0Error, Fs0Result, HashId, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE, blake3_hash,
    bundle_hash_from_chunks,
    protocol::{
        BeginUpdateRequest, BundleChunkRef, CommitBundleRequest, CommitUpdateRequest,
        CommittedBundle, DirectoryEntries, DownloadChunkRequest, FileChangeLogs, FileReadPlan,
        FileRecord, StoragePeerInfo, UploadChunkResponse,
    },
    zstd_decompress,
};
use fs0_transport::Transport;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::info;

const MAX_PENDING_UPLOAD_BUNDLES: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    pub chunks: u64,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralStatus {
    pub clients_count: u32,
    pub storages: Vec<StoragePeerInfo>,
}

#[derive(Debug)]
struct SpoolBundle {
    index: u64,
    bundle_id: HashId,
    raw_len: u64,
    chunks: Vec<BundleChunkRef>,
    uploads: Vec<SpoolChunk>,
    temp_dir: TempDir,
}

#[derive(Debug)]
struct SpoolChunk {
    chunk_id: HashId,
    raw_len: u64,
    path: PathBuf,
}

#[derive(Debug)]
struct PendingBundle {
    bundle_id: HashId,
    raw_len: u64,
    chunks: Vec<BundleChunkRef>,
    unfinished_chunk_hashes: HashSet<HashId>,
    temp_dir: TempDir,
}

#[derive(Debug)]
struct UploadCompletion {
    chunk_id: HashId,
    result: Fs0Result<Arc<UploadChunkResponse>>,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    config: ClientConfig,
    central: Arc<CentralSession>,
    transport: Transport,
    storage_sessions: Arc<Mutex<HashMap<u64, Arc<StorageSession>>>>,
}

#[derive(Debug)]
struct ChunkJob {
    chunk_id: HashId,
    ready: bool,
}

struct InternalDownloadState {
    jobs: Vec<ChunkJob>,
    remain_chunks: u64,
    first_error: Option<Fs0Error>,
    done_tx: Option<oneshot::Sender<Fs0Result<()>>>,
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
        let (done_tx, done_rx) = oneshot::channel();
        let total_chunks = plan.size.div_ceil(fs0_core::VOLUME_RAW_CHUNK_SIZE);
        let remain_chunks = total_chunks;
        let download_state = Arc::new(StdMutex::new(InternalDownloadState {
            jobs: Vec::new(),
            remain_chunks,
            first_error: None,
            done_tx: Some(done_tx),
        }));

        for bundle in &plan.bundles {
            for replica in &bundle.replicas {
                let Some(storage) = storages
                    .iter()
                    .find(|storage| storage.storage_id == replica.storage_id)
                else {
                    continue;
                };

                let session = self.storage_session(storage).await;
                let chunks = session
                    .inner
                    .list_bundle_chunks(replica.volume_id, bundle.bundle_id)
                    .await?;

                for chunk in &chunks {
                    let chunk_id = chunk.chunk_id;
                    let job_index = {
                        let mut state = download_state.lock().map_err(|_| Fs0Error::Internal {
                            message: "download state lock was poisoned".to_owned(),
                        })?;
                        let job_index = state.jobs.len();
                        state.jobs.push(ChunkJob {
                            chunk_id,
                            ready: false,
                        });
                        job_index
                    };

                    let download_state = Arc::clone(&download_state);
                    session
                        .enqueue_download(
                            DownloadChunkRequest {
                                volume_id: replica.volume_id,
                                chunk_id,
                            },
                            move |result| {
                                let Ok(mut state) = download_state.lock() else {
                                    return;
                                };

                                match result {
                                    Ok(_) => {
                                        if let Some(entry) = state.jobs.get_mut(job_index) {
                                            entry.ready = true;
                                        } else if state.first_error.is_none() {
                                            state.first_error = Some(Fs0Error::Internal {
                                                message: format!(
                                                    "download job {job_index} was not tracked"
                                                ),
                                            });
                                        }
                                    }
                                    Err(err) => {
                                        if state.first_error.is_none() {
                                            state.first_error = Some(err);
                                        }
                                    }
                                }

                                state.remain_chunks = state.remain_chunks.saturating_sub(1);
                                let completed_chunks =
                                    total_chunks.saturating_sub(state.remain_chunks);
                                info!(completed_chunks, total_chunks, "download chunks completed");
                                if state.remain_chunks == 0
                                    && let Some(done_tx) = state.done_tx.take()
                                {
                                    let result = match state.first_error.take() {
                                        Some(err) => Err(err),
                                        None => Ok(()),
                                    };
                                    let _ = done_tx.send(result);
                                }
                            },
                        )
                        .await?;
                }
            }
        }

        if remain_chunks > 0 {
            done_rx.await.map_err(|_| Fs0Error::Internal {
                message: "download completion channel closed".to_owned(),
            })??;
        }

        let chunk_jobs = {
            let mut state = download_state.lock().map_err(|_| Fs0Error::Internal {
                message: "download state lock was poisoned".to_owned(),
            })?;
            std::mem::take(&mut state.jobs)
        };
        let download_cache_dir = self
            .config
            .download_cache_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("fs0-client-cache"));

        for job in &chunk_jobs {
            if !job.ready {
                return Err(Fs0Error::Internal {
                    message: "download job was not ready before write".to_owned(),
                });
            }

            let cache_path = download_cache_dir.join(format!("{}.zst", hash_to_hex(job.chunk_id)));
            let compressed = tokio::fs::read(&cache_path).await?;
            let raw = match decompress_and_verify_chunk(
                job.chunk_id,
                compressed.as_slice(),
                VOLUME_RAW_CHUNK_SIZE,
            ) {
                Ok(raw) => raw,
                Err(error) => {
                    let _ = tokio::fs::remove_file(&cache_path).await;
                    return Err(error);
                }
            };
            writer.write_all(&raw).await?;
        }
        writer.flush().await?;

        Ok(TransferStats {
            chunks: total_chunks,
            size: plan.size,
        })
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
            let mut input_done = false;
            let mut pending_bundles = BTreeMap::new();
            let mut active_by_hash = HashMap::<HashId, Vec<u64>>::new();
            let mut active_raw_lens = HashMap::<HashId, u64>::new();
            let mut done_hashes = HashSet::new();
            let (upload_tx, mut upload_rx) = mpsc::unbounded_channel();

            loop {
                while !input_done && pending_bundles.len() < MAX_PENDING_UPLOAD_BUNDLES {
                    let Some(bundle) = read_spool_bundle(&mut reader, bundle_index).await? else {
                        input_done = true;
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
                    } else if let Some((raw_len, compressed_len)) = session
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
                        let SpoolBundle {
                            index,
                            bundle_id,
                            raw_len,
                            chunks,
                            uploads,
                            temp_dir,
                        } = bundle;
                        let unfinished_chunk_hashes =
                            uploads.iter().map(|chunk| chunk.chunk_id).collect();
                        pending_bundles.insert(
                            index,
                            PendingBundle {
                                bundle_id,
                                raw_len,
                                chunks,
                                unfinished_chunk_hashes,
                                temp_dir,
                            },
                        );
                        schedule_upload_chunks(
                            Arc::clone(&session),
                            volume_id,
                            lease.lease_id,
                            lease.file_id,
                            index,
                            uploads,
                            &mut pending_bundles,
                            &mut active_by_hash,
                            &mut active_raw_lens,
                            &done_hashes,
                            &upload_tx,
                        )
                        .await?;
                    }

                    bundle_index += 1;
                    drain_upload_completions(
                        &mut upload_rx,
                        &mut pending_bundles,
                        &mut active_by_hash,
                        &mut active_raw_lens,
                        &mut done_hashes,
                    )?;
                    commit_ready_bundles(
                        &session,
                        volume_id,
                        lease.lease_id,
                        lease.file_id,
                        &mut pending_bundles,
                        &mut suffix_bundles,
                    )
                    .await?;
                }

                drain_upload_completions(
                    &mut upload_rx,
                    &mut pending_bundles,
                    &mut active_by_hash,
                    &mut active_raw_lens,
                    &mut done_hashes,
                )?;
                commit_ready_bundles(
                    &session,
                    volume_id,
                    lease.lease_id,
                    lease.file_id,
                    &mut pending_bundles,
                    &mut suffix_bundles,
                )
                .await?;

                if input_done && pending_bundles.is_empty() && active_by_hash.is_empty() {
                    break;
                }

                if active_by_hash.is_empty() {
                    return Err(Fs0Error::Internal {
                        message: "pending upload bundles have no active chunk uploads".to_owned(),
                    });
                }

                let completion = upload_rx.recv().await.ok_or_else(|| Fs0Error::Internal {
                    message: "upload completion channel closed".to_owned(),
                })?;
                process_upload_completion(
                    completion,
                    &mut pending_bundles,
                    &mut active_by_hash,
                    &mut active_raw_lens,
                    &mut done_hashes,
                )?;
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

async fn read_spool_bundle<R>(reader: &mut R, index: u64) -> Fs0Result<Option<SpoolBundle>>
where
    R: AsyncRead + Unpin,
{
    let temp_dir = tempfile::tempdir()?;
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
        let chunk_index = chunks.len() as u64;
        let path = temp_dir.path().join(format!("chunk-{chunk_index}"));
        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(raw).await?;
        file.flush().await?;
        chunks.push(BundleChunkRef {
            chunk_index,
            chunk_id,
        });
        uploads.push(SpoolChunk {
            chunk_id,
            raw_len: read as u64,
            path,
        });
        raw_len += read as u64;
    }

    if chunks.is_empty() {
        return Ok(None);
    }

    Ok(Some(SpoolBundle {
        index,
        bundle_id: bundle_hash_from_chunks(&chunks),
        raw_len,
        chunks,
        uploads,
        temp_dir,
    }))
}

async fn schedule_upload_chunks(
    session: Arc<StorageSession>,
    volume_id: u64,
    lease_id: u64,
    file_id: u64,
    bundle_index: u64,
    uploads: Vec<SpoolChunk>,
    pending_bundles: &mut BTreeMap<u64, PendingBundle>,
    active_by_hash: &mut HashMap<HashId, Vec<u64>>,
    active_raw_lens: &mut HashMap<HashId, u64>,
    done_hashes: &HashSet<HashId>,
    upload_tx: &mpsc::UnboundedSender<UploadCompletion>,
) -> Fs0Result<()> {
    let mut seen_in_bundle = HashSet::new();

    for chunk in uploads {
        if !seen_in_bundle.insert(chunk.chunk_id) {
            continue;
        }

        if done_hashes.contains(&chunk.chunk_id) {
            if let Some(bundle) = pending_bundles.get_mut(&bundle_index) {
                bundle.unfinished_chunk_hashes.remove(&chunk.chunk_id);
            }
            continue;
        }

        if let Some(waiters) = active_by_hash.get_mut(&chunk.chunk_id) {
            waiters.push(bundle_index);
            continue;
        }

        active_by_hash.insert(chunk.chunk_id, vec![bundle_index]);
        active_raw_lens.insert(chunk.chunk_id, chunk.raw_len);

        let chunk_id = chunk.chunk_id;
        let raw_len = chunk.raw_len;
        let path = chunk.path;
        let upload_tx = upload_tx.clone();
        let raw_bytes = tokio::fs::read(&path).await?;
        if raw_bytes.len() as u64 != raw_len {
            return Err(Fs0Error::InvalidData {
                message: format!(
                    "spooled chunk {} length {} does not match raw_len {}",
                    hash_to_hex(chunk_id),
                    raw_bytes.len(),
                    raw_len
                ),
            });
        }
        let job = UploadChunkJob {
            lease_id,
            file_id,
            volume_id,
            chunk_id,
            raw_bytes,
        };
        session
            .enqueue_upload(job, move |result| {
                let _ = upload_tx.send(UploadCompletion { chunk_id, result });
            })
            .await?;
    }

    Ok(())
}

fn drain_upload_completions(
    upload_rx: &mut mpsc::UnboundedReceiver<UploadCompletion>,
    pending_bundles: &mut BTreeMap<u64, PendingBundle>,
    active_by_hash: &mut HashMap<HashId, Vec<u64>>,
    active_raw_lens: &mut HashMap<HashId, u64>,
    done_hashes: &mut HashSet<HashId>,
) -> Fs0Result<()> {
    while let Ok(completion) = upload_rx.try_recv() {
        process_upload_completion(
            completion,
            pending_bundles,
            active_by_hash,
            active_raw_lens,
            done_hashes,
        )?;
    }

    Ok(())
}

fn process_upload_completion(
    completion: UploadCompletion,
    pending_bundles: &mut BTreeMap<u64, PendingBundle>,
    active_by_hash: &mut HashMap<HashId, Vec<u64>>,
    active_raw_lens: &mut HashMap<HashId, u64>,
    done_hashes: &mut HashSet<HashId>,
) -> Fs0Result<()> {
    let waiters =
        active_by_hash
            .remove(&completion.chunk_id)
            .ok_or_else(|| Fs0Error::Internal {
                message: "completed upload chunk was not tracked".to_owned(),
            })?;
    let expected_raw_len = active_raw_lens
        .remove(&completion.chunk_id)
        .ok_or_else(|| Fs0Error::Internal {
            message: "completed upload chunk raw_len was not tracked".to_owned(),
        })?;
    let response = completion.result?;
    if response.chunk_id != completion.chunk_id || response.raw_len != expected_raw_len {
        return Err(Fs0Error::InvalidData {
            message: "uploaded chunk metadata does not match request".to_owned(),
        });
    }

    done_hashes.insert(completion.chunk_id);
    for bundle_index in waiters {
        let bundle = pending_bundles
            .get_mut(&bundle_index)
            .ok_or_else(|| Fs0Error::Internal {
                message: format!("pending bundle {bundle_index} was not tracked"),
            })?;
        bundle.unfinished_chunk_hashes.remove(&completion.chunk_id);
    }

    Ok(())
}

async fn commit_ready_bundles(
    session: &Arc<StorageSession>,
    volume_id: u64,
    lease_id: u64,
    file_id: u64,
    pending_bundles: &mut BTreeMap<u64, PendingBundle>,
    suffix_bundles: &mut BTreeMap<u64, CommittedBundle>,
) -> Fs0Result<()> {
    let ready = pending_bundles
        .iter()
        .filter_map(|(index, bundle)| bundle.unfinished_chunk_hashes.is_empty().then_some(*index))
        .collect::<Vec<_>>();

    for bundle_index in ready {
        let bundle = pending_bundles
            .remove(&bundle_index)
            .ok_or_else(|| Fs0Error::Internal {
                message: format!("ready bundle {bundle_index} was not tracked"),
            })?;
        let committed = session
            .inner
            .commit_bundle(CommitBundleRequest {
                volume_id,
                lease_id,
                file_id,
                bundle_id: bundle.bundle_id,
                chunks: bundle.chunks,
            })
            .await?;
        if committed.raw_len != bundle.raw_len {
            return Err(Fs0Error::InvalidData {
                message: "committed bundle raw_len does not match spooled bundle".to_owned(),
            });
        }
        suffix_bundles.insert(bundle_index, committed);
        drop(bundle.temp_dir);
    }

    Ok(())
}

fn hash_to_hex(hash_id: HashId) -> String {
    let mut output = String::with_capacity(64);
    for byte in hash_id.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
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
