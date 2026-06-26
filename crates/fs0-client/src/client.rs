use crate::{
    central_session::CentralSession,
    storage_session::{DownloadChunkResult, StorageSession, UploadChunkJob},
};
pub use fs0_config::ClientConfig;
use fs0_core::{
    Fs0Error, Fs0Result, HashId, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE, blake3_hash,
    bundle_hash_from_chunks,
    protocol::{
        BeginUpdateRequest, BundleChunkRef, CommitBundleRequest, CommitUpdateRequest,
        CommittedBundle, DirectoryEntries, DownloadChunkRequest, FileChangeLogs, FileReadPlan,
        FileRecord, StoragePeerInfo,
    },
};
use fs0_transport::{Transport, TransportOptions};
use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::Instant,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{info, warn};

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

struct UploadBundle {
    bundle_id: HashId,
    raw_len: u64,
    chunks: Vec<BundleChunkRef>,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    config: ClientConfig,
    central: Arc<CentralSession>,
    transport: Transport,
    storage_sessions: Arc<Mutex<HashMap<u64, Arc<StorageSession>>>>,
}

struct InternalDownloadState {
    remain_chunks: u64,
    total_chunks: u64,
    scheduling_done: bool,
    first_error: Option<Fs0Error>,
    done_tx: Option<oneshot::Sender<Fs0Result<()>>>,
}

struct InternalUploadState {
    remain_chunks: u64,
    total_chunks: u64,
    scheduling_done: bool,
    first_error: Option<Fs0Error>,
    done_tx: Option<oneshot::Sender<Fs0Result<()>>>,
}

struct DownloadWriteJob {
    raw_offset: u64,
    raw_bytes: Vec<u8>,
}

impl Fs0Client {
    pub async fn connect(config: ClientConfig) -> Fs0Result<Self> {
        let transport =
            Transport::bind(TransportOptions::new(Vec::new()).with_relay(config.relay.clone()))
                .await?;
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

    pub async fn upload_file(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        prefer_volume_name: Option<String>,
    ) -> Fs0Result<FileRecord> {
        self.upload_file_inner(
            remote_path,
            local_path.as_ref().to_path_buf(),
            prefer_volume_name,
        )
        .await
    }

    pub async fn download_file(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
    ) -> Fs0Result<TransferStats> {
        let local_path = local_path.as_ref().to_path_buf();
        let plan = self.central.get_file_read_plan(remote_path).await?;
        let storages = self.storage_peers();
        let mut output = tokio::fs::File::create(&local_path).await?;
        output.set_len(plan.size).await?;
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<DownloadWriteJob>();
        let writer = tokio::spawn(async move {
            while let Some(job) = write_rx.recv().await {
                output.seek(SeekFrom::Start(job.raw_offset)).await?;
                output.write_all(&job.raw_bytes).await?;
            }
            output.flush().await?;
            Ok::<_, Fs0Error>(())
        });

        let (done_tx, done_rx) = oneshot::channel();
        let download_state = Arc::new(StdMutex::new(InternalDownloadState {
            remain_chunks: 0,
            total_chunks: 0,
            scheduling_done: false,
            first_error: None,
            done_tx: Some(done_tx),
        }));

        let download_result = self
            .download_file_chunks(&plan, &storages, &download_state, done_rx, write_tx.clone())
            .await;
        drop(write_tx);

        let writer_result = writer.await.map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
        download_result?;
        writer_result?;

        Ok(TransferStats {
            chunks: plan.size.div_ceil(VOLUME_RAW_CHUNK_SIZE),
            size: plan.size,
        })
    }

    async fn download_file_chunks(
        &self,
        plan: &FileReadPlan,
        storages: &[StoragePeerInfo],
        download_state: &Arc<StdMutex<InternalDownloadState>>,
        done_rx: oneshot::Receiver<Fs0Result<()>>,
        write_tx: mpsc::UnboundedSender<DownloadWriteJob>,
    ) -> Fs0Result<()> {
        for bundle in &plan.bundles {
            let bundle_offset = bundle
                .bundle_index
                .checked_mul(VOLUME_BUNDLE_RAW_SIZE)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "download bundle offset overflow".to_owned(),
                })?;
            let Some((replica, storage)) = bundle.replicas.iter().find_map(|replica| {
                storages
                    .iter()
                    .find(|storage| storage.storage_id == replica.storage_id)
                    .map(|storage| (replica, storage))
            }) else {
                return Err(Fs0Error::NotFound);
            };

            let session = self.storage_session(storage).await;
            let mut chunks = session
                .inner
                .list_bundle_chunks(replica.volume_id, bundle.bundle_id)
                .await?;
            chunks.sort_by_key(|chunk| chunk.chunk_index);

            for chunk in chunks {
                let chunk_offset = chunk
                    .chunk_index
                    .checked_mul(VOLUME_RAW_CHUNK_SIZE)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "download chunk offset overflow".to_owned(),
                    })?;
                if chunk_offset >= bundle.raw_len {
                    return Err(Fs0Error::InvalidData {
                        message: "download chunk offset exceeds bundle raw_len".to_owned(),
                    });
                }
                let raw_len = VOLUME_RAW_CHUNK_SIZE.min(bundle.raw_len - chunk_offset);
                let raw_offset = bundle_offset.checked_add(chunk_offset).ok_or_else(|| {
                    Fs0Error::IntegerConversion {
                        message: "download file offset overflow".to_owned(),
                    }
                })?;
                if raw_offset
                    .checked_add(raw_len)
                    .is_none_or(|end| end > plan.size)
                {
                    return Err(Fs0Error::InvalidData {
                        message: "download chunk exceeds file size".to_owned(),
                    });
                }

                {
                    let mut state = download_state.lock().map_err(|_| Fs0Error::Internal {
                        message: "download state lock was poisoned".to_owned(),
                    })?;
                    state.remain_chunks += 1;
                    state.total_chunks += 1;
                }

                let chunk_id = chunk.chunk_id;
                let download_state = Arc::clone(download_state);
                let write_tx = write_tx.clone();
                let download_started_at = Instant::now();
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
                                Ok(DownloadChunkResult {
                                    chunk_id: response_chunk_id,
                                    raw_bytes,
                                }) => {
                                    if response_chunk_id != chunk_id
                                        || raw_bytes.len() as u64 != raw_len
                                    {
                                        warn!(
                                            %chunk_id,
                                            raw_len,
                                            response_chunk_id = %response_chunk_id,
                                            response_raw_len = raw_bytes.len(),
                                            elapsed_ms = download_started_at.elapsed().as_millis(),
                                            "download chunk metadata mismatch"
                                        );
                                        if state.first_error.is_none() {
                                            state.first_error = Some(Fs0Error::InvalidData {
                                                message:
                                                    "downloaded chunk metadata does not match request"
                                                        .to_owned(),
                                            });
                                        }
                                    } else if write_tx
                                        .send(DownloadWriteJob {
                                            raw_offset,
                                            raw_bytes,
                                        })
                                        .is_err()
                                        && state.first_error.is_none()
                                    {
                                        state.first_error = Some(Fs0Error::Internal {
                                            message: "download writer task closed".to_owned(),
                                        });
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        %chunk_id,
                                        raw_len,
                                        elapsed_ms = download_started_at.elapsed().as_millis(),
                                        error = %err,
                                        "download chunk failed"
                                    );
                                    if state.first_error.is_none() {
                                        state.first_error = Some(err);
                                    }
                                }
                            }

                            state.remain_chunks = state.remain_chunks.saturating_sub(1);
                            let completed_chunks =
                                state.total_chunks.saturating_sub(state.remain_chunks);
                            info!(
                                completed_chunks,
                                total_chunks = state.total_chunks,
                                elapsed_ms = download_started_at.elapsed().as_millis(),
                                "download chunks completed"
                            );
                            if state.scheduling_done
                                && state.remain_chunks == 0
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

        let should_wait = {
            let mut state = download_state.lock().map_err(|_| Fs0Error::Internal {
                message: "download state lock was poisoned".to_owned(),
            })?;
            state.scheduling_done = true;
            if state.remain_chunks == 0 {
                if let Some(done_tx) = state.done_tx.take() {
                    let result = match state.first_error.take() {
                        Some(err) => Err(err),
                        None => Ok(()),
                    };
                    let _ = done_tx.send(result);
                }
                false
            } else {
                true
            }
        };
        if should_wait {
            done_rx.await.map_err(|_| Fs0Error::Internal {
                message: "download completion channel closed".to_owned(),
            })??;
        }

        Ok(())
    }

    async fn upload_file_inner(
        &self,
        remote_path: &str,
        local_path: PathBuf,
        prefer_volume_name: Option<String>,
    ) -> Fs0Result<FileRecord> {
        let update_size_hint = Some(tokio::fs::metadata(&local_path).await?.len());
        let mut reader = tokio::fs::File::open(&local_path).await?;
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
            let mut upload_bundles = Vec::new();
            let session = self.storage_session(&storage).await;
            let expected_total_chunks =
                update_size_hint.map(|size| size.div_ceil(VOLUME_RAW_CHUNK_SIZE));
            let (done_tx, done_rx) = oneshot::channel();
            let upload_state = Arc::new(StdMutex::new(InternalUploadState {
                remain_chunks: 0,
                total_chunks: 0,
                scheduling_done: false,
                first_error: None,
                done_tx: Some(done_tx),
            }));

            loop {
                let mut buffer = vec![0u8; VOLUME_RAW_CHUNK_SIZE as usize];
                let mut raw_len = 0u64;
                let mut chunks = Vec::new();

                while raw_len < VOLUME_BUNDLE_RAW_SIZE {
                    let remaining = VOLUME_BUNDLE_RAW_SIZE - raw_len;
                    let read_limit = buffer.len().min(remaining as usize);
                    let read = reader.read(&mut buffer[..read_limit]).await?;
                    if read == 0 {
                        break;
                    }

                    let chunk_offset = new_size.checked_add(raw_len).ok_or_else(|| {
                        Fs0Error::IntegerConversion {
                            message: "uploaded chunk offset overflow".to_owned(),
                        }
                    })?;
                    let raw = &buffer[..read];
                    let chunk_id = blake3_hash(raw);
                    let chunk_index = chunks.len() as u64;
                    chunks.push(BundleChunkRef {
                        chunk_index,
                        chunk_id,
                    });
                    raw_len += read as u64;

                    {
                        let mut state = upload_state.lock().map_err(|_| Fs0Error::Internal {
                            message: "upload state lock was poisoned".to_owned(),
                        })?;
                        state.remain_chunks += 1;
                        state.total_chunks += 1;
                    }

                    let raw_len = read as u64;
                    let job = UploadChunkJob {
                        lease_id: lease.lease_id,
                        file_id: lease.file_id,
                        volume_id,
                        chunk_id,
                        raw_len,
                        source_path: local_path.clone(),
                        source_offset: chunk_offset,
                    };
                    let upload_state = Arc::clone(&upload_state);
                    let upload_started_at = Instant::now();
                    let enqueue_result = session
                        .enqueue_upload(job, move |result| {
                            let Ok(mut state) = upload_state.lock() else {
                                return;
                            };
                            match result {
                                Ok(response) => {
                                    if response.chunk_id != chunk_id || response.raw_len != raw_len
                                    {
                                        warn!(
                                            %chunk_id,
                                            raw_len,
                                            elapsed_ms = upload_started_at.elapsed().as_millis(),
                                            response_chunk_id = %response.chunk_id,
                                            response_raw_len = response.raw_len,
                                            "upload chunk metadata mismatch"
                                        );
                                        if state.first_error.is_none() {
                                            state.first_error = Some(Fs0Error::InvalidData {
                                                message:
                                                    "uploaded chunk metadata does not match request"
                                                        .to_owned(),
                                            });
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        %chunk_id,
                                        raw_len,
                                        elapsed_ms = upload_started_at.elapsed().as_millis(),
                                        error = %err,
                                        "upload chunk failed"
                                    );
                                    if state.first_error.is_none() {
                                        state.first_error = Some(err);
                                    }
                                }
                            }

                            state.remain_chunks = state.remain_chunks.saturating_sub(1);
                            let completed_chunks =
                                state.total_chunks.saturating_sub(state.remain_chunks);
                            let total_chunks = expected_total_chunks.unwrap_or(state.total_chunks);
                            info!(
                                completed_chunks,
                                total_chunks,
                                elapsed_ms = upload_started_at.elapsed().as_millis(),
                                "upload chunks completed"
                            );
                            if state.scheduling_done
                                && state.remain_chunks == 0
                                && let Some(done_tx) = state.done_tx.take()
                            {
                                let result = match state.first_error.take() {
                                    Some(err) => Err(err),
                                    None => Ok(()),
                                };
                                let _ = done_tx.send(result);
                            }
                        })
                        .await;
                    if let Err(err) = &enqueue_result {
                        warn!(
                            %chunk_id,
                            raw_len,
                            error = %err,
                            "failed to enqueue upload chunk"
                        );
                    }
                    enqueue_result?;
                }

                if chunks.is_empty() {
                    break;
                }

                let bundle_id = bundle_hash_from_chunks(&chunks);
                upload_bundles.push(UploadBundle {
                    bundle_id,
                    raw_len,
                    chunks,
                });
                new_size =
                    new_size
                        .checked_add(raw_len)
                        .ok_or_else(|| Fs0Error::IntegerConversion {
                            message: "uploaded file size overflow".to_owned(),
                        })?;
            }

            let should_wait = {
                let mut state = upload_state.lock().map_err(|_| Fs0Error::Internal {
                    message: "upload state lock was poisoned".to_owned(),
                })?;
                state.scheduling_done = true;
                if state.remain_chunks == 0 {
                    if let Some(done_tx) = state.done_tx.take() {
                        let result = match state.first_error.take() {
                            Some(err) => Err(err),
                            None => Ok(()),
                        };
                        let _ = done_tx.send(result);
                    }
                    false
                } else {
                    true
                }
            };
            if should_wait {
                done_rx.await.map_err(|_| Fs0Error::Internal {
                    message: "upload completion channel closed".to_owned(),
                })??;
            }

            let mut committed_bundles = Vec::with_capacity(upload_bundles.len());
            let total_bundles = upload_bundles.len();
            for bundle in upload_bundles {
                if let Some((raw_len, compressed_len)) = session
                    .inner
                    .has_bundle(volume_id, bundle.bundle_id)
                    .await?
                {
                    if raw_len != bundle.raw_len {
                        return Err(Fs0Error::InvalidData {
                            message: "existing bundle raw_len does not match uploaded bundle"
                                .to_owned(),
                        });
                    }
                    committed_bundles.push(CommittedBundle {
                        bundle_id: bundle.bundle_id,
                        raw_len,
                        compressed_len,
                    });
                    info!(
                        committed_bundles = committed_bundles.len(),
                        total_bundles, "upload bundles committed"
                    );
                    continue;
                }

                let committed_bundle = session
                    .inner
                    .commit_bundle(CommitBundleRequest {
                        volume_id,
                        lease_id: lease.lease_id,
                        file_id: lease.file_id,
                        bundle_id: bundle.bundle_id,
                        chunks: bundle.chunks,
                    })
                    .await?;
                if committed_bundle.raw_len != bundle.raw_len {
                    return Err(Fs0Error::InvalidData {
                        message: "committed bundle raw_len does not match uploaded bundle"
                            .to_owned(),
                    });
                }
                committed_bundles.push(committed_bundle);
                info!(
                    committed_bundles = committed_bundles.len(),
                    total_bundles, "upload bundles committed"
                );
            }

            Ok::<_, Fs0Error>((new_size, committed_bundles))
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
        if let Some(session) = sessions.get(&storage.storage_id) {
            return session.clone();
        }

        let session = Arc::new(StorageSession::new(
            self.config.clone(),
            self.transport.clone(),
            Arc::clone(&self.central),
            self.client_id(),
            storage.storage_id,
        ));
        sessions.insert(storage.storage_id, session.clone());
        session
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
