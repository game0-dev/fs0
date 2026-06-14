use crate::{central_session::CentralSession, storage_session::StorageSession};
pub use fs0_config::ClientConfig;
use fs0_core::{
    Fs0Error, Fs0Result, HashId,
    protocol::{
        BeginUpdateRequest, CommitUpdateRequest, CommittedBundle, DirectoryEntries, FileChangeLogs,
        FileReadPlan, FileRecord, StoragePeerInfo,
    },
};
use fs0_transport::Transport;
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageTarget {
    pub storage_id: u64,
    pub volume_id: u64,
    pub iroh_endpoint: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkUpload {
    pub chunk_id: HashId,
    pub raw_len: u64,
    pub compressed_bytes: Vec<u8>,
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

    pub async fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
        self.central.get_file_read_plan(path).await
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        self.central.get_file_read_plan_by_id(file_id).await
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

    pub async fn download<W>(&self, remote_path: &str, writer: W) -> Fs0Result<TransferStats>
    where
        W: AsyncWrite + Unpin,
    {
        let plan = self.central.get_file_read_plan(remote_path).await?;
        let storages = self.storage_peers();
        let client_id = self.client_id();

        self.download_inner(client_id, &storages, &plan, writer)
            .await
    }

    pub async fn download_range<W>(
        &self,
        remote_path: &str,
        range: ReadRange,
        writer: W,
    ) -> Fs0Result<TransferStats>
    where
        W: AsyncWrite + Unpin,
    {
        let plan = self.central.get_file_read_plan(remote_path).await?;
        let storages = self.storage_peers();
        let client_id = self.client_id();

        self.download_range_inner(client_id, &storages, &plan, range, writer)
            .await
    }

    pub async fn download_file(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
    ) -> Fs0Result<TransferStats> {
        let plan = self.central.get_file_read_plan(remote_path).await?;
        let storages = self.storage_peers();
        let client_id = self.client_id();

        self.download_file_inner(client_id, &storages, &plan, local_path)
            .await
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
            let target = storages
                .iter()
                .find_map(|storage| {
                    storage
                        .volumes
                        .iter()
                        .find(|volume| volume.volume_id == lease.volume_id)
                        .map(|volume| StorageTarget {
                            storage_id: storage.storage_id,
                            volume_id: volume.volume_id,
                            iroh_endpoint: storage.iroh_endpoint.clone(),
                        })
                })
                .ok_or(Fs0Error::NotFound)?;

            let mut new_size = 0u64;
            let mut suffix_bundles = BTreeMap::new();
            let mut upload_scheduler = self
                .upload_scheduler(self.client_id(), &target, lease.lease_id, lease.file_id)
                .await?;
            let mut bundle_index = 0u64;

            loop {
                upload_scheduler
                    .wait_for_reader_capacity(&mut suffix_bundles)
                    .await?;

                let Some(bundle) = self.read_upload_bundle(&mut reader, bundle_index).await? else {
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
                    upload_scheduler.schedule_bundle(bundle).await?;
                }

                upload_scheduler.collect_ready(&mut suffix_bundles)?;
                bundle_index += 1;
            }

            upload_scheduler.finish(&mut suffix_bundles).await?;
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

    pub(crate) async fn storage_session(&self, target: &StorageTarget) -> Arc<StorageSession> {
        let mut sessions = self.storage_sessions.lock().await;
        sessions
            .entry(target.storage_id)
            .or_insert_with(|| {
                Arc::new(StorageSession::new(
                    self.config.clone(),
                    self.transport.clone(),
                    target.storage_id,
                    self.config.upload_concurrency,
                    self.config.download_concurrency,
                ))
            })
            .clone()
    }

    pub(crate) fn download_cache_dir(&self) -> Option<&Path> {
        self.config.download_cache_dir.as_deref()
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
