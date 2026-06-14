use super::StorageSession;
use crate::{
    Fs0Error, Fs0Result,
    client::{ChunkUpload, Fs0Client, StorageTarget},
};
use fs0_core::{
    DEFAULT_ZSTD_LEVEL, HashId, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE, blake3_hash,
    bundle_hash_from_chunks,
    protocol::{
        BundleChunkRef, CommitBundleRequest, CommitBundleResponse, CommittedBundle, DataRequest,
        DataResponse, UploadChunkRequest, UploadChunkResponse,
    },
    zstd_compress,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{Mutex, Notify, OwnedSemaphorePermit},
    task::JoinSet,
};

#[derive(Debug)]
pub(crate) struct PreparedBundle {
    pub(crate) index: u64,
    pub(crate) bundle_id: HashId,
    pub(crate) raw_len: u64,
    chunks: Vec<BundleChunkRef>,
    uploads: Vec<PreparedChunk>,
}

#[derive(Debug)]
struct PreparedChunk {
    chunk_id: HashId,
    raw_bytes: Vec<u8>,
}

#[derive(Debug)]
struct UploadedBundle {
    index: u64,
    bundle: CommittedBundle,
}

#[derive(Debug)]
pub(crate) struct UploadScheduler {
    client_id: u64,
    target: StorageTarget,
    lease_id: u64,
    file_id: u64,
    session: Arc<StorageSession>,
    slots: Arc<Mutex<HashMap<HashId, Arc<ChunkUploadSlot>>>>,
    bundles: JoinSet<Fs0Result<UploadedBundle>>,
}

#[derive(Debug)]
struct ChunkUploadSlot {
    result: Mutex<Option<Fs0Result<()>>>,
    notify: Notify,
}

impl UploadScheduler {
    async fn start_upload(&self, chunk: PreparedChunk) -> Fs0Result<Arc<ChunkUploadSlot>> {
        let chunk_id = chunk.chunk_id;
        let mut is_owner = false;
        let slot = {
            let mut slots = self.slots.lock().await;
            slots
                .entry(chunk_id)
                .or_insert_with(|| {
                    is_owner = true;
                    Arc::new(ChunkUploadSlot {
                        result: Mutex::new(None),
                        notify: Notify::new(),
                    })
                })
                .clone()
        };

        if is_owner {
            let permit = self.session.acquire_upload_permit().await?;
            let slot_on_complete = Arc::clone(&slot);
            let session = Arc::clone(&self.session);
            let target = self.target.clone();
            let client_id = self.client_id;
            let lease_id = self.lease_id;
            let file_id = self.file_id;
            tokio::spawn(async move {
                let result = upload_chunk_owner(
                    session, client_id, target, lease_id, file_id, chunk, permit,
                )
                .await;
                *slot_on_complete.result.lock().await = Some(result);
                slot_on_complete.notify.notify_waiters();
            });
        }

        Ok(slot)
    }
}

impl ChunkUploadSlot {
    async fn wait(&self) -> Fs0Result<()> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let result = self.result.lock().await.clone();
                if let Some(result) = result {
                    return result;
                }
            }
            notified.as_mut().await;
        }
    }
}

async fn upload_chunk_owner(
    session: Arc<StorageSession>,
    client_id: u64,
    target: StorageTarget,
    lease_id: u64,
    file_id: u64,
    chunk: PreparedChunk,
    _permit: OwnedSemaphorePermit,
) -> Fs0Result<()> {
    let chunk_id = chunk.chunk_id;
    let raw_len = chunk.raw_bytes.len() as u64;
    let response = session
        .upload_chunk(
            client_id,
            &target,
            lease_id,
            file_id,
            ChunkUpload {
                chunk_id,
                raw_len,
                compressed_bytes: zstd_compress(&chunk.raw_bytes, DEFAULT_ZSTD_LEVEL)?,
            },
        )
        .await?;

    if response.chunk_id != chunk_id || response.raw_len != raw_len {
        return Err(Fs0Error::InvalidData {
            message: "uploaded chunk metadata does not match request".to_owned(),
        });
    }

    Ok(())
}

impl Fs0Client {
    pub(crate) async fn upload_scheduler(
        &self,
        client_id: u64,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
    ) -> Fs0Result<UploadScheduler> {
        let session = self.storage_session(target).await;
        session.ensure_connected(client_id, target).await?;

        Ok(UploadScheduler::new(
            session,
            client_id,
            target.clone(),
            lease_id,
            file_id,
        ))
    }

    pub(crate) async fn read_upload_bundle<R>(
        &self,
        reader: &mut R,
        index: u64,
    ) -> Fs0Result<Option<PreparedBundle>>
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
}

impl StorageSession {
    async fn upload_chunk(
        &self,
        client_id: u64,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        chunk: ChunkUpload,
    ) -> Fs0Result<UploadChunkResponse> {
        let response = self
            .request(
                client_id,
                target,
                DataRequest::UploadChunk(UploadChunkRequest {
                    lease_id,
                    file_id,
                    volume_id: target.volume_id,
                    chunk_id: chunk.chunk_id,
                    raw_len: chunk.raw_len,
                    compressed_bytes: chunk.compressed_bytes,
                }),
            )
            .await?;

        match response {
            DataResponse::UploadChunk(response) => Ok(response),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    async fn commit_bundle(
        &self,
        client_id: u64,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<CommittedBundle> {
        let response = self
            .request(
                client_id,
                target,
                DataRequest::CommitBundle(CommitBundleRequest {
                    lease_id,
                    file_id,
                    volume_id: target.volume_id,
                    bundle_id,
                    chunks,
                }),
            )
            .await?;

        match response {
            DataResponse::CommitBundle(CommitBundleResponse {
                raw_len,
                compressed_len,
                ..
            }) => Ok(CommittedBundle {
                bundle_id,
                raw_len,
                compressed_len,
            }),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }
}

impl UploadScheduler {
    fn new(
        session: Arc<StorageSession>,
        client_id: u64,
        target: StorageTarget,
        lease_id: u64,
        file_id: u64,
    ) -> Self {
        Self {
            client_id,
            target,
            lease_id,
            file_id,
            session,
            slots: Arc::new(Mutex::new(HashMap::new())),
            bundles: JoinSet::new(),
        }
    }

    pub(crate) async fn schedule_bundle(&mut self, bundle: PreparedBundle) -> Fs0Result<()> {
        let mut slots = Vec::with_capacity(bundle.uploads.len());
        for chunk in bundle.uploads {
            slots.push(self.start_upload(chunk).await?);
        }

        let session = Arc::clone(&self.session);
        let target = self.target.clone();
        let client_id = self.client_id;
        let lease_id = self.lease_id;
        let file_id = self.file_id;
        self.bundles.spawn(async move {
            for slot in slots {
                slot.wait().await?;
            }

            let committed = session
                .commit_bundle(
                    client_id,
                    &target,
                    lease_id,
                    file_id,
                    bundle.bundle_id,
                    bundle.chunks,
                )
                .await?;

            Ok(UploadedBundle {
                index: bundle.index,
                bundle: committed,
            })
        });

        Ok(())
    }

    pub(crate) fn collect_ready(
        &mut self,
        bundles: &mut BTreeMap<u64, CommittedBundle>,
    ) -> Fs0Result<()> {
        self.collect_ready_count(bundles)?;

        Ok(())
    }

    pub(crate) async fn wait_for_reader_capacity(
        &mut self,
        bundles: &mut BTreeMap<u64, CommittedBundle>,
    ) -> Fs0Result<()> {
        if self.collect_ready_count(bundles)? > 0
            || self.bundles.is_empty()
            || self.session.upload_available_permits() > 0
        {
            return Ok(());
        }

        let permit = self.session.acquire_upload_permit().await?;
        drop(permit);
        self.collect_ready(bundles)
    }

    pub(crate) async fn finish(
        &mut self,
        bundles: &mut BTreeMap<u64, CommittedBundle>,
    ) -> Fs0Result<()> {
        while let Some(result) = self.bundles.join_next().await {
            collect_uploaded_bundle(result, bundles)?;
        }

        Ok(())
    }

    fn collect_ready_count(
        &mut self,
        bundles: &mut BTreeMap<u64, CommittedBundle>,
    ) -> Fs0Result<usize> {
        let mut collected = 0;
        while let Some(result) = self.bundles.try_join_next() {
            collect_uploaded_bundle(result, bundles)?;
            collected += 1;
        }

        Ok(collected)
    }
}

fn collect_uploaded_bundle(
    result: Result<Fs0Result<UploadedBundle>, tokio::task::JoinError>,
    bundles: &mut BTreeMap<u64, CommittedBundle>,
) -> Fs0Result<()> {
    match result {
        Ok(Ok(uploaded)) => {
            bundles.insert(uploaded.index, uploaded.bundle);
            Ok(())
        }
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Fs0Error::Internal {
            message: err.to_string(),
        }),
    }
}
