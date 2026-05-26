use crate::data_file_cache::DataFileCache;
use crate::db::{InsertChunk, VolumeDb};
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, Fs0Error, Fs0Result, HashId, VOLUME_DEFAULT_DATA_FILE_SIZE,
    VOLUME_DB_FILE, VOLUME_FORMAT_VERSION, blake3_hash, bundle_hash_from_chunks, now_ms,
};
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMeta {
    pub volume_id: u64,
    pub format_version: u64,
    pub max_bytes: u64,
    pub active_volume_offset: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub chunk_id: HashId,
    pub compressed_hash: HashId,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleMeta {
    pub bundle_id: HashId,
    pub raw_len: u64,
    pub compressed_len: u64,
    pub chunk_count: u64,
}

#[derive(Debug)]
pub struct Volume {
    root: PathBuf,
    db: Mutex<VolumeDb>,
    files: DataFileCache,
}

impl Volume {
    pub fn init_fs(path: impl Into<PathBuf>, max_bytes: u64) -> Fs0Result<()> {
        if max_bytes < VOLUME_DEFAULT_DATA_FILE_SIZE {
            return Err(Fs0Error::InvalidConfig {
                message: "max_bytes must be at least 4GB".to_owned(),
            });
        }

        let root = path.into();
        fs::create_dir_all(&root)?;

        let db_path = root.join(VOLUME_DB_FILE);
        if db_path.exists() {
            return Err(Fs0Error::AlreadyExists {
                path: db_path.display().to_string(),
            });
        }

        let now = now_ms();
        let meta = VolumeMeta {
            volume_id: 0,
            format_version: VOLUME_FORMAT_VERSION,
            max_bytes,
            active_volume_offset: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        VolumeDb::create(&root, &meta)?;
        info!(
            max_bytes,
            root = %root.display(),
            "initialized volume"
        );

        Ok(())
    }

    pub fn init_volume_id(path: impl Into<PathBuf>, volume_id: u64) -> Fs0Result<VolumeMeta> {
        let root = path.into();
        let db_path = root.join(VOLUME_DB_FILE);
        if !db_path.exists() {
            return Err(Fs0Error::VolumeNotFound {
                path: db_path.display().to_string(),
            });
        }

        VolumeDb::open(&root)?.assign_volume_id(volume_id, now_ms())
    }

    pub fn open(
        path: impl Into<PathBuf>,
        concurrent_read: u32,
        concurrent_write: u32,
    ) -> Fs0Result<Self> {
        let root = path.into();
        let db_path = root.join(VOLUME_DB_FILE);
        if !db_path.exists() {
            return Err(Fs0Error::VolumeNotFound {
                path: db_path.display().to_string(),
            });
        }

        let db = VolumeDb::open(&root)?;
        let meta = db.meta();
        info!(
            volume_id = meta.volume_id,
            max_bytes = meta.max_bytes,
            active_volume_offset = meta.active_volume_offset,
            root = %root.display(),
            "opened volume"
        );

        let data_files = usize::try_from(meta.max_bytes.div_ceil(VOLUME_DEFAULT_DATA_FILE_SIZE))
            .map_err(|_| Fs0Error::IntegerConversion {
                message: format!("volume max_bytes {} exceeds usize", meta.max_bytes),
            })?;

        Ok(Self {
            files: DataFileCache::with_capacity(
                root.clone(),
                data_files,
                concurrent_read as usize,
                concurrent_write as usize,
            ),
            root,
            db: Mutex::new(db),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn meta(&self) -> VolumeMeta {
        self.db.lock().meta()
    }

    pub async fn put_chunk(
        &self,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Fs0Result<ChunkMeta> {
        validate_chunk(chunk_id, raw_len, &compressed_bytes)?;

        let compressed_hash = blake3_hash(&compressed_bytes);
        let compressed_len = compressed_bytes.len() as u64;
        let (insert, next_active_offset) = {
            let mut db = self.db.lock();
            match db.load_chunk(chunk_id)? {
                Some(existing) => {
                    if existing.raw_len != raw_len {
                        warn!(
                            ?chunk_id,
                            existing_raw_len = existing.raw_len,
                            raw_len,
                            "chunk id already exists with different raw length"
                        );
                        return Err(Fs0Error::HashMismatch {
                            volume_offset: existing.volume_offset,
                        });
                    }
                    debug!(
                        ?chunk_id,
                        volume_offset = existing.volume_offset,
                        raw_len = existing.raw_len,
                        compressed_len = existing.compressed_len,
                        "chunk already exists"
                    );
                    return Ok(existing);
                }
                None => {}
            }

            let meta = db.meta();
            let mut volume_offset = meta.active_volume_offset;
            let offset_in_data_file = volume_offset % VOLUME_DEFAULT_DATA_FILE_SIZE;
            if offset_in_data_file + compressed_len > VOLUME_DEFAULT_DATA_FILE_SIZE {
                volume_offset = ((volume_offset / VOLUME_DEFAULT_DATA_FILE_SIZE) + 1)
                    * VOLUME_DEFAULT_DATA_FILE_SIZE;
            }

            let next_active_offset =
                volume_offset.checked_add(compressed_len).ok_or_else(|| {
                    Fs0Error::IntegerConversion {
                        message: "volume offset overflow".to_owned(),
                    }
                })?;
            if next_active_offset > meta.max_bytes {
                warn!(
                    ?chunk_id,
                    required_end = next_active_offset,
                    max_bytes = meta.max_bytes,
                    "chunk write exceeds volume capacity"
                );
                return Err(Fs0Error::CapacityExceeded {
                    required_end: next_active_offset,
                    max_bytes: meta.max_bytes,
                });
            }

            db.persist_active_volume_offset(next_active_offset, now_ms())?;

            let insert = InsertChunk {
                chunk_id,
                compressed_hash,
                volume_offset,
                raw_len,
                compressed_len,
            };
            (insert, next_active_offset)
        };

        if let Err(err) = self
            .write_compressed_bytes(insert.volume_offset, compressed_bytes)
            .await
        {
            warn!(
                ?chunk_id,
                volume_offset = insert.volume_offset,
                compressed_len,
                error = %err,
                "failed to write chunk bytes"
            );

            self.db
                .lock()
                .persist_active_volume_offset(next_active_offset, now_ms())?;

            return Err(err);
        }

        let mut db = self.db.lock();
        let now = now_ms();
        db.insert_chunk_and_update_active_offset(&insert, next_active_offset, now)?;

        let chunk = db
            .load_chunk(chunk_id)?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })?;
        info!(
            ?chunk_id,
            volume_offset = chunk.volume_offset,
            raw_len = chunk.raw_len,
            compressed_len = chunk.compressed_len,
            "stored chunk"
        );
        Ok(chunk)
    }

    pub async fn read_chunk(&self, chunk_id: HashId) -> Fs0Result<Vec<u8>> {
        debug!(?chunk_id, "reading chunk");
        let chunk = self
            .db
            .lock()
            .load_chunk(chunk_id)?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })?;

        self.read_compressed_bytes(chunk.volume_offset, chunk.compressed_len)
            .await
    }

    pub async fn chunk_meta(&self, chunk_id: HashId) -> Fs0Result<ChunkMeta> {
        self.db
            .lock()
            .load_chunk(chunk_id)?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })
    }

    pub async fn delete_chunk(&self, chunk_id: HashId) -> Fs0Result<()> {
        self.db.lock().delete_chunk(chunk_id)?;
        info!(?chunk_id, "deleted chunk metadata");
        Ok(())
    }

    pub fn reap_idle_data_files(&self) {
        self.files.reap_idle(now_ms());
    }

    pub async fn commit_bundle(
        &self,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<BundleMeta> {
        info!(?bundle_id, chunk_count = chunks.len(), "committing bundle");

        if chunks.is_empty() {
            return Err(Fs0Error::InvalidData {
                message: "bundle must contain at least one chunk".to_owned(),
            });
        }

        for (expected_index, chunk) in chunks.iter().enumerate() {
            if chunk.chunk_index != expected_index as u64 {
                return Err(Fs0Error::InvalidData {
                    message: "bundle chunk indexes must be contiguous".to_owned(),
                });
            }
        }

        let bundle = {
            let mut db = self.db.lock();
            for chunk in &chunks {
                let meta = db
                    .load_chunk(chunk.chunk_id)?
                    .ok_or(Fs0Error::ChunkNotFound {
                        chunk_id: chunk.chunk_id,
                    })?;
                debug!(
                    ?bundle_id,
                    ?chunk.chunk_id,
                    chunk_index = chunk.chunk_index,
                    raw_len = meta.raw_len,
                    compressed_len = meta.compressed_len,
                    "bundle references chunk"
                );
            }

            if bundle_hash_from_chunks(&chunks) != bundle_id {
                warn!(
                    ?bundle_id,
                    chunk_count = chunks.len(),
                    "bundle hash mismatch while committing bundle"
                );
                return Err(Fs0Error::InvalidData {
                    message: "bundle id does not match committed chunk ids".to_owned(),
                });
            }

            db.commit_bundle(bundle_id, &chunks)?
        };

        info!(
            ?bundle_id,
            raw_len = bundle.raw_len,
            compressed_len = bundle.compressed_len,
            chunk_count = bundle.chunk_count,
            "committed bundle"
        );
        Ok(bundle)
    }

    pub async fn list_bundle_chunks(&self, bundle_id: HashId) -> Fs0Result<Vec<BundleChunkRef>> {
        let chunks = self.db.lock().list_bundle_chunks(bundle_id)?;
        if chunks.is_empty() {
            return Err(Fs0Error::BundleNotFound { bundle_id });
        }
        Ok(chunks)
    }

    pub async fn delete_bundle(&self, bundle_id: HashId) -> Fs0Result<()> {
        self.db.lock().delete_bundle(bundle_id)?;
        info!(?bundle_id, "deleted bundle");
        Ok(())
    }

    pub async fn pending_central_events(&self, limit: usize) -> Fs0Result<Vec<BundleReplicaEvent>> {
        self.db.lock().pending_central_events(limit)
    }

    pub async fn reconciliation_events(&self, limit: usize) -> Fs0Result<Vec<BundleReplicaEvent>> {
        let (volume_id, bundle_ids) = {
            let db = self.db.lock();
            (db.meta().volume_id, db.list_bundle_ids(limit)?)
        };

        let mut events = Vec::new();
        for bundle_id in bundle_ids {
            match self.verify_bundle_bytes(bundle_id).await {
                Ok((raw_len, compressed_len)) => events.push(BundleReplicaEvent {
                    event_id: 0,
                    kind: fs0_core::BundleReplicaEventKind::Stored,
                    volume_id,
                    bundle_id,
                    raw_len: Some(raw_len),
                    compressed_len: Some(compressed_len),
                }),
                Err(_) => events.push(BundleReplicaEvent {
                    event_id: 0,
                    kind: fs0_core::BundleReplicaEventKind::Deleted,
                    volume_id,
                    bundle_id,
                    raw_len: None,
                    compressed_len: None,
                }),
            }
        }
        Ok(events)
    }

    pub async fn ack_pending_central_events(&self, max_event_id: u64) -> Fs0Result<()> {
        self.db
            .lock()
            .ack_pending_central_events(max_event_id)?;
        debug!(max_event_id, "acked pending central events");
        Ok(())
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        max_event_id: u64,
        failed_at_ms: u64,
    ) -> Fs0Result<()> {
        self.db
            .lock()
            .mark_pending_central_events_failed(max_event_id, failed_at_ms)?;
        warn!(
            max_event_id,
            failed_at_ms, "marked pending central events failed"
        );
        Ok(())
    }

    async fn write_compressed_bytes(&self, volume_offset: u64, bytes: Vec<u8>) -> Fs0Result<()> {
        let data_file_index = volume_offset / VOLUME_DEFAULT_DATA_FILE_SIZE;
        let data_file_offset = volume_offset % VOLUME_DEFAULT_DATA_FILE_SIZE;

        self.files
            .write_at(data_file_index, data_file_offset, bytes)
            .await
    }

    async fn read_compressed_bytes(
        &self,
        volume_offset: u64,
        compressed_len: u64,
    ) -> Fs0Result<Vec<u8>> {
        let data_file_index = volume_offset / VOLUME_DEFAULT_DATA_FILE_SIZE;
        let data_file_offset = volume_offset % VOLUME_DEFAULT_DATA_FILE_SIZE;
        let buffer_len =
            usize::try_from(compressed_len).map_err(|_| Fs0Error::IntegerConversion {
                message: format!("compressed len {compressed_len} exceeds usize"),
            })?;
        self.files
            .read_at(data_file_index, data_file_offset, buffer_len)
            .await
    }

    async fn verify_bundle_bytes(&self, bundle_id: HashId) -> Fs0Result<(u64, u64)> {
        let chunks = self.list_bundle_chunks(bundle_id).await?;
        let mut raw_len = 0u64;
        let mut compressed_len = 0u64;
        for chunk in chunks {
            let meta = self.chunk_meta(chunk.chunk_id).await?;
            let compressed = self
                .read_compressed_bytes(meta.volume_offset, meta.compressed_len)
                .await?;
            if compressed.len() as u64 != meta.compressed_len
                || blake3_hash(&compressed) != meta.compressed_hash
            {
                return Err(Fs0Error::HashMismatch {
                    volume_offset: meta.volume_offset,
                });
            }
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
        Ok((raw_len, compressed_len))
    }

}

fn validate_chunk(_chunk_id: HashId, raw_len: u64, compressed_bytes: &[u8]) -> Fs0Result<()> {
    if raw_len == 0 {
        return Err(Fs0Error::InvalidData {
            message: "raw_len must be greater than zero".to_owned(),
        });
    }
    if compressed_bytes.is_empty() {
        return Err(Fs0Error::InvalidData {
            message: "compressed_bytes cannot be empty".to_owned(),
        });
    }
    let compressed_len = compressed_bytes.len() as u64;
    if compressed_len > VOLUME_DEFAULT_DATA_FILE_SIZE {
        return Err(Fs0Error::InvalidData {
            message: format!(
                "compressed chunk length {compressed_len} exceeds data file size {VOLUME_DEFAULT_DATA_FILE_SIZE}"
            ),
        });
    }
    Ok(())
}
