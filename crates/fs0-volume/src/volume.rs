use crate::Result;
use crate::data_file_cache::DataFileCache;
use crate::db::{InsertChunk, VolumeDb};
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, DATA_FILE_SIZE, Fs0Error, HashId, VOLUME_FORMAT_VERSION,
    VOLUME_READ_CONCURRENCY, VOLUME_WRITE_CONCURRENCY, bundle_hash_from_chunks,
};
use parking_lot::{Mutex, MutexGuard};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
pub const VOLUME_DB_FILE: &str = ".f0.volume.sqlite";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeOptions {
    pub read_concurrency: usize,
    pub write_concurrency: usize,
}

impl Default for VolumeOptions {
    fn default() -> Self {
        Self {
            read_concurrency: VOLUME_READ_CONCURRENCY,
            write_concurrency: VOLUME_WRITE_CONCURRENCY,
        }
    }
}

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
    pub fn init(path: impl Into<PathBuf>, max_bytes: u64) -> Result<Self> {
        Self::init_with_options(path, max_bytes, VolumeOptions::default())
    }

    pub fn init_with_options(
        path: impl Into<PathBuf>,
        max_bytes: u64,
        options: VolumeOptions,
    ) -> Result<Self> {
        validate_options(max_bytes)?;
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
        let db = VolumeDb::create(&root, &meta)?;
        info!(
            max_bytes,
            root = %root.display(),
            "initialized volume"
        );

        Self::from_parts(root, db, options)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_options(path, VolumeOptions::default())
    }

    pub fn open_with_options(path: impl Into<PathBuf>, options: VolumeOptions) -> Result<Self> {
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

        Self::from_parts(root, db, options)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn meta(&self) -> VolumeMeta {
        self.acquire_db_lock().meta()
    }

    pub fn assign_volume_id(&self, volume_id: u64) -> Result<VolumeMeta> {
        let meta = self
            .acquire_db_lock()
            .assign_volume_id(volume_id, now_ms())?;
        info!(
            volume_id,
            root = %self.root.display(),
            "assigned volume id"
        );
        Ok(meta)
    }

    pub async fn put_chunk(
        &self,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        validate_chunk(chunk_id, raw_len, &compressed_bytes)?;

        let compressed_len = compressed_bytes.len() as u64;
        let (insert, next_active_offset) = {
            let mut db = self.acquire_db_lock();
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
            let offset_in_data_file = volume_offset % DATA_FILE_SIZE;
            if offset_in_data_file + compressed_len > DATA_FILE_SIZE {
                volume_offset = ((volume_offset / DATA_FILE_SIZE) + 1) * DATA_FILE_SIZE;
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
            self.persist_reserved_offset(next_active_offset).await?;
            return Err(err);
        }

        let mut db = self.acquire_db_lock();
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

    async fn persist_reserved_offset(&self, reserved_active_offset: u64) -> Result<()> {
        self.acquire_db_lock()
            .persist_active_volume_offset(reserved_active_offset, now_ms())?;
        Ok(())
    }

    pub async fn read_chunk(&self, chunk_id: HashId) -> Result<Vec<u8>> {
        debug!(?chunk_id, "reading chunk");
        let chunk = {
            let db = self.acquire_db_lock();
            db.load_chunk(chunk_id)?
                .ok_or(Fs0Error::ChunkNotFound { chunk_id })?
        };
        self.read_compressed_bytes(chunk.volume_offset, chunk.compressed_len)
            .await
    }

    pub async fn chunk_meta(&self, chunk_id: HashId) -> Result<ChunkMeta> {
        let db = self.acquire_db_lock();
        db.load_chunk(chunk_id)?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })
    }

    pub async fn delete_chunk(&self, chunk_id: HashId) -> Result<()> {
        self.acquire_db_lock().delete_chunk(chunk_id)?;
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
    ) -> Result<BundleMeta> {
        info!(?bundle_id, chunk_count = chunks.len(), "committing bundle");
        validate_bundle_chunks(&chunks)?;

        {
            let db = self.acquire_db_lock();
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
        };

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

        let bundle = self.acquire_db_lock().commit_bundle(bundle_id, &chunks)?;
        info!(
            ?bundle_id,
            raw_len = bundle.raw_len,
            compressed_len = bundle.compressed_len,
            chunk_count = bundle.chunk_count,
            "committed bundle"
        );
        Ok(bundle)
    }

    pub async fn list_bundle_chunks(&self, bundle_id: HashId) -> Result<Vec<BundleChunkRef>> {
        let chunks = self.acquire_db_lock().list_bundle_chunks(bundle_id)?;
        if chunks.is_empty() {
            return Err(Fs0Error::BundleNotFound { bundle_id });
        }
        Ok(chunks)
    }

    pub async fn delete_bundle(&self, bundle_id: HashId) -> Result<()> {
        self.acquire_db_lock().delete_bundle(bundle_id)?;
        info!(?bundle_id, "deleted bundle");
        Ok(())
    }

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
        self.acquire_db_lock().pending_central_events(limit)
    }

    pub async fn ack_pending_central_events(&self, max_event_id: u64) -> Result<()> {
        self.acquire_db_lock()
            .ack_pending_central_events(max_event_id)?;
        debug!(max_event_id, "acked pending central events");
        Ok(())
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        max_event_id: u64,
        failed_at_ms: u64,
    ) -> Result<()> {
        self.acquire_db_lock()
            .mark_pending_central_events_failed(max_event_id, failed_at_ms)?;
        warn!(
            max_event_id,
            failed_at_ms, "marked pending central events failed"
        );
        Ok(())
    }

    async fn write_compressed_bytes(&self, volume_offset: u64, bytes: Vec<u8>) -> Result<()> {
        let (data_file_index, data_file_offset) = self.locate(volume_offset);
        self.files
            .write_at(data_file_index, data_file_offset, bytes)
            .await
    }

    async fn read_compressed_bytes(
        &self,
        volume_offset: u64,
        compressed_len: u64,
    ) -> Result<Vec<u8>> {
        let (data_file_index, data_file_offset) = self.locate(volume_offset);
        let buffer_len =
            usize::try_from(compressed_len).map_err(|_| Fs0Error::IntegerConversion {
                message: format!("compressed len {compressed_len} exceeds usize"),
            })?;
        self.files
            .read_at(data_file_index, data_file_offset, buffer_len)
            .await
    }

    fn locate(&self, volume_offset: u64) -> (u64, u64) {
        (
            volume_offset / DATA_FILE_SIZE,
            volume_offset % DATA_FILE_SIZE,
        )
    }

    fn acquire_db_lock(&self) -> MutexGuard<'_, VolumeDb> {
        self.db.lock()
    }

    fn from_parts(root: PathBuf, db: VolumeDb, options: VolumeOptions) -> Result<Self> {
        let data_files =
            usize::try_from(db.meta().max_bytes.div_ceil(DATA_FILE_SIZE)).map_err(|_| {
                Fs0Error::IntegerConversion {
                    message: format!("volume max_bytes {} exceeds usize", db.meta().max_bytes),
                }
            })?;
        Ok(Self {
            files: DataFileCache::with_capacity(
                root.clone(),
                data_files,
                options.read_concurrency,
                options.write_concurrency,
            ),
            root,
            db: Mutex::new(db),
        })
    }
}

fn validate_options(max_bytes: u64) -> Result<()> {
    if max_bytes == 0 {
        return Err(Fs0Error::InvalidConfig {
            message: "max_bytes must be greater than zero".to_owned(),
        });
    }
    Ok(())
}

fn validate_chunk(_chunk_id: HashId, raw_len: u64, compressed_bytes: &[u8]) -> Result<()> {
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
    if compressed_len > DATA_FILE_SIZE {
        return Err(Fs0Error::InvalidData {
            message: format!(
                "compressed chunk length {compressed_len} exceeds data file size {DATA_FILE_SIZE}"
            ),
        });
    }
    Ok(())
}

fn validate_bundle_chunks(chunks: &[BundleChunkRef]) -> Result<()> {
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
    Ok(())
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
