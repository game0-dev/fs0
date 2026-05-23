use crate::Result;
use crate::db::{InsertChunk, VolumeDb, to_usize};
use fs0_core::{BundleChunkRef, BundleReplicaEvent, Fs0Error, HashId};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub const RAW_CHUNK_SIZE: u64 = 512 * 1024;
pub const DATA_FILE_SIZE: u64 = 512 * 1024 * 1024;
pub const VOLUME_DB_FILE: &str = ".f0.volume.sqlite";
pub const VOLUME_FORMAT_VERSION: u64 = 1;

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

#[derive(Debug, Clone)]
pub struct Volume {
    root: PathBuf,
    state: Arc<Mutex<VolumeState>>,
    meta_cache: Arc<RwLock<VolumeMeta>>,
}

#[derive(Debug)]
struct VolumeState {
    db: VolumeDb,
    meta: VolumeMeta,
}

impl Volume {
    pub fn init(path: impl Into<PathBuf>, volume_id: u64, max_bytes: u64) -> Result<Self> {
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
            volume_id,
            format_version: VOLUME_FORMAT_VERSION,
            max_bytes,
            active_volume_offset: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let db = VolumeDb::create(&root, &meta)?;
        info!(
            volume_id,
            max_bytes,
            root = %root.display(),
            "initialized volume"
        );

        Ok(Self {
            root,
            state: Arc::new(Mutex::new(VolumeState {
                db,
                meta: meta.clone(),
            })),
            meta_cache: Arc::new(RwLock::new(meta)),
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let root = path.into();
        let db_path = root.join(VOLUME_DB_FILE);
        if !db_path.exists() {
            return Err(Fs0Error::VolumeNotFound {
                path: db_path.display().to_string(),
            });
        }

        let db = VolumeDb::open(&root)?;
        let meta = db.load_meta()?;
        info!(
            volume_id = meta.volume_id,
            max_bytes = meta.max_bytes,
            active_volume_offset = meta.active_volume_offset,
            root = %root.display(),
            "opened volume"
        );

        Ok(Self {
            root,
            state: Arc::new(Mutex::new(VolumeState {
                db,
                meta: meta.clone(),
            })),
            meta_cache: Arc::new(RwLock::new(meta)),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn meta(&self) -> VolumeMeta {
        self.meta_cache
            .read()
            .expect("volume meta cache lock poisoned")
            .clone()
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
            let mut state = self.state.lock().await;
            match state.db.load_chunk(chunk_id)? {
                Some(existing)
                    if existing.raw_len == raw_len && existing.compressed_len == compressed_len =>
                {
                    debug!(
                        ?chunk_id,
                        volume_offset = existing.volume_offset,
                        raw_len = existing.raw_len,
                        compressed_len = existing.compressed_len,
                        "chunk already exists"
                    );
                    return Ok(existing);
                }
                _ => {}
            }

            let mut volume_offset = state.meta.active_volume_offset;
            let offset_in_data_file = volume_offset % DATA_FILE_SIZE;
            if offset_in_data_file + compressed_len > DATA_FILE_SIZE {
                volume_offset = next_data_file_offset(volume_offset);
            }

            let next_active_offset =
                volume_offset.checked_add(compressed_len).ok_or_else(|| {
                    Fs0Error::IntegerConversion {
                        message: "volume offset overflow".to_owned(),
                    }
                })?;
            if next_active_offset > state.meta.max_bytes {
                warn!(
                    ?chunk_id,
                    required_end = next_active_offset,
                    max_bytes = state.meta.max_bytes,
                    "chunk write exceeds volume capacity"
                );
                return Err(Fs0Error::CapacityExceeded {
                    required_end: next_active_offset,
                    max_bytes: state.meta.max_bytes,
                });
            }

            state.meta.active_volume_offset = next_active_offset;
            state.meta.updated_at_ms = now_ms();
            *self
                .meta_cache
                .write()
                .expect("volume meta cache lock poisoned") = state.meta.clone();

            (
                InsertChunk {
                    chunk_id,
                    volume_offset,
                    raw_len,
                    compressed_len,
                },
                next_active_offset,
            )
        };

        if let Err(err) = self
            .write_compressed_bytes(insert.volume_offset, &compressed_bytes)
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

        let mut state = self.state.lock().await;
        let now = now_ms();
        state
            .db
            .insert_chunk_and_update_active_offset(&insert, next_active_offset, now)?;
        state.meta = state.db.load_meta()?;
        *self
            .meta_cache
            .write()
            .expect("volume meta cache lock poisoned") = state.meta.clone();

        let chunk = state
            .db
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
        let mut state = self.state.lock().await;
        let meta = state
            .db
            .persist_active_volume_offset(reserved_active_offset, now_ms())?;
        state.meta = meta;
        *self
            .meta_cache
            .write()
            .expect("volume meta cache lock poisoned") = state.meta.clone();
        Ok(())
    }

    pub async fn read_chunk(&self, chunk_id: HashId) -> Result<Vec<u8>> {
        debug!(?chunk_id, "reading chunk");
        let chunk = {
            let state = self.state.lock().await;
            state
                .db
                .load_chunk(chunk_id)?
                .ok_or(Fs0Error::ChunkNotFound { chunk_id })?
        };
        self.read_chunk_bytes(&chunk).await
    }

    pub async fn chunk_meta(&self, chunk_id: HashId) -> Result<ChunkMeta> {
        let state = self.state.lock().await;
        state
            .db
            .load_chunk(chunk_id)?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })
    }

    pub async fn delete_chunk(&self, chunk_id: HashId) -> Result<()> {
        let mut state = self.state.lock().await;
        state.db.delete_chunk(chunk_id)?;
        info!(?chunk_id, "deleted chunk metadata");
        Ok(())
    }

    pub async fn commit_bundle(
        &self,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Result<BundleMeta> {
        info!(?bundle_id, chunk_count = chunks.len(), "committing bundle");
        validate_bundle_chunks(&chunks)?;

        let chunk_metas = {
            let state = self.state.lock().await;
            let mut metas = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                let meta = state
                    .db
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
                metas.push(meta);
            }
            metas
        };

        let mut compressed_bytes = Vec::new();
        for chunk in &chunk_metas {
            compressed_bytes.extend(self.read_chunk_bytes(chunk).await?);
        }
        if fs0_core::blake3_hash(&compressed_bytes) != bundle_id {
            warn!(
                ?bundle_id,
                chunk_count = chunks.len(),
                "bundle hash mismatch while committing bundle"
            );
            return Err(Fs0Error::InvalidData {
                message: "bundle id does not match committed chunk bytes".to_owned(),
            });
        }

        let mut state = self.state.lock().await;
        let bundle = state.db.commit_bundle(bundle_id, &chunks)?;
        info!(
            ?bundle_id,
            raw_len = bundle.raw_len,
            compressed_len = bundle.compressed_len,
            chunk_count = bundle.chunk_count,
            "committed bundle"
        );
        Ok(bundle)
    }

    pub async fn bundle_meta(&self, bundle_id: HashId) -> Result<BundleMeta> {
        let state = self.state.lock().await;
        state
            .db
            .load_bundle(bundle_id)?
            .ok_or(Fs0Error::BundleNotFound { bundle_id })
    }

    pub async fn list_bundle_chunks(&self, bundle_id: HashId) -> Result<Vec<BundleChunkRef>> {
        let state = self.state.lock().await;
        let chunks = state.db.list_bundle_chunks(bundle_id)?;
        if chunks.is_empty() {
            return Err(Fs0Error::BundleNotFound { bundle_id });
        }
        Ok(chunks)
    }

    pub async fn read_bundle(&self, bundle_id: HashId) -> Result<Vec<u8>> {
        debug!(?bundle_id, "reading bundle");
        let chunks = self.list_bundle_chunks(bundle_id).await?;
        let mut bytes = Vec::new();
        for chunk in chunks {
            bytes.extend(self.read_chunk(chunk.chunk_id).await?);
        }
        if fs0_core::blake3_hash(&bytes) != bundle_id {
            warn!(?bundle_id, "bundle hash mismatch while reading bundle");
            return Err(Fs0Error::InvalidData {
                message: "bundle id does not match stored chunk bytes".to_owned(),
            });
        }
        Ok(bytes)
    }

    pub async fn delete_bundle(&self, bundle_id: HashId) -> Result<()> {
        let mut state = self.state.lock().await;
        state.db.delete_bundle(bundle_id)?;
        info!(?bundle_id, "deleted bundle");
        Ok(())
    }

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
        let state = self.state.lock().await;
        state.db.pending_central_events(limit)
    }

    pub async fn ack_pending_central_events(&self, event_ids: &[u64]) -> Result<()> {
        let mut state = self.state.lock().await;
        state.db.ack_pending_central_events(event_ids)?;
        debug!(
            event_count = event_ids.len(),
            "acked pending central events"
        );
        Ok(())
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        event_ids: &[u64],
        failed_at_ms: u64,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        state
            .db
            .mark_pending_central_events_failed(event_ids, failed_at_ms)?;
        warn!(
            event_count = event_ids.len(),
            failed_at_ms, "marked pending central events failed"
        );
        Ok(())
    }

    async fn read_chunk_bytes(&self, chunk: &ChunkMeta) -> Result<Vec<u8>> {
        let compressed_bytes = self
            .read_compressed_bytes(chunk.volume_offset, chunk.compressed_len)
            .await?;
        let actual_hash = fs0_core::blake3_hash(&compressed_bytes);
        if actual_hash != chunk.chunk_id {
            return Err(Fs0Error::HashMismatch {
                volume_offset: chunk.volume_offset,
            });
        }

        Ok(compressed_bytes)
    }

    async fn write_compressed_bytes(&self, volume_offset: u64, bytes: &[u8]) -> Result<()> {
        let (data_file_index, data_file_offset) = self.locate(volume_offset);
        let path = self.data_file_path(data_file_index);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)
            .await?;
        file.seek(SeekFrom::Start(data_file_offset)).await?;
        file.write_all(bytes).await?;
        file.sync_data().await?;
        Ok(())
    }

    async fn read_compressed_bytes(
        &self,
        volume_offset: u64,
        compressed_len: u64,
    ) -> Result<Vec<u8>> {
        let (data_file_index, data_file_offset) = self.locate(volume_offset);
        let path = self.data_file_path(data_file_index);
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(data_file_offset)).await?;
        let mut bytes = vec![0; to_usize(compressed_len, "compressed len")?];
        file.read_exact(&mut bytes).await?;
        Ok(bytes)
    }

    fn locate(&self, volume_offset: u64) -> (u64, u64) {
        (
            volume_offset / DATA_FILE_SIZE,
            volume_offset % DATA_FILE_SIZE,
        )
    }

    fn data_file_path(&self, data_file_index: u64) -> PathBuf {
        self.root.join(format!(".data.{data_file_index}"))
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

fn validate_chunk(chunk_id: HashId, raw_len: u64, compressed_bytes: &[u8]) -> Result<()> {
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
    let actual_hash = fs0_core::blake3_hash(compressed_bytes);
    if actual_hash != chunk_id {
        return Err(Fs0Error::InvalidData {
            message: "chunk id does not match compressed bytes".to_owned(),
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

fn next_data_file_offset(active_offset: u64) -> u64 {
    ((active_offset / DATA_FILE_SIZE) + 1) * DATA_FILE_SIZE
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
