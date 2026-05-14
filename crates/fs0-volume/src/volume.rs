use crate::db::{CommitFileState, InsertChunk, VolumeDb, to_usize};
use crate::error::{Result, VolumeError};
use fs0_core::ChunkId;
use fs0_core::blake3_hash;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

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
pub struct FileMeta {
    pub file_id: u64,
    pub version: u64,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub file_id: u64,
    pub chunk_index: u64,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
    pub hash: ChunkId,
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
            return Err(VolumeError::AlreadyExists(db_path));
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
            return Err(VolumeError::NotFound(db_path));
        }

        let db = VolumeDb::open(&root)?;
        let meta = db.load_meta()?;

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

    pub async fn file_meta(&self, file_id: u64) -> Result<FileMeta> {
        let state = self.state.lock().await;
        state
            .db
            .load_file_meta(file_id)?
            .ok_or(VolumeError::FileNotFound(file_id))
    }

    pub async fn put_chunk(
        &self,
        file_id: u64,
        chunk_index: u64,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        validate_chunk(raw_len, &compressed_bytes)?;

        let hash = blake3_hash(&compressed_bytes);
        let compressed_len = compressed_bytes.len() as u64;
        let (insert, next_active_offset) = {
            let mut state = self.state.lock().await;
            match state.db.load_chunk(file_id, chunk_index)? {
                Some(existing)
                    if existing.hash == hash
                        && existing.raw_len == raw_len
                        && existing.compressed_len == compressed_len =>
                {
                    return Ok(existing);
                }
                _ => {}
            }

            let mut volume_offset = state.meta.active_volume_offset;
            let offset_in_data_file = volume_offset % DATA_FILE_SIZE;
            if offset_in_data_file + compressed_len > DATA_FILE_SIZE {
                volume_offset = next_data_file_offset(volume_offset);
            }

            let next_active_offset = volume_offset
                .checked_add(compressed_len)
                .ok_or_else(|| VolumeError::IntegerConversion("volume offset overflow".into()))?;
            if next_active_offset > state.meta.max_bytes {
                return Err(VolumeError::CapacityExceeded {
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
                    file_id,
                    chunk_index,
                    volume_offset,
                    raw_len,
                    compressed_len,
                    hash,
                },
                next_active_offset,
            )
        };

        if let Err(err) = self
            .write_compressed_bytes(insert.volume_offset, &compressed_bytes)
            .await
        {
            self.persist_reserved_offset(next_active_offset).await?;
            return Err(err);
        }

        let mut state = self.state.lock().await;
        let now = now_ms();
        state
            .db
            .upsert_chunk_and_update_active_offset(&insert, next_active_offset, now)?;
        state.meta = state.db.load_meta()?;
        *self
            .meta_cache
            .write()
            .expect("volume meta cache lock poisoned") = state.meta.clone();

        state
            .db
            .load_chunk(file_id, chunk_index)?
            .ok_or(VolumeError::ChunkNotFound {
                file_id,
                chunk_index,
            })
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

    pub async fn read_chunk(&self, file_id: u64, chunk_index: u64) -> Result<Vec<u8>> {
        let chunk = {
            let state = self.state.lock().await;
            state
                .db
                .load_chunk(file_id, chunk_index)?
                .ok_or(VolumeError::ChunkNotFound {
                    file_id,
                    chunk_index,
                })?
        };
        self.read_chunk_bytes(&chunk).await
    }

    pub async fn get_chunks_meta(&self, file_id: u64, indexes: Vec<u64>) -> Result<Vec<ChunkMeta>> {
        let state = self.state.lock().await;
        state.db.load_chunks_by_indexes(file_id, &indexes)
    }

    pub async fn commit_file(
        &self,
        file_id: u64,
        version: u64,
        size_bytes: u64,
        compressed_size_bytes: u64,
    ) -> Result<FileMeta> {
        let mut state = self.state.lock().await;
        let created_at_ms = state
            .db
            .load_file_meta(file_id)?
            .map_or_else(now_ms, |file| file.created_at_ms);
        state.db.commit_file_state(
            &CommitFileState {
                file_id,
                version,
                size_bytes,
                compressed_size_bytes,
                updated_at_ms: now_ms(),
            },
            created_at_ms,
        )
    }

    pub async fn delete_file(&self, file_id: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.db.load_file_meta(file_id)?.is_none() {
            return Err(VolumeError::FileNotFound(file_id));
        }
        state.db.delete_file(file_id)
    }

    async fn read_chunk_bytes(&self, chunk: &ChunkMeta) -> Result<Vec<u8>> {
        let compressed_bytes = self
            .read_compressed_bytes(chunk.volume_offset, chunk.compressed_len)
            .await?;
        let actual_hash = blake3_hash(&compressed_bytes);
        if actual_hash != chunk.hash {
            return Err(VolumeError::HashMismatch {
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
        return Err(VolumeError::InvalidConfig(
            "max_bytes must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_chunk(raw_len: u64, compressed_bytes: &[u8]) -> Result<()> {
    if raw_len == 0 {
        return Err(VolumeError::InvalidChunk(
            "raw_len must be greater than zero".to_owned(),
        ));
    }
    if compressed_bytes.is_empty() {
        return Err(VolumeError::InvalidChunk(
            "compressed_bytes cannot be empty".to_owned(),
        ));
    }
    let compressed_len = compressed_bytes.len() as u64;
    if compressed_len > DATA_FILE_SIZE {
        return Err(VolumeError::InvalidChunk(format!(
            "compressed chunk length {compressed_len} exceeds data file size {DATA_FILE_SIZE}"
        )));
    }
    Ok(())
}

fn next_data_file_offset(active_offset: u64) -> u64 {
    ((active_offset / DATA_FILE_SIZE) + 1) * DATA_FILE_SIZE
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
