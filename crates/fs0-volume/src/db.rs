use crate::error::{Result, VolumeError};
use crate::volume::{ChunkMeta, FileMeta, VOLUME_DB_FILE, VolumeMeta};
use fs0_core::ChunkId;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug)]
pub(crate) struct VolumeDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertChunk {
    pub file_id: u64,
    pub chunk_index: u64,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
    pub hash: ChunkId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitFileState {
    pub file_id: u64,
    pub version: u64,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub updated_at_ms: u64,
}

impl VolumeDb {
    pub(crate) fn create(root: &Path, meta: &VolumeMeta) -> Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let mut conn = Connection::open(db_path)?;
        Self::configure_connection(&conn)?;
        Self::create_schema(&conn)?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO volume_meta (
                id, volume_id, format_version, max_bytes, active_volume_offset,
                created_at_ms, updated_at_ms
            ) VALUES (
                1, ?1, ?2, ?3, ?4, ?5, ?6
            )",
            params![
                Self::to_i64(meta.volume_id, "volume_id")?,
                Self::to_i64(meta.format_version, "format_version")?,
                Self::to_i64(meta.max_bytes, "max_bytes")?,
                Self::to_i64(meta.active_volume_offset, "active_volume_offset")?,
                Self::to_i64(meta.created_at_ms, "created_at_ms")?,
                Self::to_i64(meta.updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;
        Ok(Self { conn })
    }

    pub(crate) fn open(root: &Path) -> Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let conn = Connection::open(db_path)?;
        Self::configure_connection(&conn)?;
        Ok(Self { conn })
    }

    pub(crate) fn load_meta(&self) -> Result<VolumeMeta> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT volume_id, format_version, max_bytes, active_volume_offset,
                    created_at_ms, updated_at_ms
             FROM volume_meta
             WHERE id = 1",
        )?;
        stmt.query_row([], |row| {
            Ok(VolumeMeta {
                volume_id: Self::from_i64(row.get(0)?, "volume_id").map_err(Self::to_sql_error)?,
                format_version: Self::from_i64(row.get(1)?, "format_version")
                    .map_err(Self::to_sql_error)?,
                max_bytes: Self::from_i64(row.get(2)?, "max_bytes").map_err(Self::to_sql_error)?,
                active_volume_offset: Self::from_i64(row.get(3)?, "active_volume_offset")
                    .map_err(Self::to_sql_error)?,
                created_at_ms: Self::from_i64(row.get(4)?, "created_at_ms")
                    .map_err(Self::to_sql_error)?,
                updated_at_ms: Self::from_i64(row.get(5)?, "updated_at_ms")
                    .map_err(Self::to_sql_error)?,
            })
        })
        .map_err(VolumeError::from)
    }

    pub(crate) fn load_file_meta(&self, file_id: u64) -> Result<Option<FileMeta>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, version, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE file_id = ?1",
        )?;
        stmt.query_row(
            params![Self::to_i64(file_id, "file_id")?],
            Self::row_to_file_meta,
        )
        .optional()
        .map_err(VolumeError::from)
    }

    pub(crate) fn load_chunk(&self, file_id: u64, chunk_index: u64) -> Result<Option<ChunkMeta>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, chunk_index, volume_offset, raw_len, compressed_len, hash
             FROM file_chunks
             WHERE file_id = ?1 AND chunk_index = ?2",
        )?;
        stmt.query_row(
            params![
                Self::to_i64(file_id, "file_id")?,
                Self::to_i64(chunk_index, "chunk index")?,
            ],
            Self::row_to_chunk_meta,
        )
        .optional()
        .map_err(VolumeError::from)
    }

    pub(crate) fn load_chunks_by_indexes(
        &self,
        file_id: u64,
        indexes: &[u64],
    ) -> Result<Vec<ChunkMeta>> {
        if indexes.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = vec!["?"; indexes.len()].join(", ");
        let sql = format!(
            "SELECT file_id, chunk_index, volume_offset, raw_len, compressed_len, hash
             FROM file_chunks
             WHERE file_id = ?
               AND chunk_index IN ({placeholders})"
        );
        let mut params = Vec::with_capacity(indexes.len() + 1);
        params.push(Self::to_i64(file_id, "file_id")?);
        for index in indexes {
            params.push(Self::to_i64(*index, "chunk index")?);
        }

        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(params), Self::row_to_chunk_meta)?;
        let mut chunks_by_index = HashMap::with_capacity(indexes.len());
        for row in rows {
            let chunk = row?;
            chunks_by_index.insert(chunk.chunk_index, chunk);
        }

        indexes
            .iter()
            .map(|index| {
                chunks_by_index
                    .get(index)
                    .cloned()
                    .ok_or(VolumeError::ChunkNotFound {
                        file_id,
                        chunk_index: *index,
                    })
            })
            .collect()
    }

    pub(crate) fn upsert_chunk_and_update_active_offset(
        &mut self,
        chunk: &InsertChunk,
        active_volume_offset: u64,
        updated_at_ms: u64,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO file_chunks (
                file_id, chunk_index, volume_offset,
                raw_len, compressed_len, hash
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(file_id, chunk_index) DO UPDATE SET
                volume_offset = excluded.volume_offset,
                raw_len = excluded.raw_len,
                compressed_len = excluded.compressed_len,
                hash = excluded.hash",
            params![
                Self::to_i64(chunk.file_id, "file_id")?,
                Self::to_i64(chunk.chunk_index, "chunk index")?,
                Self::to_i64(chunk.volume_offset, "volume offset")?,
                Self::to_i64(chunk.raw_len, "raw len")?,
                Self::to_i64(chunk.compressed_len, "compressed len")?,
                chunk.hash.as_bytes().as_slice(),
            ],
        )?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                Self::to_i64(active_volume_offset, "active volume offset")?,
                Self::to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn persist_active_volume_offset(
        &mut self,
        active_volume_offset: u64,
        updated_at_ms: u64,
    ) -> Result<VolumeMeta> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                Self::to_i64(active_volume_offset, "active volume offset")?,
                Self::to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;
        self.load_meta()
    }

    pub(crate) fn commit_file_state(
        &mut self,
        file: &CommitFileState,
        created_at_ms: u64,
    ) -> Result<FileMeta> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO files (
                file_id, version, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(file_id) DO UPDATE SET
                version = excluded.version,
                size_bytes = excluded.size_bytes,
                compressed_size_bytes = excluded.compressed_size_bytes,
                updated_at_ms = excluded.updated_at_ms",
            params![
                Self::to_i64(file.file_id, "file_id")?,
                Self::to_i64(file.version, "file version")?,
                Self::to_i64(file.size_bytes, "file size")?,
                Self::to_i64(file.compressed_size_bytes, "compressed size")?,
                Self::to_i64(created_at_ms, "created_at_ms")?,
                Self::to_i64(file.updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;

        self.load_file_meta(file.file_id)?
            .ok_or(VolumeError::FileNotFound(file.file_id))
    }

    pub(crate) fn delete_file(&mut self, file_id: u64) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM file_chunks WHERE file_id = ?1",
            params![Self::to_i64(file_id, "file_id")?],
        )?;
        tx.execute(
            "DELETE FROM files WHERE file_id = ?1",
            params![Self::to_i64(file_id, "file_id")?],
        )?;
        tx.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn insert_compacting_data_file(
        &self,
        data_file_index: u64,
        phase: &str,
        now_ms: u64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO compacting_data_files (
                data_file_index, phase, started_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?3)",
            params![
                Self::to_i64(data_file_index, "data_file_index")?,
                phase,
                Self::to_i64(now_ms, "now_ms")?,
            ],
        )?;
        Ok(())
    }

    fn configure_connection(conn: &Connection) -> Result<()> {
        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(())
    }

    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            CREATE TABLE volume_meta (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              volume_id INTEGER NOT NULL,
              format_version INTEGER NOT NULL,
              max_bytes INTEGER NOT NULL,
              active_volume_offset INTEGER NOT NULL,
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE files (
              file_id INTEGER PRIMARY KEY,
              version INTEGER NOT NULL,
              size_bytes INTEGER NOT NULL,
              compressed_size_bytes INTEGER NOT NULL,
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL
            );

            CREATE TABLE file_chunks (
              file_id INTEGER NOT NULL,
              chunk_index INTEGER NOT NULL,
              volume_offset INTEGER NOT NULL,
              raw_len INTEGER NOT NULL,
              compressed_len INTEGER NOT NULL,
              hash BLOB NOT NULL,
              PRIMARY KEY (file_id, chunk_index)
            );

            CREATE INDEX idx_file_chunks_volume_offset ON file_chunks(volume_offset);

            CREATE TABLE compacting_data_files (
              id INTEGER PRIMARY KEY,
              data_file_index INTEGER NOT NULL,
              phase TEXT NOT NULL,
              started_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL,
              finished_at_ms INTEGER,
              result TEXT
            );
            ",
        )?;
        Ok(())
    }

    fn row_to_file_meta(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileMeta> {
        Ok(FileMeta {
            file_id: Self::from_i64(row.get(0)?, "file_id").map_err(Self::to_sql_error)?,
            version: Self::from_i64(row.get(1)?, "version").map_err(Self::to_sql_error)?,
            size_bytes: Self::from_i64(row.get(2)?, "size_bytes").map_err(Self::to_sql_error)?,
            compressed_size_bytes: Self::from_i64(row.get(3)?, "compressed_size_bytes")
                .map_err(Self::to_sql_error)?,
            created_at_ms: Self::from_i64(row.get(4)?, "created_at_ms")
                .map_err(Self::to_sql_error)?,
            updated_at_ms: Self::from_i64(row.get(5)?, "updated_at_ms")
                .map_err(Self::to_sql_error)?,
        })
    }

    fn row_to_chunk_meta(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChunkMeta> {
        let hash_bytes: Vec<u8> = row.get(5)?;
        let hash: [u8; 32] = hash_bytes.try_into().map_err(|bytes: Vec<u8>| {
            rusqlite::Error::FromSqlConversionFailure(
                bytes.len(),
                rusqlite::types::Type::Blob,
                Box::new(VolumeError::InvalidConfig(
                    "chunk hash must be 32 bytes".to_owned(),
                )),
            )
        })?;

        Ok(ChunkMeta {
            file_id: Self::from_i64(row.get(0)?, "file_id").map_err(Self::to_sql_error)?,
            chunk_index: Self::from_i64(row.get(1)?, "chunk_index").map_err(Self::to_sql_error)?,
            volume_offset: Self::from_i64(row.get(2)?, "volume_offset")
                .map_err(Self::to_sql_error)?,
            raw_len: Self::from_i64(row.get(3)?, "raw_len").map_err(Self::to_sql_error)?,
            compressed_len: Self::from_i64(row.get(4)?, "compressed_len")
                .map_err(Self::to_sql_error)?,
            hash: ChunkId(hash),
        })
    }

    fn to_i64(value: u64, name: &str) -> Result<i64> {
        i64::try_from(value).map_err(|_| {
            VolumeError::IntegerConversion(format!("{name} value {value} does not fit in i64"))
        })
    }

    fn from_i64(value: i64, name: &str) -> Result<u64> {
        u64::try_from(value).map_err(|_| {
            VolumeError::IntegerConversion(format!("{name} value {value} is negative"))
        })
    }

    fn to_sql_error(err: VolumeError) -> rusqlite::Error {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}

pub(crate) fn to_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        VolumeError::IntegerConversion(format!("{name} value {value} does not fit in usize"))
    })
}
