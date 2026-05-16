use crate::error::{Result, VolumeError};
use crate::volume::{ChunkMeta, VOLUME_DB_FILE, VolumeMeta};
use fs0_core::ChunkId;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Debug)]
pub(crate) struct VolumeDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertChunk {
    pub chunk_id: ChunkId,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
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

    pub(crate) fn load_chunk(&self, chunk_id: ChunkId) -> Result<Option<ChunkMeta>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT chunk_id, volume_offset, raw_len, compressed_len
             FROM chunks
             WHERE chunk_id = ?1",
        )?;
        stmt.query_row(
            params![chunk_id.as_bytes().as_slice()],
            Self::row_to_chunk_meta,
        )
        .optional()
        .map_err(VolumeError::from)
    }

    pub(crate) fn insert_chunk_and_update_active_offset(
        &mut self,
        chunk: &InsertChunk,
        active_volume_offset: u64,
        now_ms: u64,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO chunks (
                chunk_id, volume_offset, raw_len, compressed_len
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(chunk_id) DO NOTHING",
            params![
                chunk.chunk_id.as_bytes().as_slice(),
                Self::to_i64(chunk.volume_offset, "volume offset")?,
                Self::to_i64(chunk.raw_len, "raw len")?,
                Self::to_i64(chunk.compressed_len, "compressed len")?,
            ],
        )?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                Self::to_i64(active_volume_offset, "active volume offset")?,
                Self::to_i64(now_ms, "updated_at_ms")?,
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

    pub(crate) fn delete_chunk(&mut self, chunk_id: ChunkId) -> Result<()> {
        self.conn.execute(
            "DELETE FROM chunks WHERE chunk_id = ?1",
            params![chunk_id.as_bytes().as_slice()],
        )?;
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
        conn.execute_batch(include_str!("schema.sql"))?;
        Ok(())
    }

    fn row_to_chunk_meta(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChunkMeta> {
        let chunk_id: Vec<u8> = row.get(0)?;
        Ok(ChunkMeta {
            chunk_id: ChunkId(
                Self::blob_to_hash(chunk_id, "chunk_id").map_err(Self::to_sql_error)?,
            ),
            volume_offset: Self::from_i64(row.get(1)?, "volume_offset")
                .map_err(Self::to_sql_error)?,
            raw_len: Self::from_i64(row.get(2)?, "raw_len").map_err(Self::to_sql_error)?,
            compressed_len: Self::from_i64(row.get(3)?, "compressed_len")
                .map_err(Self::to_sql_error)?,
        })
    }

    fn blob_to_hash(value: Vec<u8>, name: &str) -> Result<[u8; 32]> {
        value.try_into().map_err(|value: Vec<u8>| {
            VolumeError::InvalidChunk(format!("{name} must be 32 bytes, got {}", value.len()))
        })
    }

    fn to_i64(value: u64, name: &str) -> Result<i64> {
        i64::try_from(value)
            .map_err(|_| VolumeError::IntegerConversion(format!("{name} {value} exceeds i64")))
    }

    fn from_i64(value: i64, name: &str) -> Result<u64> {
        u64::try_from(value)
            .map_err(|_| VolumeError::IntegerConversion(format!("{name} {value} is negative")))
    }

    fn to_sql_error(err: VolumeError) -> rusqlite::Error {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}

pub(crate) fn to_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| VolumeError::IntegerConversion(format!("{name} {value} exceeds usize")))
}
