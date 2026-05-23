use crate::Result;
use crate::volume::{BundleMeta, ChunkMeta, VOLUME_DB_FILE, VolumeMeta};
use fs0_core::Fs0Error;
use fs0_core::{BundleChunkRef, BundleReplicaEvent, BundleReplicaEventKind, HashId};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Debug)]
pub(crate) struct VolumeDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertChunk {
    pub chunk_id: HashId,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
}

trait SqliteResultExt<T> {
    fn fs0(self) -> Result<T>;
}

impl<T> SqliteResultExt<T> for rusqlite::Result<T> {
    fn fs0(self) -> Result<T> {
        self.map_err(sqlite_error)
    }
}

impl VolumeDb {
    pub(crate) fn create(root: &Path, meta: &VolumeMeta) -> Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let mut conn = Connection::open(db_path).fs0()?;
        Self::configure_connection(&conn)?;
        Self::create_schema(&conn)?;
        let tx = conn.transaction().fs0()?;
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
        )
        .fs0()?;
        tx.commit().fs0()?;
        Ok(Self { conn })
    }

    pub(crate) fn open(root: &Path) -> Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let conn = Connection::open(db_path).fs0()?;
        Self::configure_connection(&conn)?;
        Ok(Self { conn })
    }

    pub(crate) fn load_meta(&self) -> Result<VolumeMeta> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT volume_id, format_version, max_bytes, active_volume_offset,
                    created_at_ms, updated_at_ms
             FROM volume_meta
             WHERE id = 1",
            )
            .fs0()?;
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
        .fs0()
    }

    pub(crate) fn load_chunk(&self, chunk_id: HashId) -> Result<Option<ChunkMeta>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT chunk_id, volume_offset, raw_len, compressed_len
             FROM chunks
             WHERE chunk_id = ?1",
            )
            .fs0()?;
        stmt.query_row(
            params![chunk_id.as_bytes().as_slice()],
            Self::row_to_chunk_meta,
        )
        .optional()
        .fs0()
    }

    pub(crate) fn load_bundle(&self, bundle_id: HashId) -> Result<Option<BundleMeta>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bc.bundle_id,
                    COALESCE(SUM(c.raw_len), 0),
                    COALESCE(SUM(c.compressed_len), 0),
                    COUNT(*)
             FROM bundle_chunks bc
             JOIN chunks c ON c.chunk_id = bc.chunk_id
             WHERE bc.bundle_id = ?1
             GROUP BY bc.bundle_id",
            )
            .fs0()?;
        stmt.query_row(params![bundle_id.as_bytes().as_slice()], |row| {
            let bundle_id: Vec<u8> = row.get(0)?;
            Ok(BundleMeta {
                bundle_id: HashId(
                    Self::blob_to_hash(bundle_id, "bundle_id").map_err(Self::to_sql_error)?,
                ),
                raw_len: Self::from_i64(row.get(1)?, "raw_len").map_err(Self::to_sql_error)?,
                compressed_len: Self::from_i64(row.get(2)?, "compressed_len")
                    .map_err(Self::to_sql_error)?,
                chunk_count: Self::from_i64(row.get(3)?, "chunk_count")
                    .map_err(Self::to_sql_error)?,
            })
        })
        .optional()
        .fs0()
    }

    pub(crate) fn insert_chunk_and_update_active_offset(
        &mut self,
        chunk: &InsertChunk,
        active_volume_offset: u64,
        now_ms: u64,
    ) -> Result<()> {
        let tx = self.conn.transaction().fs0()?;
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
        )
        .fs0()?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                Self::to_i64(active_volume_offset, "active volume offset")?,
                Self::to_i64(now_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;
        Ok(())
    }

    pub(crate) fn persist_active_volume_offset(
        &mut self,
        active_volume_offset: u64,
        updated_at_ms: u64,
    ) -> Result<VolumeMeta> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                Self::to_i64(active_volume_offset, "active volume offset")?,
                Self::to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;
        self.load_meta()
    }

    pub(crate) fn delete_chunk(&mut self, chunk_id: HashId) -> Result<()> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "DELETE FROM chunks WHERE chunk_id = ?1",
            params![chunk_id.as_bytes().as_slice()],
        )
        .fs0()?;
        tx.commit().fs0()?;
        Ok(())
    }

    pub(crate) fn commit_bundle(
        &mut self,
        bundle_id: HashId,
        chunks: &[BundleChunkRef],
    ) -> Result<BundleMeta> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "DELETE FROM bundle_chunks WHERE bundle_id = ?1",
            params![bundle_id.as_bytes().as_slice()],
        )
        .fs0()?;
        for chunk in chunks {
            tx.execute(
                "INSERT INTO bundle_chunks (
                    bundle_id, chunk_index, chunk_id
                ) VALUES (?1, ?2, ?3)",
                params![
                    bundle_id.as_bytes().as_slice(),
                    Self::to_i64(chunk.chunk_index, "chunk_index")?,
                    chunk.chunk_id.as_bytes().as_slice(),
                ],
            )
            .fs0()?;
        }
        tx.execute(
            "INSERT INTO pending_central_events (
                event_type, bundle_id
            ) VALUES ('bundle_stored', ?1)
            ON CONFLICT(bundle_id) DO UPDATE SET
                event_type = 'bundle_stored',
                last_failed_at_ms = NULL",
            params![bundle_id.as_bytes().as_slice()],
        )
        .fs0()?;
        tx.commit().fs0()?;
        self.load_bundle(bundle_id)?
            .ok_or(Fs0Error::BundleNotFound { bundle_id })
    }

    pub(crate) fn delete_bundle(&mut self, bundle_id: HashId) -> Result<()> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "DELETE FROM bundle_chunks WHERE bundle_id = ?1",
            params![bundle_id.as_bytes().as_slice()],
        )
        .fs0()?;
        tx.execute(
            "INSERT INTO pending_central_events (
                event_type, bundle_id
            ) VALUES ('bundle_deleted', ?1)
            ON CONFLICT(bundle_id) DO UPDATE SET
                event_type = 'bundle_deleted',
                last_failed_at_ms = NULL",
            params![bundle_id.as_bytes().as_slice()],
        )
        .fs0()?;
        tx.commit().fs0()?;
        Ok(())
    }

    pub(crate) fn list_bundle_chunks(&self, bundle_id: HashId) -> Result<Vec<BundleChunkRef>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT chunk_index, chunk_id
             FROM bundle_chunks
             WHERE bundle_id = ?1
             ORDER BY chunk_index",
            )
            .fs0()?;
        let rows = stmt
            .query_map(params![bundle_id.as_bytes().as_slice()], |row| {
                let chunk_id: Vec<u8> = row.get(1)?;
                Ok(BundleChunkRef {
                    chunk_index: Self::from_i64(row.get(0)?, "chunk_index")
                        .map_err(Self::to_sql_error)?,
                    chunk_id: HashId(
                        Self::blob_to_hash(chunk_id, "chunk_id").map_err(Self::to_sql_error)?,
                    ),
                })
            })
            .fs0()?;
        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row.fs0()?);
        }
        Ok(chunks)
    }

    pub(crate) fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT e.event_id, e.event_type, e.bundle_id,
                    COALESCE(SUM(c.raw_len), 0),
                    COALESCE(SUM(c.compressed_len), 0)
             FROM pending_central_events e
             LEFT JOIN bundle_chunks bc
               ON bc.bundle_id = e.bundle_id
             LEFT JOIN chunks c ON c.chunk_id = bc.chunk_id
             GROUP BY e.event_id, e.event_type, e.bundle_id
             ORDER BY e.event_id
             LIMIT ?1",
            )
            .fs0()?;
        let volume_id = self.load_meta()?.volume_id;
        let rows = stmt
            .query_map(params![Self::to_i64(limit as u64, "limit")?], |row| {
                let event_type: String = row.get(1)?;
                let kind = match event_type.as_str() {
                    "bundle_stored" => BundleReplicaEventKind::Stored,
                    "bundle_deleted" => BundleReplicaEventKind::Deleted,
                    other => {
                        return Err(Self::to_sql_error(Fs0Error::InvalidData {
                            message: format!("invalid pending central event type: {other}"),
                        }));
                    }
                };
                let bundle_id: Vec<u8> = row.get(2)?;
                Ok(BundleReplicaEvent {
                    event_id: Self::from_i64(row.get(0)?, "event_id")
                        .map_err(Self::to_sql_error)?,
                    kind,
                    volume_id,
                    bundle_id: HashId(
                        Self::blob_to_hash(bundle_id, "bundle_id").map_err(Self::to_sql_error)?,
                    ),
                    raw_len: row
                        .get::<_, Option<i64>>(3)?
                        .map(|value| Self::from_i64(value, "raw_len"))
                        .transpose()
                        .map_err(Self::to_sql_error)?,
                    compressed_len: row
                        .get::<_, Option<i64>>(4)?
                        .map(|value| Self::from_i64(value, "compressed_len"))
                        .transpose()
                        .map_err(Self::to_sql_error)?,
                })
            })
            .fs0()?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row.fs0()?);
        }
        Ok(events)
    }

    pub(crate) fn mark_pending_central_events_failed(
        &mut self,
        event_ids: &[u64],
        failed_at_ms: u64,
    ) -> Result<()> {
        let tx = self.conn.transaction().fs0()?;
        for event_id in event_ids {
            tx.execute(
                "UPDATE pending_central_events
                 SET last_failed_at_ms = ?2
                 WHERE event_id = ?1",
                params![
                    Self::to_i64(*event_id, "event_id")?,
                    Self::to_i64(failed_at_ms, "last_failed_at_ms")?,
                ],
            )
            .fs0()?;
        }
        tx.commit().fs0()?;
        Ok(())
    }

    pub(crate) fn ack_pending_central_events(&mut self, event_ids: &[u64]) -> Result<()> {
        let tx = self.conn.transaction().fs0()?;
        for event_id in event_ids {
            tx.execute(
                "DELETE FROM pending_central_events WHERE event_id = ?1",
                params![Self::to_i64(*event_id, "event_id")?],
            )
            .fs0()?;
        }
        tx.commit().fs0()?;
        Ok(())
    }

    fn configure_connection(conn: &Connection) -> Result<()> {
        conn.pragma_update(None, "journal_mode", "DELETE").fs0()?;
        conn.pragma_update(None, "synchronous", "FULL").fs0()?;
        conn.pragma_update(None, "foreign_keys", "ON").fs0()?;
        Ok(())
    }

    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(include_str!("schema.sql")).fs0()?;
        Ok(())
    }

    fn row_to_chunk_meta(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChunkMeta> {
        let chunk_id: Vec<u8> = row.get(0)?;
        Ok(ChunkMeta {
            chunk_id: HashId(Self::blob_to_hash(chunk_id, "chunk_id").map_err(Self::to_sql_error)?),
            volume_offset: Self::from_i64(row.get(1)?, "volume_offset")
                .map_err(Self::to_sql_error)?,
            raw_len: Self::from_i64(row.get(2)?, "raw_len").map_err(Self::to_sql_error)?,
            compressed_len: Self::from_i64(row.get(3)?, "compressed_len")
                .map_err(Self::to_sql_error)?,
        })
    }

    fn blob_to_hash(value: Vec<u8>, name: &str) -> Result<[u8; 32]> {
        value
            .try_into()
            .map_err(|value: Vec<u8>| Fs0Error::InvalidData {
                message: format!("{name} must be 32 bytes, got {}", value.len()),
            })
    }

    fn to_i64(value: u64, name: &str) -> Result<i64> {
        i64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("{name} {value} exceeds i64"),
        })
    }

    fn from_i64(value: i64, name: &str) -> Result<u64> {
        u64::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("{name} {value} is negative"),
        })
    }

    fn to_sql_error(err: Fs0Error) -> rusqlite::Error {
        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
    }
}

pub(crate) fn to_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("{name} {value} exceeds usize"),
    })
}

fn sqlite_error(err: rusqlite::Error) -> Fs0Error {
    Fs0Error::Sqlite {
        message: err.to_string(),
    }
}
