use crate::volume::{BundleMeta, ChunkMeta, VolumeMeta};
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, BundleReplicaEventKind, Fs0Error, Fs0Result, HashId,
    VOLUME_DB_FILE, hash_id_from_vec, i64_to_u64, u64_to_i64,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Debug)]
pub(crate) struct VolumeDb {
    conn: Connection,
    meta: VolumeMeta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertChunk {
    pub chunk_id: HashId,
    pub compressed_hash: HashId,
    pub volume_offset: u64,
    pub raw_len: u64,
    pub compressed_len: u64,
}

trait SqliteResultExt<T> {
    fn fs0(self) -> Fs0Result<T>;
}

impl<T> SqliteResultExt<T> for rusqlite::Result<T> {
    fn fs0(self) -> Fs0Result<T> {
        self.map_err(|err| Fs0Error::Sqlite {
            message: err.to_string(),
        })
    }
}

impl VolumeDb {
    pub(crate) fn create(root: &Path, meta: &VolumeMeta) -> Fs0Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let mut conn = Connection::open(db_path).fs0()?;

        conn.pragma_update(None, "journal_mode", "DELETE").fs0()?;
        conn.pragma_update(None, "synchronous", "FULL").fs0()?;
        conn.pragma_update(None, "foreign_keys", "ON").fs0()?;
        conn.execute_batch(include_str!("schema.sql")).fs0()?;

        let tx = conn.transaction().fs0()?;
        tx.execute(
            "INSERT INTO volume_meta (
                id, volume_id, format_version, max_bytes, active_volume_offset,
                created_at_ms, updated_at_ms
            ) VALUES (
                1, ?1, ?2, ?3, ?4, ?5, ?6
            )",
            params![
                u64_to_i64(meta.volume_id, "volume_id")?,
                u64_to_i64(meta.format_version, "format_version")?,
                u64_to_i64(meta.max_bytes, "max_bytes")?,
                u64_to_i64(meta.active_volume_offset, "active_volume_offset")?,
                u64_to_i64(meta.created_at_ms, "created_at_ms")?,
                u64_to_i64(meta.updated_at_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;

        Ok(Self {
            conn,
            meta: meta.clone(),
        })
    }

    pub(crate) fn open(root: &Path) -> Fs0Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let conn = Connection::open(db_path).fs0()?;

        conn.pragma_update(None, "journal_mode", "DELETE").fs0()?;
        conn.pragma_update(None, "synchronous", "FULL").fs0()?;
        conn.pragma_update(None, "foreign_keys", "ON").fs0()?;

        let meta = Self::load_meta_from_conn(&conn)?;

        Ok(Self { conn, meta })
    }

    pub(crate) fn meta(&self) -> VolumeMeta {
        self.meta.clone()
    }

    pub(crate) fn assign_volume_id(
        &mut self,
        volume_id: u64,
        updated_at_ms: u64,
    ) -> Fs0Result<VolumeMeta> {
        if self.meta.volume_id != 0 && self.meta.volume_id != volume_id {
            return Err(Fs0Error::InvalidData {
                message: format!(
                    "volume already has id {}, cannot assign {volume_id}",
                    self.meta.volume_id
                ),
            });
        }

        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "UPDATE volume_meta
             SET volume_id = ?1,
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;

        self.meta.volume_id = volume_id;
        self.meta.updated_at_ms = updated_at_ms;

        Ok(self.meta.clone())
    }

    fn load_meta_from_conn(conn: &Connection) -> Fs0Result<VolumeMeta> {
        let mut stmt = conn
            .prepare_cached(
                "SELECT volume_id, format_version, max_bytes, active_volume_offset,
                        created_at_ms, updated_at_ms
                 FROM volume_meta
                 WHERE id = 1",
            )
            .fs0()?;

        stmt.query_row([], |row| {
            Ok(VolumeMeta {
                volume_id: i64_to_u64(row.get(0)?, "volume_id")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                format_version: i64_to_u64(row.get(1)?, "format_version")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                max_bytes: i64_to_u64(row.get(2)?, "max_bytes")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                active_volume_offset: i64_to_u64(row.get(3)?, "active_volume_offset")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                created_at_ms: i64_to_u64(row.get(4)?, "created_at_ms")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                updated_at_ms: i64_to_u64(row.get(5)?, "updated_at_ms")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
            })
        })
        .fs0()
    }

    pub(crate) fn load_chunk(&self, chunk_id: HashId) -> Fs0Result<Option<ChunkMeta>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT chunk_id, compressed_hash, volume_offset, raw_len, compressed_len
             FROM chunks
             WHERE chunk_id = ?1",
            )
            .fs0()?;

        stmt.query_row(params![chunk_id.as_bytes().as_slice()], |row| {
            let chunk_id: Vec<u8> = row.get(0)?;
            let compressed_hash: Vec<u8> = row.get(1)?;

            Ok(ChunkMeta {
                chunk_id: hash_id_from_vec(chunk_id)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                compressed_hash: hash_id_from_vec(compressed_hash)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                volume_offset: i64_to_u64(row.get(2)?, "volume_offset")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                raw_len: i64_to_u64(row.get(3)?, "raw_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                compressed_len: i64_to_u64(row.get(4)?, "compressed_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
            })
        })
        .optional()
        .fs0()
    }

    pub(crate) fn load_bundle(&self, bundle_id: HashId) -> Fs0Result<Option<BundleMeta>> {
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
                bundle_id: hash_id_from_vec(bundle_id)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                raw_len: i64_to_u64(row.get(1)?, "raw_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                compressed_len: i64_to_u64(row.get(2)?, "compressed_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                chunk_count: i64_to_u64(row.get(3)?, "chunk_count")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
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
    ) -> Fs0Result<()> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "INSERT INTO chunks (
                chunk_id, compressed_hash, volume_offset, raw_len, compressed_len
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(chunk_id) DO NOTHING",
            params![
                chunk.chunk_id.as_bytes().as_slice(),
                chunk.compressed_hash.as_bytes().as_slice(),
                u64_to_i64(chunk.volume_offset, "volume offset")?,
                u64_to_i64(chunk.raw_len, "raw len")?,
                u64_to_i64(chunk.compressed_len, "compressed len")?,
            ],
        )
        .fs0()?;

        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                u64_to_i64(active_volume_offset, "active volume offset")?,
                u64_to_i64(now_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;

        self.meta.active_volume_offset = self.meta.active_volume_offset.max(active_volume_offset);
        self.meta.updated_at_ms = now_ms;

        Ok(())
    }

    pub(crate) fn persist_active_volume_offset(
        &mut self,
        active_volume_offset: u64,
        updated_at_ms: u64,
    ) -> Fs0Result<VolumeMeta> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                u64_to_i64(active_volume_offset, "active volume offset")?,
                u64_to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;

        self.meta = Self::load_meta_from_conn(&self.conn)?;

        Ok(self.meta.clone())
    }

    pub(crate) fn delete_chunk(&mut self, chunk_id: HashId) -> Fs0Result<()> {
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
    ) -> Fs0Result<BundleMeta> {
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
                    u64_to_i64(chunk.chunk_index, "chunk_index")?,
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

    pub(crate) fn delete_bundle(&mut self, bundle_id: HashId) -> Fs0Result<()> {
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

    pub(crate) fn list_bundle_chunks(&self, bundle_id: HashId) -> Fs0Result<Vec<BundleChunkRef>> {
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
                    chunk_index: i64_to_u64(row.get(0)?, "chunk_index")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    chunk_id: hash_id_from_vec(chunk_id)
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                })
            })
            .fs0()?;

        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row.fs0()?);
        }

        Ok(chunks)
    }

    pub(crate) fn list_bundle_ids(&self, limit: usize) -> Fs0Result<Vec<HashId>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bundle_id
                 FROM bundle_chunks
                 GROUP BY bundle_id
                 ORDER BY bundle_id
                 LIMIT ?1",
            )
            .fs0()?;

        let rows = stmt
            .query_map(params![u64_to_i64(limit as u64, "limit")?], |row| {
                let bundle_id: Vec<u8> = row.get(0)?;

                hash_id_from_vec(bundle_id)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
            })
            .fs0()?;

        let mut bundle_ids = Vec::new();
        for row in rows {
            bundle_ids.push(row.fs0()?);
        }

        Ok(bundle_ids)
    }

    pub(crate) fn pending_central_events(
        &self,
        limit: usize,
    ) -> Fs0Result<Vec<BundleReplicaEvent>> {
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

        let volume_id = self.meta.volume_id;
        let rows = stmt
            .query_map(params![u64_to_i64(limit as u64, "limit")?], |row| {
                let event_type: String = row.get(1)?;
                let kind = match event_type.as_str() {
                    "bundle_stored" => BundleReplicaEventKind::Stored,
                    "bundle_deleted" => BundleReplicaEventKind::Deleted,
                    other => {
                        return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                            Fs0Error::InvalidData {
                                message: format!("invalid pending central event type: {other}"),
                            },
                        )));
                    }
                };
                let bundle_id: Vec<u8> = row.get(2)?;

                Ok(BundleReplicaEvent {
                    event_id: i64_to_u64(row.get(0)?, "event_id")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    kind,
                    volume_id,
                    bundle_id: hash_id_from_vec(bundle_id)
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    raw_len: row
                        .get::<_, Option<i64>>(3)?
                        .map(|value| i64_to_u64(value, "raw_len"))
                        .transpose()
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    compressed_len: row
                        .get::<_, Option<i64>>(4)?
                        .map(|value| i64_to_u64(value, "compressed_len"))
                        .transpose()
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
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
        max_event_id: u64,
        failed_at_ms: u64,
    ) -> Fs0Result<()> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "UPDATE pending_central_events
             SET last_failed_at_ms = ?2
             WHERE event_id <= ?1",
            params![
                u64_to_i64(max_event_id, "max_event_id")?,
                u64_to_i64(failed_at_ms, "last_failed_at_ms")?,
            ],
        )
        .fs0()?;
        tx.commit().fs0()?;

        Ok(())
    }

    pub(crate) fn ack_pending_central_events(&mut self, max_event_id: u64) -> Fs0Result<()> {
        let tx = self.conn.transaction().fs0()?;
        tx.execute(
            "DELETE FROM pending_central_events WHERE event_id <= ?1",
            params![u64_to_i64(max_event_id, "max_event_id")?],
        )
        .fs0()?;
        tx.commit().fs0()?;

        Ok(())
    }
}
