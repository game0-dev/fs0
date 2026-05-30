use crate::volume::{BundleMeta, ChunkMeta, VolumeMeta};
use fs0_core::{
    Fs0Error, Fs0Result, HashId, SqliteRowExt, VOLUME_DB_FILE,
    protocol::{BundleChunkRef, BundleReplicaEvent, BundleReplicaEventKind},
    utils::u64_to_i64,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter, types::Type};
use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    path::Path,
};

#[derive(Debug)]
pub(crate) struct VolumeDb {
    conn: Connection,
    meta: VolumeMeta,
}

impl VolumeDb {
    pub(crate) fn create(root: &Path, meta: &VolumeMeta) -> Fs0Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let mut conn = Connection::open(db_path)?;

        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(include_str!("schema.sql"))?;

        let tx = conn.transaction()?;
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
        )?;
        tx.commit()?;

        Ok(Self {
            conn,
            meta: meta.clone(),
        })
    }

    pub(crate) fn open(root: &Path) -> Fs0Result<Self> {
        let db_path = root.join(VOLUME_DB_FILE);
        let conn = Connection::open(db_path)?;

        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

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

        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE volume_meta
             SET volume_id = ?1,
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;

        self.meta.volume_id = volume_id;
        self.meta.updated_at_ms = updated_at_ms;

        Ok(self.meta.clone())
    }

    fn load_meta_from_conn(conn: &Connection) -> Fs0Result<VolumeMeta> {
        let mut stmt = conn.prepare_cached(
            "SELECT volume_id, format_version, max_bytes, active_volume_offset,
                        created_at_ms, updated_at_ms
                 FROM volume_meta
                 WHERE id = 1",
        )?;

        Ok(stmt.query_row([], |row| {
            Ok(VolumeMeta {
                volume_id: row.u64(0, "volume_id")?,
                format_version: row.u64(1, "format_version")?,
                max_bytes: row.u64(2, "max_bytes")?,
                active_volume_offset: row.u64(3, "active_volume_offset")?,
                created_at_ms: row.u64(4, "created_at_ms")?,
                updated_at_ms: row.u64(5, "updated_at_ms")?,
            })
        })?)
    }

    pub(crate) fn load_chunk(&self, chunk_id: HashId) -> Fs0Result<Option<ChunkMeta>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT chunk_id, compressed_hash, volume_offset, raw_len, compressed_len
             FROM chunks
             WHERE chunk_id = ?1",
        )?;

        Ok(stmt
            .query_row(params![chunk_id.as_bytes().as_slice()], |row| {
                Ok(ChunkMeta {
                    chunk_id: row.hash_id(0, "chunk_id")?,
                    compressed_hash: row.hash_id(1, "compressed_hash")?,
                    volume_offset: row.u64(2, "volume_offset")?,
                    raw_len: row.u64(3, "raw_len")?,
                    compressed_len: row.u64(4, "compressed_len")?,
                })
            })
            .optional()?)
    }

    pub(crate) fn load_chunks_by_ids(
        &self,
        chunk_ids: &[HashId],
    ) -> Fs0Result<HashMap<HashId, ChunkMeta>> {
        if chunk_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders = std::iter::repeat_n("?", chunk_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT chunk_id, compressed_hash, volume_offset, raw_len, compressed_len
             FROM chunks
             WHERE chunk_id IN ({placeholders})"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let params = chunk_ids
            .iter()
            .map(|chunk_id| chunk_id.as_bytes().as_slice());
        let rows = stmt.query_map(params_from_iter(params), |row| {
            Ok(ChunkMeta {
                chunk_id: row.hash_id(0, "chunk_id")?,
                compressed_hash: row.hash_id(1, "compressed_hash")?,
                volume_offset: row.u64(2, "volume_offset")?,
                raw_len: row.u64(3, "raw_len")?,
                compressed_len: row.u64(4, "compressed_len")?,
            })
        })?;

        let mut chunks = HashMap::with_capacity(chunk_ids.len());
        for row in rows {
            let chunk = row?;
            chunks.insert(chunk.chunk_id, chunk);
        }

        Ok(chunks)
    }

    pub(crate) fn load_bundle(&self, bundle_id: HashId) -> Fs0Result<Option<BundleMeta>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT bc.bundle_id,
                    COALESCE(SUM(c.raw_len), 0),
                    COALESCE(SUM(c.compressed_len), 0),
                    COUNT(*)
             FROM bundle_chunks bc
             JOIN chunks c ON c.chunk_id = bc.chunk_id
             WHERE bc.bundle_id = ?1
             GROUP BY bc.bundle_id",
        )?;

        Ok(stmt
            .query_row(params![bundle_id.as_bytes().as_slice()], |row| {
                Ok(BundleMeta {
                    bundle_id: row.hash_id(0, "bundle_id")?,
                    raw_len: row.u64(1, "raw_len")?,
                    compressed_len: row.u64(2, "compressed_len")?,
                    chunk_count: row.u64(3, "chunk_count")?,
                })
            })
            .optional()?)
    }

    pub(crate) fn insert_chunk(&mut self, chunk: &ChunkMeta) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
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
        )?;
        tx.commit()?;

        Ok(())
    }

    pub(crate) fn reserve_active_volume_offset(
        &mut self,
        active_volume_offset: u64,
        updated_at_ms: u64,
    ) -> Fs0Result<VolumeMeta> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE volume_meta
             SET active_volume_offset = MAX(active_volume_offset, ?1),
                 updated_at_ms = ?2
             WHERE id = 1",
            params![
                u64_to_i64(active_volume_offset, "active volume offset")?,
                u64_to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        tx.commit()?;

        self.meta = Self::load_meta_from_conn(&self.conn)?;

        Ok(self.meta.clone())
    }

    pub(crate) fn delete_chunk(&mut self, chunk_id: HashId) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM chunks WHERE chunk_id = ?1",
            params![chunk_id.as_bytes().as_slice()],
        )?;
        tx.commit()?;

        Ok(())
    }

    pub(crate) fn commit_bundle(
        &mut self,
        bundle_id: HashId,
        chunks: &[BundleChunkRef],
    ) -> Fs0Result<BundleMeta> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM bundle_chunks WHERE bundle_id = ?1",
            params![bundle_id.as_bytes().as_slice()],
        )?;

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
            )?;
        }

        Self::insert_bundle_change_record(&tx, bundle_id, BundleReplicaEventKind::Stored)?;
        tx.commit()?;

        self.load_bundle(bundle_id)?
            .ok_or(Fs0Error::BundleNotFound { bundle_id })
    }

    pub(crate) fn delete_bundle(&mut self, bundle_id: HashId) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM bundle_chunks WHERE bundle_id = ?1",
            params![bundle_id.as_bytes().as_slice()],
        )?;

        Self::insert_bundle_change_record(&tx, bundle_id, BundleReplicaEventKind::Deleted)?;
        tx.commit()?;

        Ok(())
    }

    pub(crate) fn list_bundle_chunks(&self, bundle_id: HashId) -> Fs0Result<Vec<BundleChunkRef>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT chunk_index, chunk_id
             FROM bundle_chunks
             WHERE bundle_id = ?1
             ORDER BY chunk_index",
        )?;

        let rows = stmt.query_map(params![bundle_id.as_bytes().as_slice()], |row| {
            Ok(BundleChunkRef {
                chunk_index: row.u64(0, "chunk_index")?,
                chunk_id: row.hash_id(1, "chunk_id")?,
            })
        })?;

        let mut chunks = Vec::new();
        for row in rows {
            chunks.push(row?);
        }

        Ok(chunks)
    }

    pub(crate) fn get_bundle_change_records(
        &self,
        limit: usize,
    ) -> Fs0Result<Vec<BundleReplicaEvent>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.event_id, e.event_type, e.bundle_id,
                    COALESCE(SUM(c.raw_len), 0),
                    COALESCE(SUM(c.compressed_len), 0)
             FROM bundle_change_records e
             LEFT JOIN bundle_chunks bc
               ON bc.bundle_id = e.bundle_id
             LEFT JOIN chunks c ON c.chunk_id = bc.chunk_id
             GROUP BY e.event_id, e.event_type, e.bundle_id
             ORDER BY e.event_id
             LIMIT ?1",
        )?;

        let volume_id = self.meta.volume_id;
        let rows = stmt.query_map(params![u64_to_i64(limit as u64, "limit")?], |row| {
            let event_type: String = row.get(1)?;
            let kind = match event_type.as_str() {
                "bundle_stored" => BundleReplicaEventKind::Stored,
                "bundle_deleted" => BundleReplicaEventKind::Deleted,
                other => {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        1,
                        Type::Text,
                        Box::new(IoError::new(
                            ErrorKind::InvalidData,
                            format!("invalid bundle change record type: {other}"),
                        )),
                    ));
                }
            };

            Ok(BundleReplicaEvent {
                event_id: row.u64(0, "event_id")?,
                kind,
                volume_id,
                bundle_id: row.hash_id(2, "bundle_id")?,
                raw_len: row.optional_u64(3, "raw_len")?,
                compressed_len: row.optional_u64(4, "compressed_len")?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }

        Ok(events)
    }

    pub(crate) fn remove_bundle_change_records(&mut self, max_event_id: u64) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM bundle_change_records WHERE event_id <= ?1",
            params![u64_to_i64(max_event_id, "max_event_id")?],
        )?;
        tx.commit()?;

        Ok(())
    }

    fn insert_bundle_change_record(
        tx: &Transaction<'_>,
        bundle_id: HashId,
        kind: BundleReplicaEventKind,
    ) -> Fs0Result<()> {
        let event_type = match kind {
            BundleReplicaEventKind::Stored => "bundle_stored",
            BundleReplicaEventKind::Deleted => "bundle_deleted",
        };
        tx.execute(
            "INSERT INTO bundle_change_records (
                event_type, bundle_id
            ) VALUES (?1, ?2)
            ON CONFLICT(bundle_id) DO UPDATE SET
                event_type = ?1",
            params![event_type, bundle_id.as_bytes().as_slice()],
        )?;

        Ok(())
    }
}
