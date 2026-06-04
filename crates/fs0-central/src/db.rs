use crate::{Fs0Result, file_catalog::FileCatalog};
use fs0_core::{
    APPEND_LEASE_TTL_MS, Fs0Error, HashId, SqliteRowExt, VOLUME_BUNDLE_RAW_SIZE,
    protocol::{
        AppendLease, BeginAppendRequest, BundleReplicaEvent, BundleReplicaEventKind,
        CommitAppendRequest, CommittedBundle, DirectoryEntries, FileBundleRef, FileChangeLog,
        FileChangeLogKind, FileChangeLogs, FileReadPlan, FileRecord, StorageVolumeInfo,
    },
    utils::{i64_to_u64, join_fs0_path, now_ms, split_fs0_path_dir_and_name, u64_to_i64},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Debug)]
struct LeaseRecord {
    file_id: u64,
    base_size_bytes: u64,
    offset_bytes: u64,
}

#[derive(Debug)]
struct BundleTotals {
    file_bundle_count: u64,
    ready_bundle_count: u64,
    metadata_conflict_count: u64,
    raw_size_bytes: u64,
    compressed_size_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct CentralDb {
    conn: Connection,
    files: FileCatalog,
}
impl CentralDb {
    pub(crate) fn open(path: impl AsRef<Path>) -> Fs0Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(include_str!("schema.sql"))?;

        Ok(Self {
            conn,
            files: FileCatalog::new(),
        })
    }

    pub(crate) fn create_volume(
        &mut self,
        name: String,
        max_bytes: u64,
    ) -> Fs0Result<StorageVolumeInfo> {
        self.conn.execute(
            "INSERT INTO volumes (name, max_bytes, max_volume_offset)
             VALUES (?1, ?2, 0)",
            params![name.as_str(), u64_to_i64(max_bytes, "max_bytes")?],
        )?;
        let volume_id = i64_to_u64(self.conn.last_insert_rowid(), "volume_id")?;
        self.get_volume(volume_id)
    }

    pub(crate) fn get_volume(&self, volume_id: u64) -> Fs0Result<StorageVolumeInfo> {
        self.conn
            .query_row(
                "SELECT volume_id, name, max_bytes, max_volume_offset
                 FROM volumes
                 WHERE volume_id = ?1",
                params![u64_to_i64(volume_id, "volume_id")?],
                row_to_storage_volume_info,
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn update_volume_offset(
        &mut self,
        volume_id: u64,
        max_volume_offset: u64,
    ) -> Fs0Result<StorageVolumeInfo> {
        self.conn.execute(
            "UPDATE volumes
             SET max_volume_offset = ?2
             WHERE volume_id = ?1",
            params![
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(max_volume_offset, "max_volume_offset")?,
            ],
        )?;
        if self.conn.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }

        self.get_volume(volume_id)
    }

    pub(crate) fn begin_append(
        &mut self,
        request: BeginAppendRequest,
        volume_id: u64,
    ) -> Fs0Result<AppendLease> {
        let now = now_ms();
        let expires_at_ms = now + APPEND_LEASE_TTL_MS;
        let tx = self.conn.transaction()?;
        let (file_id, base_size) = match self.files.get_file_by_path(&tx, &request.path) {
            Ok(file) => {
                if request.offset > file.size_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
                (file.file_id, file.size_bytes)
            }
            Err(Fs0Error::NotFound) => {
                if request.offset != 0 {
                    return Err(Fs0Error::NotFound);
                }
                (self.files.create_file_at_path(&tx, &request.path, now)?, 0)
            }
            Err(err) => return Err(err),
        };

        // 清理掉所有过期的 append lease
        tx.execute(
            "DELETE FROM append_leases
             WHERE expires_at_ms <= ?1",
            params![u64_to_i64(now, "expires_at_ms")?],
        )?;

        let active_lease = tx
            .query_row(
                "SELECT lease_id
                 FROM append_leases
                 WHERE file_id = ?1
                 LIMIT 1",
                params![u64_to_i64(file_id, "file_id")?],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if active_lease.is_some() {
            return Err(Fs0Error::AlreadyExists { path: request.path });
        }

        tx.execute(
            "INSERT INTO append_leases (
                file_id, volume_id, base_size_bytes,
                offset_bytes, prefer_volume_name, expires_at_ms, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(base_size, "base_size_bytes")?,
                u64_to_i64(request.offset, "offset_bytes")?,
                request.prefer_volume_name.as_deref(),
                u64_to_i64(expires_at_ms, "expires_at_ms")?,
                u64_to_i64(now, "created_at_ms")?,
            ],
        )?;
        let lease_id = i64_to_u64(tx.last_insert_rowid(), "lease_id")?;
        tx.commit()?;

        Ok(AppendLease {
            lease_id,
            file_id,
            volume_id,
            base_size,
            offset: request.offset,
            expires_at_ms,
            prefer_volume_name: request.prefer_volume_name,
        })
    }

    pub(crate) fn commit_append(
        &mut self,
        request: CommitAppendRequest,
    ) -> Fs0Result<FileReadPlan> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let lease = Self::get_active_lease(&tx, request.lease_id, request.file_id)?;

        if lease.base_size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict);
        }
        if request.new_size < lease.offset_bytes {
            return Err(Fs0Error::InvalidRequest);
        }

        let file = self.files.get_file_row_by_id(&tx, lease.file_id)?;
        if file.size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict);
        }

        let first_bundle_index = lease.offset_bytes / VOLUME_BUNDLE_RAW_SIZE;
        let prefix_totals = Self::file_bundle_totals(&tx, lease.file_id, Some(first_bundle_index))?;
        Self::validate_bundle_totals_ready(&prefix_totals)?;

        let (submitted_raw_size_bytes, _) = submitted_bundle_totals(&request.bundles)?;
        let first_bundle_index_usize =
            usize::try_from(first_bundle_index).map_err(|_| Fs0Error::IntegerConversion {
                message: format!("first_bundle_index {first_bundle_index} exceeds usize"),
            })?;
        let bundles_to_insert = if submitted_raw_size_bytes == request.new_size {
            let submitted_prefix = request
                .bundles
                .get(..first_bundle_index_usize)
                .ok_or(Fs0Error::InvalidRequest)?;
            let (submitted_prefix_raw, submitted_prefix_compressed) =
                submitted_bundle_totals(submitted_prefix)?;
            if submitted_prefix_raw != prefix_totals.raw_size_bytes
                || submitted_prefix_compressed != prefix_totals.compressed_size_bytes
            {
                return Err(Fs0Error::InvalidRequest);
            }

            request
                .bundles
                .get(first_bundle_index_usize..)
                .ok_or(Fs0Error::InvalidRequest)?
        } else {
            let suffix_size_bytes = prefix_totals
                .raw_size_bytes
                .checked_add(submitted_raw_size_bytes)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "committed bundle raw size overflow".to_owned(),
                })?;
            if suffix_size_bytes != request.new_size {
                return Err(Fs0Error::InvalidRequest);
            }

            request.bundles.as_slice()
        };

        tx.execute(
            "DELETE FROM file_bundles
             WHERE file_id = ?1 AND bundle_index >= ?2",
            params![
                u64_to_i64(lease.file_id, "file_id")?,
                u64_to_i64(first_bundle_index, "first_bundle_index")?,
            ],
        )?;
        let mut bundle_index = first_bundle_index;
        for bundle in bundles_to_insert {
            let (min_raw_len, max_raw_len, min_compressed_len, max_compressed_len, replica_count) =
                tx.query_row(
                    "SELECT MIN(raw_len), MAX(raw_len),
                        MIN(compressed_len), MAX(compressed_len),
                        COUNT(*)
                 FROM bundle_replicas
                 WHERE bundle_id = ?1",
                    params![bundle.bundle_id.as_bytes().as_slice()],
                    |row| {
                        Ok((
                            row.optional_u64(0, "min_raw_len")?,
                            row.optional_u64(1, "max_raw_len")?,
                            row.optional_u64(2, "min_compressed_len")?,
                            row.optional_u64(3, "max_compressed_len")?,
                            row.u64(4, "replica_count")?,
                        ))
                    },
                )?;
            if replica_count == 0 {
                return Err(Fs0Error::ChunkNotReady);
            }
            let (
                Some(stored_raw_len),
                Some(max_raw_len),
                Some(stored_compressed_len),
                Some(max_compressed_len),
            ) = (
                min_raw_len,
                max_raw_len,
                min_compressed_len,
                max_compressed_len,
            )
            else {
                return Err(Fs0Error::ChunkNotReady);
            };
            if stored_raw_len != max_raw_len || stored_compressed_len != max_compressed_len {
                return Err(Fs0Error::InvalidData {
                    message: "bundle replica metadata conflict".to_owned(),
                });
            }
            if stored_raw_len != bundle.raw_len || stored_compressed_len != bundle.compressed_len {
                return Err(Fs0Error::InvalidRequest);
            }
            tx.execute(
                "INSERT INTO file_bundles (
                    file_id, bundle_index, bundle_id
                 ) VALUES (?1, ?2, ?3)",
                params![
                    u64_to_i64(lease.file_id, "file_id")?,
                    u64_to_i64(bundle_index, "bundle_index")?,
                    bundle.bundle_id.as_bytes().as_slice(),
                ],
            )?;
            bundle_index =
                bundle_index
                    .checked_add(1)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle_index overflow".to_owned(),
                    })?;
        }
        let final_totals = Self::file_bundle_totals(&tx, lease.file_id, None)?;
        Self::validate_bundle_totals_ready(&final_totals)?;
        if final_totals.raw_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }
        let compressed_size_bytes = final_totals.compressed_size_bytes;

        tx.execute(
            "UPDATE files
             SET size_bytes = ?2,
                 compressed_size_bytes = ?3,
                 updated_at_ms = ?4
             WHERE file_id = ?1",
            params![
                u64_to_i64(lease.file_id, "file_id")?,
                u64_to_i64(request.new_size, "size_bytes")?,
                u64_to_i64(compressed_size_bytes, "compressed_size_bytes")?,
                u64_to_i64(now, "updated_at_ms")?,
            ],
        )?;
        tx.execute(
            "DELETE FROM append_leases
             WHERE lease_id = ?1",
            params![u64_to_i64(request.lease_id, "lease_id")?],
        )?;
        let file_dir = self.files.get_dir_path(&tx, file.dir_id)?;
        Self::insert_file_change_log(
            &tx,
            if file.size_bytes == 0 {
                FileChangeLogKind::Created
            } else {
                FileChangeLogKind::Updated
            },
            None,
            Some((file_dir.as_str(), file.name.as_str())),
            Some(lease.file_id),
            now,
        )?;
        tx.commit()?;

        self.get_file_read_plan_by_id(lease.file_id)
    }

    pub(crate) fn active_append_lease_volume(&self, lease_id: u64, file_id: u64) -> Fs0Result<u64> {
        self.conn
            .query_row(
                "SELECT volume_id
                 FROM append_leases
                 WHERE lease_id = ?1
                   AND file_id = ?2
                   AND expires_at_ms > ?3",
                params![
                    u64_to_i64(lease_id, "lease_id")?,
                    u64_to_i64(file_id, "file_id")?,
                    u64_to_i64(now_ms(), "now_ms")?,
                ],
                |row| row.u64(0, "volume_id"),
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn abort_append(&mut self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        Self::get_active_lease(&tx, lease_id, file_id)?;

        tx.execute(
            "DELETE FROM append_leases
             WHERE lease_id = ?1",
            params![u64_to_i64(lease_id, "lease_id")?],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn create_dir(&mut self, path: &str) -> Fs0Result<u64> {
        let tx = self.conn.transaction()?;
        let dir_id = self.files.create_dir(&tx, path)?;
        tx.commit()?;
        Ok(dir_id)
    }

    pub(crate) fn remove_dir(&mut self, path: &str) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        self.files.remove_dir(&tx, path)?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Fs0Result<DirectoryEntries> {
        let tx = self.conn.unchecked_transaction()?;
        self.files.list_directory(&tx, dir, limit, cursor)
    }

    pub(crate) fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Fs0Result<FileChangeLogs> {
        let limit = limit.clamp(1, 1024) as usize;
        let mut stmt = self.conn.prepare_cached(
            "SELECT event_id, event_type, file_id,
                    old_dir, old_name, new_dir, new_name, created_at_ms
             FROM file_events
             WHERE event_id > ?1
             ORDER BY event_id
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(
            params![
                u64_to_i64(after_event_id, "after_event_id")?,
                u64_to_i64(limit as u64 + 1, "limit")?,
            ],
            row_to_file_change_log,
        )?;
        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        let next_event_id = if events.len() > limit {
            events.truncate(limit);
            events.last().map(|event| event.event_id)
        } else {
            None
        };
        Ok(FileChangeLogs {
            operations: events,
            next_event_id,
        })
    }

    pub(crate) fn bundle_replica_volumes(&self, bundle_id: HashId) -> Fs0Result<Vec<u64>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT volume_id
             FROM bundle_replicas
             WHERE bundle_id = ?1
             ORDER BY volume_id",
        )?;
        let rows = stmt.query_map(params![bundle_id.as_bytes().as_slice()], |row| {
            row.u64(0, "volume_id")
        })?;
        let mut replicas = Vec::new();
        for row in rows {
            replicas.push(row?);
        }
        Ok(replicas)
    }

    pub(crate) fn record_bundle_events(
        &mut self,
        events: Vec<BundleReplicaEvent>,
    ) -> Fs0Result<()> {
        let tx = self.conn.transaction()?;
        for event in events {
            match event.kind {
                BundleReplicaEventKind::Stored => {
                    let raw_len = event.raw_len.ok_or(Fs0Error::InvalidRequest)?;
                    let compressed_len = event.compressed_len.ok_or(Fs0Error::InvalidRequest)?;
                    if raw_len == 0 || compressed_len == 0 {
                        return Err(Fs0Error::InvalidRequest);
                    }
                    let existing = tx
                        .query_row(
                            "SELECT raw_len, compressed_len
                             FROM bundle_replicas
                             WHERE bundle_id = ?1
                             LIMIT 1",
                            params![event.bundle_id.as_bytes().as_slice()],
                            |row| Ok((row.u64(0, "raw_len")?, row.u64(1, "compressed_len")?)),
                        )
                        .optional()?;
                    if let Some((stored_raw_len, stored_compressed_len)) = existing {
                        if stored_raw_len != raw_len || stored_compressed_len != compressed_len {
                            return Err(Fs0Error::InvalidData {
                                message: "bundle replica metadata conflict".to_owned(),
                            });
                        }
                    }
                    tx.execute(
                        "INSERT INTO bundle_replicas (
                            bundle_id, volume_id, raw_len, compressed_len
                         ) VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(bundle_id, volume_id) DO NOTHING",
                        params![
                            event.bundle_id.as_bytes().as_slice(),
                            u64_to_i64(event.volume_id, "volume_id")?,
                            u64_to_i64(raw_len, "raw_len")?,
                            u64_to_i64(compressed_len, "compressed_len")?,
                        ],
                    )?;
                }
                BundleReplicaEventKind::Deleted => {
                    tx.execute(
                        "DELETE FROM bundle_replicas
                         WHERE bundle_id = ?1 AND volume_id = ?2",
                        params![
                            event.bundle_id.as_bytes().as_slice(),
                            u64_to_i64(event.volume_id, "volume_id")?,
                        ],
                    )?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn get_file_by_path(&self, path: &str) -> Fs0Result<FileRecord> {
        let tx = self.conn.unchecked_transaction()?;
        self.files.get_file_by_path(&tx, path)
    }

    pub(crate) fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
        let file = self.get_file_by_path(path)?;
        self.get_file_read_plan_by_id(file.file_id)
    }

    pub(crate) fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        let tx = self.conn.unchecked_transaction()?;
        let file = self.files.get_file_by_id(&tx, file_id)?;
        let mut stmt = tx.prepare_cached(
            "SELECT fb.bundle_index, fb.bundle_id, br.raw_len, br.compressed_len
             FROM file_bundles fb
             LEFT JOIN (
                SELECT bundle_id,
                       MAX(raw_len) AS raw_len,
                       MAX(compressed_len) AS compressed_len
                FROM bundle_replicas
                GROUP BY bundle_id
             ) br ON br.bundle_id = fb.bundle_id
             WHERE fb.file_id = ?1
             ORDER BY bundle_index",
        )?;
        let rows = stmt.query_map(params![u64_to_i64(file.file_id, "file_id")?], |row| {
            Ok((
                row.u64(0, "bundle_index")?,
                row.hash_id(1, "bundle_id")?,
                row.optional_u64(2, "raw_len")?,
                row.optional_u64(3, "compressed_len")?,
            ))
        })?;
        let mut bundles = Vec::new();
        for row in rows {
            let (bundle_index, bundle_id, raw_len, compressed_len) = row?;
            let Some(raw_len) = raw_len else {
                return Err(Fs0Error::ChunkNotReady);
            };
            let Some(compressed_len) = compressed_len else {
                return Err(Fs0Error::ChunkNotReady);
            };
            bundles.push(FileBundleRef {
                bundle_index,
                raw_len,
                compressed_len,
                bundle_id,
                replicas: Vec::new(),
            });
        }
        Ok(FileReadPlan {
            file_id: file.file_id,
            path: file.path,
            size: file.size_bytes,
            bundles,
        })
    }

    pub(crate) fn delete_file(&mut self, path: &str) -> Fs0Result<()> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let file = self.files.get_file_by_path(&tx, path)?;
        let (old_dir, old_name) = split_fs0_path_dir_and_name(&file.path)?;
        self.files.delete_file_by_id(&tx, file.file_id)?;
        Self::insert_file_change_log(
            &tx,
            FileChangeLogKind::Deleted,
            Some((old_dir.as_str(), old_name.as_str())),
            None,
            Some(file.file_id),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_file_by_id(&mut self, file_id: u64) -> Fs0Result<()> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let file = self.files.get_file_by_id(&tx, file_id)?;
        let (old_dir, old_name) = split_fs0_path_dir_and_name(&file.path)?;
        self.files.delete_file_by_id(&tx, file_id)?;
        Self::insert_file_change_log(
            &tx,
            FileChangeLogKind::Deleted,
            Some((old_dir.as_str(), old_name.as_str())),
            None,
            Some(file_id),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn copy_file(
        &mut self,
        source_path: &str,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        let source_file_id = {
            let tx = self.conn.unchecked_transaction()?;
            self.files.get_file_by_path(&tx, source_path)?.file_id
        };
        self.copy_file_by_id(source_file_id, target_path)
    }

    pub(crate) fn copy_file_by_id(
        &mut self,
        source_file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        let now = now_ms();
        let tx = self.conn.transaction()?;
        self.files
            .copy_file_by_id(&tx, source_file_id, target_path, now)?;
        let target_file_id = i64_to_u64(tx.last_insert_rowid(), "target_file_id")?;
        tx.execute(
            "INSERT INTO file_bundles (file_id, bundle_index, bundle_id)
             SELECT ?1, bundle_index, bundle_id
             FROM file_bundles
             WHERE file_id = ?2",
            params![
                u64_to_i64(target_file_id, "target_file_id")?,
                u64_to_i64(source_file_id, "source_file_id")?,
            ],
        )?;
        Self::insert_file_change_log(
            &tx,
            FileChangeLogKind::Created,
            None,
            Some((target_dir.as_str(), target_name.as_str())),
            Some(target_file_id),
            now,
        )?;
        tx.commit()?;
        let tx = self.conn.unchecked_transaction()?;
        self.files.get_file_by_id(&tx, target_file_id)
    }

    pub(crate) fn rename_file(
        &mut self,
        source_path: &str,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        let file_id = {
            let tx = self.conn.unchecked_transaction()?;
            self.files.get_file_by_path(&tx, source_path)?.file_id
        };
        self.rename_file_by_id(file_id, target_path)
    }

    pub(crate) fn rename_file_by_id(
        &mut self,
        file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let file = self.files.get_file_by_id(&tx, file_id)?;
        let (old_dir, old_name) = split_fs0_path_dir_and_name(&file.path)?;
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        self.files
            .rename_file_by_id(&tx, file_id, target_path, now)?;
        Self::insert_file_change_log(
            &tx,
            FileChangeLogKind::Moved,
            Some((old_dir.as_str(), old_name.as_str())),
            Some((target_dir.as_str(), target_name.as_str())),
            Some(file_id),
            now,
        )?;
        tx.commit()?;
        let tx = self.conn.unchecked_transaction()?;
        self.files.get_file_by_id(&tx, file_id)
    }

    fn get_active_lease(
        tx: &rusqlite::Transaction<'_>,
        lease_id: u64,
        file_id: u64,
    ) -> Fs0Result<LeaseRecord> {
        tx.query_row(
            "SELECT file_id, base_size_bytes, offset_bytes
             FROM append_leases
             WHERE lease_id = ?1
               AND file_id = ?2
               AND expires_at_ms > ?3",
            params![
                u64_to_i64(lease_id, "lease_id")?,
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(now_ms(), "now_ms")?,
            ],
            |row| {
                Ok(LeaseRecord {
                    file_id: row.u64(0, "file_id")?,
                    base_size_bytes: row.u64(1, "base_size_bytes")?,
                    offset_bytes: row.u64(2, "offset_bytes")?,
                })
            },
        )
        .optional()?
        .ok_or(Fs0Error::NotFound)
    }

    fn file_bundle_totals(
        tx: &rusqlite::Transaction<'_>,
        file_id: u64,
        max_bundle_index: Option<u64>,
    ) -> Fs0Result<BundleTotals> {
        tx.query_row(
            "SELECT COUNT(*),
                    COUNT(br.bundle_id),
                    COALESCE(SUM(br.raw_len), 0),
                    COALESCE(SUM(br.compressed_len), 0),
                    COALESCE(SUM(
                        CASE
                            WHEN br.bundle_id IS NOT NULL
                             AND (
                                br.raw_len != br.max_raw_len
                                OR br.compressed_len != br.max_compressed_len
                             )
                            THEN 1
                            ELSE 0
                        END
                    ), 0)
             FROM file_bundles fb
             LEFT JOIN (
                SELECT bundle_id,
                       MIN(raw_len) AS raw_len,
                       MAX(raw_len) AS max_raw_len,
                       MIN(compressed_len) AS compressed_len,
                       MAX(compressed_len) AS max_compressed_len
                FROM bundle_replicas
                GROUP BY bundle_id
             ) br ON br.bundle_id = fb.bundle_id
             WHERE fb.file_id = ?1
               AND (?2 IS NULL OR fb.bundle_index < ?2)",
            params![
                u64_to_i64(file_id, "file_id")?,
                max_bundle_index
                    .map(|bundle_index| u64_to_i64(bundle_index, "max_bundle_index"))
                    .transpose()?,
            ],
            |row| {
                Ok(BundleTotals {
                    file_bundle_count: row.u64(0, "file_bundle_count")?,
                    ready_bundle_count: row.u64(1, "ready_bundle_count")?,
                    raw_size_bytes: row.u64(2, "raw_size_bytes")?,
                    compressed_size_bytes: row.u64(3, "compressed_size_bytes")?,
                    metadata_conflict_count: row.u64(4, "metadata_conflict_count")?,
                })
            },
        )
        .map_err(Fs0Error::from)
    }

    fn validate_bundle_totals_ready(totals: &BundleTotals) -> Fs0Result<()> {
        if totals.file_bundle_count != totals.ready_bundle_count {
            return Err(Fs0Error::ChunkNotReady);
        }
        if totals.metadata_conflict_count != 0 {
            return Err(Fs0Error::InvalidData {
                message: "bundle replica metadata conflict".to_owned(),
            });
        }

        Ok(())
    }

    fn insert_file_change_log(
        tx: &rusqlite::Transaction<'_>,
        kind: FileChangeLogKind,
        old_target: Option<(&str, &str)>,
        new_target: Option<(&str, &str)>,
        file_id: Option<u64>,
        created_at_ms: u64,
    ) -> Fs0Result<()> {
        tx.execute(
            "INSERT INTO file_events (
                event_type, old_dir, old_name, new_dir, new_name,
                file_id, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                file_change_log_kind(kind),
                old_target.map(|target| target.0),
                old_target.map(|target| target.1),
                new_target.map(|target| target.0),
                new_target.map(|target| target.1),
                file_id.map(|id| u64_to_i64(id, "file_id")).transpose()?,
                u64_to_i64(created_at_ms, "created_at_ms")?,
            ],
        )?;
        Ok(())
    }
}

fn submitted_bundle_totals(bundles: &[CommittedBundle]) -> Fs0Result<(u64, u64)> {
    let mut raw_size_bytes = 0u64;
    let mut compressed_size_bytes = 0u64;
    for bundle in bundles {
        raw_size_bytes = raw_size_bytes.checked_add(bundle.raw_len).ok_or_else(|| {
            Fs0Error::IntegerConversion {
                message: "submitted bundle raw size overflow".to_owned(),
            }
        })?;
        compressed_size_bytes = compressed_size_bytes
            .checked_add(bundle.compressed_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "submitted bundle compressed size overflow".to_owned(),
            })?;
    }

    Ok((raw_size_bytes, compressed_size_bytes))
}

fn row_to_storage_volume_info(row: &rusqlite::Row<'_>) -> rusqlite::Result<StorageVolumeInfo> {
    Ok(StorageVolumeInfo {
        volume_id: row.u64(0, "volume_id")?,
        name: row.get(1)?,
        max_bytes: row.u64(2, "max_bytes")?,
        max_volume_offset: row.u64(3, "max_volume_offset")?,
        read_only: false,
    })
}

fn row_to_file_change_log(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileChangeLog> {
    let old_dir: Option<String> = row.get(3)?;
    let old_name: Option<String> = row.get(4)?;
    let new_dir: Option<String> = row.get(5)?;
    let new_name: Option<String> = row.get(6)?;
    let event_type: String = row.get(1)?;
    Ok(FileChangeLog {
        event_id: row.u64(0, "event_id")?,
        kind: match event_type.as_str() {
            "created" => FileChangeLogKind::Created,
            "updated" => FileChangeLogKind::Updated,
            "moved" => FileChangeLogKind::Moved,
            "deleted" => FileChangeLogKind::Deleted,
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid file event type {event_type}"),
                    )),
                ));
            }
        },
        file_id: row.optional_u64(2, "file_id")?,
        old_path: match (old_dir.as_deref(), old_name.as_deref()) {
            (Some(dir), Some(name)) => Some(join_fs0_path(dir, name).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?),
            _ => None,
        },
        new_path: match (new_dir.as_deref(), new_name.as_deref()) {
            (Some(dir), Some(name)) => Some(join_fs0_path(dir, name).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?),
            _ => None,
        },
        created_at_ms: row.u64(7, "created_at_ms")?,
    })
}

fn file_change_log_kind(kind: FileChangeLogKind) -> &'static str {
    match kind {
        FileChangeLogKind::Created => "created",
        FileChangeLogKind::Updated => "updated",
        FileChangeLogKind::Moved => "moved",
        FileChangeLogKind::Deleted => "deleted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn open_test_db() -> CentralDb {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(include_str!("schema.sql")).unwrap();

        CentralDb {
            conn,
            files: FileCatalog::new(),
        }
    }

    fn bundle_id(byte: u8) -> HashId {
        HashId::new([byte; 32])
    }

    fn committed_bundle(byte: u8, raw_len: u64, compressed_len: u64) -> CommittedBundle {
        CommittedBundle {
            bundle_id: bundle_id(byte),
            raw_len,
            compressed_len,
        }
    }

    fn begin_append(db: &mut CentralDb, volume_id: u64, path: &str, offset: u64) -> AppendLease {
        db.begin_append(
            BeginAppendRequest {
                path: path.to_owned(),
                offset,
                prefer_volume_name: None,
                append_size_hint: None,
            },
            volume_id,
        )
        .unwrap()
    }

    fn commit_append(
        db: &mut CentralDb,
        lease: &AppendLease,
        new_size: u64,
        bundles: Vec<CommittedBundle>,
    ) -> Fs0Result<FileReadPlan> {
        db.commit_append(CommitAppendRequest {
            lease_id: lease.lease_id,
            file_id: lease.file_id,
            base_size: lease.base_size,
            new_size,
            bundles,
        })
    }

    fn record_bundle(
        db: &mut CentralDb,
        volume_id: u64,
        byte: u8,
        raw_len: u64,
        compressed_len: u64,
    ) {
        db.record_bundle_events(vec![BundleReplicaEvent {
            event_id: 0,
            kind: BundleReplicaEventKind::Stored,
            volume_id,
            bundle_id: bundle_id(byte),
            raw_len: Some(raw_len),
            compressed_len: Some(compressed_len),
        }])
        .unwrap();
    }

    fn assert_error<T>(result: Fs0Result<T>, expected: Fs0Error) {
        match result {
            Ok(_) => panic!("expected error {expected:?}"),
            Err(err) => assert_eq!(err, expected),
        }
    }

    fn assert_plan_bundles(plan: &FileReadPlan, expected: &[(u64, u8, u64, u64)]) {
        let actual = plan
            .bundles
            .iter()
            .map(|bundle| {
                (
                    bundle.bundle_index,
                    bundle.bundle_id.as_bytes()[0],
                    bundle.raw_len,
                    bundle.compressed_len,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    fn seed_two_bundle_file(db: &mut CentralDb, volume_id: u64) -> FileReadPlan {
        record_bundle(db, volume_id, 1, VOLUME_BUNDLE_RAW_SIZE, 11);
        record_bundle(db, volume_id, 2, 40, 7);
        let lease = begin_append(db, volume_id, "/file.bin", 0);

        commit_append(
            db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 40,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(2, 40, 7),
            ],
        )
        .unwrap()
    }

    #[test]
    fn commit_append_accepts_suffix_bundles_from_first_bundle_index() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        let original = seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_append(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![committed_bundle(3, 50, 9)],
        )
        .unwrap();
        let file = db.get_file_by_path("/file.bin").unwrap();

        assert_eq!(lease.base_size, original.size);
        assert_eq!(plan.size, VOLUME_BUNDLE_RAW_SIZE + 50);
        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_append_accepts_full_file_bundles_and_skips_existing_prefix() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_append(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(3, 50, 9),
            ],
        )
        .unwrap();
        let file = db.get_file_by_path("/file.bin").unwrap();

        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_append_rejects_raw_total_that_does_not_match_new_size() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_append(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 11, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_append_rejects_compressed_len_that_does_not_match_replica_metadata() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_append(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 10, vec![committed_bundle(1, 10, 6)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_append_rejects_full_file_prefix_compressed_mismatch() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        assert_error(
            commit_append(
                &mut db,
                &lease,
                VOLUME_BUNDLE_RAW_SIZE + 50,
                vec![
                    committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 12),
                    committed_bundle(3, 50, 9),
                ],
            ),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_append_rejects_bundle_without_ready_replica() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        let lease = begin_append(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 10, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::ChunkNotReady,
        );
    }

    #[test]
    fn commit_append_rejects_missing_replica_in_preserved_prefix() {
        let mut db = open_test_db();
        let volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        db.conn
            .execute(
                "DELETE FROM bundle_replicas WHERE bundle_id = ?1",
                params![bundle_id(1).as_bytes().as_slice()],
            )
            .unwrap();
        let lease = begin_append(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        assert_error(
            commit_append(
                &mut db,
                &lease,
                VOLUME_BUNDLE_RAW_SIZE + 50,
                vec![committed_bundle(3, 50, 9)],
            ),
            Fs0Error::ChunkNotReady,
        );
    }

    #[test]
    fn commit_append_rejects_conflicting_replica_metadata() {
        let mut db = open_test_db();
        let primary_volume_id = db
            .create_volume("primary".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        let replica_volume_id = db
            .create_volume("replica".to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        record_bundle(&mut db, primary_volume_id, 1, 10, 5);
        db.conn
            .execute(
                "INSERT INTO bundle_replicas (
                    bundle_id, volume_id, raw_len, compressed_len
                 ) VALUES (?1, ?2, 11, 5)",
                params![
                    bundle_id(1).as_bytes().as_slice(),
                    u64_to_i64(replica_volume_id, "replica_volume_id").unwrap(),
                ],
            )
            .unwrap();
        let lease = begin_append(&mut db, primary_volume_id, "/file.bin", 0);

        assert_error(
            commit_append(&mut db, &lease, 10, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidData {
                message: "bundle replica metadata conflict".to_owned(),
            },
        );
    }
}
