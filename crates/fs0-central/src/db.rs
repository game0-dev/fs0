use crate::Result;
use fs0_core::{
    APPEND_LEASE_TTL_MS, AppendLease, BeginAppendRequest, BundleReplicaEventKind,
    BundleReplicaReport, CommittedBundle, DirectoryEntries, DirectoryEntry, FileBundleRef,
    FileChangeLog, FileChangeLogKind, FileChangeLogs, FileReadPlan, FileRecord, Fs0Error, HashId,
    hash_id_from_vec, i64_to_u64, join_fs0_path, now_ms, split_fs0_path, u64_to_i64,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VolumeRecord {
    pub(crate) volume_id: u64,
    pub(crate) name: String,
    pub(crate) max_bytes: u64,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug)]
pub(crate) struct CentralDb {
    conn: Connection,
}
impl CentralDb {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "DELETE")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(include_str!("schema.sql"))?;
        Ok(Self { conn })
    }

    pub(crate) fn create_volume(&mut self, name: String, max_bytes: u64) -> Result<VolumeRecord> {
        let now = now_ms();
        self.conn.execute(
            "INSERT INTO volumes (name, max_bytes, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?3)",
            params![
                name.as_str(),
                u64_to_i64(max_bytes, "max_bytes")?,
                u64_to_i64(now, "created_at_ms")?,
            ],
        )?;
        let volume_id = i64_to_u64(self.conn.last_insert_rowid(), "volume_id")?;
        self.get_volume(volume_id)?.ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn get_volume(&self, volume_id: u64) -> Result<Option<VolumeRecord>> {
        self.conn
            .query_row(
                "SELECT volume_id, name, max_bytes, created_at_ms, updated_at_ms
                 FROM volumes
                 WHERE volume_id = ?1",
                params![u64_to_i64(volume_id, "volume_id")?],
                row_to_volume_record,
            )
            .optional()
            .map_err(Fs0Error::from)
    }

    pub(crate) fn volume_replica_usage(&self, volume_id: u64) -> Result<(u64, u64)> {
        self.conn
            .query_row(
                "SELECT COALESCE(SUM(b.raw_len), 0),
                    COALESCE(SUM(b.compressed_len), 0)
             FROM bundle_replicas br
             JOIN bundles b ON b.bundle_id = br.bundle_id
             WHERE br.volume_id = ?1",
                params![u64_to_i64(volume_id, "volume_id")?],
                |row| {
                    Ok((
                        i64_to_u64(row.get(0)?, "raw_bytes").map_err(|err| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                        })?,
                        i64_to_u64(row.get(1)?, "compressed_bytes").map_err(|err| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                        })?,
                    ))
                },
            )
            .map_err(Fs0Error::from)
    }

    pub(crate) fn begin_append(
        &mut self,
        request: BeginAppendRequest,
        client_id: u64,
        volume_id: u64,
    ) -> Result<AppendLease> {
        let now = now_ms();
        let expires_at_ms = now + APPEND_LEASE_TTL_MS;
        let (dir, name) = split_fs0_path(request.path.as_str())?;
        let tx = self.conn.transaction()?;
        let file_id = match Self::load_file_by_dir_name(&tx, &dir, &name)? {
            Some(file) => {
                if file.size_bytes != request.expected_size {
                    return Err(Fs0Error::VersionConflict);
                }
                file.file_id
            }
            None => {
                if !request.create {
                    return Err(Fs0Error::NotFound);
                }
                if request.expected_size != 0 {
                    return Err(Fs0Error::VersionConflict);
                }
                Self::create_file(&tx, &dir, &name, now)?
            }
        };
        tx.execute(
            "DELETE FROM append_leases
             WHERE file_id = ?1
               AND expires_at_ms <= ?2",
            params![
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(now, "expires_at_ms")?
            ],
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
                file_id, client_id, volume_id, base_size_bytes, prefer_volume_name,
                expires_at_ms, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(client_id, "client_id")?,
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(request.expected_size, "base_size_bytes")?,
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
            base_size: request.expected_size,
            expires_at_ms,
            prefer_volume_name: request.prefer_volume_name,
        })
    }

    pub(crate) fn commit_append(
        &mut self,
        request: fs0_core::CommitAppendRequest,
    ) -> Result<FileReadPlan> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let lease = Self::load_active_lease(&tx, request.lease_id)?;
        if lease.base_size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict);
        }
        let appended_len = request
            .new_size
            .checked_sub(request.base_size)
            .ok_or(Fs0Error::InvalidRequest)?;

        let file = Self::load_file_by_id_tx(&tx, lease.file_id)?.ok_or(Fs0Error::NotFound)?;
        if file.size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict);
        }
        let next_bundle_index = tx.query_row(
            "SELECT COALESCE(MAX(bundle_index) + 1, 0)
             FROM file_bundles
             WHERE file_id = ?1",
            params![u64_to_i64(lease.file_id, "file_id")?],
            |row| {
                i64_to_u64(row.get(0)?, "next_bundle_index")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
            },
        )?;
        Self::validate_committed_bundles(&request.bundles, appended_len, next_bundle_index)?;
        for bundle in &request.bundles {
            let (stored_raw_len, stored_compressed_len, replica_count) = tx
                .query_row(
                    "SELECT b.raw_len, b.compressed_len, COUNT(br.volume_id)
                     FROM bundles b
                     LEFT JOIN bundle_replicas br
                       ON br.bundle_id = b.bundle_id
                     WHERE b.bundle_id = ?1
                     GROUP BY b.bundle_id, b.raw_len, b.compressed_len",
                    params![bundle.bundle_id.as_bytes().as_slice()],
                    |row| {
                        Ok((
                            i64_to_u64(row.get(0)?, "raw_len").map_err(|err| {
                                rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                            })?,
                            i64_to_u64(row.get(1)?, "compressed_len").map_err(|err| {
                                rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                            })?,
                            i64_to_u64(row.get(2)?, "replica_count").map_err(|err| {
                                rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                            })?,
                        ))
                    },
                )
                .optional()?
                .ok_or(Fs0Error::ChunkNotReady)?;
            if replica_count == 0 {
                return Err(Fs0Error::ChunkNotReady);
            }
            if stored_raw_len != bundle.raw_len || stored_compressed_len != bundle.compressed_len {
                return Err(Fs0Error::InvalidRequest);
            }
            let file_bundle_exists = tx.query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM file_bundles
                    WHERE file_id = ?1 AND bundle_index = ?2
                 )",
                params![
                    u64_to_i64(lease.file_id, "file_id")?,
                    u64_to_i64(bundle.bundle_index, "bundle_index")?,
                ],
                |row| row.get::<_, bool>(0),
            )?;
            if file_bundle_exists {
                return Err(Fs0Error::InvalidRequest);
            }
            tx.execute(
                "INSERT INTO file_bundles (
                    file_id, bundle_index, bundle_id
                 ) VALUES (?1, ?2, ?3)",
                params![
                    u64_to_i64(lease.file_id, "file_id")?,
                    u64_to_i64(bundle.bundle_index, "bundle_index")?,
                    bundle.bundle_id.as_bytes().as_slice(),
                ],
            )?;
        }
        let compressed_size_bytes = tx.query_row(
            "SELECT COALESCE(SUM(b.compressed_len), 0)
             FROM file_bundles fb
             JOIN bundles b ON b.bundle_id = fb.bundle_id
             WHERE fb.file_id = ?1",
            params![u64_to_i64(lease.file_id, "file_id")?],
            |row| {
                i64_to_u64(row.get(0)?, "compressed_size_bytes")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
            },
        )?;

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
        let (file_dir, file_name) = split_fs0_path(&file.path)?;
        Self::insert_file_change_log(
            &tx,
            if request.base_size == 0 {
                FileChangeLogKind::Created
            } else {
                FileChangeLogKind::Updated
            },
            None,
            Some((file_dir.as_str(), file_name.as_str())),
            Some(lease.file_id),
            now,
        )?;
        tx.commit()?;

        self.get_file_read_plan_by_id(lease.file_id)
    }

    pub(crate) fn abort_append(&mut self, lease_id: u64) -> Result<()> {
        let tx = self.conn.transaction()?;
        let _lease = Self::load_active_lease(&tx, lease_id)?;
        tx.execute(
            "DELETE FROM append_leases
             WHERE lease_id = ?1",
            params![u64_to_i64(lease_id, "lease_id")?],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn get_file_by_path(&self, path: &str) -> Result<Option<FileRecord>> {
        let (dir, name) = split_fs0_path(path)?;
        let file = self
            .conn
            .query_row(
                "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                        created_at_ms, updated_at_ms
                 FROM files
                 WHERE dir = ?1 AND name = ?2",
                params![dir, name],
                row_to_file_record,
            )
            .optional()?;
        Ok(file)
    }

    pub(crate) fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Result<DirectoryEntries> {
        let limit = limit.clamp(1, 1024) as usize;
        let offset = cursor.unwrap_or(0);
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE dir = ?1
             ORDER BY name
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![
                dir,
                u64_to_i64(limit as u64 + 1, "limit")?,
                u64_to_i64(offset, "cursor")?,
            ],
            |row| {
                let dir: String = row.get(1)?;
                let name: String = row.get(2)?;
                Ok(DirectoryEntry {
                    file_id: i64_to_u64(row.get(0)?, "file_id")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    name: name.clone(),
                    path: join_fs0_path(&dir, &name)
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    size_bytes: i64_to_u64(row.get(3)?, "size_bytes")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    compressed_size_bytes: i64_to_u64(row.get(4)?, "compressed_size_bytes")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    created_at_ms: i64_to_u64(row.get(5)?, "created_at_ms")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    updated_at_ms: i64_to_u64(row.get(6)?, "updated_at_ms")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                })
            },
        )?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        let next_cursor = if entries.len() > limit {
            entries.truncate(limit);
            Some(offset + limit as u64)
        } else {
            None
        };
        Ok(DirectoryEntries {
            entries,
            next_cursor,
        })
    }

    pub(crate) fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Result<FileChangeLogs> {
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

    pub(crate) fn bundle_replica_volumes(&self, bundle_id: HashId) -> Result<Vec<u64>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT volume_id
             FROM bundle_replicas
             WHERE bundle_id = ?1
             ORDER BY volume_id",
        )?;
        let rows = stmt.query_map(params![bundle_id.as_bytes().as_slice()], |row| {
            i64_to_u64(row.get(0)?, "volume_id")
                .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
        })?;
        let mut replicas = Vec::new();
        for row in rows {
            replicas.push(row?);
        }
        Ok(replicas)
    }

    pub(crate) fn record_bundle_events(&mut self, events: BundleReplicaReport) -> Result<()> {
        let tx = self.conn.transaction()?;
        for event in events.events {
            match event.kind {
                BundleReplicaEventKind::Stored => {
                    let raw_len = event.raw_len.ok_or(Fs0Error::InvalidRequest)?;
                    let compressed_len = event.compressed_len.ok_or(Fs0Error::InvalidRequest)?;
                    if raw_len == 0 || compressed_len == 0 {
                        return Err(Fs0Error::InvalidRequest);
                    }
                    tx.execute(
                        "INSERT INTO bundles (
                            bundle_id, raw_len, compressed_len
                         ) VALUES (?1, ?2, ?3)
                         ON CONFLICT(bundle_id) DO NOTHING",
                        params![
                            event.bundle_id.as_bytes().as_slice(),
                            u64_to_i64(raw_len, "raw_len")?,
                            u64_to_i64(compressed_len, "compressed_len")?,
                        ],
                    )?;
                    let (stored_raw_len, stored_compressed_len) = tx.query_row(
                        "SELECT raw_len, compressed_len
                         FROM bundles
                         WHERE bundle_id = ?1",
                        params![event.bundle_id.as_bytes().as_slice()],
                        |row| {
                            Ok((
                                i64_to_u64(row.get(0)?, "raw_len").map_err(|err| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                                })?,
                                i64_to_u64(row.get(1)?, "compressed_len").map_err(|err| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                                })?,
                            ))
                        },
                    )?;
                    if stored_raw_len != raw_len || stored_compressed_len != compressed_len {
                        return Err(Fs0Error::InvalidData {
                            message: "bundle metadata conflict".to_owned(),
                        });
                    }
                    tx.execute(
                        "INSERT INTO bundle_replicas (
                            bundle_id, volume_id
                         ) VALUES (?1, ?2)
                         ON CONFLICT(bundle_id, volume_id) DO NOTHING",
                        params![
                            event.bundle_id.as_bytes().as_slice(),
                            u64_to_i64(event.volume_id, "volume_id")?,
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

    pub(crate) fn get_file_read_plan(&self, path: &str) -> Result<FileReadPlan> {
        let file = self.get_file_by_path(path)?.ok_or(Fs0Error::NotFound)?;
        self.get_file_read_plan_by_id(file.file_id)
    }

    pub(crate) fn get_file_read_plan_by_id(&self, file_id: u64) -> Result<FileReadPlan> {
        let file = Self::load_file_by_id(&self.conn, file_id)?.ok_or(Fs0Error::NotFound)?;
        let mut stmt = self.conn.prepare_cached(
            "SELECT fb.bundle_index, fb.bundle_id, b.raw_len, b.compressed_len
             FROM file_bundles fb
             JOIN bundles b ON b.bundle_id = fb.bundle_id
             WHERE fb.file_id = ?1
             ORDER BY bundle_index",
        )?;
        let rows = stmt.query_map(params![u64_to_i64(file.file_id, "file_id")?], |row| {
            let bundle_id = hash_id_from_vec(row.get(1)?)
                .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
            Ok((
                i64_to_u64(row.get(0)?, "bundle_index")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                bundle_id,
                i64_to_u64(row.get(2)?, "raw_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                i64_to_u64(row.get(3)?, "compressed_len")
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
            ))
        })?;
        let mut bundles = Vec::new();
        for row in rows {
            let (bundle_index, bundle_id, raw_len, compressed_len) = row?;
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

    pub(crate) fn delete_file(&mut self, path: &str) -> Result<()> {
        let (dir, name) = split_fs0_path(path)?;
        let tx = self.conn.transaction()?;
        let file = Self::load_file_by_dir_name(&tx, &dir, &name)?.ok_or(Fs0Error::NotFound)?;
        Self::delete_file_by_id_tx(&tx, file.file_id, now_ms())?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn delete_file_by_id(&mut self, file_id: u64) -> Result<()> {
        let tx = self.conn.transaction()?;
        Self::delete_file_by_id_tx(&tx, file_id, now_ms())?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn copy_file(&mut self, source_path: &str, target_path: &str) -> Result<FileRecord> {
        let (source_dir, source_name) = split_fs0_path(source_path)?;
        let source = self
            .conn
            .query_row(
                "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                        created_at_ms, updated_at_ms
                 FROM files
                 WHERE dir = ?1 AND name = ?2",
                params![source_dir, source_name],
                row_to_file_record,
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)?;
        self.copy_file_by_id(source.file_id, target_path)
    }

    pub(crate) fn copy_file_by_id(
        &mut self,
        source_file_id: u64,
        target_path: &str,
    ) -> Result<FileRecord> {
        let (target_dir, target_name) = split_fs0_path(target_path)?;
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let source = Self::load_file_by_id_tx(&tx, source_file_id)?.ok_or(Fs0Error::NotFound)?;
        tx.execute(
            "INSERT INTO files (
                dir, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            params![
                target_dir,
                target_name,
                u64_to_i64(source.size_bytes, "size_bytes")?,
                u64_to_i64(source.compressed_size_bytes, "compressed_size_bytes")?,
                u64_to_i64(now, "created_at_ms")?,
            ],
        )?;
        let target_file_id = i64_to_u64(tx.last_insert_rowid(), "file_id")?;
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
            Some((&target_dir, &target_name)),
            Some(target_file_id),
            now,
        )?;
        tx.commit()?;
        Self::load_file_by_id(&self.conn, target_file_id)?.ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn rename_file(
        &mut self,
        source_path: &str,
        target_path: &str,
    ) -> Result<FileRecord> {
        let source = self
            .get_file_by_path(source_path)?
            .ok_or(Fs0Error::NotFound)?;
        self.rename_file_record(source, target_path)
    }

    pub(crate) fn rename_file_by_id(
        &mut self,
        file_id: u64,
        target_path: &str,
    ) -> Result<FileRecord> {
        let file = Self::load_file_by_id(&self.conn, file_id)?.ok_or(Fs0Error::NotFound)?;
        self.rename_file_record(file, target_path)
    }

    fn rename_file_record(&mut self, file: FileRecord, target_path: &str) -> Result<FileRecord> {
        let (old_dir, old_name) = split_fs0_path(&file.path)?;
        let (target_dir, target_name) = split_fs0_path(target_path)?;
        let now = now_ms();
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE files
             SET dir = ?2, name = ?3, updated_at_ms = ?4
             WHERE file_id = ?1",
            params![
                u64_to_i64(file.file_id, "file_id")?,
                target_dir,
                target_name,
                u64_to_i64(now, "updated_at_ms")?,
            ],
        )?;
        if tx.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }
        Self::insert_file_change_log(
            &tx,
            FileChangeLogKind::Moved,
            Some((old_dir.as_str(), old_name.as_str())),
            Some((&target_dir, &target_name)),
            Some(file.file_id),
            now,
        )?;
        tx.commit()?;
        Self::load_file_by_id(&self.conn, file.file_id)?.ok_or(Fs0Error::NotFound)
    }

    fn create_file(tx: &rusqlite::Transaction<'_>, dir: &str, name: &str, now: u64) -> Result<u64> {
        tx.execute(
            "INSERT INTO files (
                dir, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, 0, 0, ?3, ?3)",
            params![dir, name, u64_to_i64(now, "created_at_ms")?],
        )?;
        i64_to_u64(tx.last_insert_rowid(), "file_id")
    }

    fn delete_file_by_id_tx(tx: &rusqlite::Transaction<'_>, file_id: u64, now: u64) -> Result<()> {
        let file = Self::load_file_by_id_tx(tx, file_id)?.ok_or(Fs0Error::NotFound)?;
        let (old_dir, old_name) = split_fs0_path(&file.path)?;
        tx.execute(
            "DELETE FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
        )?;
        Self::insert_file_change_log(
            tx,
            FileChangeLogKind::Deleted,
            Some((old_dir.as_str(), old_name.as_str())),
            None,
            Some(file_id),
            now,
        )
    }

    fn load_file_by_dir_name(
        tx: &rusqlite::Transaction<'_>,
        dir: &str,
        name: &str,
    ) -> Result<Option<FileRecord>> {
        tx.query_row(
            "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE dir = ?1 AND name = ?2",
            params![dir, name],
            row_to_file_record,
        )
        .optional()
        .map_err(Fs0Error::from)
    }

    fn load_file_by_id(conn: &Connection, file_id: u64) -> Result<Option<FileRecord>> {
        conn.query_row(
            "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
            row_to_file_record,
        )
        .optional()
        .map_err(Fs0Error::from)
    }

    fn load_file_by_id_tx(
        tx: &rusqlite::Transaction<'_>,
        file_id: u64,
    ) -> Result<Option<FileRecord>> {
        tx.query_row(
            "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
            row_to_file_record,
        )
        .optional()
        .map_err(Fs0Error::from)
    }

    fn load_active_lease(tx: &rusqlite::Transaction<'_>, lease_id: u64) -> Result<LeaseRecord> {
        tx.query_row(
            "SELECT file_id, base_size_bytes
             FROM append_leases
             WHERE lease_id = ?1
               AND expires_at_ms > ?2",
            params![
                u64_to_i64(lease_id, "lease_id")?,
                u64_to_i64(now_ms(), "now_ms")?,
            ],
            |row| {
                Ok(LeaseRecord {
                    file_id: i64_to_u64(row.get(0)?, "file_id")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    base_size_bytes: i64_to_u64(row.get(1)?, "base_size_bytes")
                        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                })
            },
        )
        .optional()?
        .ok_or(Fs0Error::NotFound)
    }

    fn validate_committed_bundles(
        bundles: &[CommittedBundle],
        appended_len: u64,
        expected_first_index: u64,
    ) -> Result<()> {
        let mut expected_index = expected_first_index;
        let mut total_raw_len = 0u64;
        for bundle in bundles {
            if bundle.bundle_index != expected_index {
                return Err(Fs0Error::InvalidRequest);
            }
            if bundle.raw_len == 0 || bundle.compressed_len == 0 {
                return Err(Fs0Error::InvalidRequest);
            }
            total_raw_len = total_raw_len.checked_add(bundle.raw_len).ok_or_else(|| {
                Fs0Error::IntegerConversion {
                    message: "committed bundle raw_len overflow".to_owned(),
                }
            })?;
            expected_index =
                expected_index
                    .checked_add(1)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle_index overflow".to_owned(),
                    })?;
        }
        if total_raw_len != appended_len {
            return Err(Fs0Error::InvalidRequest);
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
    ) -> Result<()> {
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

#[derive(Debug)]
struct LeaseRecord {
    file_id: u64,
    base_size_bytes: u64,
}

fn row_to_volume_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<VolumeRecord> {
    Ok(VolumeRecord {
        volume_id: i64_to_u64(row.get(0)?, "volume_id")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        name: row.get(1)?,
        max_bytes: i64_to_u64(row.get(2)?, "max_bytes")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        created_at_ms: i64_to_u64(row.get(3)?, "created_at_ms")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        updated_at_ms: i64_to_u64(row.get(4)?, "updated_at_ms")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
    })
}

fn row_to_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let dir: String = row.get(1)?;
    let name: String = row.get(2)?;
    Ok(FileRecord {
        file_id: i64_to_u64(row.get(0)?, "file_id")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        path: join_fs0_path(&dir, &name)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        size_bytes: i64_to_u64(row.get(3)?, "size_bytes")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        compressed_size_bytes: i64_to_u64(row.get(4)?, "compressed_size_bytes")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        created_at_ms: i64_to_u64(row.get(5)?, "created_at_ms")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        updated_at_ms: i64_to_u64(row.get(6)?, "updated_at_ms")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
    })
}

fn row_to_file_change_log(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileChangeLog> {
    let old_dir: Option<String> = row.get(3)?;
    let old_name: Option<String> = row.get(4)?;
    let new_dir: Option<String> = row.get(5)?;
    let new_name: Option<String> = row.get(6)?;
    Ok(FileChangeLog {
        event_id: i64_to_u64(row.get(0)?, "event_id")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        kind: parse_file_change_log_kind(row.get::<_, String>(1)?.as_str())
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        file_id: row
            .get::<_, Option<i64>>(2)?
            .map(|value| i64_to_u64(value, "file_id"))
            .transpose()
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
        old_path: match (old_dir.as_deref(), old_name.as_deref()) {
            (Some(dir), Some(name)) => Some(
                join_fs0_path(dir, name)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
            ),
            _ => None,
        },
        new_path: match (new_dir.as_deref(), new_name.as_deref()) {
            (Some(dir), Some(name)) => Some(
                join_fs0_path(dir, name)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
            ),
            _ => None,
        },
        created_at_ms: i64_to_u64(row.get(7)?, "created_at_ms")
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
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

fn parse_file_change_log_kind(value: &str) -> Result<FileChangeLogKind> {
    match value {
        "created" => Ok(FileChangeLogKind::Created),
        "updated" => Ok(FileChangeLogKind::Updated),
        "moved" => Ok(FileChangeLogKind::Moved),
        "deleted" => Ok(FileChangeLogKind::Deleted),
        _ => Err(Fs0Error::InvalidRequest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs0_core::{BeginAppendRequest, BundleReplicaEvent, CommitAppendRequest};

    fn test_db() -> (tempfile::TempDir, CentralDb, u64) {
        let temp = tempfile::tempdir().unwrap();
        let mut db = CentralDb::open(temp.path().join("central.db")).unwrap();
        let volume = db.create_volume("test".to_owned(), 1024 * 1024).unwrap();
        (temp, db, volume.volume_id)
    }

    fn begin_create(db: &mut CentralDb, volume_id: u64) -> AppendLease {
        db.begin_append(
            BeginAppendRequest {
                path: "/file".to_owned(),
                expected_size: 0,
                create: true,
                prefer_volume_name: None,
                idempotency_key: None,
            },
            1,
            volume_id,
        )
        .unwrap()
    }

    fn begin_append(db: &mut CentralDb, volume_id: u64, expected_size: u64) -> AppendLease {
        db.begin_append(
            BeginAppendRequest {
                path: "/file".to_owned(),
                expected_size,
                create: false,
                prefer_volume_name: None,
                idempotency_key: None,
            },
            1,
            volume_id,
        )
        .unwrap()
    }

    fn record_bundle(db: &mut CentralDb, volume_id: u64, bundle_id: HashId, raw_len: u64) {
        db.record_bundle_events(BundleReplicaReport {
            events: vec![BundleReplicaEvent {
                event_id: 1,
                kind: BundleReplicaEventKind::Stored,
                volume_id,
                bundle_id,
                raw_len: Some(raw_len),
                compressed_len: Some(raw_len / 2 + 1),
            }],
        })
        .unwrap();
    }

    fn committed_bundle(bundle_index: u64, bundle_id: HashId, raw_len: u64) -> CommittedBundle {
        CommittedBundle {
            bundle_index,
            bundle_id,
            raw_len,
            compressed_len: raw_len / 2 + 1,
        }
    }

    #[test]
    fn commit_append_rejects_sparse_layout() {
        let (_temp, mut db, volume_id) = test_db();
        let lease = begin_create(&mut db, volume_id);
        let bundle_id = HashId([1; 32]);
        record_bundle(&mut db, volume_id, bundle_id, 1024);

        let result = db.commit_append(CommitAppendRequest {
            lease_id: lease.lease_id,
            base_size: 0,
            new_size: 64 * 1024 * 1024,
            bundles: vec![committed_bundle(100, bundle_id, 1024)],
        });

        assert!(matches!(result, Err(Fs0Error::InvalidRequest)));
    }

    #[test]
    fn commit_append_rejects_non_contiguous_append_index() {
        let (_temp, mut db, volume_id) = test_db();
        let first_lease = begin_create(&mut db, volume_id);
        let first_bundle_id = HashId([1; 32]);
        record_bundle(&mut db, volume_id, first_bundle_id, 12);
        db.commit_append(CommitAppendRequest {
            lease_id: first_lease.lease_id,
            base_size: 0,
            new_size: 12,
            bundles: vec![committed_bundle(0, first_bundle_id, 12)],
        })
        .unwrap();

        let second_lease = begin_append(&mut db, volume_id, 12);
        let second_bundle_id = HashId([2; 32]);
        record_bundle(&mut db, volume_id, second_bundle_id, 12);
        let result = db.commit_append(CommitAppendRequest {
            lease_id: second_lease.lease_id,
            base_size: 12,
            new_size: 24,
            bundles: vec![committed_bundle(2, second_bundle_id, 12)],
        });

        assert!(matches!(result, Err(Fs0Error::InvalidRequest)));
    }

    #[test]
    fn commit_append_rejects_bundle_metadata_mismatch() {
        let (_temp, mut db, volume_id) = test_db();
        let lease = begin_create(&mut db, volume_id);
        let bundle_id = HashId([1; 32]);
        record_bundle(&mut db, volume_id, bundle_id, 12);

        let result = db.commit_append(CommitAppendRequest {
            lease_id: lease.lease_id,
            base_size: 0,
            new_size: 13,
            bundles: vec![committed_bundle(0, bundle_id, 13)],
        });

        assert!(matches!(result, Err(Fs0Error::InvalidRequest)));
    }

    #[test]
    fn record_bundle_events_rejects_metadata_conflicts() {
        let (_temp, mut db, volume_id) = test_db();
        let bundle_id = HashId([1; 32]);
        record_bundle(&mut db, volume_id, bundle_id, 12);

        let result = db.record_bundle_events(BundleReplicaReport {
            events: vec![BundleReplicaEvent {
                event_id: 2,
                kind: BundleReplicaEventKind::Stored,
                volume_id,
                bundle_id,
                raw_len: Some(13),
                compressed_len: Some(7),
            }],
        });

        assert!(matches!(result, Err(Fs0Error::InvalidData { .. })));
    }
}
