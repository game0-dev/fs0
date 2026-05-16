use crate::{CentralError, Result};
use fs0_core::{
    AbortAppendRequest, AppendLease, BeginAppendRequest, ChunkId, CommittedChunk,
    CreateVolumeRequest, DirectoryEntries, DirectoryEntry, FileChunkRef, FileEvent, FileEventKind,
    FileEvents, FileManifest, FileRecord, Fs0Path, ListFileEventsRequest, ReplicaLocation,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const APPEND_LEASE_TTL_MS: u64 = 30_000;

#[derive(Debug)]
pub(crate) struct CentralDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRecord {
    pub volume_id: u64,
    pub name: Option<String>,
    pub max_bytes: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl CentralDb {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        configure_connection(&conn)?;
        create_schema(&conn)?;
        Ok(Self { conn })
    }

    pub(crate) fn create_volume(&mut self, request: CreateVolumeRequest) -> Result<VolumeRecord> {
        let now = now_ms();
        self.conn.execute(
            "INSERT INTO volumes (name, max_bytes, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?3)",
            params![
                request.name.as_deref(),
                to_i64(request.max_bytes, "max_bytes")?,
                to_i64(now, "created_at_ms")?,
            ],
        )?;
        let volume_id = from_i64(self.conn.last_insert_rowid(), "volume_id")?;
        self.get_volume(volume_id)?.ok_or_else(|| {
            CentralError::not_found(format!(
                "volume {volume_id} was not found in central metadata"
            ))
        })
    }

    pub(crate) fn get_volume(&self, volume_id: u64) -> Result<Option<VolumeRecord>> {
        self.conn
            .query_row(
                "SELECT volume_id, name, max_bytes, created_at_ms, updated_at_ms
                 FROM volumes
                 WHERE volume_id = ?1",
                params![to_i64(volume_id, "volume_id")?],
                row_to_volume_record,
            )
            .optional()
            .map_err(CentralError::from)
    }

    pub(crate) fn begin_append(
        &mut self,
        request: BeginAppendRequest,
        client_id: u64,
    ) -> Result<AppendLease> {
        let now = now_ms();
        let expires_at_ms = now + APPEND_LEASE_TTL_MS;
        let (dir, name) = split_path(request.path.as_str())?;
        let tx = self.conn.transaction()?;
        let file_id = match load_file_identity_tx(&tx, &dir, &name)? {
            Some(file) => {
                if file.size_bytes != request.expected_size {
                    return Err(CentralError::version_conflict());
                }
                file.file_id
            }
            None => {
                if !request.create {
                    return Err(CentralError::not_found(format!(
                        "file was not found in central metadata: {}",
                        request.path
                    )));
                }
                if request.expected_size != 0 {
                    return Err(CentralError::version_conflict());
                }
                create_file(&tx, &dir, &name, now)?
            }
        };
        let volume_id = select_append_volume(&tx, request.prefer_volume_name.as_deref())?;

        tx.execute(
            "UPDATE append_leases
             SET state = 'expired'
             WHERE file_id = ?1
               AND state = 'active'
               AND expires_at_ms <= ?2",
            params![to_i64(file_id, "file_id")?, to_i64(now, "expires_at_ms")?],
        )?;

        let active_lease = tx
            .query_row(
                "SELECT lease_id
                 FROM append_leases
                 WHERE file_id = ?1
                   AND state = 'active'
                 LIMIT 1",
                params![to_i64(file_id, "file_id")?],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if active_lease.is_some() {
            return Err(CentralError::control(
                fs0_core::ControlErrorCode::AlreadyExists,
                format!("append lease already exists for {}", request.path),
            ));
        }

        tx.execute(
            "INSERT INTO append_leases (
                file_id, client_id, volume_id, base_size_bytes, prefer_volume_name,
                state, expires_at_ms, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?7)",
            params![
                to_i64(file_id, "file_id")?,
                to_i64(client_id, "client_id")?,
                to_i64(volume_id, "volume_id")?,
                to_i64(request.expected_size, "base_size_bytes")?,
                request.prefer_volume_name.as_deref(),
                to_i64(expires_at_ms, "expires_at_ms")?,
                to_i64(now, "created_at_ms")?,
            ],
        )?;
        let lease_id = from_i64(tx.last_insert_rowid(), "lease_id")?;
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
    ) -> Result<FileManifest> {
        let now = now_ms();
        let tx = self.conn.transaction()?;
        let lease = load_active_lease(&tx, request.lease_id)?;
        if lease.base_size_bytes != request.base_size {
            return Err(CentralError::version_conflict());
        }
        if request.new_size < request.base_size {
            return Err(CentralError::invalid_request(
                "new_size is smaller than base_size",
            ));
        }
        validate_committed_chunks(&request.chunks, request.new_size)?;

        let file = load_file_by_id_tx(&tx, lease.file_id)?.ok_or_else(|| {
            CentralError::not_found(format!("file {} was not found", lease.file_id))
        })?;
        if file.size_bytes != request.base_size {
            return Err(CentralError::version_conflict());
        }
        let compressed_size_bytes = request
            .chunks
            .iter()
            .map(|chunk| chunk.compressed_len)
            .try_fold(0u64, |sum, len| sum.checked_add(len))
            .ok_or_else(|| {
                CentralError::IntegerConversion("compressed size overflow".to_owned())
            })?;

        tx.execute(
            "DELETE FROM file_chunks WHERE file_id = ?1",
            params![to_i64(lease.file_id, "file_id")?],
        )?;
        for chunk in &request.chunks {
            upsert_chunk(&tx, chunk)?;
            for replica in &chunk.replicas {
                insert_chunk_replica(&tx, chunk.chunk_id, replica)?;
            }
            tx.execute(
                "INSERT INTO file_chunks (
                    file_id, chunk_index, chunk_id
                 ) VALUES (?1, ?2, ?3)",
                params![
                    to_i64(lease.file_id, "file_id")?,
                    to_i64(chunk.chunk_index, "chunk_index")?,
                    chunk.chunk_id.as_bytes().as_slice(),
                ],
            )?;
        }

        tx.execute(
            "UPDATE files
             SET size_bytes = ?2,
                 compressed_size_bytes = ?3,
                 updated_at_ms = ?4
             WHERE file_id = ?1",
            params![
                to_i64(lease.file_id, "file_id")?,
                to_i64(request.new_size, "size_bytes")?,
                to_i64(compressed_size_bytes, "compressed_size_bytes")?,
                to_i64(now, "updated_at_ms")?,
            ],
        )?;
        tx.execute(
            "UPDATE append_leases
             SET state = 'committed'
             WHERE lease_id = ?1",
            params![to_i64(request.lease_id, "lease_id")?],
        )?;
        insert_file_event(
            &tx,
            if request.base_size == 0 {
                FileEventKind::Created
            } else {
                FileEventKind::Updated
            },
            None,
            Some((&file.dir, &file.name)),
            Some(lease.file_id),
            now,
        )?;
        tx.commit()?;

        self.get_file_manifest_by_id(lease.file_id)
    }

    pub(crate) fn abort_append(&mut self, request: AbortAppendRequest) -> Result<()> {
        let tx = self.conn.transaction()?;
        let _lease = load_active_lease(&tx, request.lease_id)?;
        tx.execute(
            "UPDATE append_leases
             SET state = 'aborted'
             WHERE lease_id = ?1",
            params![to_i64(request.lease_id, "lease_id")?],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn lease_prefer_volume_name(&self, lease_id: u64) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT prefer_volume_name
                 FROM append_leases
                 WHERE lease_id = ?1",
                params![to_i64(lease_id, "lease_id")?],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .ok_or_else(|| {
                CentralError::not_found(format!("append lease {lease_id} was not found"))
            })
    }

    pub(crate) fn get_file_by_path(&self, path: &Fs0Path) -> Result<Option<FileRecord>> {
        let (dir, name) = split_path(path.as_str())?;
        let file = self
            .conn
            .query_row(
                "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                        created_at_ms, updated_at_ms
                 FROM files
                 WHERE dir = ?1 AND name = ?2",
                params![dir, name],
                row_to_file_identity,
            )
            .optional()?;
        file.map(file_to_record).transpose()
    }

    pub(crate) fn list_files(&self) -> Result<Vec<FileRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             ORDER BY dir, name",
        )?;
        let rows = stmt.query_map([], row_to_file_identity)?;
        let mut files = Vec::new();
        for row in rows {
            files.push(file_to_record(row?)?);
        }
        Ok(files)
    }

    pub(crate) fn list_directory(
        &self,
        dir: &Fs0Path,
        limit: u32,
        cursor: Option<u64>,
    ) -> Result<DirectoryEntries> {
        let limit = limit.clamp(1, 1024) as usize;
        let offset = cursor.unwrap_or(0);
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, dir, name, size_bytes
             FROM files
             WHERE dir = ?1
             ORDER BY name
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![
                dir.as_str(),
                to_i64(limit as u64 + 1, "limit")?,
                to_i64(offset, "cursor")?,
            ],
            |row| {
                let dir: String = row.get(1)?;
                let name: String = row.get(2)?;
                Ok(DirectoryEntry {
                    file_id: from_i64(row.get(0)?, "file_id").map_err(to_sql_error)?,
                    name: name.clone(),
                    path: join_path(&dir, &name).map_err(to_sql_error)?,
                    size_bytes: from_i64(row.get(3)?, "size_bytes").map_err(to_sql_error)?,
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

    pub(crate) fn list_file_events(&self, request: ListFileEventsRequest) -> Result<FileEvents> {
        let limit = request.limit.clamp(1, 1024) as usize;
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
                to_i64(request.after_event_id, "after_event_id")?,
                to_i64(limit as u64 + 1, "limit")?,
            ],
            row_to_file_event,
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
        Ok(FileEvents {
            events,
            next_event_id,
        })
    }

    pub(crate) fn chunk_replicas(&self, chunk_id: ChunkId) -> Result<Vec<ReplicaLocation>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT volume_id
             FROM chunk_replicas
             WHERE chunk_id = ?1
             ORDER BY volume_id",
        )?;
        let rows = stmt.query_map(params![chunk_id.as_bytes().as_slice()], |row| {
            Ok(ReplicaLocation {
                storage_id: 0,
                volume_id: from_i64(row.get(0)?, "volume_id").map_err(to_sql_error)?,
            })
        })?;
        let mut replicas = Vec::new();
        for row in rows {
            replicas.push(row?);
        }
        Ok(replicas)
    }

    pub(crate) fn get_file_manifest(&self, path: &Fs0Path) -> Result<FileManifest> {
        let file = self
            .get_file_by_path(path)?
            .ok_or_else(|| CentralError::not_found(format!("file was not found: {path}")))?;
        self.get_file_manifest_by_id(file.file_id)
    }

    fn get_file_manifest_by_id(&self, file_id: u64) -> Result<FileManifest> {
        let file = load_file_by_id(&self.conn, file_id)?.ok_or_else(|| {
            CentralError::not_found(format!("file {file_id} was not found in central metadata"))
        })?;
        let mut stmt = self.conn.prepare_cached(
            "SELECT fc.chunk_index, fc.chunk_id, c.raw_len, c.compressed_len
             FROM file_chunks fc
             JOIN chunks c ON c.chunk_id = fc.chunk_id
             WHERE fc.file_id = ?1
             ORDER BY chunk_index",
        )?;
        let rows = stmt.query_map(params![to_i64(file.file_id, "file_id")?], |row| {
            let chunk_id = blob_to_chunk_id(row.get(1)?, "chunk_id").map_err(to_sql_error)?;
            Ok((
                from_i64(row.get(0)?, "chunk_index").map_err(to_sql_error)?,
                chunk_id,
                from_i64(row.get(2)?, "raw_len").map_err(to_sql_error)?,
                from_i64(row.get(3)?, "compressed_len").map_err(to_sql_error)?,
            ))
        })?;
        let mut chunks = Vec::new();
        for row in rows {
            let (chunk_index, chunk_id, raw_len, compressed_len) = row?;
            chunks.push(FileChunkRef {
                chunk_index,
                raw_len,
                compressed_len,
                chunk_id,
                replicas: self.chunk_replicas(chunk_id)?,
            });
        }
        Ok(FileManifest {
            file_id: file.file_id,
            path: join_path(&file.dir, &file.name)?,
            size: file.size_bytes,
            chunks,
        })
    }
}

#[derive(Debug)]
struct FileIdentity {
    file_id: u64,
    dir: String,
    name: String,
    size_bytes: u64,
    compressed_size_bytes: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug)]
struct LeaseRecord {
    file_id: u64,
    base_size_bytes: u64,
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

fn create_file(tx: &rusqlite::Transaction<'_>, dir: &str, name: &str, now: u64) -> Result<u64> {
    tx.execute(
        "INSERT INTO files (
            dir, name, size_bytes, compressed_size_bytes,
            created_at_ms, updated_at_ms
        ) VALUES (?1, ?2, 0, 0, ?3, ?3)",
        params![dir, name, to_i64(now, "created_at_ms")?],
    )?;
    from_i64(tx.last_insert_rowid(), "file_id")
}

fn select_append_volume(
    tx: &rusqlite::Transaction<'_>,
    prefer_volume_name: Option<&str>,
) -> Result<u64> {
    if let Some(name) = prefer_volume_name {
        if let Some(volume_id) = tx
            .query_row(
                "SELECT volume_id
                 FROM volumes
                 WHERE name = ?1
                 ORDER BY volume_id
                 LIMIT 1",
                params![name],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            return from_i64(volume_id, "volume_id");
        }
    }
    let volume_id = tx
        .query_row(
            "SELECT volume_id
         FROM volumes
         ORDER BY volume_id
         LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or_else(|| CentralError::not_found("no volume is registered in central metadata"))?;
    from_i64(volume_id, "volume_id")
}

fn load_file_identity_tx(
    tx: &rusqlite::Transaction<'_>,
    dir: &str,
    name: &str,
) -> Result<Option<FileIdentity>> {
    tx.query_row(
        "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
         FROM files
         WHERE dir = ?1 AND name = ?2",
        params![dir, name],
        row_to_file_identity,
    )
    .optional()
    .map_err(CentralError::from)
}

fn load_file_by_id(conn: &Connection, file_id: u64) -> Result<Option<FileIdentity>> {
    conn.query_row(
        "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
         FROM files
         WHERE file_id = ?1",
        params![to_i64(file_id, "file_id")?],
        row_to_file_identity,
    )
    .optional()
    .map_err(CentralError::from)
}

fn load_file_by_id_tx(
    tx: &rusqlite::Transaction<'_>,
    file_id: u64,
) -> Result<Option<FileIdentity>> {
    tx.query_row(
        "SELECT file_id, dir, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
         FROM files
         WHERE file_id = ?1",
        params![to_i64(file_id, "file_id")?],
        row_to_file_identity,
    )
    .optional()
    .map_err(CentralError::from)
}

fn load_active_lease(tx: &rusqlite::Transaction<'_>, lease_id: u64) -> Result<LeaseRecord> {
    tx.query_row(
        "SELECT file_id, base_size_bytes
         FROM append_leases
         WHERE lease_id = ?1
           AND state = 'active'",
        params![to_i64(lease_id, "lease_id")?],
        |row| {
            Ok(LeaseRecord {
                file_id: from_i64(row.get(0)?, "file_id").map_err(to_sql_error)?,
                base_size_bytes: from_i64(row.get(1)?, "base_size_bytes").map_err(to_sql_error)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| CentralError::not_found(format!("append lease {lease_id} was not found")))
}

fn validate_committed_chunks(chunks: &[CommittedChunk], new_size: u64) -> Result<()> {
    let mut total_size = 0u64;
    for (expected_index, chunk) in chunks.iter().enumerate() {
        if chunk.chunk_index != expected_index as u64 {
            return Err(CentralError::invalid_request(
                "chunk indexes must be contiguous",
            ));
        }
        if chunk.raw_len == 0 || chunk.compressed_len == 0 {
            return Err(CentralError::invalid_request(
                "chunk lengths must be non-zero",
            ));
        }
        if chunk.replicas.is_empty() {
            return Err(CentralError::invalid_request(
                "chunk must have at least one replica",
            ));
        }
        total_size = total_size
            .checked_add(chunk.raw_len)
            .ok_or_else(|| CentralError::IntegerConversion("file size overflow".to_owned()))?;
    }
    if total_size != new_size {
        return Err(CentralError::invalid_request(
            "committed chunk sizes do not match new_size",
        ));
    }
    Ok(())
}

fn upsert_chunk(tx: &rusqlite::Transaction<'_>, chunk: &CommittedChunk) -> Result<()> {
    tx.execute(
        "INSERT INTO chunks (
            chunk_id, raw_len, compressed_len
         ) VALUES (?1, ?2, ?3)
         ON CONFLICT(chunk_id) DO UPDATE SET
            raw_len = excluded.raw_len,
            compressed_len = excluded.compressed_len",
        params![
            chunk.chunk_id.as_bytes().as_slice(),
            to_i64(chunk.raw_len, "raw_len")?,
            to_i64(chunk.compressed_len, "compressed_len")?,
        ],
    )?;
    Ok(())
}

fn insert_chunk_replica(
    tx: &rusqlite::Transaction<'_>,
    chunk_id: ChunkId,
    replica: &ReplicaLocation,
) -> Result<()> {
    tx.execute(
        "INSERT INTO chunk_replicas (
            chunk_id, volume_id
         ) VALUES (?1, ?2)
         ON CONFLICT(chunk_id, volume_id) DO NOTHING",
        params![
            chunk_id.as_bytes().as_slice(),
            to_i64(replica.volume_id, "volume_id")?,
        ],
    )?;
    Ok(())
}

fn insert_file_event(
    tx: &rusqlite::Transaction<'_>,
    kind: FileEventKind,
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
            file_event_kind(kind),
            old_target.map(|target| target.0),
            old_target.map(|target| target.1),
            new_target.map(|target| target.0),
            new_target.map(|target| target.1),
            file_id.map(|id| to_i64(id, "file_id")).transpose()?,
            to_i64(created_at_ms, "created_at_ms")?,
        ],
    )?;
    Ok(())
}

fn row_to_volume_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<VolumeRecord> {
    Ok(VolumeRecord {
        volume_id: from_i64(row.get(0)?, "volume_id").map_err(to_sql_error)?,
        name: row.get(1)?,
        max_bytes: from_i64(row.get(2)?, "max_bytes").map_err(to_sql_error)?,
        created_at_ms: from_i64(row.get(3)?, "created_at_ms").map_err(to_sql_error)?,
        updated_at_ms: from_i64(row.get(4)?, "updated_at_ms").map_err(to_sql_error)?,
    })
}

fn row_to_file_identity(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileIdentity> {
    Ok(FileIdentity {
        file_id: from_i64(row.get(0)?, "file_id").map_err(to_sql_error)?,
        dir: row.get(1)?,
        name: row.get(2)?,
        size_bytes: from_i64(row.get(3)?, "size_bytes").map_err(to_sql_error)?,
        compressed_size_bytes: from_i64(row.get(4)?, "compressed_size_bytes")
            .map_err(to_sql_error)?,
        created_at_ms: from_i64(row.get(5)?, "created_at_ms").map_err(to_sql_error)?,
        updated_at_ms: from_i64(row.get(6)?, "updated_at_ms").map_err(to_sql_error)?,
    })
}

fn row_to_file_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileEvent> {
    let old_dir: Option<String> = row.get(3)?;
    let old_name: Option<String> = row.get(4)?;
    let new_dir: Option<String> = row.get(5)?;
    let new_name: Option<String> = row.get(6)?;
    Ok(FileEvent {
        event_id: from_i64(row.get(0)?, "event_id").map_err(to_sql_error)?,
        kind: parse_file_event_kind(row.get::<_, String>(1)?.as_str()).map_err(to_sql_error)?,
        file_id: row
            .get::<_, Option<i64>>(2)?
            .map(|value| from_i64(value, "file_id"))
            .transpose()
            .map_err(to_sql_error)?,
        old_path: join_optional_path(old_dir.as_deref(), old_name.as_deref())
            .map_err(to_sql_error)?,
        new_path: join_optional_path(new_dir.as_deref(), new_name.as_deref())
            .map_err(to_sql_error)?,
        created_at_ms: from_i64(row.get(7)?, "created_at_ms").map_err(to_sql_error)?,
    })
}

fn file_to_record(file: FileIdentity) -> Result<FileRecord> {
    Ok(FileRecord {
        file_id: file.file_id,
        path: join_path(&file.dir, &file.name)?,
        size_bytes: file.size_bytes,
        compressed_size_bytes: file.compressed_size_bytes,
        created_at_ms: file.created_at_ms,
        updated_at_ms: file.updated_at_ms,
    })
}

fn split_path(path: &str) -> Result<(String, String)> {
    if path == "/" {
        return Err(CentralError::invalid_request(
            "root path cannot be a file".to_owned(),
        ));
    }
    let (parent, name) = path.rsplit_once('/').ok_or_else(|| {
        CentralError::invalid_request(format!("path must be absolute with a file name: {path}"))
    })?;
    if name.is_empty() {
        return Err(CentralError::invalid_request(format!(
            "path must include a file name: {path}"
        )));
    }
    let parent = if parent.is_empty() { "/" } else { parent };
    Ok((parent.to_owned(), name.to_owned()))
}

fn join_path(dir: &str, name: &str) -> Result<Fs0Path> {
    if dir == "/" {
        Fs0Path::new(format!("/{name}")).map_err(CentralError::from)
    } else {
        Fs0Path::new(format!("{dir}/{name}")).map_err(CentralError::from)
    }
}

fn join_optional_path(dir: Option<&str>, name: Option<&str>) -> Result<Option<Fs0Path>> {
    match (dir, name) {
        (Some(dir), Some(name)) => join_path(dir, name).map(Some),
        _ => Ok(None),
    }
}

fn blob_to_chunk_id(value: Vec<u8>, name: &str) -> Result<ChunkId> {
    let bytes = value.try_into().map_err(|value: Vec<u8>| {
        CentralError::invalid_request(format!("{name} must be 32 bytes, got {}", value.len()))
    })?;
    Ok(ChunkId(bytes))
}

fn file_event_kind(kind: FileEventKind) -> &'static str {
    match kind {
        FileEventKind::Created => "created",
        FileEventKind::Updated => "updated",
        FileEventKind::Moved => "moved",
        FileEventKind::Deleted => "deleted",
    }
}

fn parse_file_event_kind(value: &str) -> Result<FileEventKind> {
    match value {
        "created" => Ok(FileEventKind::Created),
        "updated" => Ok(FileEventKind::Updated),
        "moved" => Ok(FileEventKind::Moved),
        "deleted" => Ok(FileEventKind::Deleted),
        _ => Err(CentralError::invalid_request(format!(
            "unknown file event type: {value}"
        ))),
    }
}

fn to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        CentralError::IntegerConversion(format!("{name} value {value} does not fit in i64"))
    })
}

fn from_i64(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| CentralError::IntegerConversion(format!("{name} value {value} is negative")))
}

fn to_sql_error(err: CentralError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
