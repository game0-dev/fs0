use crate::Fs0Result;
use fs0_core::{
    Fs0Error, HashId, SqliteRowExt,
    protocol::{FileChangeLog, FileChangeLogKind, FileChangeLogs},
    utils::{join_fs0_path, u64_to_i64},
};
use rusqlite::params;
use tracing::error;

use super::CentralTx;

impl CentralTx<'_> {
    pub(crate) fn insert_bundle_replica(
        &self,
        bundle_id: HashId,
        volume_id: u64,
        raw_len: u64,
        compressed_len: u64,
    ) -> Fs0Result<()> {
        if raw_len == 0 || compressed_len == 0 {
            return Err(Fs0Error::InvalidRequest);
        }

        let peer_lengths = self.inner.query_row(
            "SELECT MIN(raw_len), MAX(raw_len),
                    MIN(compressed_len), MAX(compressed_len)
             FROM bundle_replicas
             WHERE bundle_id = ?1 AND volume_id != ?2",
            params![
                bundle_id.as_bytes().as_slice(),
                u64_to_i64(volume_id, "volume_id")?,
            ],
            |row| {
                Ok((
                    row.optional_u64(0, "min_raw_len")?,
                    row.optional_u64(1, "max_raw_len")?,
                    row.optional_u64(2, "min_compressed_len")?,
                    row.optional_u64(3, "max_compressed_len")?,
                ))
            },
        )?;
        if let (
            Some(min_raw_len),
            Some(max_raw_len),
            Some(min_compressed_len),
            Some(max_compressed_len),
        ) = peer_lengths
            && (min_raw_len != raw_len
                || max_raw_len != raw_len
                || min_compressed_len != compressed_len
                || max_compressed_len != compressed_len)
        {
            error!(
                bundle_id = ?bundle_id,
                volume_id,
                raw_len,
                compressed_len,
                min_raw_len,
                max_raw_len,
                min_compressed_len,
                max_compressed_len,
                "bundle replica metadata conflict"
            );
        }

        self.inner.execute(
            "INSERT INTO bundle_replicas (
                bundle_id, volume_id, raw_len, compressed_len
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(bundle_id, volume_id) DO UPDATE SET
                raw_len = excluded.raw_len,
                compressed_len = excluded.compressed_len
             WHERE bundle_replicas.raw_len != excluded.raw_len
                OR bundle_replicas.compressed_len != excluded.compressed_len",
            params![
                bundle_id.as_bytes().as_slice(),
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(raw_len, "raw_len")?,
                u64_to_i64(compressed_len, "compressed_len")?,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn delete_bundle_replica(&self, bundle_id: HashId, volume_id: u64) -> Fs0Result<()> {
        self.inner.execute(
            "DELETE FROM bundle_replicas
             WHERE bundle_id = ?1 AND volume_id = ?2",
            params![
                bundle_id.as_bytes().as_slice(),
                u64_to_i64(volume_id, "volume_id")?,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn insert_file_change_log(
        &self,
        kind: FileChangeLogKind,
        old_target: Option<(&str, &str)>,
        new_target: Option<(&str, &str)>,
        file_id: Option<u64>,
        created_at_ms: u64,
    ) -> Fs0Result<()> {
        self.inner.execute(
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

    pub(crate) fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Fs0Result<FileChangeLogs> {
        let limit = limit.clamp(1, 1024) as usize;
        let mut stmt = self.inner.prepare_cached(
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
