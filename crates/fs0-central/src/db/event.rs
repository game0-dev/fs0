use crate::Fs0Result;
use fs0_core::{
    SqliteRowExt,
    protocol::{FileChangeLog, FileChangeLogKind, FileChangeLogs},
    utils::{join_fs0_path, u64_to_i64},
};
use rusqlite::params;

use super::CentralTx;

impl CentralTx<'_> {
    pub(crate) fn insert_file_change_log(
        &self,
        kind: FileChangeLogKind,
        new_target: Option<(&str, &str)>,
        file_id: Option<u64>,
        created_at_ms: u64,
    ) -> Fs0Result<()> {
        self.inner.execute(
            "INSERT INTO file_events (
                event_type, new_dir, new_name, file_id, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                file_change_log_kind(kind),
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
                    new_dir, new_name, created_at_ms
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
    let new_dir: Option<String> = row.get(3)?;
    let new_name: Option<String> = row.get(4)?;
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
        new_path: match (new_dir.as_deref(), new_name.as_deref()) {
            (Some(dir), Some(name)) => Some(join_fs0_path(dir, name).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?),
            _ => None,
        },
        created_at_ms: row.u64(5, "created_at_ms")?,
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
