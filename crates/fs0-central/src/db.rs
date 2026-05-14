use crate::{CentralError, Result};
use fs0_core::{DirectoryEntries, DirectoryEntry, FileRecord, Fs0Path};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub(crate) struct CentralDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFileLocation {
    pub path: Fs0Path,
    pub base_version: u64,
    pub base_size_bytes: u64,
    pub new_version: u64,
    pub new_size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub volume_ids: Vec<u64>,
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
        Self::open_conn(conn)
    }

    fn open_conn(conn: Connection) -> Result<Self> {
        configure_connection(&conn)?;
        create_schema(&conn)?;
        Ok(Self { conn })
    }

    pub(crate) fn create_volume(
        &mut self,
        name: Option<&str>,
        max_bytes: u64,
    ) -> Result<VolumeRecord> {
        let now = now_ms();
        self.conn.execute(
            "INSERT INTO volumes (name, max_bytes, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?3)",
            params![
                name,
                to_i64(max_bytes, "max_bytes")?,
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

    pub(crate) fn commit_file_location(
        &mut self,
        request: CommitFileLocation,
    ) -> Result<FileRecord> {
        let now = now_ms();
        let (parent_path, name) = split_path(request.path.as_str())?;
        let tx = self.conn.transaction()?;
        let existing = tx
            .query_row(
                "SELECT file_id, version, size_bytes
                 FROM files
                 WHERE path = ?1",
                params![request.path.as_str()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;

        let file_id = match existing {
            Some((file_id, _, _)) => {
                let changed = tx.execute(
                    "UPDATE files
                     SET version = ?4,
                         size_bytes = ?5,
                         compressed_size_bytes = ?6,
                         updated_at_ms = ?7
                     WHERE file_id = ?1
                       AND version = ?2
                       AND size_bytes = ?3",
                    params![
                        file_id,
                        to_i64(request.base_version, "base_version")?,
                        to_i64(request.base_size_bytes, "base_size_bytes")?,
                        to_i64(request.new_version, "new_version")?,
                        to_i64(request.new_size_bytes, "new_size_bytes")?,
                        to_i64(request.compressed_size_bytes, "compressed_size_bytes")?,
                        to_i64(now, "updated_at_ms")?,
                    ],
                )?;
                if changed != 1 {
                    return Err(CentralError::version_conflict());
                }
                from_i64(file_id, "file_id")?
            }
            None => {
                if request.base_version != 0 || request.base_size_bytes != 0 {
                    return Err(CentralError::not_found(format!(
                        "file was not found in central metadata: {}",
                        request.path
                    )));
                }
                tx.execute(
                    "INSERT INTO files (
                        path, parent_path, name, version, size_bytes,
                        compressed_size_bytes, created_at_ms, updated_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                    params![
                        request.path.as_str(),
                        parent_path,
                        name,
                        to_i64(request.new_version, "new_version")?,
                        to_i64(request.new_size_bytes, "new_size_bytes")?,
                        to_i64(request.compressed_size_bytes, "compressed_size_bytes")?,
                        to_i64(now, "created_at_ms")?,
                    ],
                )?;
                from_i64(tx.last_insert_rowid(), "file_id")?
            }
        };

        tx.execute(
            "DELETE FROM file_volumes WHERE file_id = ?1",
            params![to_i64(file_id, "file_id")?],
        )?;

        let mut volume_ids = request.volume_ids;
        volume_ids.sort_unstable();
        volume_ids.dedup();
        for volume_id in &volume_ids {
            if get_volume_in_tx(&tx, *volume_id)?.is_none() {
                return Err(CentralError::not_found(format!(
                    "volume {volume_id} was not found in central metadata"
                )));
            }
            tx.execute(
                "INSERT INTO file_volumes (file_id, volume_id)
                 VALUES (?1, ?2)",
                params![
                    to_i64(file_id, "file_id")?,
                    to_i64(*volume_id, "volume_id")?,
                ],
            )?;
        }
        tx.execute(
            "INSERT INTO namespace_events (
                event_type, path, file_id, file_version, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "FileCommitted",
                request.path.as_str(),
                to_i64(file_id, "file_id")?,
                to_i64(request.new_version, "new_version")?,
                to_i64(now, "created_at_ms")?,
            ],
        )?;
        tx.commit()?;

        self.get_file_by_id(file_id)?.ok_or_else(|| {
            CentralError::not_found(format!("file {file_id} was not found in central metadata"))
        })
    }

    pub(crate) fn get_file_by_path(&self, path: &Fs0Path) -> Result<Option<FileRecord>> {
        let file_id = self
            .conn
            .query_row(
                "SELECT file_id FROM files WHERE path = ?1",
                params![path.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|file_id| from_i64(file_id, "file_id"))
            .transpose()?;

        match file_id {
            Some(file_id) => self.get_file_by_id(file_id),
            None => Ok(None),
        }
    }

    pub(crate) fn get_file_by_id(&self, file_id: u64) -> Result<Option<FileRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, path, version, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE file_id = ?1",
        )?;
        let row = stmt
            .query_row(params![to_i64(file_id, "file_id")?], row_to_file_record)
            .optional()?;

        match row {
            Some(mut record) => {
                record.volume_ids = self.load_file_volumes(record.file_id)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn list_directory(
        &self,
        parent_path: &Fs0Path,
        limit: u32,
        cursor: Option<u64>,
    ) -> Result<DirectoryEntries> {
        let limit = limit.clamp(1, 1024) as usize;
        let offset = cursor.unwrap_or(0);
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, name, path, version, size_bytes
             FROM files
             WHERE parent_path = ?1
             ORDER BY name
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![
                parent_path.as_str(),
                to_i64(limit as u64 + 1, "limit")?,
                to_i64(offset, "cursor")?,
            ],
            |row| {
                let path: String = row.get(2)?;
                Ok(DirectoryEntry {
                    file_id: from_i64(row.get(0)?, "file_id").map_err(to_sql_error)?,
                    name: row.get(1)?,
                    path: Fs0Path::new(path)
                        .map_err(CentralError::from)
                        .map_err(to_sql_error)?,
                    version: from_i64(row.get(3)?, "version").map_err(to_sql_error)?,
                    size_bytes: from_i64(row.get(4)?, "size_bytes").map_err(to_sql_error)?,
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

    pub(crate) fn list_files(&self) -> Result<Vec<FileRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT file_id, path, version, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             ORDER BY path",
        )?;
        let rows = stmt.query_map([], row_to_file_record)?;
        let mut files = Vec::new();
        for row in rows {
            let mut record = row?;
            record.volume_ids = self.load_file_volumes(record.file_id)?;
            files.push(record);
        }
        Ok(files)
    }

    fn load_file_volumes(&self, file_id: u64) -> Result<Vec<u64>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT volume_id
             FROM file_volumes
             WHERE file_id = ?1
             ORDER BY volume_id",
        )?;
        let rows = stmt.query_map(params![to_i64(file_id, "file_id")?], |row| {
            from_i64(row.get::<_, i64>(0)?, "volume_id").map_err(to_sql_error)
        })?;
        let mut volume_ids = Vec::new();
        for row in rows {
            volume_ids.push(row?);
        }
        Ok(volume_ids)
    }
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
        DROP TABLE IF EXISTS storages;

        CREATE TABLE IF NOT EXISTS volumes (
          volume_id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT,
          max_bytes INTEGER NOT NULL,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS files (
          file_id INTEGER PRIMARY KEY AUTOINCREMENT,
          path TEXT NOT NULL UNIQUE,
          parent_path TEXT NOT NULL,
          name TEXT NOT NULL,
          version INTEGER NOT NULL,
          size_bytes INTEGER NOT NULL,
          compressed_size_bytes INTEGER NOT NULL,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_files_parent_name
          ON files(parent_path, name);

        CREATE TABLE IF NOT EXISTS file_volumes (
          file_id INTEGER NOT NULL,
          volume_id INTEGER NOT NULL,
          PRIMARY KEY (file_id, volume_id),
          FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
          FOREIGN KEY (volume_id) REFERENCES volumes(volume_id)
        );

        CREATE INDEX IF NOT EXISTS idx_file_volumes_volume_id
          ON file_volumes(volume_id);

        CREATE TABLE IF NOT EXISTS append_leases (
          lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
          file_id INTEGER NOT NULL,
          client_id INTEGER NOT NULL,
          base_version INTEGER NOT NULL,
          base_size_bytes INTEGER NOT NULL,
          fencing_token INTEGER NOT NULL,
          expires_at_ms INTEGER NOT NULL,
          state TEXT NOT NULL,
          FOREIGN KEY (file_id) REFERENCES files(file_id)
        );

        CREATE TABLE IF NOT EXISTS namespace_events (
          epoch INTEGER PRIMARY KEY AUTOINCREMENT,
          event_type TEXT NOT NULL,
          path TEXT NOT NULL,
          file_id INTEGER,
          file_version INTEGER,
          created_at_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jobs (
          job_id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_type TEXT NOT NULL,
          target_volume_id INTEGER,
          payload BLOB NOT NULL,
          state TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        );
        ",
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

fn row_to_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let path: String = row.get(1)?;
    Ok(FileRecord {
        file_id: from_i64(row.get(0)?, "file_id").map_err(to_sql_error)?,
        path: Fs0Path::new(path)
            .map_err(CentralError::from)
            .map_err(to_sql_error)?,
        version: from_i64(row.get(2)?, "version").map_err(to_sql_error)?,
        size_bytes: from_i64(row.get(3)?, "size_bytes").map_err(to_sql_error)?,
        compressed_size_bytes: from_i64(row.get(4)?, "compressed_size_bytes")
            .map_err(to_sql_error)?,
        created_at_ms: from_i64(row.get(5)?, "created_at_ms").map_err(to_sql_error)?,
        updated_at_ms: from_i64(row.get(6)?, "updated_at_ms").map_err(to_sql_error)?,
        volume_ids: Vec::new(),
    })
}

fn get_volume_in_tx(tx: &rusqlite::Transaction<'_>, volume_id: u64) -> Result<Option<u64>> {
    tx.query_row(
        "SELECT volume_id FROM volumes WHERE volume_id = ?1",
        params![to_i64(volume_id, "volume_id")?],
        |row| row.get::<_, i64>(0),
    )
    .optional()?
    .map(|id| from_i64(id, "volume_id"))
    .transpose()
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
