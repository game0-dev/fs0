use crate::Fs0Result;
use fs0_core::{
    Fs0Error, SqliteRowExt,
    protocol::{DirectoryEntries, DirectoryEntry, FileRecord},
    utils::{
        i64_to_u64, join_fs0_path, split_fs0_path_components, split_fs0_path_dir_and_name,
        u64_to_i64,
    },
};
use parking_lot::Mutex;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::HashMap;

const ROOT_DIR_ID: u64 = 0;
const DIR_CACHE_MAX_ENTRIES: usize = 8192;
const DIR_CACHE_TRIM_TO_ENTRIES: usize = 6144;

#[derive(Debug)]
pub(crate) struct FileCatalog {
    dir_cache: Mutex<DirCache>,
}

#[derive(Debug)]
pub(crate) struct FileRow {
    pub(crate) file_id: u64,
    pub(crate) dir_id: u64,
    pub(crate) name: String,
    pub(crate) size_bytes: u64,
    pub(crate) compressed_size_bytes: u64,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

#[derive(Debug)]
struct DirRow {
    dir_id: u64,
    parent_dir_id: Option<u64>,
    name: String,
}

#[derive(Debug)]
struct DirCache {
    path_to_entry: HashMap<String, DirCacheEntry>,
    id_to_path: HashMap<u64, String>,
    next_access: u64,
}

#[derive(Debug)]
struct DirCacheEntry {
    dir_id: u64,
    last_access: u64,
}

impl DirCache {
    fn new() -> Self {
        Self {
            path_to_entry: HashMap::new(),
            id_to_path: HashMap::new(),
            next_access: 1,
        }
    }

    fn get_id(&mut self, path: &str) -> Option<u64> {
        if path == "/" {
            return Some(ROOT_DIR_ID);
        }

        let last_access = self.next_access();
        let entry = self.path_to_entry.get_mut(path)?;
        entry.last_access = last_access;
        Some(entry.dir_id)
    }

    fn get_path(&mut self, dir_id: u64) -> Option<String> {
        if dir_id == ROOT_DIR_ID {
            return Some("/".to_owned());
        }

        let path = self.id_to_path.get(&dir_id)?.clone();
        let last_access = self.next_access();
        if let Some(entry) = self.path_to_entry.get_mut(&path) {
            entry.last_access = last_access;
        }
        Some(path)
    }

    fn remember(&mut self, dir_id: u64, path: &str) {
        if dir_id == ROOT_DIR_ID || path == "/" {
            return;
        }

        let last_access = self.next_access();
        self.path_to_entry.insert(
            path.to_owned(),
            DirCacheEntry {
                dir_id,
                last_access,
            },
        );
        self.id_to_path.insert(dir_id, path.to_owned());
        if self.path_to_entry.len() <= DIR_CACHE_MAX_ENTRIES {
            return;
        }

        let remove_count = self.path_to_entry.len() - DIR_CACHE_TRIM_TO_ENTRIES;
        let mut candidates = self
            .path_to_entry
            .iter()
            .map(|(path, entry)| (path.clone(), entry.dir_id, entry.last_access))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| candidate.2);
        for (path, dir_id, _last_access) in candidates.into_iter().take(remove_count) {
            self.path_to_entry.remove(&path);
            self.id_to_path.remove(&dir_id);
        }
    }

    fn forget(&mut self, dir_id: u64, path: &str) {
        if dir_id == ROOT_DIR_ID || path == "/" {
            return;
        }

        self.path_to_entry.remove(path);
        self.id_to_path.remove(&dir_id);
    }

    fn next_access(&mut self) -> u64 {
        let access = self.next_access;
        self.next_access = self.next_access.saturating_add(1);
        access
    }
}

impl FileCatalog {
    pub(crate) fn new() -> Self {
        Self {
            dir_cache: Mutex::new(DirCache::new()),
        }
    }

    pub(crate) fn create_dir(&self, tx: &Transaction<'_>, path: &str) -> Fs0Result<u64> {
        let mut dir_id = ROOT_DIR_ID;
        let mut current_path = "/".to_owned();
        for name in split_fs0_path_components(path)? {
            current_path = join_fs0_path(&current_path, name)?;
            if let Some(cached_dir_id) = self.dir_cache.lock().get_id(&current_path) {
                dir_id = cached_dir_id;
                continue;
            }

            let existing = tx
                .query_row(
                    "SELECT dir_id
                     FROM dirs
                     WHERE parent_dir_id = ?1 AND name = ?2",
                    params![u64_to_i64(dir_id, "parent_dir_id")?, name],
                    |row| row.u64(0, "dir_id"),
                )
                .optional()?;

            match existing {
                Some(existing_dir_id) => {
                    dir_id = existing_dir_id;
                    self.dir_cache.lock().remember(dir_id, &current_path);
                }
                None => {
                    self.ensure_child_name_available(tx, dir_id, name, None, &current_path)?;
                    tx.execute(
                        "INSERT INTO dirs (parent_dir_id, name)
                         VALUES (?1, ?2)",
                        params![u64_to_i64(dir_id, "parent_dir_id")?, name],
                    )?;
                    dir_id = i64_to_u64(tx.last_insert_rowid(), "dir_id")?;
                }
            }
        }
        Ok(dir_id)
    }

    pub(crate) fn remove_dir(&self, tx: &Transaction<'_>, path: &str) -> Fs0Result<()> {
        if path == "/" {
            return Err(Fs0Error::InvalidRequest);
        }

        let dir_id = self.get_dir_id_by_path(tx, path)?;
        if dir_id == ROOT_DIR_ID {
            return Err(Fs0Error::InvalidRequest);
        }

        let child_dirs = tx.query_row(
            "SELECT COUNT(*)
             FROM dirs
             WHERE parent_dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
            |row| row.u64(0, "child_dirs"),
        )?;
        let child_files = tx.query_row(
            "SELECT COUNT(*)
             FROM files
             WHERE dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
            |row| row.u64(0, "child_files"),
        )?;
        if child_dirs != 0 || child_files != 0 {
            return Err(Fs0Error::InvalidRequest);
        }

        tx.execute(
            "DELETE FROM dirs
             WHERE dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
        )?;
        self.dir_cache.lock().forget(dir_id, path);
        Ok(())
    }

    pub(crate) fn list_directory(
        &self,
        tx: &Transaction<'_>,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Fs0Result<DirectoryEntries> {
        let limit = limit.clamp(1, 1024) as usize;
        let offset = cursor.unwrap_or(0);
        let dir_id = self.get_dir_id_by_path(tx, dir)?;
        let mut stmt = tx.prepare_cached(
            "SELECT file_id, dir_id, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE dir_id = ?1
             ORDER BY name
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(
            params![
                u64_to_i64(dir_id, "dir_id")?,
                u64_to_i64(limit as u64 + 1, "limit")?,
                u64_to_i64(offset, "cursor")?,
            ],
            |row| {
                let name: String = row.get(2)?;
                Ok(DirectoryEntry {
                    file_id: row.u64(0, "file_id")?,
                    name: name.clone(),
                    path: join_fs0_path(dir, &name).map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(err),
                        )
                    })?,
                    size_bytes: row.u64(3, "size_bytes")?,
                    compressed_size_bytes: row.u64(4, "compressed_size_bytes")?,
                    created_at_ms: row.u64(5, "created_at_ms")?,
                    updated_at_ms: row.u64(6, "updated_at_ms")?,
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

    pub(crate) fn get_file_by_path(
        &self,
        tx: &Transaction<'_>,
        path: &str,
    ) -> Fs0Result<FileRecord> {
        let (dir, name) = split_fs0_path_dir_and_name(path)?;
        let dir_id = self.get_dir_id_by_path(tx, &dir)?;
        let file = tx
            .query_row(
                "SELECT file_id, dir_id, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
                 FROM files
                 WHERE dir_id = ?1 AND name = ?2",
                params![u64_to_i64(dir_id, "dir_id")?, name],
                row_to_file_row,
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)?;
        Ok(FileRecord {
            file_id: file.file_id,
            path: join_fs0_path(&dir, &file.name)?,
            size_bytes: file.size_bytes,
            compressed_size_bytes: file.compressed_size_bytes,
            created_at_ms: file.created_at_ms,
            updated_at_ms: file.updated_at_ms,
        })
    }

    pub(crate) fn get_file_by_id(
        &self,
        tx: &Transaction<'_>,
        file_id: u64,
    ) -> Fs0Result<FileRecord> {
        let file = self.get_file_row_by_id(tx, file_id)?;
        let dir = self.get_dir_path(tx, file.dir_id)?;
        Ok(FileRecord {
            file_id: file.file_id,
            path: join_fs0_path(&dir, &file.name)?,
            size_bytes: file.size_bytes,
            compressed_size_bytes: file.compressed_size_bytes,
            created_at_ms: file.created_at_ms,
            updated_at_ms: file.updated_at_ms,
        })
    }

    pub(crate) fn get_file_row_by_id(
        &self,
        tx: &Transaction<'_>,
        file_id: u64,
    ) -> Fs0Result<FileRow> {
        tx.query_row(
            "SELECT file_id, dir_id, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
             FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
            row_to_file_row,
        )
        .optional()?
        .ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn create_file_at_path(
        &self,
        tx: &Transaction<'_>,
        path: &str,
        now: u64,
    ) -> Fs0Result<u64> {
        let (dir, name) = split_fs0_path_dir_and_name(path)?;
        let dir_id = self.create_dir(tx, &dir)?;
        self.ensure_child_name_available(tx, dir_id, &name, None, path)?;
        tx.execute(
            "INSERT INTO files (
                dir_id, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, 0, 0, ?3, ?3)",
            params![
                u64_to_i64(dir_id, "dir_id")?,
                name,
                u64_to_i64(now, "created_at_ms")?
            ],
        )?;
        i64_to_u64(tx.last_insert_rowid(), "file_id")
    }

    pub(crate) fn delete_file_by_id(&self, tx: &Transaction<'_>, file_id: u64) -> Fs0Result<()> {
        tx.execute(
            "DELETE FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
        )?;
        if tx.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }
        Ok(())
    }

    pub(crate) fn copy_file_by_id(
        &self,
        tx: &Transaction<'_>,
        source_file_id: u64,
        target_path: &str,
        now: u64,
    ) -> Fs0Result<()> {
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        let source = self.get_file_row_by_id(tx, source_file_id)?;
        let target_dir_id = self.create_dir(tx, &target_dir)?;
        self.ensure_child_name_available(tx, target_dir_id, &target_name, None, target_path)?;
        tx.execute(
            "INSERT INTO files (
                dir_id, name, size_bytes, compressed_size_bytes,
                created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            params![
                u64_to_i64(target_dir_id, "target_dir_id")?,
                target_name,
                u64_to_i64(source.size_bytes, "size_bytes")?,
                u64_to_i64(source.compressed_size_bytes, "compressed_size_bytes")?,
                u64_to_i64(now, "created_at_ms")?,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn rename_file_by_id(
        &self,
        tx: &Transaction<'_>,
        file_id: u64,
        target_path: &str,
        now: u64,
    ) -> Fs0Result<()> {
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        let target_dir_id = self.create_dir(tx, &target_dir)?;
        self.ensure_child_name_available(
            tx,
            target_dir_id,
            &target_name,
            Some(file_id),
            target_path,
        )?;
        tx.execute(
            "UPDATE files
             SET dir_id = ?2, name = ?3, updated_at_ms = ?4
             WHERE file_id = ?1",
            params![
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(target_dir_id, "target_dir_id")?,
                target_name,
                u64_to_i64(now, "updated_at_ms")?,
            ],
        )?;
        if tx.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }

        Ok(())
    }

    pub(crate) fn get_dir_path(&self, tx: &Transaction<'_>, dir_id: u64) -> Fs0Result<String> {
        if let Some(path) = self.dir_cache.lock().get_path(dir_id) {
            return Ok(path);
        }

        let mut current_dir_id = dir_id;
        let mut names = Vec::new();
        loop {
            let dir = tx
                .query_row(
                    "SELECT dir_id, parent_dir_id, name
                     FROM dirs
                     WHERE dir_id = ?1",
                    params![u64_to_i64(current_dir_id, "dir_id")?],
                    |row| {
                        Ok(DirRow {
                            dir_id: row.u64(0, "dir_id")?,
                            parent_dir_id: row.optional_u64(1, "parent_dir_id")?,
                            name: row.get(2)?,
                        })
                    },
                )
                .optional()?
                .ok_or(Fs0Error::NotFound)?;
            if dir.dir_id == ROOT_DIR_ID {
                break;
            }
            names.push(dir.name);
            current_dir_id = dir.parent_dir_id.ok_or(Fs0Error::InvalidData {
                message: format!("dir {current_dir_id} has no parent"),
            })?;
        }
        names.reverse();
        let path = format!("/{}", names.join("/"));
        self.dir_cache.lock().remember(dir_id, &path);
        Ok(path)
    }

    fn get_dir_id_by_path(&self, tx: &Transaction<'_>, path: &str) -> Fs0Result<u64> {
        if let Some(dir_id) = self.dir_cache.lock().get_id(path) {
            return Ok(dir_id);
        }

        let mut dir_id = ROOT_DIR_ID;
        let mut current_path = "/".to_owned();
        for name in split_fs0_path_components(path)? {
            current_path = join_fs0_path(&current_path, name)?;
            if let Some(cached_dir_id) = self.dir_cache.lock().get_id(&current_path) {
                dir_id = cached_dir_id;
                continue;
            }

            dir_id = tx
                .query_row(
                    "SELECT dir_id
                     FROM dirs
                     WHERE parent_dir_id = ?1 AND name = ?2",
                    params![u64_to_i64(dir_id, "parent_dir_id")?, name],
                    |row| row.u64(0, "dir_id"),
                )
                .optional()?
                .ok_or(Fs0Error::NotFound)?;
            self.dir_cache.lock().remember(dir_id, &current_path);
        }
        Ok(dir_id)
    }

    fn ensure_child_name_available(
        &self,
        tx: &Transaction<'_>,
        parent_dir_id: u64,
        name: &str,
        existing_file_id: Option<u64>,
        path: &str,
    ) -> Fs0Result<()> {
        let child_dir_exists = tx
            .query_row(
                "SELECT 1
                 FROM dirs
                 WHERE parent_dir_id = ?1 AND name = ?2
                 LIMIT 1",
                params![u64_to_i64(parent_dir_id, "parent_dir_id")?, name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if child_dir_exists {
            return Err(Fs0Error::AlreadyExists {
                path: path.to_owned(),
            });
        }

        let child_file = tx
            .query_row(
                "SELECT file_id
                 FROM files
                 WHERE dir_id = ?1 AND name = ?2
                 LIMIT 1",
                params![u64_to_i64(parent_dir_id, "parent_dir_id")?, name],
                |row| row.u64(0, "file_id"),
            )
            .optional()?;
        if child_file.is_some_and(|file_id| Some(file_id) != existing_file_id) {
            return Err(Fs0Error::AlreadyExists {
                path: path.to_owned(),
            });
        }

        Ok(())
    }
}

fn row_to_file_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRow> {
    Ok(FileRow {
        file_id: row.u64(0, "file_id")?,
        dir_id: row.u64(1, "dir_id")?,
        name: row.get(2)?,
        size_bytes: row.u64(3, "size_bytes")?,
        compressed_size_bytes: row.u64(4, "compressed_size_bytes")?,
        created_at_ms: row.u64(5, "created_at_ms")?,
        updated_at_ms: row.u64(6, "updated_at_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn open_test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(include_str!("schema.sql")).unwrap();
        conn
    }

    fn assert_error<T>(result: Fs0Result<T>, expected: Fs0Error) {
        match result {
            Ok(_) => panic!("expected error {expected:?}"),
            Err(err) => assert_eq!(err, expected),
        }
    }

    fn entry_names(entries: &DirectoryEntries) -> Vec<&str> {
        entries
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect()
    }

    #[test]
    fn root_dir_is_special_cased() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        assert_eq!(catalog.create_dir(&tx, "/").unwrap(), ROOT_DIR_ID);
        assert_eq!(catalog.get_dir_id_by_path(&tx, "/").unwrap(), ROOT_DIR_ID);
        assert_eq!(catalog.get_dir_path(&tx, ROOT_DIR_ID).unwrap(), "/");
        assert!(catalog.dir_cache.lock().path_to_entry.is_empty());
    }

    #[test]
    fn create_dir_creates_parents_and_reuses_existing_dirs() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let dir_id = catalog.create_dir(&tx, "/a/b/c").unwrap();
        let same_dir_id = catalog.create_dir(&tx, "/a/b/c").unwrap();
        let dir_count: u64 = tx
            .query_row("SELECT COUNT(*) FROM dirs", [], |row| {
                row.u64(0, "dir_count")
            })
            .unwrap();

        assert_ne!(dir_id, ROOT_DIR_ID);
        assert_eq!(same_dir_id, dir_id);
        assert_eq!(dir_count, 4);
    }

    #[test]
    fn get_dir_id_by_path_caches_committed_dirs() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let dir_id = catalog.create_dir(&tx, "/cached/path").unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        assert!(catalog.dir_cache.lock().get_id("/cached/path").is_none());
        assert_eq!(
            catalog.get_dir_id_by_path(&tx, "/cached/path").unwrap(),
            dir_id
        );
        assert_eq!(
            catalog.dir_cache.lock().get_id("/cached/path"),
            Some(dir_id)
        );
    }

    #[test]
    fn get_dir_path_caches_id_to_path_lookup() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let dir_id = catalog.create_dir(&tx, "/path/from/id").unwrap();
        assert_eq!(catalog.get_dir_path(&tx, dir_id).unwrap(), "/path/from/id");
        assert_eq!(
            catalog.dir_cache.lock().get_path(dir_id).as_deref(),
            Some("/path/from/id")
        );
    }

    #[test]
    fn create_dir_does_not_cache_uncommitted_new_dirs() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let dir_id = catalog.create_dir(&tx, "/rollback").unwrap();
        assert_ne!(dir_id, ROOT_DIR_ID);
        assert!(catalog.dir_cache.lock().get_id("/rollback").is_none());

        tx.rollback().unwrap();
        let tx = conn.transaction().unwrap();
        assert!(catalog.dir_cache.lock().get_id("/rollback").is_none());
        assert!(catalog.create_dir(&tx, "/rollback").is_ok());
    }

    #[test]
    fn invalid_paths_are_rejected() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        for path in ["", "relative", "/a//b", "/a/.", "/a/.."] {
            assert_error(
                catalog.create_dir(&tx, path),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
        }

        for path in ["/", "relative", "/a/", "/a//b", "/a/.", "/a/.."] {
            assert_error(
                catalog.create_file_at_path(&tx, path, 1),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
            assert_error(
                catalog.get_file_by_path(&tx, path),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
        }
    }

    #[test]
    fn get_file_by_path_and_id_build_file_records() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let file_id = catalog
            .create_file_at_path(&tx, "/docs/readme.txt", 123)
            .unwrap();
        let by_path = catalog.get_file_by_path(&tx, "/docs/readme.txt").unwrap();
        let by_id = catalog.get_file_by_id(&tx, file_id).unwrap();

        assert_eq!(by_path.file_id, file_id);
        assert_eq!(by_path.path, "/docs/readme.txt");
        assert_eq!(by_path.size_bytes, 0);
        assert_eq!(by_path.compressed_size_bytes, 0);
        assert_eq!(by_path.created_at_ms, 123);
        assert_eq!(by_path.updated_at_ms, 123);
        assert_eq!(by_id.path, by_path.path);
        assert_eq!(by_id.file_id, by_path.file_id);
    }

    #[test]
    fn get_file_returns_not_found_for_missing_file_or_parent() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        catalog.create_dir(&tx, "/docs").unwrap();

        assert_error(
            catalog.get_file_by_path(&tx, "/docs/missing.txt"),
            Fs0Error::NotFound,
        );
        assert_error(
            catalog.get_file_by_path(&tx, "/missing/file.txt"),
            Fs0Error::NotFound,
        );
        assert_error(catalog.get_file_by_id(&tx, 999), Fs0Error::NotFound);
        assert_error(catalog.get_file_row_by_id(&tx, 999), Fs0Error::NotFound);
    }

    #[test]
    fn create_file_rejects_duplicate_file_and_dir_name_conflicts() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        catalog.create_dir(&tx, "/dir").unwrap();
        catalog.create_file_at_path(&tx, "/file.txt", 1).unwrap();

        assert_error(
            catalog.create_file_at_path(&tx, "/file.txt", 1),
            Fs0Error::AlreadyExists {
                path: "/file.txt".to_owned(),
            },
        );
        assert_error(
            catalog.create_file_at_path(&tx, "/dir", 1),
            Fs0Error::AlreadyExists {
                path: "/dir".to_owned(),
            },
        );
        assert_error(
            catalog.create_dir(&tx, "/file.txt"),
            Fs0Error::AlreadyExists {
                path: "/file.txt".to_owned(),
            },
        );
    }

    #[test]
    fn list_directory_orders_files_and_paginates() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        catalog.create_file_at_path(&tx, "/docs/c.txt", 1).unwrap();
        catalog.create_file_at_path(&tx, "/docs/a.txt", 1).unwrap();
        catalog.create_file_at_path(&tx, "/docs/b.txt", 1).unwrap();

        let first_page = catalog.list_directory(&tx, "/docs", 2, None).unwrap();
        assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
        assert_eq!(first_page.entries[0].path, "/docs/a.txt");
        assert_eq!(first_page.next_cursor, Some(2));

        let second_page = catalog
            .list_directory(&tx, "/docs", 2, first_page.next_cursor)
            .unwrap();
        assert_eq!(entry_names(&second_page), vec!["c.txt"]);
        assert_eq!(second_page.next_cursor, None);
    }

    #[test]
    fn list_directory_clamps_zero_limit_to_one() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        catalog.create_file_at_path(&tx, "/docs/a.txt", 1).unwrap();
        catalog.create_file_at_path(&tx, "/docs/b.txt", 1).unwrap();

        let page = catalog.list_directory(&tx, "/docs", 0, None).unwrap();

        assert_eq!(entry_names(&page), vec!["a.txt"]);
        assert_eq!(page.next_cursor, Some(1));
    }

    #[test]
    fn list_directory_returns_not_found_for_missing_dir() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        assert_error(
            catalog.list_directory(&tx, "/missing", 100, None),
            Fs0Error::NotFound,
        );
    }

    #[test]
    fn copy_file_creates_target_parent_and_copies_sizes() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let source_file_id = catalog.create_file_at_path(&tx, "/source.bin", 10).unwrap();
        tx.execute(
            "UPDATE files
             SET size_bytes = 100, compressed_size_bytes = 60
             WHERE file_id = ?1",
            params![u64_to_i64(source_file_id, "file_id").unwrap()],
        )
        .unwrap();

        catalog
            .copy_file_by_id(&tx, source_file_id, "/copies/source.bin", 20)
            .unwrap();
        let copied = catalog.get_file_by_path(&tx, "/copies/source.bin").unwrap();

        assert_ne!(copied.file_id, source_file_id);
        assert_eq!(copied.size_bytes, 100);
        assert_eq!(copied.compressed_size_bytes, 60);
        assert_eq!(copied.created_at_ms, 20);
        assert_eq!(copied.updated_at_ms, 20);
    }

    #[test]
    fn copy_file_rejects_missing_source_and_target_conflict() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let source_file_id = catalog.create_file_at_path(&tx, "/source.bin", 10).unwrap();
        catalog.create_file_at_path(&tx, "/target.bin", 10).unwrap();

        assert_error(
            catalog.copy_file_by_id(&tx, 999, "/missing-copy.bin", 20),
            Fs0Error::NotFound,
        );
        assert_error(
            catalog.copy_file_by_id(&tx, source_file_id, "/target.bin", 20),
            Fs0Error::AlreadyExists {
                path: "/target.bin".to_owned(),
            },
        );
    }

    #[test]
    fn rename_file_moves_file_and_updates_timestamp() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let file_id = catalog
            .create_file_at_path(&tx, "/docs/source.txt", 10)
            .unwrap();
        catalog
            .rename_file_by_id(&tx, file_id, "/archive/renamed.txt", 20)
            .unwrap();

        assert_error(
            catalog.get_file_by_path(&tx, "/docs/source.txt"),
            Fs0Error::NotFound,
        );
        let renamed = catalog
            .get_file_by_path(&tx, "/archive/renamed.txt")
            .unwrap();
        assert_eq!(renamed.file_id, file_id);
        assert_eq!(renamed.created_at_ms, 10);
        assert_eq!(renamed.updated_at_ms, 20);
        assert_eq!(
            catalog.get_file_by_id(&tx, file_id).unwrap().path,
            "/archive/renamed.txt"
        );
    }

    #[test]
    fn rename_file_rejects_missing_source_and_name_conflicts() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let source_file_id = catalog.create_file_at_path(&tx, "/source.txt", 10).unwrap();
        catalog.create_file_at_path(&tx, "/target.txt", 10).unwrap();
        catalog.create_dir(&tx, "/existing-dir").unwrap();

        assert_error(
            catalog.rename_file_by_id(&tx, 999, "/new.txt", 20),
            Fs0Error::NotFound,
        );
        assert_error(
            catalog.rename_file_by_id(&tx, source_file_id, "/target.txt", 20),
            Fs0Error::AlreadyExists {
                path: "/target.txt".to_owned(),
            },
        );
        assert_error(
            catalog.rename_file_by_id(&tx, source_file_id, "/existing-dir", 20),
            Fs0Error::AlreadyExists {
                path: "/existing-dir".to_owned(),
            },
        );
    }

    #[test]
    fn rename_file_to_same_path_is_allowed() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let file_id = catalog.create_file_at_path(&tx, "/same.txt", 10).unwrap();
        catalog
            .rename_file_by_id(&tx, file_id, "/same.txt", 20)
            .unwrap();
        let file = catalog.get_file_by_id(&tx, file_id).unwrap();

        assert_eq!(file.path, "/same.txt");
        assert_eq!(file.updated_at_ms, 20);
    }

    #[test]
    fn delete_file_removes_existing_file_and_reports_missing_file() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let file_id = catalog
            .create_file_at_path(&tx, "/delete-me.txt", 10)
            .unwrap();

        assert_error(catalog.delete_file_by_id(&tx, 999), Fs0Error::NotFound);
        catalog.delete_file_by_id(&tx, file_id).unwrap();
        assert_error(catalog.get_file_by_id(&tx, file_id), Fs0Error::NotFound);
        assert_error(
            catalog.get_file_by_path(&tx, "/delete-me.txt"),
            Fs0Error::NotFound,
        );
    }

    #[test]
    fn remove_dir_rejects_root_missing_and_non_empty_dirs() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        catalog
            .create_file_at_path(&tx, "/parent/file.txt", 10)
            .unwrap();
        catalog.create_dir(&tx, "/parent/child").unwrap();

        assert_error(catalog.remove_dir(&tx, "/"), Fs0Error::InvalidRequest);
        assert_error(catalog.remove_dir(&tx, "/missing"), Fs0Error::NotFound);
        assert_error(catalog.remove_dir(&tx, "/parent"), Fs0Error::InvalidRequest);
    }

    #[test]
    fn remove_dir_evicts_cached_path() {
        let mut conn = open_test_conn();
        let catalog = FileCatalog::new();
        let tx = conn.transaction().unwrap();

        let dir_id = catalog.create_dir(&tx, "/empty").unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        assert_eq!(catalog.get_dir_id_by_path(&tx, "/empty").unwrap(), dir_id);
        assert_eq!(catalog.dir_cache.lock().get_id("/empty"), Some(dir_id));

        catalog.remove_dir(&tx, "/empty").unwrap();
        assert!(catalog.dir_cache.lock().get_id("/empty").is_none());
    }

    #[test]
    fn dir_cache_trims_old_entries_and_keeps_recently_accessed_entries() {
        let mut cache = DirCache::new();

        for dir_id in 1..=DIR_CACHE_MAX_ENTRIES as u64 {
            cache.remember(dir_id, &format!("/dir-{dir_id}"));
        }
        assert_eq!(cache.get_id("/dir-1"), Some(1));

        let new_dir_id = DIR_CACHE_MAX_ENTRIES as u64 + 1;
        let new_path = format!("/dir-{new_dir_id}");
        cache.remember(new_dir_id, &new_path);

        assert_eq!(cache.path_to_entry.len(), DIR_CACHE_TRIM_TO_ENTRIES);
        assert_eq!(cache.id_to_path.len(), DIR_CACHE_TRIM_TO_ENTRIES);
        assert_eq!(cache.get_id("/dir-1"), Some(1));
        assert!(cache.get_id("/dir-2").is_none());
        assert_eq!(
            cache.get_path(new_dir_id).as_deref(),
            Some(new_path.as_str())
        );
    }
}
