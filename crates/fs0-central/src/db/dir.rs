use crate::Fs0Result;
use fs0_core::{
    Fs0Error, SqliteRowExt,
    protocol::{DirectoryEntries, DirectoryEntry, FileRecord},
    utils::{i64_to_u64, join_fs0_path, split_fs0_path_components, u64_to_i64},
};
use parking_lot::Mutex;
use rusqlite::{OptionalExtension, params};
use std::collections::HashMap;

use super::{CentralTx, file::FileRow};

const ROOT_DIR_ID: u64 = 0;
const DIR_CACHE_MAX_ENTRIES: usize = 8192;
const DIR_CACHE_TRIM_TO_ENTRIES: usize = 6144;

#[derive(Debug)]
pub(super) struct DirCache {
    inner: Mutex<DirCacheState>,
}

#[derive(Debug)]
struct DirRow {
    dir_id: u64,
    parent_dir_id: Option<u64>,
    name: String,
}

#[derive(Debug)]
struct DirCacheState {
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
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(DirCacheState::new()),
        }
    }

    fn lock(&self) -> parking_lot::MutexGuard<'_, DirCacheState> {
        self.inner.lock()
    }
}

impl DirCacheState {
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

impl CentralTx<'_> {
    pub(crate) fn create_dir(&self, path: &str) -> Fs0Result<u64> {
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

            let existing = self
                .inner
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
                    self.ensure_child_name_available(dir_id, name, None, &current_path)?;
                    self.inner.execute(
                        "INSERT INTO dirs (parent_dir_id, name)
                         VALUES (?1, ?2)",
                        params![u64_to_i64(dir_id, "parent_dir_id")?, name],
                    )?;
                    dir_id = i64_to_u64(self.inner.last_insert_rowid(), "dir_id")?;
                }
            }
        }

        Ok(dir_id)
    }

    #[allow(dead_code)]
    pub(crate) fn remove_dir(&self, path: &str) -> Fs0Result<()> {
        if path == "/" {
            return Err(Fs0Error::InvalidRequest);
        }

        let dir_id = self.get_dir_id_by_path(path)?;
        if dir_id == ROOT_DIR_ID {
            return Err(Fs0Error::InvalidRequest);
        }

        let child_dirs = self.inner.query_row(
            "SELECT COUNT(*)
             FROM dirs
             WHERE parent_dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
            |row| row.u64(0, "child_dirs"),
        )?;
        let child_files = self.inner.query_row(
            "SELECT COUNT(*)
             FROM files
             WHERE dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
            |row| row.u64(0, "child_files"),
        )?;
        if child_dirs != 0 || child_files != 0 {
            return Err(Fs0Error::InvalidRequest);
        }

        self.inner.execute(
            "DELETE FROM dirs
             WHERE dir_id = ?1",
            params![u64_to_i64(dir_id, "dir_id")?],
        )?;
        self.dir_cache.lock().forget(dir_id, path);
        Ok(())
    }

    pub(crate) fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Fs0Result<DirectoryEntries> {
        let limit = limit.clamp(1, 1024) as usize;
        let offset = cursor.unwrap_or(0);
        let dir_id = self.get_dir_id_by_path(dir)?;
        let mut stmt = self.inner.prepare_cached(
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

    pub(crate) fn file_record(&self, file: &FileRow) -> Fs0Result<FileRecord> {
        let dir = self.get_dir_path_by_id(file.dir_id)?;
        Ok(FileRecord {
            file_id: file.file_id,
            path: join_fs0_path(&dir, &file.name)?,
            size_bytes: file.size_bytes,
            compressed_size_bytes: file.compressed_size_bytes,
            created_at_ms: file.created_at_ms,
            updated_at_ms: file.updated_at_ms,
        })
    }

    pub(crate) fn get_dir_path_by_id(&self, dir_id: u64) -> Fs0Result<String> {
        if let Some(path) = self.dir_cache.lock().get_path(dir_id) {
            return Ok(path);
        }

        let mut current_dir_id = dir_id;
        let mut names = Vec::new();
        loop {
            let dir = self
                .inner
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

    pub(super) fn get_dir_id_by_path(&self, path: &str) -> Fs0Result<u64> {
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

            dir_id = self
                .inner
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

    pub(super) fn ensure_child_name_available(
        &self,
        parent_dir_id: u64,
        name: &str,
        existing_file_id: Option<u64>,
        path: &str,
    ) -> Fs0Result<()> {
        let child_dir_exists = self
            .inner
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

        let child_file = self
            .inner
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::CentralDb;

    fn open_test_db() -> CentralDb {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(include_str!("../schema.sql")).unwrap();

        CentralDb {
            conn,
            dir_cache: DirCache::new(),
        }
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
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        assert_eq!(tx.create_dir("/").unwrap(), ROOT_DIR_ID);
        assert_eq!(tx.get_dir_id_by_path("/").unwrap(), ROOT_DIR_ID);
        assert_eq!(tx.get_dir_path_by_id(ROOT_DIR_ID).unwrap(), "/");
        assert!(tx.dir_cache.lock().path_to_entry.is_empty());
    }

    #[test]
    fn create_dir_creates_parents_and_reuses_existing_dirs() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let dir_id = tx.create_dir("/a/b/c").unwrap();
        let same_dir_id = tx.create_dir("/a/b/c").unwrap();
        let dir_count: u64 = tx
            .inner
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
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let dir_id = tx.create_dir("/cached/path").unwrap();
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        assert!(tx.dir_cache.lock().get_id("/cached/path").is_none());
        assert_eq!(tx.get_dir_id_by_path("/cached/path").unwrap(), dir_id);
        assert_eq!(tx.dir_cache.lock().get_id("/cached/path"), Some(dir_id));
    }

    #[test]
    fn get_dir_path_caches_id_to_path_lookup() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let dir_id = tx.create_dir("/path/from/id").unwrap();
        assert_eq!(tx.get_dir_path_by_id(dir_id).unwrap(), "/path/from/id");
        assert_eq!(
            tx.dir_cache.lock().get_path(dir_id).as_deref(),
            Some("/path/from/id")
        );
    }

    #[test]
    fn create_dir_does_not_cache_uncommitted_new_dirs() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let dir_id = tx.create_dir("/rollback").unwrap();
        assert_ne!(dir_id, ROOT_DIR_ID);
        assert!(tx.dir_cache.lock().get_id("/rollback").is_none());

        tx.rollback().unwrap();
        let tx = db.tx().unwrap();
        assert!(tx.dir_cache.lock().get_id("/rollback").is_none());
        assert!(tx.create_dir("/rollback").is_ok());
    }

    #[test]
    fn invalid_paths_are_rejected() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        for path in ["", "relative", "/a//b", "/a/.", "/a/.."] {
            assert_error(
                tx.create_dir(path),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
        }

        for path in ["/", "relative", "/a/", "/a//b", "/a/.", "/a/.."] {
            assert_error(
                tx.create_file(path, 1),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
            assert_error(
                tx.get_file_by_path(path),
                Fs0Error::InvalidPath {
                    path: path.to_owned(),
                },
            );
        }
    }

    #[test]
    fn get_file_by_path_and_id_build_file_records() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let file_id = tx.create_file("/docs/readme.txt", 123).unwrap().file_id;
        let by_path = tx.get_file_by_path("/docs/readme.txt").unwrap();
        let by_id = tx.get_file_by_id(file_id).unwrap();
        let by_path_record = tx.file_record(&by_path).unwrap();
        let by_id_record = tx.file_record(&by_id).unwrap();

        assert_eq!(by_path.file_id, file_id);
        assert_eq!(by_path_record.path, "/docs/readme.txt");
        assert_eq!(by_path.size_bytes, 0);
        assert_eq!(by_path.compressed_size_bytes, 0);
        assert_eq!(by_path.created_at_ms, 123);
        assert_eq!(by_path.updated_at_ms, 123);
        assert_eq!(by_id_record.path, by_path_record.path);
        assert_eq!(by_id.file_id, by_path.file_id);
    }

    #[test]
    fn get_file_returns_not_found_for_missing_file_or_parent() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        tx.create_dir("/docs").unwrap();

        assert_error(tx.get_file_by_path("/docs/missing.txt"), Fs0Error::NotFound);
        assert_error(tx.get_file_by_path("/missing/file.txt"), Fs0Error::NotFound);
        assert_error(tx.get_file_by_id(999), Fs0Error::NotFound);
        assert_error(tx.get_file_by_id(999), Fs0Error::NotFound);
    }

    #[test]
    fn create_file_rejects_duplicate_file_and_dir_name_conflicts() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        tx.create_dir("/dir").unwrap();
        tx.create_file("/file.txt", 1).unwrap();

        assert_error(
            tx.create_file("/file.txt", 1),
            Fs0Error::AlreadyExists {
                path: "/file.txt".to_owned(),
            },
        );
        assert_error(
            tx.create_file("/dir", 1),
            Fs0Error::AlreadyExists {
                path: "/dir".to_owned(),
            },
        );
        assert_error(
            tx.create_dir("/file.txt"),
            Fs0Error::AlreadyExists {
                path: "/file.txt".to_owned(),
            },
        );
    }

    #[test]
    fn list_directory_orders_files_and_paginates() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        tx.create_file("/docs/c.txt", 1).unwrap();
        tx.create_file("/docs/a.txt", 1).unwrap();
        tx.create_file("/docs/b.txt", 1).unwrap();

        let first_page = tx.list_directory("/docs", 2, None).unwrap();
        assert_eq!(entry_names(&first_page), vec!["a.txt", "b.txt"]);
        assert_eq!(first_page.entries[0].path, "/docs/a.txt");
        assert_eq!(first_page.next_cursor, Some(2));

        let second_page = tx
            .list_directory("/docs", 2, first_page.next_cursor)
            .unwrap();
        assert_eq!(entry_names(&second_page), vec!["c.txt"]);
        assert_eq!(second_page.next_cursor, None);
    }

    #[test]
    fn list_directory_clamps_zero_limit_to_one() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        tx.create_file("/docs/a.txt", 1).unwrap();
        tx.create_file("/docs/b.txt", 1).unwrap();

        let page = tx.list_directory("/docs", 0, None).unwrap();

        assert_eq!(entry_names(&page), vec!["a.txt"]);
        assert_eq!(page.next_cursor, Some(1));
    }

    #[test]
    fn list_directory_returns_not_found_for_missing_dir() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        assert_error(tx.list_directory("/missing", 100, None), Fs0Error::NotFound);
    }

    #[test]
    fn copy_file_creates_target_parent_and_copies_sizes() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let source_file_id = tx.create_file("/source.bin", 10).unwrap().file_id;
        tx.inner
            .execute(
                "UPDATE files
             SET size_bytes = 100, compressed_size_bytes = 60
             WHERE file_id = ?1",
                params![u64_to_i64(source_file_id, "file_id").unwrap()],
            )
            .unwrap();

        tx.copy_file_by_id(source_file_id, "/copies/source.bin", 20)
            .unwrap();
        let copied = tx.get_file_by_path("/copies/source.bin").unwrap();

        assert_ne!(copied.file_id, source_file_id);
        assert_eq!(copied.size_bytes, 100);
        assert_eq!(copied.compressed_size_bytes, 60);
        assert_eq!(copied.created_at_ms, 20);
        assert_eq!(copied.updated_at_ms, 20);
    }

    #[test]
    fn copy_file_rejects_missing_source_and_target_conflict() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let source_file_id = tx.create_file("/source.bin", 10).unwrap().file_id;
        tx.create_file("/target.bin", 10).unwrap();

        assert_error(
            tx.copy_file_by_id(999, "/missing-copy.bin", 20),
            Fs0Error::NotFound,
        );
        assert_error(
            tx.copy_file_by_id(source_file_id, "/target.bin", 20),
            Fs0Error::AlreadyExists {
                path: "/target.bin".to_owned(),
            },
        );
    }

    #[test]
    fn rename_file_moves_file_and_updates_timestamp() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let file_id = tx.create_file("/docs/source.txt", 10).unwrap().file_id;
        tx.rename_file_by_id(file_id, "/archive/renamed.txt", 20)
            .unwrap();

        assert_error(tx.get_file_by_path("/docs/source.txt"), Fs0Error::NotFound);
        let renamed = tx.get_file_by_path("/archive/renamed.txt").unwrap();
        assert_eq!(renamed.file_id, file_id);
        assert_eq!(renamed.created_at_ms, 10);
        assert_eq!(renamed.updated_at_ms, 20);
        assert_eq!(
            tx.file_record(&tx.get_file_by_id(file_id).unwrap())
                .unwrap()
                .path,
            "/archive/renamed.txt"
        );
    }

    #[test]
    fn rename_file_rejects_missing_source_and_name_conflicts() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let source_file_id = tx.create_file("/source.txt", 10).unwrap().file_id;
        tx.create_file("/target.txt", 10).unwrap();
        tx.create_dir("/existing-dir").unwrap();

        assert_error(
            tx.rename_file_by_id(999, "/new.txt", 20),
            Fs0Error::NotFound,
        );
        assert_error(
            tx.rename_file_by_id(source_file_id, "/target.txt", 20),
            Fs0Error::AlreadyExists {
                path: "/target.txt".to_owned(),
            },
        );
        assert_error(
            tx.rename_file_by_id(source_file_id, "/existing-dir", 20),
            Fs0Error::AlreadyExists {
                path: "/existing-dir".to_owned(),
            },
        );
    }

    #[test]
    fn rename_file_to_same_path_is_allowed() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let file_id = tx.create_file("/same.txt", 10).unwrap().file_id;
        tx.rename_file_by_id(file_id, "/same.txt", 20).unwrap();
        let file = tx.get_file_by_id(file_id).unwrap();
        let record = tx.file_record(&file).unwrap();

        assert_eq!(record.path, "/same.txt");
        assert_eq!(file.updated_at_ms, 20);
    }

    #[test]
    fn delete_file_removes_existing_file_and_reports_missing_file() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let file_id = tx.create_file("/delete-me.txt", 10).unwrap().file_id;

        assert_error(tx.delete_file_by_id(999), Fs0Error::NotFound);
        tx.delete_file_by_id(file_id).unwrap();
        assert_error(tx.get_file_by_id(file_id), Fs0Error::NotFound);
        assert_error(tx.get_file_by_path("/delete-me.txt"), Fs0Error::NotFound);
    }

    #[test]
    fn remove_dir_rejects_root_missing_and_non_empty_dirs() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        tx.create_file("/parent/file.txt", 10).unwrap();
        tx.create_dir("/parent/child").unwrap();

        assert_error(tx.remove_dir("/"), Fs0Error::InvalidRequest);
        assert_error(tx.remove_dir("/missing"), Fs0Error::NotFound);
        assert_error(tx.remove_dir("/parent"), Fs0Error::InvalidRequest);
    }

    #[test]
    fn remove_dir_evicts_cached_path() {
        let mut db = open_test_db();
        let tx = db.tx().unwrap();

        let dir_id = tx.create_dir("/empty").unwrap();
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        assert_eq!(tx.get_dir_id_by_path("/empty").unwrap(), dir_id);
        assert_eq!(tx.dir_cache.lock().get_id("/empty"), Some(dir_id));

        tx.remove_dir("/empty").unwrap();
        assert!(tx.dir_cache.lock().get_id("/empty").is_none());
    }

    #[test]
    fn dir_cache_trims_old_entries_and_keeps_recently_accessed_entries() {
        let cache = DirCache::new();

        for dir_id in 1..=DIR_CACHE_MAX_ENTRIES as u64 {
            cache.lock().remember(dir_id, &format!("/dir-{dir_id}"));
        }
        assert_eq!(cache.lock().get_id("/dir-1"), Some(1));

        let new_dir_id = DIR_CACHE_MAX_ENTRIES as u64 + 1;
        let new_path = format!("/dir-{new_dir_id}");
        cache.lock().remember(new_dir_id, &new_path);

        assert_eq!(cache.lock().path_to_entry.len(), DIR_CACHE_TRIM_TO_ENTRIES);
        assert_eq!(cache.lock().id_to_path.len(), DIR_CACHE_TRIM_TO_ENTRIES);
        assert_eq!(cache.lock().get_id("/dir-1"), Some(1));
        assert!(cache.lock().get_id("/dir-2").is_none());
        assert_eq!(
            cache.lock().get_path(new_dir_id).as_deref(),
            Some(new_path.as_str())
        );
    }
}
