use crate::Fs0Result;
use fs0_core::{
    Fs0Error, SqliteRowExt,
    utils::{i64_to_u64, split_fs0_path_dir_and_name, u64_to_i64},
};
use rusqlite::{OptionalExtension, params};

use super::CentralTx;

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

impl CentralTx<'_> {
    pub(crate) fn create_file(&self, path: &str, now: u64) -> Fs0Result<FileRow> {
        let (dir, name) = split_fs0_path_dir_and_name(path)?;
        let dir_id = self.create_dir(&dir)?;
        self.ensure_child_name_available(dir_id, &name, None, path)?;
        self.inner.execute(
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
        let file_id = i64_to_u64(self.inner.last_insert_rowid(), "file_id")?;
        self.get_file_by_id(file_id)
    }

    pub(crate) fn get_file_by_path(&self, path: &str) -> Fs0Result<FileRow> {
        let (dir, name) = split_fs0_path_dir_and_name(path)?;
        let dir_id = self.get_dir_id_by_path(&dir)?;
        self.inner
            .query_row(
                "SELECT file_id, dir_id, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
                 FROM files
                 WHERE dir_id = ?1 AND name = ?2",
                params![u64_to_i64(dir_id, "dir_id")?, name],
                row_to_file,
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn get_file_by_id(&self, file_id: u64) -> Fs0Result<FileRow> {
        self.inner
            .query_row(
                "SELECT file_id, dir_id, name, size_bytes, compressed_size_bytes,
                    created_at_ms, updated_at_ms
                 FROM files
                 WHERE file_id = ?1",
                params![u64_to_i64(file_id, "file_id")?],
                row_to_file,
            )
            .optional()?
            .ok_or(Fs0Error::NotFound)
    }

    pub(crate) fn delete_file_by_id(&self, file_id: u64) -> Fs0Result<()> {
        self.inner.execute(
            "DELETE FROM files
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
        )?;
        if self.inner.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }
        Ok(())
    }

    pub(crate) fn copy_file_by_id(
        &self,
        source_file_id: u64,
        target_path: &str,
        now: u64,
    ) -> Fs0Result<FileRow> {
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        let source = self.get_file_by_id(source_file_id)?;
        let target_dir_id = self.create_dir(&target_dir)?;
        self.ensure_child_name_available(target_dir_id, &target_name, None, target_path)?;
        self.inner.execute(
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
        let target_file_id = i64_to_u64(self.inner.last_insert_rowid(), "target_file_id")?;
        self.get_file_by_id(target_file_id)
    }

    pub(crate) fn rename_file_by_id(
        &self,
        file_id: u64,
        target_path: &str,
        now: u64,
    ) -> Fs0Result<FileRow> {
        let (target_dir, target_name) = split_fs0_path_dir_and_name(target_path)?;
        let target_dir_id = self.create_dir(&target_dir)?;
        self.ensure_child_name_available(target_dir_id, &target_name, Some(file_id), target_path)?;
        self.inner.execute(
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
        if self.inner.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }
        self.get_file_by_id(file_id)
    }

    pub(crate) fn update_file_after_append(
        &self,
        file_id: u64,
        new_size: u64,
        compressed_size_bytes: u64,
        updated_at_ms: u64,
    ) -> Fs0Result<FileRow> {
        self.inner.execute(
            "UPDATE files
             SET size_bytes = ?2,
                 compressed_size_bytes = ?3,
                 updated_at_ms = ?4
             WHERE file_id = ?1",
            params![
                u64_to_i64(file_id, "file_id")?,
                u64_to_i64(new_size, "size_bytes")?,
                u64_to_i64(compressed_size_bytes, "compressed_size_bytes")?,
                u64_to_i64(updated_at_ms, "updated_at_ms")?,
            ],
        )?;
        if self.inner.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }
        self.get_file_by_id(file_id)
    }
}

fn row_to_file(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRow> {
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
