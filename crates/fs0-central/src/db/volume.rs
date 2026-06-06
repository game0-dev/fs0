use crate::Fs0Result;
use fs0_core::{
    Fs0Error, SqliteRowExt,
    protocol::StorageVolumeInfo,
    utils::{i64_to_u64, u64_to_i64},
};
use rusqlite::{OptionalExtension, params};

use super::CentralTx;

impl CentralTx<'_> {
    pub(crate) fn create_volume(
        &self,
        name: String,
        max_bytes: u64,
    ) -> Fs0Result<StorageVolumeInfo> {
        self.inner.execute(
            "INSERT INTO volumes (name, max_bytes, max_volume_offset)
             VALUES (?1, ?2, 0)",
            params![name.as_str(), u64_to_i64(max_bytes, "max_bytes")?],
        )?;
        let volume_id = i64_to_u64(self.inner.last_insert_rowid(), "volume_id")?;
        self.get_volume(volume_id)
    }

    pub(crate) fn get_volume(&self, volume_id: u64) -> Fs0Result<StorageVolumeInfo> {
        self.inner
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
        &self,
        volume_id: u64,
        max_volume_offset: u64,
    ) -> Fs0Result<StorageVolumeInfo> {
        self.inner.execute(
            "UPDATE volumes
             SET max_volume_offset = ?2
             WHERE volume_id = ?1",
            params![
                u64_to_i64(volume_id, "volume_id")?,
                u64_to_i64(max_volume_offset, "max_volume_offset")?,
            ],
        )?;
        if self.inner.changes() == 0 {
            return Err(Fs0Error::NotFound);
        }

        self.get_volume(volume_id)
    }
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
