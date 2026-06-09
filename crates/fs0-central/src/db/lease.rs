use crate::Fs0Result;
use fs0_core::{
    Fs0Error, SqliteRowExt,
    protocol::UpdateLease,
    utils::{i64_to_u64, now_ms, u64_to_i64},
};
use rusqlite::{OptionalExtension, params};

use super::CentralTx;

#[derive(Debug)]
pub(crate) struct LeaseRecord {
    pub(crate) file_id: u64,
    pub(crate) base_size_bytes: u64,
    pub(crate) offset_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct CreateUpdateLease {
    pub(crate) file_id: u64,
    pub(crate) volume_id: u64,
    pub(crate) base_size_bytes: u64,
    pub(crate) offset_bytes: u64,
    pub(crate) prefer_volume_name: Option<String>,
    pub(crate) expires_at_ms: u64,
    pub(crate) created_at_ms: u64,
}

impl CentralTx<'_> {
    pub(crate) fn delete_expired_update_leases(&self, now_ms: u64) -> Fs0Result<()> {
        self.inner.execute(
            "DELETE FROM update_leases
             WHERE expires_at_ms <= ?1",
            params![u64_to_i64(now_ms, "expires_at_ms")?],
        )?;
        Ok(())
    }

    pub(crate) fn file_has_active_update_lease(&self, file_id: u64) -> Fs0Result<bool> {
        let active_lease = self
            .inner
            .query_row(
                "SELECT lease_id
                 FROM update_leases
                 WHERE file_id = ?1
                 LIMIT 1",
                params![u64_to_i64(file_id, "file_id")?],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        Ok(active_lease.is_some())
    }

    pub(crate) fn create_update_lease(&self, lease: CreateUpdateLease) -> Fs0Result<UpdateLease> {
        self.inner.execute(
            "INSERT INTO update_leases (
                file_id, volume_id, base_size_bytes,
                offset_bytes, prefer_volume_name, expires_at_ms, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                u64_to_i64(lease.file_id, "file_id")?,
                u64_to_i64(lease.volume_id, "volume_id")?,
                u64_to_i64(lease.base_size_bytes, "base_size_bytes")?,
                u64_to_i64(lease.offset_bytes, "offset_bytes")?,
                lease.prefer_volume_name.as_deref(),
                u64_to_i64(lease.expires_at_ms, "expires_at_ms")?,
                u64_to_i64(lease.created_at_ms, "created_at_ms")?,
            ],
        )?;
        let lease_id = i64_to_u64(self.inner.last_insert_rowid(), "lease_id")?;

        Ok(UpdateLease {
            lease_id,
            file_id: lease.file_id,
            volume_id: lease.volume_id,
            base_size: lease.base_size_bytes,
            offset: lease.offset_bytes,
            expires_at_ms: lease.expires_at_ms,
            prefer_volume_name: lease.prefer_volume_name,
        })
    }

    pub(crate) fn load_active_update_lease(
        &self,
        lease_id: u64,
        file_id: u64,
    ) -> Fs0Result<LeaseRecord> {
        self.inner
            .query_row(
                "SELECT file_id, base_size_bytes, offset_bytes
                 FROM update_leases
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

    pub(crate) fn active_update_lease_volume(&self, lease_id: u64, file_id: u64) -> Fs0Result<u64> {
        self.inner
            .query_row(
                "SELECT volume_id
                 FROM update_leases
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

    pub(crate) fn delete_update_lease(&self, lease_id: u64) -> Fs0Result<()> {
        self.inner.execute(
            "DELETE FROM update_leases
             WHERE lease_id = ?1",
            params![u64_to_i64(lease_id, "lease_id")?],
        )?;
        Ok(())
    }
}
