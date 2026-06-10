use crate::Fs0Result;
use fs0_core::{Fs0Error, HashId, SqliteRowExt, utils::u64_to_i64};
use rusqlite::{params, params_from_iter};
use tracing::error;

use super::CentralTx;

const BUNDLE_ID_QUERY_BATCH_SIZE: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleReplicaRow {
    pub(crate) bundle_id: HashId,
    pub(crate) volume_id: u64,
    pub(crate) raw_len: u64,
    pub(crate) compressed_len: u64,
}

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

    pub(crate) fn get_bundle_replicas_by_id(
        &self,
        bundle_ids: &[HashId],
    ) -> Fs0Result<Vec<BundleReplicaRow>> {
        if bundle_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut bundles = Vec::new();
        for batch in bundle_ids.chunks(BUNDLE_ID_QUERY_BATCH_SIZE) {
            let placeholders = std::iter::repeat_n("?", batch.len())
                .collect::<Vec<_>>()
                .join(",");
            let query = format!(
                "SELECT bundle_id, volume_id, raw_len, compressed_len
                 FROM bundle_replicas
                 WHERE bundle_id IN ({placeholders})
                 ORDER BY bundle_id, volume_id"
            );
            let params = batch
                .iter()
                .map(|bundle_id| bundle_id.as_bytes().as_slice());
            let mut stmt = self.inner.prepare(&query)?;
            let rows = stmt.query_map(params_from_iter(params), row_to_bundle)?;
            for row in rows {
                bundles.push(row?);
            }
        }

        Ok(bundles)
    }
}

fn row_to_bundle(row: &rusqlite::Row<'_>) -> rusqlite::Result<BundleReplicaRow> {
    Ok(BundleReplicaRow {
        bundle_id: row.hash_id(0, "bundle_id")?,
        volume_id: row.u64(1, "volume_id")?,
        raw_len: row.u64(2, "raw_len")?,
        compressed_len: row.u64(3, "compressed_len")?,
    })
}
