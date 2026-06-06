use crate::Fs0Result;
use fs0_core::{Fs0Error, HashId, SqliteRowExt, protocol::CommittedBundle};
use rusqlite::params;
use std::collections::HashSet;

use super::CentralTx;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleReplicaRow {
    pub(crate) bundle_id: HashId,
    pub(crate) volume_id: u64,
    pub(crate) raw_len: u64,
    pub(crate) compressed_len: u64,
}

impl CentralTx<'_> {
    pub(crate) fn get_bundles_by_id(&self, bundle_id: HashId) -> Fs0Result<Vec<BundleReplicaRow>> {
        let mut stmt = self.inner.prepare_cached(
            "SELECT bundle_id, volume_id, raw_len, compressed_len
             FROM bundle_replicas
             WHERE bundle_id = ?1
             ORDER BY volume_id",
        )?;
        let rows = stmt.query_map(params![bundle_id.as_bytes().as_slice()], row_to_bundle)?;
        let mut bundles = Vec::new();
        for row in rows {
            bundles.push(row?);
        }
        Ok(bundles)
    }

    pub(crate) fn get_bundles_by_ids(
        &self,
        bundle_ids: &[HashId],
    ) -> Fs0Result<Vec<BundleReplicaRow>> {
        let mut bundles = Vec::new();
        for bundle_id in bundle_ids {
            bundles.extend(self.get_bundles_by_id(*bundle_id)?);
        }
        Ok(bundles)
    }

    pub(crate) fn get_uniq_bundles_by_ids(
        &self,
        bundle_ids: &[HashId],
    ) -> Fs0Result<Vec<CommittedBundle>> {
        let mut seen = HashSet::new();
        let mut bundles = Vec::new();
        for bundle_id in bundle_ids {
            if !seen.insert(*bundle_id) {
                continue;
            }

            let replicas = self.get_bundles_by_id(*bundle_id)?;
            let first = replicas.first().ok_or(Fs0Error::ChunkNotReady)?;
            for replica in &replicas[1..] {
                if replica.raw_len != first.raw_len
                    || replica.compressed_len != first.compressed_len
                {
                    return Err(Fs0Error::InvalidData {
                        message: "bundle replica metadata conflict".to_owned(),
                    });
                }
            }

            bundles.push(CommittedBundle {
                bundle_id: *bundle_id,
                raw_len: first.raw_len,
                compressed_len: first.compressed_len,
            });
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
