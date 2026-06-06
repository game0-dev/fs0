use crate::Fs0Result;
use fs0_core::{Fs0Error, HashId, SqliteRowExt, protocol::CommittedBundle, utils::u64_to_i64};
use rusqlite::params;
use std::collections::HashMap;

use super::CentralTx;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileBundleRow {
    pub(crate) bundle_index: u64,
    pub(crate) bundle_id: HashId,
}

impl CentralTx<'_> {
    pub(crate) fn delete_file_bundles_by_file_id(&self, file_id: u64) -> Fs0Result<()> {
        self.inner.execute(
            "DELETE FROM file_bundles
             WHERE file_id = ?1",
            params![u64_to_i64(file_id, "file_id")?],
        )?;
        Ok(())
    }

    pub(crate) fn upsert_file_bundles_by_file_id(
        &self,
        file_id: u64,
        bundles: &[CommittedBundle],
    ) -> Fs0Result<()> {
        self.delete_file_bundles_by_file_id(file_id)?;
        for (bundle_index, bundle) in bundles.iter().enumerate() {
            self.inner.execute(
                "INSERT INTO file_bundles (
                    file_id, bundle_index, bundle_id
                 ) VALUES (?1, ?2, ?3)",
                params![
                    u64_to_i64(file_id, "file_id")?,
                    u64_to_i64(bundle_index as u64, "bundle_index")?,
                    bundle.bundle_id.as_bytes().as_slice(),
                ],
            )?;
        }

        Ok(())
    }

    pub(crate) fn copy_file_bundles(
        &self,
        source_file_id: u64,
        target_file_id: u64,
    ) -> Fs0Result<()> {
        self.inner.execute(
            "INSERT INTO file_bundles (file_id, bundle_index, bundle_id)
             SELECT ?1, bundle_index, bundle_id
             FROM file_bundles
             WHERE file_id = ?2",
            params![
                u64_to_i64(target_file_id, "target_file_id")?,
                u64_to_i64(source_file_id, "source_file_id")?,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn get_file_bundles_by_file_id(
        &self,
        file_id: u64,
    ) -> Fs0Result<Vec<FileBundleRow>> {
        let mut stmt = self.inner.prepare_cached(
            "SELECT bundle_index, bundle_id
             FROM file_bundles
             WHERE file_id = ?1
             ORDER BY bundle_index",
        )?;
        let rows = stmt.query_map(params![u64_to_i64(file_id, "file_id")?], |row| {
            Ok(FileBundleRow {
                bundle_index: row.u64(0, "bundle_index")?,
                bundle_id: row.hash_id(1, "bundle_id")?,
            })
        })?;
        let mut bundles = Vec::new();
        for row in rows {
            bundles.push(row?);
        }
        Ok(bundles)
    }

    pub(crate) fn calculate_size_by_file_id(&self, file_id: u64) -> Fs0Result<(u64, u64)> {
        let file_bundles = self.get_file_bundles_by_file_id(file_id)?;
        let bundle_ids = file_bundles
            .iter()
            .map(|bundle| bundle.bundle_id)
            .collect::<Vec<_>>();
        let ready_bundles = self
            .get_uniq_bundles_by_ids(&bundle_ids)?
            .into_iter()
            .map(|bundle| (bundle.bundle_id, bundle))
            .collect::<HashMap<_, _>>();

        let mut raw_size_bytes = 0u64;
        let mut compressed_size_bytes = 0u64;
        for file_bundle in file_bundles {
            let bundle = ready_bundles
                .get(&file_bundle.bundle_id)
                .ok_or(Fs0Error::ChunkNotReady)?;
            raw_size_bytes = raw_size_bytes.checked_add(bundle.raw_len).ok_or_else(|| {
                Fs0Error::IntegerConversion {
                    message: "file bundle raw size overflow".to_owned(),
                }
            })?;
            compressed_size_bytes = compressed_size_bytes
                .checked_add(bundle.compressed_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "file bundle compressed size overflow".to_owned(),
                })?;
        }

        Ok((raw_size_bytes, compressed_size_bytes))
    }
}
