use super::{Fs0Client, ReadRange, StorageTarget, TransferStats};
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    VOLUME_RAW_CHUNK_SIZE, blake3_hash, bundle_hash_from_chunks,
    protocol::{FileBundleRef, ReplicaLocation},
    zstd_decompress,
};
use std::path::Path;
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
struct VerifiedChunk {
    chunk_index: u64,
    raw_len: u64,
    compressed_len: u64,
    raw: Vec<u8>,
}

impl Fs0Client {
    pub async fn read_to_vec(&self, remote_path: &str) -> Fs0Result<Vec<u8>> {
        self.read_range_to_vec(remote_path, ReadRange::default())
            .await
    }

    pub async fn read_range_to_vec(
        &self,
        remote_path: &str,
        range: ReadRange,
    ) -> Fs0Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.download_to_writer(remote_path, &mut bytes, range)
            .await?;

        Ok(bytes)
    }

    pub async fn download_to_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        range: ReadRange,
    ) -> Fs0Result<TransferStats> {
        let file = tokio::fs::File::create(local_path).await?;
        self.download_to_writer(remote_path, file, range).await
    }

    pub async fn download_to_writer<W>(
        &self,
        remote_path: &str,
        mut writer: W,
        range: ReadRange,
    ) -> Fs0Result<TransferStats>
    where
        W: AsyncWrite + Unpin,
    {
        let plan = self.get_file_read_plan(remote_path).await?;
        let mut remaining = range.len.unwrap_or(u64::MAX);
        let mut current_offset = 0u64;
        let mut stats = TransferStats::default();

        for bundle in &plan.bundles {
            if remaining == 0 {
                break;
            }

            let bundle_start = current_offset;
            let bundle_end = bundle_start.saturating_add(bundle.raw_len);
            current_offset = bundle_end;
            if bundle_end <= range.offset {
                continue;
            }

            let chunks = self.download_bundle_from_replicas(bundle).await?;
            for chunk in chunks {
                if remaining == 0 {
                    break;
                }

                let chunk_start = bundle_start + chunk.chunk_index * VOLUME_RAW_CHUNK_SIZE;
                let chunk_end = chunk_start.saturating_add(chunk.raw_len);
                if chunk_end <= range.offset {
                    continue;
                }

                let start = range.offset.saturating_sub(chunk_start) as usize;
                let available = chunk.raw.len().saturating_sub(start);
                let take = available.min(remaining as usize);
                writer.write_all(&chunk.raw[start..start + take]).await?;

                remaining -= take as u64;
                stats.raw_bytes += take as u64;
                stats.compressed_bytes += chunk.compressed_len;
                stats.chunks += 1;
            }

            stats.bundles += 1;
        }

        writer.flush().await?;

        Ok(stats)
    }

    async fn download_bundle_from_replicas(
        &self,
        bundle: &FileBundleRef,
    ) -> Fs0Result<Vec<VerifiedChunk>> {
        let mut last_error = None;

        for target in self.read_targets(bundle.replicas.as_slice()) {
            match self.download_verified_bundle(&target, bundle).await {
                Ok(chunks) => return Ok(chunks),
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or(Fs0Error::NotFound))
    }

    async fn download_verified_bundle(
        &self,
        target: &StorageTarget,
        bundle: &FileBundleRef,
    ) -> Fs0Result<Vec<VerifiedChunk>> {
        let chunks = self.list_bundle_chunks(target, bundle.bundle_id).await?;
        if bundle_hash_from_chunks(&chunks) != bundle.bundle_id {
            return Err(Fs0Error::InvalidData {
                message: "bundle id does not match listed chunk ids".to_owned(),
            });
        }

        let mut verified = Vec::with_capacity(chunks.len());
        let mut total_raw_len = 0u64;
        let mut total_compressed_len = 0u64;
        for chunk in chunks {
            let (raw_len, compressed_len) = self
                .storage_has_chunk(target, chunk.chunk_id)
                .await?
                .ok_or(Fs0Error::ChunkNotFound {
                chunk_id: chunk.chunk_id,
            })?;
            let compressed = self.download_chunk(target, chunk.chunk_id).await?;
            if compressed.len() as u64 != compressed_len {
                return Err(Fs0Error::InvalidData {
                    message: "downloaded compressed length does not match chunk metadata"
                        .to_owned(),
                });
            }

            let raw_len_usize =
                usize::try_from(raw_len).map_err(|_| Fs0Error::IntegerConversion {
                    message: format!("raw_len {raw_len} exceeds usize"),
                })?;
            let raw = zstd_decompress(&compressed, raw_len_usize)?;
            if raw.len() as u64 != raw_len || blake3_hash(&raw) != chunk.chunk_id {
                return Err(Fs0Error::HashMismatch { volume_offset: 0 });
            }

            total_raw_len =
                total_raw_len
                    .checked_add(raw_len)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle raw_len overflow".to_owned(),
                    })?;
            total_compressed_len = total_compressed_len
                .checked_add(compressed_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "bundle compressed_len overflow".to_owned(),
                })?;
            verified.push(VerifiedChunk {
                chunk_index: chunk.chunk_index,
                raw_len,
                compressed_len,
                raw,
            });
        }

        if total_raw_len != bundle.raw_len || total_compressed_len != bundle.compressed_len {
            return Err(Fs0Error::InvalidData {
                message: "downloaded bundle lengths do not match read plan".to_owned(),
            });
        }

        Ok(verified)
    }

    fn read_targets(&self, replicas: &[ReplicaLocation]) -> Vec<StorageTarget> {
        let storages = self.storages.read();
        replicas
            .iter()
            .filter_map(|replica| {
                storages
                    .iter()
                    .find(|storage| storage.storage_id == replica.storage_id)
                    .map(|storage| StorageTarget {
                        storage_id: storage.storage_id,
                        volume_id: replica.volume_id,
                        iroh_endpoint: storage.iroh_endpoint.clone(),
                    })
            })
            .collect()
    }
}
