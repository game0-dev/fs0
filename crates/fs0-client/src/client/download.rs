use super::{Fs0Client, ReadRange, StorageTarget, TransferStats};
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    HashId, VOLUME_RAW_CHUNK_SIZE, blake3_hash, bundle_hash_from_chunks,
    protocol::{BundleChunkRef, FileBundleRef, ReplicaLocation},
    zstd_decompress,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs,
    io::{AsyncWrite, AsyncWriteExt},
};

#[derive(Debug)]
struct VerifiedChunk {
    chunk_index: u64,
    raw_len: u64,
    compressed_len: u64,
    source: ChunkSource,
    raw: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
enum ChunkSource {
    Cache,
    Network,
}

#[derive(Debug)]
struct CachedChunk {
    raw_len: u64,
    compressed_len: u64,
    raw: Vec<u8>,
}

#[derive(Debug)]
struct VerifiedChunkBytes {
    raw_len: u64,
    compressed_len: u64,
    source: ChunkSource,
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
                match chunk.source {
                    ChunkSource::Cache => {
                        stats.cached_compressed_bytes += chunk.compressed_len;
                        stats.cached_chunks += 1;
                    }
                    ChunkSource::Network => {
                        stats.downloaded_compressed_bytes += chunk.compressed_len;
                        stats.downloaded_chunks += 1;
                    }
                }
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

        let chunks_by_id = self
            .download_unique_chunks(target, chunks.as_slice(), self.options.download_concurrency)
            .await?;
        let (verified, total_raw_len, total_compressed_len) =
            expand_verified_chunks(chunks, &chunks_by_id)?;

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

    async fn download_unique_chunks(
        &self,
        target: &StorageTarget,
        chunks: &[BundleChunkRef],
        concurrency: usize,
    ) -> Fs0Result<HashMap<HashId, VerifiedChunkBytes>> {
        let mut pending = Vec::new();
        for chunk in chunks {
            if !pending.contains(&chunk.chunk_id) {
                pending.push(chunk.chunk_id);
            }
        }

        let concurrency = concurrency.max(1);
        let mut tasks = tokio::task::JoinSet::new();
        let mut chunks = HashMap::with_capacity(pending.len());
        let mut pending = pending.into_iter();

        loop {
            while tasks.len() < concurrency {
                let Some(chunk_id) = pending.next() else {
                    break;
                };
                let client = self.clone();
                let target = target.clone();
                tasks.spawn(async move { client.download_unique_chunk(&target, chunk_id).await });
            }

            if tasks.is_empty() {
                break;
            }

            match tasks.join_next().await {
                Some(Ok(Ok((chunk_id, chunk)))) => {
                    chunks.insert(chunk_id, chunk);
                }
                Some(Ok(Err(err))) => {
                    tasks.abort_all();
                    return Err(err);
                }
                Some(Err(err)) => {
                    tasks.abort_all();
                    return Err(Fs0Error::Internal {
                        message: err.to_string(),
                    });
                }
                None => break,
            }
        }

        Ok(chunks)
    }

    async fn download_unique_chunk(
        &self,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<(HashId, VerifiedChunkBytes)> {
        let chunk = match self.read_cached_chunk(chunk_id).await? {
            Some(cached) => VerifiedChunkBytes {
                raw_len: cached.raw_len,
                compressed_len: cached.compressed_len,
                source: ChunkSource::Cache,
                raw: cached.raw,
            },
            None => {
                let (raw_len, compressed_len, compressed, raw) =
                    self.download_verified_chunk(target, chunk_id).await?;
                self.write_cached_chunk(chunk_id, &compressed).await;
                VerifiedChunkBytes {
                    raw_len,
                    compressed_len,
                    source: ChunkSource::Network,
                    raw,
                }
            }
        };

        Ok((chunk_id, chunk))
    }

    async fn download_verified_chunk(
        &self,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<(u64, u64, Vec<u8>, Vec<u8>)> {
        let (raw_len, compressed_len) = self
            .storage_has_chunk(target, chunk_id)
            .await?
            .ok_or(Fs0Error::ChunkNotFound { chunk_id })?;
        let compressed = self.download_chunk(target, chunk_id).await?;
        if compressed.len() as u64 != compressed_len {
            return Err(Fs0Error::InvalidData {
                message: "downloaded compressed length does not match chunk metadata".to_owned(),
            });
        }

        let raw = decompress_and_verify_chunk(chunk_id, &compressed, raw_len)?;

        Ok((raw_len, compressed_len, compressed, raw))
    }

    async fn read_cached_chunk(&self, chunk_id: HashId) -> Fs0Result<Option<CachedChunk>> {
        let Some(path) = self.cache_chunk_path(chunk_id) else {
            return Ok(None);
        };
        let compressed = match fs::read(&path).await {
            Ok(compressed) => compressed,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                let _ = fs::remove_file(&path).await;
                return Ok(None);
            }
        };
        let compressed_len = compressed.len() as u64;

        match decompress_and_verify_chunk(chunk_id, &compressed, VOLUME_RAW_CHUNK_SIZE) {
            Ok(raw) => Ok(Some(CachedChunk {
                raw_len: raw.len() as u64,
                compressed_len,
                raw,
            })),
            Err(_) => {
                let _ = fs::remove_file(path).await;
                Ok(None)
            }
        }
    }

    async fn write_cached_chunk(&self, chunk_id: HashId, compressed: &[u8]) {
        let Some(path) = self.cache_chunk_path(chunk_id) else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        if fs::create_dir_all(parent).await.is_err() {
            return;
        }

        let tmp_path = cache_tmp_path(&path);
        if write_cache_file(&tmp_path, compressed).await.is_err() {
            let _ = fs::remove_file(&tmp_path).await;
            return;
        }
        if fs::rename(&tmp_path, &path).await.is_err() {
            let _ = fs::remove_file(&tmp_path).await;
        }
    }

    fn cache_chunk_path(&self, chunk_id: HashId) -> Option<PathBuf> {
        if !self.options.download_cache_enabled {
            return None;
        }

        self.options
            .download_cache_dir
            .as_ref()
            .map(|dir| dir.join(hash_id_hex(chunk_id)))
    }
}

fn decompress_and_verify_chunk(
    chunk_id: HashId,
    compressed: &[u8],
    max_raw_len: u64,
) -> Fs0Result<Vec<u8>> {
    let max_raw_len = usize::try_from(max_raw_len).map_err(|_| Fs0Error::IntegerConversion {
        message: format!("raw_len {max_raw_len} exceeds usize"),
    })?;
    let raw = zstd_decompress(compressed, max_raw_len)?;
    if raw.len() as u64 > VOLUME_RAW_CHUNK_SIZE || blake3_hash(&raw) != chunk_id {
        return Err(Fs0Error::HashMismatch { volume_offset: 0 });
    }

    Ok(raw)
}

fn expand_verified_chunks(
    chunks: Vec<BundleChunkRef>,
    chunks_by_id: &HashMap<HashId, VerifiedChunkBytes>,
) -> Fs0Result<(Vec<VerifiedChunk>, u64, u64)> {
    let mut verified = Vec::with_capacity(chunks.len());
    let mut total_raw_len = 0u64;
    let mut total_compressed_len = 0u64;

    for chunk in chunks {
        let bytes = chunks_by_id
            .get(&chunk.chunk_id)
            .ok_or(Fs0Error::ChunkNotFound {
                chunk_id: chunk.chunk_id,
            })?;
        total_raw_len = total_raw_len.checked_add(bytes.raw_len).ok_or_else(|| {
            Fs0Error::IntegerConversion {
                message: "bundle raw_len overflow".to_owned(),
            }
        })?;
        total_compressed_len = total_compressed_len
            .checked_add(bytes.compressed_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "bundle compressed_len overflow".to_owned(),
            })?;
        verified.push(VerifiedChunk {
            chunk_index: chunk.chunk_index,
            raw_len: bytes.raw_len,
            compressed_len: bytes.compressed_len,
            source: bytes.source,
            raw: bytes.raw.clone(),
        });
    }

    Ok((verified, total_raw_len, total_compressed_len))
}

async fn write_cache_file(path: &Path, bytes: &[u8]) -> Fs0Result<()> {
    let mut file = fs::File::create(path).await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    Ok(())
}

fn cache_tmp_path(path: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(
        "{}.tmp-{}-{nonce}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("chunk"),
        std::process::id()
    ))
}

fn hash_id_hex(hash_id: HashId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(64);
    for byte in hash_id.as_bytes() {
        hex.push(HEX[(byte >> 4) as usize] as char);
        hex.push(HEX[(byte & 0x0f) as usize] as char);
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs0_core::{DEFAULT_ZSTD_LEVEL, zstd_compress};

    #[test]
    fn hash_id_hex_uses_lowercase_64_byte_hex() {
        let hash_id = HashId::new([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]);

        assert_eq!(
            hash_id_hex(hash_id),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }

    #[test]
    fn cached_chunk_can_decompress_with_max_chunk_size() {
        let raw = b"small cached chunk";
        let chunk_id = blake3_hash(raw);
        let compressed = zstd_compress(raw, DEFAULT_ZSTD_LEVEL).unwrap();

        let decoded =
            decompress_and_verify_chunk(chunk_id, &compressed, VOLUME_RAW_CHUNK_SIZE).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn corrupted_cached_chunk_fails_verification() {
        let raw = b"small cached chunk";
        let chunk_id = blake3_hash(raw);
        let mut compressed = zstd_compress(raw, DEFAULT_ZSTD_LEVEL).unwrap();
        compressed.push(0);

        assert!(decompress_and_verify_chunk(chunk_id, &compressed, VOLUME_RAW_CHUNK_SIZE).is_err());
    }

    #[test]
    fn expand_verified_chunks_reuses_duplicate_chunk_bytes() {
        let raw = b"duplicate chunk".to_vec();
        let chunk_id = blake3_hash(&raw);
        let chunks = vec![
            BundleChunkRef {
                chunk_index: 0,
                chunk_id,
            },
            BundleChunkRef {
                chunk_index: 1,
                chunk_id,
            },
        ];
        let mut chunks_by_id = HashMap::new();
        chunks_by_id.insert(
            chunk_id,
            VerifiedChunkBytes {
                raw_len: raw.len() as u64,
                compressed_len: 8,
                source: ChunkSource::Network,
                raw,
            },
        );

        let (verified, total_raw_len, total_compressed_len) =
            expand_verified_chunks(chunks, &chunks_by_id).unwrap();

        assert_eq!(verified.len(), 2);
        assert_eq!(verified[0].chunk_index, 0);
        assert_eq!(verified[1].chunk_index, 1);
        assert_eq!(total_raw_len, verified[0].raw_len + verified[1].raw_len);
        assert_eq!(total_compressed_len, 16);
    }
}
