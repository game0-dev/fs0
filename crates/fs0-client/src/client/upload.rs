use super::{ChunkUpload, Fs0Client, ReadRange, StorageTarget, WriteOptions};
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    DEFAULT_ZSTD_LEVEL, VOLUME_BUNDLE_RAW_SIZE, VOLUME_RAW_CHUNK_SIZE, blake3_hash,
    bundle_hash_from_chunks,
    protocol::{
        BeginUpdateRequest, BundleChunkRef, CommitUpdateRequest, CommittedBundle, FileReadPlan,
        UpdateLease,
    },
    zstd_compress,
};
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt};

impl Fs0Client {
    pub async fn put_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan> {
        let local_path = local_path.as_ref();
        let update_size_hint = Some(tokio::fs::metadata(local_path).await?.len());
        let file = tokio::fs::File::open(local_path).await?;

        self.put_from_reader_with_size_hint(remote_path, file, options, update_size_hint)
            .await
    }

    pub async fn update_path(
        &self,
        remote_path: &str,
        local_path: impl AsRef<Path>,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan> {
        let local_path = local_path.as_ref();
        let update_size_hint = Some(tokio::fs::metadata(local_path).await?.len());
        let file = tokio::fs::File::open(local_path).await?;

        self.update_from_reader_with_size_hint(remote_path, file, options, update_size_hint)
            .await
    }

    pub async fn put_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.put_from_reader_with_size_hint(remote_path, reader, options, None)
            .await
    }

    pub async fn put_from_reader_with_size_hint<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        update_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.write_from_reader(remote_path, reader, options, 0, update_size_hint)
            .await
    }

    pub async fn update_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        self.update_from_reader_with_size_hint(remote_path, reader, options, None)
            .await
    }

    pub async fn update_from_reader_with_size_hint<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        update_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let offset = match options.offset {
            Some(offset) => offset,
            None => self.get_file_read_plan(remote_path).await?.size,
        };

        self.write_from_reader(remote_path, reader, options, offset, update_size_hint)
            .await
    }

    async fn write_from_reader<R>(
        &self,
        remote_path: &str,
        reader: R,
        options: WriteOptions,
        offset: u64,
        update_size_hint: Option<u64>,
    ) -> Fs0Result<FileReadPlan>
    where
        R: AsyncRead + Unpin,
    {
        let lease = self
            .begin_update(BeginUpdateRequest {
                path: remote_path.to_owned(),
                offset,
                prefer_volume_name: options.prefer_volume_name,
                update_size_hint,
            })
            .await?;
        let rewrite_offset = match self.rewrite_offset_for_lease(&lease).await {
            Ok(rewrite_offset) => rewrite_offset,
            Err(err) => {
                let _ = self.abort_update(lease.lease_id, lease.file_id).await;
                return Err(err);
            }
        };
        let prefix = if lease.offset > rewrite_offset {
            match self
                .read_range_to_vec(
                    remote_path,
                    ReadRange {
                        offset: rewrite_offset,
                        len: Some(lease.offset - rewrite_offset),
                    },
                )
                .await
            {
                Ok(prefix) => prefix,
                Err(err) => {
                    let _ = self.abort_update(lease.lease_id, lease.file_id).await;
                    return Err(err);
                }
            }
        } else {
            Vec::new()
        };
        let mut rewritten_reader = std::io::Cursor::new(prefix).chain(reader);

        match self
            .write_lease_from_reader(lease.clone(), rewrite_offset, &mut rewritten_reader)
            .await
        {
            Ok((new_size, bundles)) => {
                let commit = self
                    .commit_update(CommitUpdateRequest {
                        lease_id: lease.lease_id,
                        file_id: lease.file_id,
                        base_size: lease.base_size,
                        new_size,
                        bundles,
                    })
                    .await;
                if commit.is_err() {
                    let _ = self.abort_update(lease.lease_id, lease.file_id).await;
                }

                commit
            }
            Err(err) => {
                let _ = self.abort_update(lease.lease_id, lease.file_id).await;
                Err(err)
            }
        }
    }

    async fn rewrite_offset_for_lease(&self, lease: &UpdateLease) -> Fs0Result<u64> {
        if lease.base_size == 0 {
            return Ok(0);
        }

        let plan = self.get_file_read_plan_by_id(lease.file_id).await?;
        if plan.size != lease.base_size {
            return Err(Fs0Error::VersionConflict {
                message: "file changed while update lease was active".to_owned(),
            });
        }

        rewrite_offset_for_plan(&plan, lease.offset)
    }

    async fn write_lease_from_reader<R>(
        &self,
        lease: UpdateLease,
        rewrite_offset: u64,
        mut reader: R,
    ) -> Fs0Result<(u64, Vec<CommittedBundle>)>
    where
        R: AsyncRead + Unpin,
    {
        let target = self.upload_target(&lease)?;
        let mut buffer = vec![0u8; VOLUME_RAW_CHUNK_SIZE as usize];
        let mut next_size = rewrite_offset;
        let mut current_bundle_raw = 0u64;
        let mut current_chunks = Vec::new();
        let mut current_uploads = Vec::new();
        let mut committed = Vec::new();

        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }

            let raw = &buffer[..read];
            let compressed = zstd_compress(raw, DEFAULT_ZSTD_LEVEL)?;
            let chunk_id = blake3_hash(raw);
            let chunk_index = current_chunks.len() as u64;

            current_chunks.push(BundleChunkRef {
                chunk_index,
                chunk_id,
            });
            current_bundle_raw += read as u64;
            next_size += read as u64;
            current_uploads.push(ChunkUpload {
                chunk_id,
                raw_len: read as u64,
                compressed_bytes: compressed,
            });

            if current_bundle_raw >= VOLUME_BUNDLE_RAW_SIZE {
                let bundle_id = bundle_hash_from_chunks(&current_chunks);
                self.upload_chunks_if_missing(
                    &target,
                    lease.lease_id,
                    lease.file_id,
                    std::mem::take(&mut current_uploads),
                )
                .await?;
                let bundle = self
                    .commit_bundle(
                        &target,
                        lease.lease_id,
                        lease.file_id,
                        bundle_id,
                        std::mem::take(&mut current_chunks),
                    )
                    .await?;

                committed.push(CommittedBundle {
                    bundle_id,
                    raw_len: bundle.raw_len,
                    compressed_len: bundle.compressed_len,
                });
                current_bundle_raw = 0;
            }
        }

        if !current_chunks.is_empty() {
            let bundle_id = bundle_hash_from_chunks(&current_chunks);
            self.upload_chunks_if_missing(&target, lease.lease_id, lease.file_id, current_uploads)
                .await?;
            let bundle = self
                .commit_bundle(
                    &target,
                    lease.lease_id,
                    lease.file_id,
                    bundle_id,
                    current_chunks,
                )
                .await?;

            committed.push(CommittedBundle {
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            });
        }

        Ok((next_size, committed))
    }

    fn upload_target(&self, lease: &UpdateLease) -> Fs0Result<StorageTarget> {
        self.storages
            .read()
            .iter()
            .find_map(|storage| {
                storage
                    .volumes
                    .iter()
                    .find(|volume| volume.volume_id == lease.volume_id)
                    .map(|volume| StorageTarget {
                        storage_id: storage.storage_id,
                        volume_id: volume.volume_id,
                        iroh_endpoint: storage.iroh_endpoint.clone(),
                    })
            })
            .ok_or(Fs0Error::NotFound)
    }
}

fn rewrite_offset_for_plan(plan: &FileReadPlan, offset: u64) -> Fs0Result<u64> {
    if offset > plan.size {
        return Err(Fs0Error::InvalidRequest);
    }

    let mut current_offset = 0u64;
    for bundle in &plan.bundles {
        let bundle_end = current_offset.checked_add(bundle.raw_len).ok_or_else(|| {
            Fs0Error::IntegerConversion {
                message: "file bundle offset overflow".to_owned(),
            }
        })?;
        if offset < bundle_end {
            return Ok(current_offset);
        }
        current_offset = bundle_end;
    }

    if offset == current_offset {
        Ok(current_offset)
    } else {
        Err(Fs0Error::InvalidRequest)
    }
}
