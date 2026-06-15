use crate::server::StorageServer;
use fs0_core::{
    Fs0Error, Fs0Result, HashId, blake3_hash,
    protocol::{
        CommitBundleRequest, CommitBundleResponse, DataResponse, DownloadChunkRequest,
        UploadChunkRequest, UploadChunkResponse,
    },
    utils::now_ms,
    zstd_decompress,
};

pub(super) async fn has_chunk(
    server: &StorageServer,
    volume_id: u64,
    chunk_id: HashId,
) -> Fs0Result<DataResponse> {
    match server.volume(volume_id)?.chunk_meta(chunk_id).await {
        Ok(meta) => Ok(DataResponse::HasChunk {
            exists: true,
            raw_len: Some(meta.raw_len),
            compressed_len: Some(meta.compressed_len),
        }),
        Err(Fs0Error::ChunkNotFound { .. }) => Ok(DataResponse::HasChunk {
            exists: false,
            raw_len: None,
            compressed_len: None,
        }),
        Err(err) => Err(err),
    }
}

pub(super) async fn upload_chunk(
    server: &StorageServer,
    request: UploadChunkRequest,
) -> Fs0Result<DataResponse> {
    {
        let now = now_ms();
        let mut upload_leases = server.upload_leases.write();
        upload_leases.retain(|_, lease| lease.expires_at_ms > now);
        match upload_leases.get(&request.lease_id) {
            Some(lease)
                if lease.file_id == request.file_id && lease.volume_id == request.volume_id => {}
            _ => return Err(Fs0Error::Unauthorized),
        }
    }

    if server.config.check_hash_before_write {
        let raw_len_usize =
            usize::try_from(request.raw_len).map_err(|_| Fs0Error::IntegerConversion {
                message: format!("raw_len {} exceeds usize", request.raw_len),
            })?;
        let raw = zstd_decompress(&request.compressed_bytes, raw_len_usize)?;
        if raw.len() as u64 != request.raw_len {
            return Err(Fs0Error::InvalidData {
                message: format!(
                    "decompressed chunk length {} does not match raw_len {}",
                    raw.len(),
                    request.raw_len
                ),
            });
        }
        if blake3_hash(&raw) != request.chunk_id {
            return Err(Fs0Error::HashMismatch { volume_offset: 0 });
        }
    }

    let volume = server.volume(request.volume_id)?;
    let meta = volume
        .put_chunk(request.chunk_id, request.raw_len, request.compressed_bytes)
        .await?;

    Ok(DataResponse::UploadChunk(UploadChunkResponse {
        chunk_id: request.chunk_id,
        raw_len: meta.raw_len,
        compressed_len: meta.compressed_len,
    }))
}

pub(super) async fn download_chunk(
    server: &StorageServer,
    request: DownloadChunkRequest,
) -> Fs0Result<DataResponse> {
    let (_meta, bytes) = server
        .volume(request.volume_id)?
        .read_chunk(request.chunk_id)
        .await?;
    Ok(DataResponse::DownloadChunk {
        compressed_bytes: bytes,
    })
}

pub(super) async fn has_bundle(
    server: &StorageServer,
    volume_id: u64,
    bundle_id: HashId,
) -> Fs0Result<DataResponse> {
    let volume = server.volume(volume_id)?;
    let chunks = match volume.list_bundle_chunks(bundle_id).await {
        Ok(chunks) => chunks,
        Err(Fs0Error::BundleNotFound { .. }) => {
            return Ok(DataResponse::HasBundle {
                exists: false,
                raw_len: None,
                compressed_len: None,
            });
        }
        Err(err) => return Err(err),
    };

    let mut raw_len = 0u64;
    let mut compressed_len = 0u64;
    for chunk in chunks {
        let meta = volume.chunk_meta(chunk.chunk_id).await?;
        raw_len = raw_len
            .checked_add(meta.raw_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "bundle raw_len overflow".to_owned(),
            })?;
        compressed_len = compressed_len
            .checked_add(meta.compressed_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "bundle compressed_len overflow".to_owned(),
            })?;
    }

    Ok(DataResponse::HasBundle {
        exists: true,
        raw_len: Some(raw_len),
        compressed_len: Some(compressed_len),
    })
}

pub(super) async fn commit_bundle(
    server: &StorageServer,
    request: CommitBundleRequest,
) -> Fs0Result<DataResponse> {
    {
        let now = now_ms();
        let mut upload_leases = server.upload_leases.write();
        upload_leases.retain(|_, lease| lease.expires_at_ms > now);
        match upload_leases.get(&request.lease_id) {
            Some(lease)
                if lease.file_id == request.file_id && lease.volume_id == request.volume_id => {}
            _ => return Err(Fs0Error::Unauthorized),
        }
    }

    let bundle_id = request.bundle_id;
    let volume = server.volume(request.volume_id)?;
    let bundle = volume.commit_bundle(bundle_id, request.chunks).await?;
    server
        .bundle_reporter
        .sync_volume(&server.central_connection, &volume)
        .await?;

    Ok(DataResponse::CommitBundle(CommitBundleResponse {
        bundle_id,
        raw_len: bundle.raw_len,
        compressed_len: bundle.compressed_len,
    }))
}

pub(super) async fn list_bundle_chunks(
    server: &StorageServer,
    volume_id: u64,
    bundle_id: HashId,
) -> Fs0Result<DataResponse> {
    let chunks = server
        .volume(volume_id)?
        .list_bundle_chunks(bundle_id)
        .await?;
    Ok(DataResponse::ListBundleChunks { chunks })
}
