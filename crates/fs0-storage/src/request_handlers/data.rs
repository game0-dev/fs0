use crate::server::StorageServer;
use fs0_core::{
    Fs0Error, Fs0Result,
    protocol::{DataRequest, DataResponse},
};
use std::sync::Arc;

pub(super) async fn handle_data_request(
    server: Arc<StorageServer>,
    _client_id: u64,
    request: DataRequest,
) -> Fs0Result<DataResponse> {
    match request {
        DataRequest::Authenticate { .. } => Err(Fs0Error::InvalidRequest),
        DataRequest::HasChunk {
            volume_id,
            chunk_id,
        } => match server.has_chunk(volume_id, chunk_id).await {
            Ok(Some(meta)) => Ok(DataResponse::HasChunk {
                exists: true,
                raw_len: Some(meta.raw_len),
                compressed_len: Some(meta.compressed_len),
            }),
            Ok(None) => Ok(DataResponse::HasChunk {
                exists: false,
                raw_len: None,
                compressed_len: None,
            }),
            Err(err) => Err(err),
        },
        DataRequest::UploadChunk {
            lease_id,
            file_id,
            volume_id,
            chunk_id,
            raw_len,
            compressed_bytes,
        } => {
            let compressed_len = compressed_bytes.len() as u64;
            match server
                .put_chunk(
                    lease_id,
                    file_id,
                    volume_id,
                    chunk_id,
                    raw_len,
                    compressed_bytes,
                )
                .await
            {
                Ok(_) => Ok(DataResponse::UploadChunk {
                    chunk_id,
                    raw_len,
                    compressed_len,
                }),
                Err(err) => Err(err),
            }
        }
        DataRequest::DownloadChunk {
            volume_id,
            chunk_id,
        } => match server.read_chunk(volume_id, chunk_id).await {
            Ok(bytes) => Ok(DataResponse::DownloadChunk {
                compressed_bytes: bytes,
            }),
            Err(err) => Err(err),
        },
        DataRequest::HasBundle {
            volume_id,
            bundle_id,
        } => match server.bundle_meta(volume_id, bundle_id).await {
            Ok(Some((raw_len, compressed_len))) => Ok(DataResponse::HasBundle {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            }),
            Ok(None) => Ok(DataResponse::HasBundle {
                exists: false,
                raw_len: None,
                compressed_len: None,
            }),
            Err(err) => Err(err),
        },
        DataRequest::CommitBundle {
            lease_id,
            file_id,
            volume_id,
            bundle_id,
            chunks,
        } => match server
            .commit_bundle(lease_id, file_id, volume_id, bundle_id, chunks)
            .await
        {
            Ok(bundle) => Ok(DataResponse::CommitBundle {
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            }),
            Err(err) => Err(err),
        },
        DataRequest::ListBundleChunks {
            volume_id,
            bundle_id,
        } => match server.list_bundle_chunks(volume_id, bundle_id).await {
            Ok(chunks) => Ok(DataResponse::ListBundleChunks { chunks }),
            Err(err) => Err(err),
        },
    }
}
