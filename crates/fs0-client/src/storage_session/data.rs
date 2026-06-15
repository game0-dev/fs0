use super::StorageSessionInner;
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    HashId,
    protocol::{
        BundleChunkRef, CommitBundleRequest, CommitBundleResponse, CommittedBundle, DataRequest,
        DataResponse, DownloadChunkRequest, UploadChunkRequest, UploadChunkResponse,
    },
};

impl StorageSessionInner {
    pub(crate) async fn has_bundle(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Fs0Result<Option<(u64, u64)>> {
        let response = self
            .request(DataRequest::HasBundle {
                volume_id,
                bundle_id,
            })
            .await?;

        match response {
            DataResponse::HasBundle {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            } => Ok(Some((raw_len, compressed_len))),
            DataResponse::HasBundle { exists: false, .. } => Ok(None),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn upload_chunk(
        &self,
        request: UploadChunkRequest,
    ) -> Fs0Result<UploadChunkResponse> {
        let response = self.request(DataRequest::UploadChunk(request)).await?;

        match response {
            DataResponse::UploadChunk(response) => Ok(response),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn commit_bundle(
        &self,
        request: CommitBundleRequest,
    ) -> Fs0Result<CommittedBundle> {
        let bundle_id = request.bundle_id;
        let response = self.request(DataRequest::CommitBundle(request)).await?;

        match response {
            DataResponse::CommitBundle(CommitBundleResponse {
                raw_len,
                compressed_len,
                ..
            }) => Ok(CommittedBundle {
                bundle_id,
                raw_len,
                compressed_len,
            }),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn download_chunk(&self, request: DownloadChunkRequest) -> Fs0Result<Vec<u8>> {
        let response = self.request(DataRequest::DownloadChunk(request)).await?;

        match response {
            DataResponse::DownloadChunk { compressed_bytes } => Ok(compressed_bytes),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn list_bundle_chunks(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Fs0Result<Vec<BundleChunkRef>> {
        let response = self
            .request(DataRequest::ListBundleChunks {
                volume_id,
                bundle_id,
            })
            .await?;

        match response {
            DataResponse::ListBundleChunks { chunks } => Ok(chunks),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }
}
