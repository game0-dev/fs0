use crate::client::{Fs0Client, StorageTarget};
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    HashId,
    protocol::{BundleChunkRef, DataRequest, DataResponse},
};

impl Fs0Client {
    pub(crate) async fn storage_has_chunk(
        &self,
        client_id: u64,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<Option<(u64, u64)>> {
        let response = self
            .storage_rpc(
                client_id,
                target,
                DataRequest::HasChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                },
            )
            .await?;

        match response {
            DataResponse::HasChunk {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            } => Ok(Some((raw_len, compressed_len))),
            DataResponse::HasChunk { exists: false, .. } => Ok(None),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn list_bundle_chunks(
        &self,
        client_id: u64,
        target: &StorageTarget,
        bundle_id: HashId,
    ) -> Fs0Result<Vec<BundleChunkRef>> {
        let response = self
            .storage_rpc(
                client_id,
                target,
                DataRequest::ListBundleChunks {
                    volume_id: target.volume_id,
                    bundle_id,
                },
            )
            .await?;

        match response {
            DataResponse::ListBundleChunks { chunks } => Ok(chunks),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn download_chunk(
        &self,
        client_id: u64,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<Vec<u8>> {
        let response = self
            .storage_rpc(
                client_id,
                target,
                DataRequest::DownloadChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                },
            )
            .await?;

        match response {
            DataResponse::DownloadChunk { compressed_bytes } => Ok(compressed_bytes),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        }
    }

    async fn storage_rpc(
        &self,
        client_id: u64,
        target: &StorageTarget,
        request: DataRequest,
    ) -> Fs0Result<DataResponse> {
        self.storage_session(target)
            .await
            .request(client_id, target, request)
            .await
    }
}
