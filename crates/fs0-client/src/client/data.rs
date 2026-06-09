use super::{
    ChunkUpload, ChunkUploadResult, Fs0Client, StorageTarget, endpoint::decode_endpoint_addr,
};
use crate::{Fs0Error, Fs0Result};
use fs0_core::{
    HashId, TRANSPORT_DATA_ALPN,
    protocol::{
        BundleChunkRef, CommittedBundle, DataRequest, DataResponse, ProtocolRequest,
        ProtocolResponse, StoragePeerInfo,
    },
};
use fs0_transport::Connection;
use std::sync::Arc;

impl Fs0Client {
    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Fs0Result<()> {
        let endpoint = decode_endpoint_addr(&peer.iroh_endpoint)?;
        let connection = self.endpoint.connect(endpoint, TRANSPORT_DATA_ALPN).await?;
        connection.close(b"fs0 data ping complete");
        Ok(())
    }

    pub async fn ping_first_storage_peer(&self) -> Fs0Result<StoragePeerInfo> {
        let mut peers = self.storage_peers();
        if peers.is_empty() {
            return Err(Fs0Error::NotFound);
        }

        let peer = peers.remove(0);
        self.ping_storage_peer(&peer).await?;

        Ok(peer)
    }

    pub async fn storage_has_chunk(
        &self,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<Option<(u64, u64)>> {
        let response = self
            .storage_rpc(
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
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunk_if_missing(
        &self,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Fs0Result<bool> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::UploadChunk {
                    lease_id,
                    file_id,
                    volume_id: target.volume_id,
                    chunk_id,
                    raw_len,
                    compressed_bytes,
                },
            )
            .await?;

        match response {
            DataResponse::UploadChunk { .. } => Ok(true),
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunks_if_missing(
        &self,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        chunks: Vec<ChunkUpload>,
    ) -> Fs0Result<Vec<ChunkUploadResult>> {
        self.upload_chunks_if_missing_with_concurrency(
            target,
            lease_id,
            file_id,
            chunks,
            self.options.upload_concurrency,
        )
        .await
    }

    pub async fn upload_chunks_if_missing_with_concurrency(
        &self,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        chunks: Vec<ChunkUpload>,
        concurrency: usize,
    ) -> Fs0Result<Vec<ChunkUploadResult>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = concurrency.max(1);
        let connection = Arc::new(
            self.connect_authenticated_data(&target.iroh_endpoint)
                .await?,
        );
        let mut chunk_iter = chunks.into_iter().enumerate();
        let mut upload_tasks = tokio::task::JoinSet::new();
        let mut results = Vec::new();

        loop {
            while upload_tasks.len() < concurrency {
                let Some((index, chunk)) = chunk_iter.next() else {
                    break;
                };

                let connection = connection.clone();
                let volume_id = target.volume_id;
                upload_tasks.spawn(async move {
                    upload_chunk_if_missing_on_connection(
                        index, connection, lease_id, file_id, volume_id, chunk,
                    )
                    .await
                });
            }

            if upload_tasks.is_empty() {
                break;
            }

            match upload_tasks.join_next().await {
                Some(Ok(Ok(result))) => results.push(result),
                Some(Ok(Err(err))) => {
                    upload_tasks.abort_all();
                    connection.close(b"fs0 upload failed");
                    return Err(err);
                }
                Some(Err(err)) => {
                    upload_tasks.abort_all();
                    connection.close(b"fs0 upload task failed");
                    return Err(Fs0Error::Internal {
                        message: err.to_string(),
                    });
                }
                None => break,
            }
        }

        connection.close(b"fs0 upload complete");
        results.sort_by_key(|(index, _)| *index);

        Ok(results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>())
    }

    pub(super) async fn commit_bundle(
        &self,
        target: &StorageTarget,
        lease_id: u64,
        file_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Fs0Result<CommittedBundle> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::CommitBundle {
                    lease_id,
                    file_id,
                    volume_id: target.volume_id,
                    bundle_id,
                    chunks,
                },
            )
            .await?;

        match response {
            DataResponse::CommitBundle {
                raw_len,
                compressed_len,
                ..
            } => Ok(CommittedBundle {
                bundle_id,
                raw_len,
                compressed_len,
            }),
            response => unexpected_data_response(response),
        }
    }

    pub(super) async fn list_bundle_chunks(
        &self,
        target: &StorageTarget,
        bundle_id: HashId,
    ) -> Fs0Result<Vec<BundleChunkRef>> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::ListBundleChunks {
                    volume_id: target.volume_id,
                    bundle_id,
                },
            )
            .await?;

        match response {
            DataResponse::ListBundleChunks { chunks } => Ok(chunks),
            response => unexpected_data_response(response),
        }
    }

    pub(super) async fn download_chunk(
        &self,
        target: &StorageTarget,
        chunk_id: HashId,
    ) -> Fs0Result<Vec<u8>> {
        let response = self
            .storage_rpc(
                target,
                DataRequest::DownloadChunk {
                    volume_id: target.volume_id,
                    chunk_id,
                },
            )
            .await?;

        match response {
            DataResponse::DownloadChunk { compressed_bytes } => Ok(compressed_bytes),
            response => unexpected_data_response(response),
        }
    }

    pub(super) async fn storage_rpc(
        &self,
        target: &StorageTarget,
        request: DataRequest,
    ) -> Fs0Result<DataResponse> {
        let connection = self
            .connect_authenticated_data(&target.iroh_endpoint)
            .await?;
        let response = request_data(&connection, request).await;
        connection.close(b"fs0 data rpc complete");

        response
    }

    pub(super) async fn connect_authenticated_data(
        &self,
        data_endpoint: &[u8],
    ) -> Fs0Result<Connection> {
        let data_endpoint = decode_endpoint_addr(data_endpoint)?;
        let client_id = self.client_id();
        let connection = self
            .endpoint
            .connect(data_endpoint, TRANSPORT_DATA_ALPN)
            .await?;
        match connection
            .rpc(ProtocolRequest::Data(DataRequest::Authenticate {
                client_id,
                client_token: self.token.clone(),
            }))
            .await?
        {
            ProtocolResponse::Data(DataResponse::Authenticate {
                client_id: authenticated_client_id,
            }) if authenticated_client_id == client_id => Ok(connection),
            ProtocolResponse::Data(DataResponse::Error(err)) | ProtocolResponse::Error(err) => {
                Err(err)
            }
            response => unexpected_protocol_data_response(response),
        }
    }
}

async fn upload_chunk_if_missing_on_connection(
    index: usize,
    connection: Arc<Connection>,
    lease_id: u64,
    file_id: u64,
    volume_id: u64,
    chunk: ChunkUpload,
) -> Fs0Result<(usize, ChunkUploadResult)> {
    match connection
        .rpc(ProtocolRequest::Data(DataRequest::UploadChunk {
            lease_id,
            file_id,
            volume_id,
            chunk_id: chunk.chunk_id,
            raw_len: chunk.raw_len,
            compressed_bytes: chunk.compressed_bytes,
        }))
        .await?
    {
        ProtocolResponse::Data(DataResponse::UploadChunk { .. }) => Ok((
            index,
            ChunkUploadResult {
                chunk_id: chunk.chunk_id,
                uploaded: true,
            },
        )),
        ProtocolResponse::Data(DataResponse::Error(err)) | ProtocolResponse::Error(err) => Err(err),
        response => unexpected_protocol_data_response(response),
    }
}

async fn request_data(connection: &Connection, request: DataRequest) -> Fs0Result<DataResponse> {
    match connection.rpc(ProtocolRequest::Data(request)).await? {
        ProtocolResponse::Data(DataResponse::Error(err)) | ProtocolResponse::Error(err) => Err(err),
        ProtocolResponse::Data(response) => Ok(response),
        response => unexpected_protocol_data_response(response),
    }
}

fn unexpected_data_response<T>(response: DataResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected data response: {response:?}"),
    })
}

fn unexpected_protocol_data_response<T>(response: ProtocolResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected data response: {response:?}"),
    })
}
