use crate::server::StorageServer;
use fs0_core::{
    Fs0Error,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use fs0_transport::Connection;
use std::sync::Arc;
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};

pub(crate) fn spawn_client_connection_loop(
    server: Arc<StorageServer>,
    connection: Connection,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        handle_client_connection(server, connection, shutdown_notify).await;
    })
}

async fn handle_client_connection(
    server: Arc<StorageServer>,
    connection: Connection,
    shutdown_notify: Arc<Notify>,
) {
    if server.is_exiting() {
        return;
    }

    let authenticated_client_id = Arc::new(Mutex::new(None));
    tokio::select! {
        _ = shutdown_notify.notified() => {}
        _ = connection.serve({
            let server = server.clone();
            let authenticated_client_id = authenticated_client_id.clone();
            move |request| {
                let server = server.clone();
                let authenticated_client_id = authenticated_client_id.clone();
                async move {
                    if let Some(client_id) = *authenticated_client_id.lock().await {
                        return Ok(Some(
                            handle_authenticated_data_request(server, client_id, request).await,
                        ));
                    }

                    match request {
                        ProtocolRequest::Data(DataRequest::Authenticate {
                            client_id,
                            client_token,
                        }) => match server.validate_client_auth(client_id, client_token).await {
                            Ok(()) => {
                                *authenticated_client_id.lock().await = Some(client_id);
                                Ok(Some(ProtocolResponse::Data(DataResponse::Authenticate {
                                    client_id,
                                })))
                            }
                            Err(err) => Ok(Some(ProtocolResponse::Error(err))),
                        },
                        _ => Ok(Some(ProtocolResponse::Error(Fs0Error::Unauthorized))),
                    }
                }
            }
        }) => {}
    }
}

async fn handle_authenticated_data_request(
    server: Arc<StorageServer>,
    client_id: u64,
    request: ProtocolRequest,
) -> ProtocolResponse {
    match request {
        ProtocolRequest::Data(DataRequest::Authenticate { .. }) => {
            ProtocolResponse::Error(Fs0Error::InvalidRequest)
        }
        ProtocolRequest::Data(request) => {
            ProtocolResponse::Data(handle_data_request(server, client_id, request).await)
        }
        _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
    }
}

async fn handle_data_request(
    server: Arc<StorageServer>,
    _client_id: u64,
    request: DataRequest,
) -> DataResponse {
    match request {
        DataRequest::Authenticate { .. } => DataResponse::Error(Fs0Error::InvalidRequest),
        DataRequest::HasChunk {
            volume_id,
            chunk_id,
        } => match server.has_chunk(volume_id, chunk_id).await {
            Ok(Some(meta)) => DataResponse::HasChunk {
                exists: true,
                raw_len: Some(meta.raw_len),
                compressed_len: Some(meta.compressed_len),
            },
            Ok(None) => DataResponse::HasChunk {
                exists: false,
                raw_len: None,
                compressed_len: None,
            },
            Err(err) => DataResponse::Error(err),
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
                Ok(_) => DataResponse::UploadChunk {
                    chunk_id,
                    raw_len,
                    compressed_len,
                },
                Err(err) => DataResponse::Error(err),
            }
        }
        DataRequest::DownloadChunk {
            volume_id,
            chunk_id,
        } => match server.read_chunk(volume_id, chunk_id).await {
            Ok(bytes) => DataResponse::DownloadChunk {
                compressed_bytes: bytes,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::HasBundle {
            volume_id,
            bundle_id,
        } => match server.bundle_meta(volume_id, bundle_id).await {
            Ok(Some((raw_len, compressed_len))) => DataResponse::HasBundle {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            },
            Ok(None) => DataResponse::HasBundle {
                exists: false,
                raw_len: None,
                compressed_len: None,
            },
            Err(err) => DataResponse::Error(err),
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
            Ok(bundle) => DataResponse::CommitBundle {
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::ListBundleChunks {
            volume_id,
            bundle_id,
        } => match server.list_bundle_chunks(volume_id, bundle_id).await {
            Ok(chunks) => DataResponse::ListBundleChunks { chunks },
            Err(err) => DataResponse::Error(err),
        },
    }
}
