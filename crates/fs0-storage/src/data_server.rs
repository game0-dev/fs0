use crate::server::StorageServer;
use fs0_core::{DataRequest, DataResponse, Fs0Error};
use fs0_transport::{read_frame, write_frame};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, SendStream},
};
use std::sync::{Arc, Weak};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub(crate) fn spawn_data_accept_loop(
    endpoint: Endpoint,
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let shutdown_notify = shutdown_notify.clone();
                    tokio::spawn(async move {
                        let Ok(connection) = incoming.await else {
                            return;
                        };
                        handle_data_connection(server, connection, shutdown_notify).await;
                    });
                }
            }
        }
    })
}

async fn handle_data_connection(
    server: Arc<StorageServer>,
    connection: Connection,
    shutdown_notify: Arc<Notify>,
) {
    let mut authenticated_client_id = None;

    loop {
        if server.is_exiting() {
            break;
        }

        tokio::select! {
            _ = shutdown_notify.notified() => break,
            stream = connection.accept_bi() => {
                let Ok((send, recv)) = stream else {
                    break;
                };

                if authenticated_client_id.is_none() {
                    authenticated_client_id = authenticate_data_connection(server.clone(), send, recv).await;
                    if authenticated_client_id.is_none() {
                        break;
                    }
                    continue;
                }

                let client_id = authenticated_client_id.expect("authenticated client id is set");
                let server = server.clone();
                tokio::spawn(async move {
                    handle_data_stream(server, client_id, send, recv).await;
                });
            }
        }
    }
}

async fn authenticate_data_connection(
    server: Arc<StorageServer>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Option<u64> {
    let response = match read_frame::<DataRequest, _>(&mut recv).await {
        Ok(DataRequest::Authenticate {
            client_id,
            client_token,
        }) => match server.validate_client_auth(client_id, client_token).await {
            Ok(()) => {
                let _ = write_frame(&mut send, &DataResponse::Authenticate { client_id }).await;
                let _ = send.finish();
                return Some(client_id);
            }
            Err(err) => DataResponse::Error(err),
        },
        Ok(_) => DataResponse::Error(Fs0Error::Unauthorized),
        Err(err) => DataResponse::Error(err),
    };

    let _ = write_frame(&mut send, &response).await;
    let _ = send.finish();
    None
}

async fn handle_data_stream(
    server: Arc<StorageServer>,
    client_id: u64,
    mut send: SendStream,
    mut recv: RecvStream,
) {
    let response = match read_frame::<DataRequest, _>(&mut recv).await {
        Ok(DataRequest::Authenticate { .. }) => DataResponse::Error(Fs0Error::InvalidRequest),
        Ok(request) => handle_data_request(server, client_id, request).await,
        Err(err) => DataResponse::Error(err),
    };

    let _ = write_frame(&mut send, &response).await;
    let _ = send.finish();
}

async fn handle_data_request(
    server: Arc<StorageServer>,
    client_id: u64,
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
            volume_id,
            chunk_id,
            compressed_hash,
            raw_len,
            compressed_bytes,
        } => {
            let compressed_len = compressed_bytes.len() as u64;
            match server
                .put_chunk(
                    client_id,
                    volume_id,
                    chunk_id,
                    compressed_hash,
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
            volume_id,
            bundle_id,
            chunks,
        } => match server
            .commit_bundle(client_id, volume_id, bundle_id, chunks)
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
