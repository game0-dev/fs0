use crate::server::StorageServer;
use fs0_core::{DataRequest, DataResponse, Fs0Error, blake3_hash};
use fs0_transport::{read_frame, write_frame};
use iroh::{
    Endpoint,
    endpoint::{RecvStream, SendStream},
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
                                    let server = server.clone();
                                    tokio::spawn(async move {
                                        handle_data_stream(server, send, recv).await;
                                    });
                                }
                            }
                        }
                    });
                }
            }
        }
    })
}

async fn handle_data_stream(
    server: Arc<StorageServer>,
    mut send: SendStream,
    mut recv: RecvStream,
) {
    let Ok(request) = read_frame::<DataRequest, _>(&mut recv).await else {
        return;
    };
    let response = handle_data_request(server, request).await;
    let _ = write_frame(&mut send, &response).await;
    let _ = send.finish();
}

async fn handle_data_request(server: Arc<StorageServer>, request: DataRequest) -> DataResponse {
    match request {
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
            raw_len,
            compressed_bytes,
        } => {
            let compressed_len = compressed_bytes.len() as u64;
            if blake3_hash(&compressed_bytes) != chunk_id {
                return DataResponse::Error(Fs0Error::HashMismatch { volume_offset: 0 });
            }
            match server
                .put_chunk(volume_id, chunk_id, raw_len, compressed_bytes)
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
        } => match server.commit_bundle(volume_id, bundle_id, chunks).await {
            Ok(bundle) => DataResponse::CommitBundle {
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::DownloadBundle {
            volume_id,
            bundle_id,
        } => match server.read_bundle(volume_id, bundle_id).await {
            Ok(bytes) => DataResponse::DownloadBundle {
                compressed_bytes: bytes,
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
