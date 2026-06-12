use crate::{request_handlers, server::StorageServer};
use fs0_core::{
    Fs0Error, TRANSPORT_DATA_ALPN, VOLUME_DATA_FILE_IDLE_TTL_MS,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use fs0_transport::Transport;
use std::sync::{Arc, Weak};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::{Duration, interval},
};

pub(super) fn spawn_connection_accept_loop(
    transport: Transport,
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                accepted = transport.accept() => {
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let accepted = match accepted {
                        Ok(Some(accepted)) => accepted,
                        Ok(None) => break,
                        Err(_) => continue,
                    };

                    match accepted.alpn() {
                        TRANSPORT_DATA_ALPN => {
                            let shutdown_notify = shutdown_notify.clone();
                            let connection_server = server.clone();
                            server.tasks.lock().push(tokio::spawn(async move {
                                if connection_server.is_exiting() {
                                    return;
                                }

                                let authenticated_client_id = Arc::new(Mutex::new(None));
                                tokio::select! {
                                    _ = shutdown_notify.notified() => {}
                                    _ = accepted.serve({
                                        move |request| {
                                            let server = connection_server.clone();
                                            let authenticated_client_id = authenticated_client_id.clone();
                                            async move {
                                                let response = if authenticated_client_id.lock().await.is_some() {
                                                    match request {
                                                        ProtocolRequest::Data(DataRequest::Authenticate { .. }) => {
                                                            Err(Fs0Error::InvalidRequest)
                                                        }
                                                        ProtocolRequest::Data(request) => {
                                                            request_handlers::handle_data_request(&server, request).await
                                                        }
                                                        _ => Err(Fs0Error::InvalidRequest),
                                                    }
                                                } else {
                                                    match request {
                                                        ProtocolRequest::Data(DataRequest::Authenticate {
                                                            client_id,
                                                            client_token,
                                                        }) => match server
                                                            .central_connection
                                                            .validate_client_auth(client_id, client_token)
                                                            .await
                                                        {
                                                            Ok(()) => {
                                                                *authenticated_client_id.lock().await = Some(client_id);
                                                                Ok(DataResponse::Authenticate { client_id })
                                                            }
                                                            Err(err) => Err(err),
                                                        },
                                                        _ => Err(Fs0Error::Unauthorized),
                                                    }
                                                };

                                                Ok(Some(match response {
                                                    Ok(response) => ProtocolResponse::Data(response),
                                                    Err(err) => ProtocolResponse::Error(err),
                                                }))
                                            }
                                        }
                                    }) => {}
                                }
                            }));
                        }
                        _ => accepted.close(b"unsupported storage alpn"),
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_bundle_reporter_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            let _ = server
                .bundle_reporter
                .sync_all(&server.central_connection, &server.volumes)
                .await;
        }
    })
}

pub(super) fn spawn_idle_file_close_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(VOLUME_DATA_FILE_IDLE_TTL_MS));

        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            server.close_idle_data_files();
        }
    })
}
