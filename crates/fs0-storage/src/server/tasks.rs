use crate::{request_handlers, server::StorageServer};
use fs0_core::{
    Fs0Error, TRANSPORT_DATA_ALPN, VOLUME_DATA_FILE_IDLE_TTL_MS,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use fs0_transport::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use std::sync::{Arc, Weak};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::{Duration, interval},
};
use tracing::{info, warn};

pub(super) fn spawn_connection_router(server: &Arc<StorageServer>) -> Router {
    server
        .transport()
        .router()
        .accept(
            TRANSPORT_DATA_ALPN,
            StorageDataProtocol {
                server: Arc::downgrade(server),
            },
        )
        .spawn()
}

#[derive(Debug)]
struct StorageDataProtocol {
    server: Weak<StorageServer>,
}

impl ProtocolHandler for StorageDataProtocol {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let connection = Connection::new(connection);
        let Some(server) = self.server.upgrade() else {
            connection.close(b"storage shutdown");
            return Ok(());
        };
        if server.is_exiting() {
            connection.close(b"storage shutdown");
            return Ok(());
        }

        info!("storage accepted data connection");
        let authenticated_client_id = Arc::new(Mutex::new(None));
        let shutdown_notify = server.shutdown_notify.clone();
        let connection_server = server.clone();
        let accept_task = connection.spawn_accept(
            {
                move |request| {
                    let server = connection_server.clone();
                    let authenticated_client_id = authenticated_client_id.clone();
                    async move {
                        let response = if authenticated_client_id.lock().await.is_some() {
                            match request {
                                ProtocolRequest::Data(DataRequest::Authenticate { .. }) => {
                                    warn!("storage received duplicate data authentication");
                                    Err(Fs0Error::InvalidRequest)
                                }
                                ProtocolRequest::Data(request) => {
                                    info!("storage received data request");
                                    let response = request_handlers::handle_data_request(&server, request).await;
                                    if let Err(err) = &response {
                                        warn!(error = %err, "storage data request failed");
                                    }
                                    response
                                }
                                _ => {
                                    warn!("storage received non-data request on data connection");
                                    Err(Fs0Error::InvalidRequest)
                                }
                            }
                        } else {
                            match request {
                                ProtocolRequest::Data(DataRequest::Authenticate {
                                    client_id,
                                    client_token,
                                }) => {
                                    info!(client_id, "storage received data authenticate request");
                                    match server
                                        .central_connection
                                        .validate_client_auth(client_id, client_token)
                                        .await
                                    {
                                        Ok(()) => {
                                            *authenticated_client_id.lock().await = Some(client_id);
                                            info!(client_id, "storage data connection authenticated");
                                            Ok(DataResponse::Authenticate { client_id })
                                        }
                                        Err(err) => {
                                            warn!(client_id, error = %err, "storage data authentication failed");
                                            Err(err)
                                        }
                                    }
                                }
                                _ => {
                                    warn!("storage received unauthenticated data request");
                                    Err(Fs0Error::Unauthorized)
                                }
                            }
                        };

                        Ok(Some(match response {
                            Ok(response) => ProtocolResponse::Data(response),
                            Err(err) => ProtocolResponse::Error(err),
                        }))
                    }
                }
            },
            async move {
                shutdown_notify.notified().await;
            },
        );
        accept_task
            .await
            .map_err(AcceptError::from_err)?
            .map_err(AcceptError::from_err)?;

        connection.close(b"storage data connection closed");
        info!("storage data connection closed");
        Ok(())
    }

    async fn shutdown(&self) {
        if let Some(server) = self.server.upgrade() {
            server.shutdown_notify.notify_waiters();
        }
    }
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
