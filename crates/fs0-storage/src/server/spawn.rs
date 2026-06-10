use crate::{
    Fs0Error,
    request_handlers::{handle_control_request, handle_data_protocol_request},
    server::StorageServer,
};
use fs0_core::{
    TRANSPORT_DATA_ALPN,
    protocol::{ProtocolRequest, ProtocolResponse},
};
use fs0_transport::{Connection, Transport};
use std::sync::{Arc, Weak};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};

pub(super) fn spawn_connection_accept_loop(
    endpoint: Transport,
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                accepted = endpoint.accept() => {
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
                    let alpn = accepted.alpn().to_vec();
                    let connection = accepted;

                    match alpn.as_slice() {
                        TRANSPORT_DATA_ALPN => {
                            server.tasks.push(spawn_client_connection_loop(
                                server.clone(),
                                connection,
                                shutdown_notify.clone(),
                            ));
                        }
                        _ => connection.close(b"unsupported storage alpn"),
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_control_accept_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(server) = server.upgrade() else {
            return;
        };
        if server.is_exiting() {
            return;
        }

        tokio::select! {
            _ = shutdown_notify.notified() => {}
            _ = server.central_connection.serve({
                let server = server.clone();
                move |request| {
                    let server = server.clone();
                    async move {
                        let response = match request {
                            ProtocolRequest::Control(request) => match handle_control_request(&server, request) {
                                Ok(response) => ProtocolResponse::Control(response),
                                Err(err) => ProtocolResponse::Error(err),
                            },
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            }) => {}
        }
    })
}

fn spawn_client_connection_loop(
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
                    let mut authenticated_client_id = authenticated_client_id.lock().await;
                    Ok(Some(
                        handle_data_protocol_request(
                            server,
                            &mut authenticated_client_id,
                            request,
                        )
                        .await,
                    ))
                }
            }
        }) => {}
    }
}
