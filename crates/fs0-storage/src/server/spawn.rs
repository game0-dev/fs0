use super::client_connection::ClientConnection;
use crate::server::StorageServer;
use fs0_core::TRANSPORT_DATA_ALPN;
use fs0_transport::{Connection, Transport};
use std::sync::{Arc, Weak};
use tokio::{sync::Notify, task::JoinHandle};

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
    ClientConnection::new(connection)
        .serve(server, shutdown_notify)
        .await;
}
