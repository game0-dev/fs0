use crate::{
    Fs0Error,
    request_handlers::{ControlConnectionIdentity, handle_control_request, unregister_identity},
    server::CentralServer,
};
use fs0_core::protocol::{ProtocolRequest, ProtocolResponse};
use fs0_transport::{Connection, Transport};
use iroh_relay::server::Server as RelayServer;
use std::sync::{Arc, Weak};
use tokio::{sync::Notify, task::JoinHandle};

pub(super) fn spawn_central_tasks(
    transport: Transport,
    relay: RelayServer,
    server: Weak<CentralServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let accept_task = spawn_accept_loop(transport, server, shutdown_notify);
        let _ = accept_task.await;
        let _ = relay.shutdown().await;
    })
}

fn spawn_accept_loop(
    endpoint: Transport,
    server: Weak<CentralServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                connection = endpoint.accept() => {
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let connection = match connection {
                        Ok(Some(connection)) => connection,
                        Ok(None) => break,
                        Err(_) => continue,
                    };
                    let shutdown_notify = shutdown_notify.clone();
                    tokio::spawn(async move {
                        handle_control_connection(server, connection, shutdown_notify).await;
                    });
                }
            }
        }
    })
}

async fn handle_control_connection(
    server: Arc<CentralServer>,
    connection: Connection,
    shutdown_notify: Arc<Notify>,
) {
    let identity = Arc::new(tokio::sync::Mutex::new(ControlConnectionIdentity::default()));
    if !server.is_exiting() {
        tokio::select! {
            _ = shutdown_notify.notified() => {}
            _ = connection.serve({
                let server = server.clone();
                let connection = connection.clone();
                let identity = identity.clone();
                move |request| {
                    let server = server.clone();
                    let connection = connection.clone();
                    let identity = identity.clone();
                    async move {
                        let mut identity = identity.lock().await;
                        let response = match request {
                            ProtocolRequest::Control(request) => {
                                ProtocolResponse::Control(handle_control_request(
                                    &server,
                                    &connection,
                                    request,
                                    &mut identity,
                                )
                                .await)
                            }
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            }) => {}
        }
    }

    unregister_identity(&server, *identity.lock().await);
    connection.close(b"central control closed");
}
