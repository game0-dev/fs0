use crate::{
    Fs0Error,
    request_handlers::handle_control_request,
    server::{CentralServer, ControlConnectionIdentity},
};
use fs0_core::protocol::{ProtocolRequest, ProtocolResponse};
use fs0_transport::{Connection, Transport};
use iroh_relay::server::Server as RelayServer;
use std::sync::{Arc, Weak};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub(super) fn spawn_central_tasks(
    transport: Transport,
    relay: RelayServer,
    server: Weak<CentralServer>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(shutdown_notify) = server
            .upgrade()
            .map(|server| server.shutdown_notify.clone())
        else {
            let _ = relay.shutdown().await;
            return;
        };

        loop {
            tokio::select! {
                connection = transport.accept() => {
                    let Some(server) = server.upgrade() else {
                        break;
                    };
                    if server.is_exiting() {
                        break;
                    }

                    let connection = match connection {
                        Ok(Some(connection)) => connection,
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "central failed to accept control connection");
                            continue;
                        }
                    };
                    info!("central accepted control connection");
                    tokio::spawn(async move {
                        handle_control_connection(server, connection).await;
                    });
                },
                _ = shutdown_notify.notified() => break,
            }
        }

        let _ = relay.shutdown().await;
    })
}

async fn handle_control_connection(server: Arc<CentralServer>, connection: Connection) {
    let identity = Arc::new(tokio::sync::Mutex::new(ControlConnectionIdentity::default()));
    if !server.is_exiting() {
        tokio::select! {
            _ = server.shutdown_notify.notified() => {}
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
                            ProtocolRequest::Control(request) => handle_control_request(
                                &server,
                                &connection,
                                request,
                                &mut identity,
                            )
                            .await,
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            }) => {}
        }
    }

    let event = server.unregister_identity(*identity.lock().await);
    if let Some(event) = event {
        server.broadcast_event(event).await;
    }
    connection.close(b"central control closed");
    info!("central control connection closed");
}
