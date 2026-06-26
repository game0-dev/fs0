use crate::{
    Fs0Error,
    request_handlers::handle_control_request,
    server::{CentralServer, ControlConnectionIdentity},
};
use fs0_core::protocol::{ProtocolRequest, ProtocolResponse};
use fs0_transport::{Connection, Transport};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh_relay::server::Server as RelayServer;
use std::{
    fmt,
    sync::{Arc, Weak},
};
use tracing::{info, warn};

pub(super) fn spawn_central_tasks(
    transport: Transport,
    relay: RelayServer,
    server: Weak<CentralServer>,
) -> CentralTasks {
    let router = transport
        .router()
        .accept(
            fs0_core::TRANSPORT_CONTROL_ALPN,
            CentralControlProtocol { server },
        )
        .spawn();

    CentralTasks { router, relay }
}

pub(super) struct CentralTasks {
    router: Router,
    relay: RelayServer,
}

impl CentralTasks {
    pub(super) async fn shutdown(self) -> crate::Fs0Result<()> {
        let router_result = self
            .router
            .shutdown()
            .await
            .map_err(|err| Fs0Error::Internal {
                message: format!("central transport router shutdown failed: {err}"),
            });
        let _ = self.relay.shutdown().await;
        router_result
    }
}

impl fmt::Debug for CentralTasks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CentralTasks")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct CentralControlProtocol {
    server: Weak<CentralServer>,
}

impl ProtocolHandler for CentralControlProtocol {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let connection = Connection::new(connection);
        let Some(server) = self.server.upgrade() else {
            connection.close(b"central shutdown");
            return Ok(());
        };
        if server.is_exiting() {
            connection.close(b"central shutdown");
            return Ok(());
        }

        info!("central accepted control connection");
        handle_control_connection(server, connection).await;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Some(server) = self.server.upgrade() {
            server.shutdown_notify.notify_waiters();
        }
    }
}

async fn handle_control_connection(server: Arc<CentralServer>, connection: Connection) {
    let identity = Arc::new(tokio::sync::Mutex::new(ControlConnectionIdentity::default()));
    if !server.is_exiting() {
        let shutdown_notify = server.shutdown_notify.clone();
        let accept_task = connection.spawn_accept(
            {
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
                                handle_control_request(&server, &connection, request, &mut identity)
                                    .await
                            }
                            _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                        };
                        Ok(Some(response))
                    }
                }
            },
            async move {
                shutdown_notify.notified().await;
            },
        );
        match accept_task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = %err, "central control connection accept failed"),
            Err(err) => warn!(error = %err, "central control connection task failed"),
        }
    }

    let event = server.unregister_identity(*identity.lock().await);
    if let Some(event) = event {
        server.broadcast_event(event).await;
    }
    connection.close(b"central control closed");
    info!("central control connection closed");
}
