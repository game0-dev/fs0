use super::StorageServer;
use crate::request_handlers;
use fs0_core::{
    Fs0Error,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use fs0_transport::Connection;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub(super) struct ClientConnection {
    connection: Connection,
    authenticated_client_id: Arc<Mutex<Option<u64>>>,
}

impl ClientConnection {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            connection,
            authenticated_client_id: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) async fn serve(self, server: Arc<StorageServer>, shutdown_notify: Arc<Notify>) {
        if server.is_exiting() {
            return;
        }

        tokio::select! {
            _ = shutdown_notify.notified() => {}
            _ = self.connection.serve({
                let authenticated_client_id = self.authenticated_client_id.clone();
                move |request| {
                    let server = server.clone();
                    let authenticated_client_id = authenticated_client_id.clone();
                    async move {
                        Ok(Some(handle_data_protocol_request(
                            &server,
                            authenticated_client_id,
                            request,
                        ).await))
                    }
                }
            }) => {}
        }
    }
}

async fn handle_data_protocol_request(
    server: &StorageServer,
    authenticated_client_id: Arc<Mutex<Option<u64>>>,
    request: ProtocolRequest,
) -> ProtocolResponse {
    if authenticated_client_id.lock().await.is_some() {
        return handle_authenticated_data_request(server, request).await;
    }

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
                ProtocolResponse::Data(DataResponse::Authenticate { client_id })
            }
            Err(err) => ProtocolResponse::Error(err),
        },
        _ => ProtocolResponse::Error(Fs0Error::Unauthorized),
    }
}

async fn handle_authenticated_data_request(
    server: &StorageServer,
    request: ProtocolRequest,
) -> ProtocolResponse {
    let response = match request {
        ProtocolRequest::Data(DataRequest::Authenticate { .. }) => Err(Fs0Error::InvalidRequest),
        ProtocolRequest::Data(request) => {
            request_handlers::handle_data_request(server, request).await
        }
        _ => Err(Fs0Error::InvalidRequest),
    };

    match response {
        Ok(response) => ProtocolResponse::Data(response),
        Err(err) => ProtocolResponse::Error(err),
    }
}
