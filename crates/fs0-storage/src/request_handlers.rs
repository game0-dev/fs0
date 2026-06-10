mod control;
mod data;

use crate::server::StorageServer;
use fs0_core::{
    Fs0Error, Fs0Result,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use std::sync::Arc;

pub(crate) fn handle_control_request(
    server: &StorageServer,
    request: fs0_core::protocol::ControlRequest,
) -> Fs0Result<fs0_core::protocol::ControlResponse> {
    control::handle_control_request(server, request)
}

pub(crate) async fn handle_data_protocol_request(
    server: Arc<StorageServer>,
    authenticated_client_id: &mut Option<u64>,
    request: ProtocolRequest,
) -> ProtocolResponse {
    if let Some(client_id) = *authenticated_client_id {
        return handle_authenticated_data_request(server, client_id, request).await;
    }

    match request {
        ProtocolRequest::Data(DataRequest::Authenticate {
            client_id,
            client_token,
        }) => match server.validate_client_auth(client_id, client_token).await {
            Ok(()) => {
                *authenticated_client_id = Some(client_id);
                ProtocolResponse::Data(DataResponse::Authenticate { client_id })
            }
            Err(err) => ProtocolResponse::Error(err),
        },
        _ => ProtocolResponse::Error(Fs0Error::Unauthorized),
    }
}

async fn handle_authenticated_data_request(
    server: Arc<StorageServer>,
    client_id: u64,
    request: ProtocolRequest,
) -> ProtocolResponse {
    match request {
        ProtocolRequest::Data(DataRequest::Authenticate { .. }) => {
            ProtocolResponse::Error(Fs0Error::InvalidRequest)
        }
        ProtocolRequest::Data(request) => {
            match data::handle_data_request(server, client_id, request).await {
                Ok(response) => ProtocolResponse::Data(response),
                Err(err) => ProtocolResponse::Error(err),
            }
        }
        _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
    }
}
