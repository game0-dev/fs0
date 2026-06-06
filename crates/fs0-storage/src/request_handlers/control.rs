use crate::server::StorageServer;
use fs0_core::{
    Fs0Error,
    protocol::{ControlRequest, ControlResponse},
};

pub(super) fn handle_control_request(
    server: &StorageServer,
    request: ControlRequest,
) -> ControlResponse {
    match request {
        ControlRequest::GrantUploadLease(lease) => match server.grant_upload_lease(lease) {
            Ok(lease_id) => ControlResponse::GrantUploadLease { lease_id },
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::RevokeUploadLease { lease_id } => {
            server.revoke_upload_lease(lease_id);
            ControlResponse::RevokeUploadLease
        }
        _ => ControlResponse::Error(Fs0Error::InvalidRequest),
    }
}
