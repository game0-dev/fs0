use crate::server::StorageServer;
use fs0_core::{
    Fs0Error, Fs0Result,
    protocol::{ControlRequest, ControlResponse},
};

pub(super) fn handle_control_request(
    server: &StorageServer,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    match request {
        ControlRequest::GrantUploadLease(lease) => match server.grant_upload_lease(lease) {
            Ok(lease_id) => Ok(ControlResponse::GrantUploadLease { lease_id }),
            Err(err) => Err(err),
        },
        ControlRequest::RevokeUploadLease { lease_id } => {
            server.revoke_upload_lease(lease_id);
            Ok(ControlResponse::RevokeUploadLease)
        }
        _ => Err(Fs0Error::InvalidRequest),
    }
}
