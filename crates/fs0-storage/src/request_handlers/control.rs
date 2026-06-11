use crate::server::{StorageServer, UploadLeaseState};
use fs0_core::{
    Fs0Error, Fs0Result,
    protocol::{ControlResponse, GrantUploadLeaseRequest},
};

pub(super) fn grant_upload_lease(
    server: &StorageServer,
    lease: GrantUploadLeaseRequest,
) -> Fs0Result<ControlResponse> {
    if !server.volumes.contains_key(&lease.volume_id) {
        return Err(Fs0Error::UnknownVolume);
    }

    server.upload_leases.write().insert(
        lease.lease_id,
        UploadLeaseState {
            file_id: lease.file_id,
            volume_id: lease.volume_id,
            expires_at_ms: lease.expires_at_ms,
        },
    );

    Ok(ControlResponse::GrantUploadLease {
        lease_id: lease.lease_id,
    })
}

pub(super) fn revoke_upload_lease(
    server: &StorageServer,
    lease_id: u64,
) -> Fs0Result<ControlResponse> {
    server.upload_leases.write().remove(&lease_id);
    Ok(ControlResponse::RevokeUploadLease)
}
