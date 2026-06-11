mod control;
mod data;

use crate::server::StorageServer;
use fs0_core::{
    Fs0Error, Fs0Result,
    protocol::{ControlRequest, ControlResponse, DataRequest, DataResponse},
};

pub(crate) fn handle_control_request(
    server: &StorageServer,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    match request {
        ControlRequest::GrantUploadLease(lease) => control::grant_upload_lease(server, lease),
        ControlRequest::RevokeUploadLease { lease_id } => {
            control::revoke_upload_lease(server, lease_id)
        }
        _ => Err(Fs0Error::InvalidRequest),
    }
}

pub(crate) async fn handle_data_request(
    server: &StorageServer,
    request: DataRequest,
) -> Fs0Result<DataResponse> {
    match request {
        DataRequest::Authenticate { .. } => Err(Fs0Error::InvalidRequest),
        DataRequest::HasChunk {
            volume_id,
            chunk_id,
        } => data::has_chunk(server, volume_id, chunk_id).await,
        DataRequest::UploadChunk(request) => data::upload_chunk(server, request).await,
        DataRequest::DownloadChunk {
            volume_id,
            chunk_id,
        } => data::download_chunk(server, volume_id, chunk_id).await,
        DataRequest::HasBundle {
            volume_id,
            bundle_id,
        } => data::has_bundle(server, volume_id, bundle_id).await,
        DataRequest::CommitBundle(request) => data::commit_bundle(server, request).await,
        DataRequest::ListBundleChunks {
            volume_id,
            bundle_id,
        } => data::list_bundle_chunks(server, volume_id, bundle_id).await,
    }
}
