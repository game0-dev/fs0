mod client;
mod storage;
mod update;

use crate::{
    Fs0Error, Fs0Result,
    server::{CentralServer, ControlConnectionIdentity},
};
use fs0_core::{
    FS0_VERSION,
    protocol::{ControlRequest, ControlResponse, ProtocolResponse},
};
use fs0_transport::Connection;

pub(crate) async fn handle_control_request(
    server: &CentralServer,
    connection: &Connection,
    request: ControlRequest,
    identity: &mut ControlConnectionIdentity,
) -> ProtocolResponse {
    let response = match request {
        ControlRequest::RegisterClient {
            name: _,
            token,
            version,
        } => {
            match validate_registration_version(&version)
                .and_then(|_| server.register_client(token, connection.clone()))
            {
                Ok((client_id, storages)) => {
                    *identity = ControlConnectionIdentity::Client(client_id);
                    Ok(ControlResponse::RegisterClient {
                        client_id,
                        storages,
                    })
                }
                Err(err) => Err(err),
            }
        }
        ControlRequest::RegisterStorage {
            name,
            token,
            version,
            volumes,
            iroh_endpoint,
        } => {
            match validate_registration_version(&version).and_then(|_| {
                server.register_storage(name, token, volumes, iroh_endpoint, connection.clone())
            }) {
                Ok((storage_id, storages)) => {
                    *identity = ControlConnectionIdentity::Storage(storage_id);
                    Ok(ControlResponse::RegisterStorage {
                        storage_id,
                        storages,
                    })
                }
                Err(err) => Err(err),
            }
        }
        request => {
            if let ControlConnectionIdentity::Client(_) = *identity {
                handle_client_request(server, request).await
            } else if let ControlConnectionIdentity::Storage(storage_id) = *identity {
                handle_storage_request(server, storage_id, request).await
            } else {
                Err(Fs0Error::Unauthorized)
            }
        }
    };

    match response {
        Ok(response) => ProtocolResponse::Control(response),
        Err(err) => ProtocolResponse::Error(err),
    }
}

fn validate_registration_version(version: &str) -> Fs0Result<()> {
    if version == FS0_VERSION {
        return Ok(());
    }

    Err(Fs0Error::Fs0Version {
        required: FS0_VERSION.to_owned(),
        actual: version.to_owned(),
    })
}

async fn handle_client_request(
    server: &CentralServer,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    match request {
        ControlRequest::CentralStatus => client::central_status(server),
        ControlRequest::ListDirectory { dir, limit, cursor } => {
            client::list_directory(server, &dir, limit, cursor)
        }
        ControlRequest::GetFileReadPlan { path } => client::get_file_read_plan(server, &path),
        ControlRequest::GetFileReadPlanById { file_id } => {
            client::get_file_read_plan_by_id(server, file_id)
        }
        ControlRequest::DeleteFile { path } => client::delete_file(server, &path),
        ControlRequest::DeleteFileById { file_id } => client::delete_file_by_id(server, file_id),
        ControlRequest::CopyFile {
            source_path,
            target_path,
        } => client::copy_file(server, &source_path, &target_path),
        ControlRequest::CopyFileById {
            source_file_id,
            target_path,
        } => client::copy_file_by_id(server, source_file_id, &target_path),
        ControlRequest::RenameFile {
            source_path,
            target_path,
        } => client::rename_file(server, &source_path, &target_path),
        ControlRequest::RenameFileById {
            file_id,
            target_path,
        } => client::rename_file_by_id(server, file_id, &target_path),
        ControlRequest::GetFileChangeLogs {
            after_event_id,
            limit,
        } => client::get_file_change_logs(server, after_event_id, limit),
        ControlRequest::BeginUpdate(request) => update::begin_update(server, request).await,
        ControlRequest::CommitUpdate(request) => update::commit_update(server, request).await,
        ControlRequest::AbortUpdate { lease_id, file_id } => {
            update::abort_update(server, lease_id, file_id).await
        }
        _ => Err(Fs0Error::Unauthorized),
    }
}

async fn handle_storage_request(
    server: &CentralServer,
    storage_id: u64,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    match request {
        ControlRequest::CreateVolume { name, max_bytes } => {
            storage::create_volume(server, name, max_bytes)
        }
        ControlRequest::ValidateClientAuth {
            client_id,
            client_token,
        } => storage::validate_client_auth(server, client_id, client_token),
        ControlRequest::ReportBundleReplica { events } => {
            storage::report_bundle_replica(server, storage_id, events)
        }
        ControlRequest::UpdateStorageVolumeOffset {
            volume_id,
            max_volume_offset,
        } => {
            storage::update_storage_volume_offset(server, storage_id, volume_id, max_volume_offset)
        }
        _ => Err(Fs0Error::Unauthorized),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_matching_registration_version() {
        assert_eq!(validate_registration_version(FS0_VERSION), Ok(()));
    }

    #[test]
    fn rejects_mismatched_registration_version() {
        let err = validate_registration_version("0.0.1").unwrap_err();

        match err {
            Fs0Error::Fs0Version { required, actual } => {
                assert_eq!(required, FS0_VERSION);
                assert_eq!(actual, "0.0.1");
            }
            err => panic!("unexpected error: {err:?}"),
        }
    }
}
