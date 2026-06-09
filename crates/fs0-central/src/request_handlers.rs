mod append;
mod client;
mod storage;

use crate::{Fs0Error, Fs0Result, server::CentralServer};
use fs0_core::{
    FS0_VERSION,
    protocol::{ControlRequest, ControlResponse, StoragePeerInfo},
};
use fs0_transport::Connection;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ControlConnectionIdentity {
    #[default]
    Anonymous,
    Client(u64),
    Storage(u64),
}

pub(crate) async fn handle_control_request(
    server: &CentralServer,
    connection: &Connection,
    request: ControlRequest,
    identity: &mut ControlConnectionIdentity,
) -> ControlResponse {
    let response = match request {
        ControlRequest::RegisterClient {
            name: _,
            token,
            version,
        } => {
            match validate_registration_version(&version)
                .and_then(|_| register_client(server, token, connection.clone()))
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
                storage::register_storage(
                    server,
                    name,
                    token,
                    volumes,
                    iroh_endpoint,
                    connection.clone(),
                )
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

    response.unwrap_or_else(ControlResponse::Error)
}

fn validate_registration_version(version: &str) -> Fs0Result<()> {
    if version == FS0_VERSION {
        return Ok(());
    }

    Err(Fs0Error::VersionConflict {
        message: format!(
            "fs0 version mismatch: expected {FS0_VERSION}, got {version}; please update fs0 client and storage binaries"
        ),
    })
}

async fn handle_client_request(
    server: &CentralServer,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    match request {
        ControlRequest::CreateVolume { name, max_bytes } => {
            client::create_volume(server, name, max_bytes)
        }
        ControlRequest::CentralStatus => storage::central_status(server),
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
        ControlRequest::BeginAppend(request) => append::begin_append(server, request).await,
        ControlRequest::CommitAppend(request) => append::commit_append(server, request).await,
        ControlRequest::AbortAppend { lease_id, file_id } => {
            append::abort_append(server, lease_id, file_id).await
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
        ControlRequest::CentralStatus => storage::central_status(server),
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

pub(crate) fn unregister_identity(server: &CentralServer, identity: ControlConnectionIdentity) {
    match identity {
        ControlConnectionIdentity::Anonymous => {}
        ControlConnectionIdentity::Client(client_id) => unregister_client(server, client_id),
        ControlConnectionIdentity::Storage(storage_id) => {
            storage::unregister_storage(server, storage_id);
        }
    }
}

fn register_client(
    server: &CentralServer,
    token: String,
    connection: Connection,
) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
    if !server.token_allowed(&token) {
        return Err(Fs0Error::Unauthorized);
    }

    let client_id = server.next_id();
    server.clients.write().insert(
        client_id,
        crate::server::ClientControlConnection { token, connection },
    );

    Ok((client_id, server.storage_peers_snapshot()))
}

fn unregister_client(server: &CentralServer, client_id: u64) {
    if let Some(client) = server.clients.write().remove(&client_id) {
        client.connection.close(b"central client unregistered");
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
            Fs0Error::VersionConflict { message } => {
                assert!(message.contains(FS0_VERSION));
                assert!(message.contains("0.0.1"));
                assert!(message.contains("update"));
            }
            err => panic!("unexpected error: {err:?}"),
        }
    }
}
