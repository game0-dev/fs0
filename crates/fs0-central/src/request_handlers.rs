mod append;
mod client;
mod storage;

use crate::{Fs0Error, server::CentralServer};
use fs0_core::protocol::{ControlRequest, ControlResponse};
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
    match request {
        ControlRequest::RegisterClient { name: _, token } => {
            if !matches!(*identity, ControlConnectionIdentity::Anonymous) {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match register_client(server, token, connection.clone()) {
                Ok((client_id, storages)) => {
                    *identity = ControlConnectionIdentity::Client(client_id);
                    ControlResponse::RegisterClient {
                        client_id,
                        storages,
                    }
                }
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RegisterStorage {
            name,
            token,
            volumes,
            iroh_endpoint,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Anonymous) {
                return ControlResponse::Error(Fs0Error::InvalidRequest);
            }

            match storage::register_storage(
                server,
                name,
                token,
                volumes,
                iroh_endpoint,
                connection.clone(),
            ) {
                Ok((storage_id, storages)) => {
                    *identity = ControlConnectionIdentity::Storage(storage_id);
                    ControlResponse::RegisterStorage {
                        storage_id,
                        storages,
                    }
                }
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::ValidateClientAuth {
            client_id,
            client_token,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Storage(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match storage::validate_client_auth(server, client_id, client_token) {
                Ok(()) => ControlResponse::ValidateClientAuth { client_id },
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CreateVolume { name, max_bytes } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::create_volume(server, name, max_bytes) {
                Ok(volume_id) => ControlResponse::CreateVolume { volume_id },
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CentralStatus => match storage::central_status(server) {
            Ok((clients_count, storages)) => ControlResponse::CentralStatus {
                clients_count,
                storages,
            },
            Err(err) => ControlResponse::Error(err),
        },
        ControlRequest::ListDirectory { dir, limit, cursor } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::list_directory(server, &dir, limit, cursor) {
                Ok(entries) => ControlResponse::ListDirectory(entries),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlan { path } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::get_file_read_plan(server, &path) {
                Ok(plan) => ControlResponse::GetFileReadPlan(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileReadPlanById { file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::get_file_read_plan_by_id(server, file_id) {
                Ok(plan) => ControlResponse::GetFileReadPlanById(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFile { path } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::delete_file(server, &path) {
                Ok(()) => ControlResponse::DeleteFile,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::DeleteFileById { file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::delete_file_by_id(server, file_id) {
                Ok(()) => ControlResponse::DeleteFileById,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CopyFile {
            source_path,
            target_path,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::copy_file(server, &source_path, &target_path) {
                Ok(file) => ControlResponse::CopyFile(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CopyFileById {
            source_file_id,
            target_path,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::copy_file_by_id(server, source_file_id, &target_path) {
                Ok(file) => ControlResponse::CopyFileById(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RenameFile {
            source_path,
            target_path,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::rename_file(server, &source_path, &target_path) {
                Ok(file) => ControlResponse::RenameFile(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::RenameFileById {
            file_id,
            target_path,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::rename_file_by_id(server, file_id, &target_path) {
                Ok(file) => ControlResponse::RenameFileById(file),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GetFileChangeLogs {
            after_event_id,
            limit,
        } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match client::get_file_change_logs(server, after_event_id, limit) {
                Ok(logs) => ControlResponse::GetFileChangeLogs(logs),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::BeginAppend(request) => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match append::begin_append(server, request).await {
                Ok(lease) => ControlResponse::BeginAppend(lease),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::CommitAppend(request) => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match append::commit_append(server, request).await {
                Ok(plan) => ControlResponse::CommitAppend(plan),
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::AbortAppend { lease_id, file_id } => {
            if !matches!(*identity, ControlConnectionIdentity::Client(_)) {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            }

            match append::abort_append(server, lease_id, file_id).await {
                Ok(()) => ControlResponse::AbortAppend,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::ReportBundleReplica { events } => {
            let ControlConnectionIdentity::Storage(storage_id) = *identity else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match storage::report_bundle_replica(server, storage_id, events) {
                Ok(()) => ControlResponse::ReportBundleReplica,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::UpdateStorageVolumeOffset {
            volume_id,
            max_volume_offset,
        } => {
            let ControlConnectionIdentity::Storage(storage_id) = *identity else {
                return ControlResponse::Error(Fs0Error::Unauthorized);
            };

            match storage::update_storage_volume_offset(
                server,
                storage_id,
                volume_id,
                max_volume_offset,
            ) {
                Ok(()) => ControlResponse::UpdateStorageVolumeOffset,
                Err(err) => ControlResponse::Error(err),
            }
        }
        ControlRequest::GrantUploadLease(_) | ControlRequest::RevokeUploadLease { .. } => {
            ControlResponse::Error(Fs0Error::InvalidRequest)
        }
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
) -> crate::Fs0Result<(u64, Vec<fs0_core::protocol::StoragePeerInfo>)> {
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
