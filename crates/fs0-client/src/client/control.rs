use super::{
    CentralStatus, ControlRequestError, Fs0Client, ListOptions, request_control_result,
    unexpected_control_response,
};
use crate::Fs0Result;
use fs0_core::protocol::{
    BeginUpdateRequest, CommitUpdateRequest, ControlRequest, ControlResponse, DirectoryEntries,
    FileChangeLogs, FileReadPlan, FileRecord, UpdateLease,
};

impl Fs0Client {
    pub async fn central_status(&self) -> Fs0Result<CentralStatus> {
        match self.request(ControlRequest::CentralStatus).await? {
            ControlResponse::CentralStatus {
                clients_count,
                storages,
            } => {
                self.set_storage_peers(storages.clone());
                Ok(CentralStatus {
                    clients_count,
                    storages,
                })
            }
            response => unexpected_control_response(response),
        }
    }

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Fs0Result<u64> {
        match self
            .request(ControlRequest::CreateVolume { name, max_bytes })
            .await?
        {
            ControlResponse::CreateVolume { volume_id } => Ok(volume_id),
            response => unexpected_control_response(response),
        }
    }

    pub async fn list_directory(
        &self,
        dir: &str,
        options: ListOptions,
    ) -> Fs0Result<DirectoryEntries> {
        match self
            .request(ControlRequest::ListDirectory {
                dir: dir.to_owned(),
                limit: options.limit,
                cursor: options.cursor,
            })
            .await?
        {
            ControlResponse::ListDirectory(entries) => Ok(entries),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlan {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::GetFileReadPlan(plan) | ControlResponse::GetFileReadPlanById(plan) => {
                Ok(plan)
            }
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            response => unexpected_control_response(response),
        }
    }

    pub async fn delete_file(&self, path: &str) -> Fs0Result<()> {
        match self
            .request(ControlRequest::DeleteFile {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::DeleteFile | ControlResponse::DeleteFileById => Ok(()),
            response => unexpected_control_response(response),
        }
    }

    pub async fn delete_file_by_id(&self, file_id: u64) -> Fs0Result<()> {
        match self
            .request(ControlRequest::DeleteFileById { file_id })
            .await?
        {
            ControlResponse::DeleteFileById => Ok(()),
            response => unexpected_control_response(response),
        }
    }

    pub async fn copy_file(&self, source_path: &str, target_path: &str) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::CopyFile {
                source_path: source_path.to_owned(),
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::CopyFile(file) | ControlResponse::CopyFileById(file) => Ok(file),
            response => unexpected_control_response(response),
        }
    }

    pub async fn copy_file_by_id(
        &self,
        source_file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::CopyFileById {
                source_file_id,
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::CopyFileById(file) => Ok(file),
            response => unexpected_control_response(response),
        }
    }

    pub async fn rename_file(&self, source_path: &str, target_path: &str) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::RenameFile {
                source_path: source_path.to_owned(),
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::RenameFile(file) | ControlResponse::RenameFileById(file) => Ok(file),
            response => unexpected_control_response(response),
        }
    }

    pub async fn rename_file_by_id(
        &self,
        file_id: u64,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::RenameFileById {
                file_id,
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::RenameFileById(file) => Ok(file),
            response => unexpected_control_response(response),
        }
    }

    pub async fn begin_update(&self, request: BeginUpdateRequest) -> Fs0Result<UpdateLease> {
        match self.request(ControlRequest::BeginUpdate(request)).await? {
            ControlResponse::BeginUpdate(lease) => Ok(lease),
            response => unexpected_control_response(response),
        }
    }

    pub async fn commit_update(&self, request: CommitUpdateRequest) -> Fs0Result<FileRecord> {
        match self.request(ControlRequest::CommitUpdate(request)).await? {
            ControlResponse::CommitUpdate(file) => Ok(file),
            response => unexpected_control_response(response),
        }
    }

    pub async fn abort_update(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        match self
            .request(ControlRequest::AbortUpdate { lease_id, file_id })
            .await?
        {
            ControlResponse::AbortUpdate => Ok(()),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Fs0Result<FileChangeLogs> {
        match self
            .request(ControlRequest::GetFileChangeLogs {
                after_event_id,
                limit,
            })
            .await?
        {
            ControlResponse::GetFileChangeLogs(logs) => Ok(logs),
            response => unexpected_control_response(response),
        }
    }

    pub(super) async fn request(&self, request: ControlRequest) -> Fs0Result<ControlResponse> {
        let mut control = self.control.lock().await;
        if control.is_closed() {
            self.reconnect_control(&mut control).await?;
        }

        match request_control_result(&control, request.clone()).await {
            Ok(response) => Ok(response),
            Err(ControlRequestError::Response(err)) => Err(err),
            Err(ControlRequestError::Rpc(err)) => {
                self.reconnect_control(&mut control).await?;
                if retry_control_request_after_rpc_error(&request) {
                    return request_control_result(&control, request)
                        .await
                        .map_err(ControlRequestError::into_error);
                }

                Err(err)
            }
        }
    }
}

fn retry_control_request_after_rpc_error(request: &ControlRequest) -> bool {
    match request {
        ControlRequest::CentralStatus
        | ControlRequest::ListDirectory { .. }
        | ControlRequest::GetFileReadPlan { .. }
        | ControlRequest::GetFileReadPlanById { .. }
        | ControlRequest::GetFileChangeLogs { .. } => true,
        ControlRequest::RegisterClient { .. }
        | ControlRequest::RegisterStorage { .. }
        | ControlRequest::CreateVolume { .. }
        | ControlRequest::DeleteFile { .. }
        | ControlRequest::DeleteFileById { .. }
        | ControlRequest::CopyFile { .. }
        | ControlRequest::CopyFileById { .. }
        | ControlRequest::RenameFile { .. }
        | ControlRequest::RenameFileById { .. }
        | ControlRequest::BeginUpdate(_)
        | ControlRequest::CommitUpdate(_)
        | ControlRequest::AbortUpdate { .. }
        | ControlRequest::GrantUploadLease(_)
        | ControlRequest::RevokeUploadLease { .. }
        | ControlRequest::ReportBundleReplica { .. }
        | ControlRequest::UpdateStorageVolumeOffset { .. }
        | ControlRequest::ValidateClientAuth { .. } => false,
    }
}
