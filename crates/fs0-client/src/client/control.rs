use super::{CentralStatus, Fs0Client, ListOptions, request_control, unexpected_control_response};
use crate::Fs0Result;
use fs0_core::protocol::{
    AppendLease, BeginAppendRequest, CommitAppendRequest, ControlRequest, ControlResponse,
    DirectoryEntries, FileChangeLogs, FileReadPlan,
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
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn create_volume(&self, name: String, max_bytes: u64) -> Fs0Result<u64> {
        match self
            .request(ControlRequest::CreateVolume { name, max_bytes })
            .await?
        {
            ControlResponse::CreateVolume { volume_id } => Ok(volume_id),
            ControlResponse::Error(err) => Err(err),
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
            ControlResponse::Error(err) => Err(err),
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
            ControlResponse::GetFileReadPlan(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
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
            ControlResponse::DeleteFile => Ok(()),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Fs0Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::BeginAppend(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Fs0Result<FileReadPlan> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::CommitAppend(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn abort_append(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        match self
            .request(ControlRequest::AbortAppend { lease_id, file_id })
            .await?
        {
            ControlResponse::AbortAppend => Ok(()),
            ControlResponse::Error(err) => Err(err),
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
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub(super) async fn request(&self, request: ControlRequest) -> Fs0Result<ControlResponse> {
        request_control(&self.control, request).await
    }
}
