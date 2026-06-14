use super::CentralSession;
use crate::{Fs0Result, client::CentralStatus};
use fs0_core::{
    HashId,
    protocol::{
        BeginUpdateRequest, CommitUpdateRequest, ControlRequest, ControlResponse, DirectoryEntries,
        FileChangeLogs, FileReadPlan, FileRecord, UpdateLease,
    },
};

impl CentralSession {
    pub(crate) async fn central_status(&self) -> Fs0Result<CentralStatus> {
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
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn create_volume(&self, name: String, max_bytes: u64) -> Fs0Result<u64> {
        match self
            .request(ControlRequest::CreateVolume { name, max_bytes })
            .await?
        {
            ControlResponse::CreateVolume { volume_id } => Ok(volume_id),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn list_directory(
        &self,
        dir: &str,
        limit: u32,
        cursor: Option<u64>,
    ) -> Fs0Result<DirectoryEntries> {
        match self
            .request(ControlRequest::ListDirectory {
                dir: dir.to_owned(),
                limit,
                cursor,
            })
            .await?
        {
            ControlResponse::ListDirectory(entries) => Ok(entries),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn get_file_read_plan(&self, path: &str) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlan {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::GetFileReadPlan(plan) | ControlResponse::GetFileReadPlanById(plan) => {
                Ok(plan)
            }
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn get_file_read_plan_by_id(&self, file_id: u64) -> Fs0Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn has_bundle(
        &self,
        bundle_id: HashId,
        volume_id: Option<u64>,
    ) -> Fs0Result<bool> {
        match self
            .request(ControlRequest::HasBundle {
                bundle_id,
                volume_id,
            })
            .await?
        {
            ControlResponse::HasBundle { exists } => Ok(exists),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn delete_file(&self, path: &str) -> Fs0Result<()> {
        match self
            .request(ControlRequest::DeleteFile {
                path: path.to_owned(),
            })
            .await?
        {
            ControlResponse::DeleteFile | ControlResponse::DeleteFileById => Ok(()),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn delete_file_by_id(&self, file_id: u64) -> Fs0Result<()> {
        match self
            .request(ControlRequest::DeleteFileById { file_id })
            .await?
        {
            ControlResponse::DeleteFileById => Ok(()),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn copy_file(
        &self,
        source_path: &str,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::CopyFile {
                source_path: source_path.to_owned(),
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::CopyFile(file) | ControlResponse::CopyFileById(file) => Ok(file),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn copy_file_by_id(
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
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn rename_file(
        &self,
        source_path: &str,
        target_path: &str,
    ) -> Fs0Result<FileRecord> {
        match self
            .request(ControlRequest::RenameFile {
                source_path: source_path.to_owned(),
                target_path: target_path.to_owned(),
            })
            .await?
        {
            ControlResponse::RenameFile(file) | ControlResponse::RenameFileById(file) => Ok(file),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn rename_file_by_id(
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
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn begin_update(&self, request: BeginUpdateRequest) -> Fs0Result<UpdateLease> {
        match self.request(ControlRequest::BeginUpdate(request)).await? {
            ControlResponse::BeginUpdate(lease) => Ok(lease),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn commit_update(
        &self,
        request: CommitUpdateRequest,
    ) -> Fs0Result<FileRecord> {
        match self.request(ControlRequest::CommitUpdate(request)).await? {
            ControlResponse::CommitUpdate(file) => Ok(file),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn abort_update(&self, lease_id: u64, file_id: u64) -> Fs0Result<()> {
        match self
            .request(ControlRequest::AbortUpdate { lease_id, file_id })
            .await?
        {
            ControlResponse::AbortUpdate => Ok(()),
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn get_file_change_logs(
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
            response => Err(fs0_core::Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }
}
