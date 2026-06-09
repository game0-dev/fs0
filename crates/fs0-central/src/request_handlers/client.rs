use crate::{Fs0Result, db::CentralTx, server::CentralServer};
use fs0_core::{
    protocol::{ControlResponse, FileChangeLogKind, FileReadPlan, FileRecord, ReplicaLocation},
    utils::now_ms,
};
use std::collections::{HashMap, HashSet};

pub(super) fn create_volume(
    server: &CentralServer,
    name: String,
    max_bytes: u64,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let volume = tx.create_volume(name, max_bytes)?;
    tx.commit()?;
    Ok(ControlResponse::CreateVolume {
        volume_id: volume.volume_id,
    })
}

pub(super) fn list_directory(
    server: &CentralServer,
    dir: &str,
    limit: u32,
    cursor: Option<u64>,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let entries = tx.list_directory(dir, limit, cursor)?;
    tx.commit()?;
    Ok(ControlResponse::ListDirectory(entries))
}

pub(super) fn get_file_read_plan(server: &CentralServer, path: &str) -> Fs0Result<ControlResponse> {
    let plan = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let file = tx.get_file_by_path(path)?;
        let plan = super::update::get_file_read_plan_tx(&tx, file.file_id)?;
        tx.commit()?;
        plan
    };

    hydrate_read_plan_replicas(server, plan).map(ControlResponse::GetFileReadPlan)
}

pub(super) fn get_file_read_plan_by_id(
    server: &CentralServer,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let plan = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let plan = super::update::get_file_read_plan_tx(&tx, file_id)?;
        tx.commit()?;
        plan
    };

    hydrate_read_plan_replicas(server, plan).map(ControlResponse::GetFileReadPlanById)
}

pub(super) fn hydrate_read_plan_replicas(
    server: &CentralServer,
    mut plan: FileReadPlan,
) -> Fs0Result<FileReadPlan> {
    let replica_volume_ids_by_bundle = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let mut seen = HashSet::new();
        let bundle_ids = plan
            .bundles
            .iter()
            .filter_map(|bundle| seen.insert(bundle.bundle_id).then_some(bundle.bundle_id))
            .collect::<Vec<_>>();
        let mut volumes: HashMap<_, Vec<u64>> = HashMap::new();
        for replica in tx.get_bundles_by_ids(&bundle_ids)? {
            volumes
                .entry(replica.bundle_id)
                .or_default()
                .push(replica.volume_id);
        }
        tx.commit()?;
        volumes
    };

    for bundle in &mut plan.bundles {
        let volume_ids = replica_volume_ids_by_bundle
            .get(&bundle.bundle_id)
            .cloned()
            .unwrap_or_default();
        let online_volumes = server.online_volumes.read();
        bundle.replicas = volume_ids
            .into_iter()
            .filter_map(|volume_id| {
                online_volumes
                    .get(&volume_id)
                    .map(|storage_id| ReplicaLocation {
                        storage_id: *storage_id,
                        volume_id,
                    })
            })
            .collect();
    }

    Ok(plan)
}

pub(super) fn delete_file(server: &CentralServer, path: &str) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let file = tx.get_file_by_path(path)?;
    let record = tx.file_record(&file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&record.path)?;
    tx.delete_file_bundles_by_file_id(file.file_id)?;
    tx.delete_file_by_id(file.file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Deleted,
        Some((old_dir.as_str(), old_name.as_str())),
        None,
        Some(file.file_id),
        now,
    )?;
    tx.commit()?;
    Ok(ControlResponse::DeleteFile)
}

pub(super) fn delete_file_by_id(
    server: &CentralServer,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let file = tx.get_file_by_id(file_id)?;
    let record = tx.file_record(&file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&record.path)?;
    tx.delete_file_bundles_by_file_id(file_id)?;
    tx.delete_file_by_id(file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Deleted,
        Some((old_dir.as_str(), old_name.as_str())),
        None,
        Some(file_id),
        now,
    )?;
    tx.commit()?;
    Ok(ControlResponse::DeleteFileById)
}

pub(super) fn copy_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let source = tx.get_file_by_path(source_path)?;
    let file = copy_file_tx(&tx, source.file_id, target_path)?;
    tx.commit()?;
    Ok(ControlResponse::CopyFile(file))
}

pub(super) fn copy_file_by_id(
    server: &CentralServer,
    source_file_id: u64,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = copy_file_tx(&tx, source_file_id, target_path)?;
    tx.commit()?;
    Ok(ControlResponse::CopyFileById(file))
}

fn copy_file_tx(
    tx: &CentralTx<'_>,
    source_file_id: u64,
    target_path: &str,
) -> Fs0Result<FileRecord> {
    let now = now_ms();
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.copy_file_by_id(source_file_id, target_path, now)?;
    tx.copy_file_bundles(source_file_id, file.file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Created,
        None,
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file.file_id),
        now,
    )?;
    tx.file_record(&file)
}

pub(super) fn rename_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let source = tx.get_file_by_path(source_path)?;
    let file = rename_file_tx(&tx, source.file_id, target_path)?;
    tx.commit()?;
    Ok(ControlResponse::RenameFile(file))
}

pub(super) fn rename_file_by_id(
    server: &CentralServer,
    file_id: u64,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = rename_file_tx(&tx, file_id, target_path)?;
    tx.commit()?;
    Ok(ControlResponse::RenameFileById(file))
}

fn rename_file_tx(tx: &CentralTx<'_>, file_id: u64, target_path: &str) -> Fs0Result<FileRecord> {
    let now = now_ms();
    let old_file = tx.get_file_by_id(file_id)?;
    let old_record = tx.file_record(&old_file)?;
    let (old_dir, old_name) = fs0_core::utils::split_fs0_path_dir_and_name(&old_record.path)?;
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.rename_file_by_id(file_id, target_path, now)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Moved,
        Some((old_dir.as_str(), old_name.as_str())),
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file_id),
        now,
    )?;
    tx.file_record(&file)
}

pub(super) fn get_file_change_logs(
    server: &CentralServer,
    after_event_id: u64,
    limit: u32,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let logs = tx.get_file_change_logs(after_event_id, limit)?;
    tx.commit()?;
    Ok(ControlResponse::GetFileChangeLogs(logs))
}
