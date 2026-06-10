use crate::{Fs0Error, Fs0Result, server::CentralServer};
use fs0_core::{
    protocol::{ControlResponse, FileBundleRef, FileChangeLogKind, FileReadPlan, ReplicaLocation},
    utils::now_ms,
};
use std::collections::{HashMap, HashSet};

pub(super) fn central_status(server: &CentralServer) -> Fs0Result<ControlResponse> {
    Ok(ControlResponse::CentralStatus {
        clients_count: server.clients.read().len() as u32,
        storages: server.storage_peers_snapshot(),
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
    let file_id = file_id_by_path(server, path)?;
    get_file_read_plan_by_id(server, file_id)
}

pub(super) fn get_file_read_plan_by_id(
    server: &CentralServer,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = tx.get_file_by_id(file_id)?;
    let record = tx.file_record(&file)?;
    let file_bundles = tx.get_file_bundles_by_file_id(file_id)?;

    let uniq_bundle_ids = file_bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let online_volumes = server.online_volumes.read();

    let mut bundle_replicas = HashMap::new();
    for replica in tx.get_bundle_replicas_by_id(&uniq_bundle_ids)? {
        if let Some(storage_id) = online_volumes.get(&replica.volume_id) {
            let bundle =
                bundle_replicas
                    .entry(replica.bundle_id)
                    .or_insert_with(|| BundleReadTarget {
                        raw_len: replica.raw_len,
                        compressed_len: replica.compressed_len,
                        replicas: Vec::new(),
                    });

            bundle.replicas.push(ReplicaLocation {
                storage_id: *storage_id,
                volume_id: replica.volume_id,
            });
        }
    }
    drop(online_volumes);

    let mut bundles = Vec::with_capacity(file_bundles.len());
    for file_bundle in file_bundles {
        let bundle = bundle_replicas
            .get(&file_bundle.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;

        bundles.push(FileBundleRef {
            bundle_index: file_bundle.bundle_index,
            raw_len: bundle.raw_len,
            compressed_len: bundle.compressed_len,
            bundle_id: file_bundle.bundle_id,
            replicas: bundle.replicas.clone(),
        });
    }

    Ok(ControlResponse::GetFileReadPlanById(FileReadPlan {
        file_id: record.file_id,
        path: record.path,
        size: record.size_bytes,
        bundles,
    }))
}

#[derive(Debug)]
struct BundleReadTarget {
    raw_len: u64,
    compressed_len: u64,
    replicas: Vec<ReplicaLocation>,
}

pub(super) fn delete_file(server: &CentralServer, path: &str) -> Fs0Result<ControlResponse> {
    let file_id = file_id_by_path(server, path)?;
    delete_file_by_id(server, file_id)
}

pub(super) fn delete_file_by_id(
    server: &CentralServer,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    tx.delete_file_bundles_by_file_id(file_id)?;
    tx.delete_file_by_id(file_id)?;
    tx.insert_file_change_log(FileChangeLogKind::Deleted, None, Some(file_id), now)?;
    tx.commit()?;
    Ok(ControlResponse::DeleteFileById)
}

pub(super) fn copy_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let source_file_id = file_id_by_path(server, source_path)?;
    copy_file_by_id(server, source_file_id, target_path)
}

pub(super) fn copy_file_by_id(
    server: &CentralServer,
    source_file_id: u64,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.copy_file_by_id(source_file_id, target_path, now)?;
    tx.copy_file_bundles(source_file_id, file.file_id)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Created,
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file.file_id),
        now,
    )?;
    let record = tx.file_record(&file)?;
    tx.commit()?;
    Ok(ControlResponse::CopyFileById(record))
}

pub(super) fn rename_file(
    server: &CentralServer,
    source_path: &str,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let file_id = file_id_by_path(server, source_path)?;
    rename_file_by_id(server, file_id, target_path)
}

pub(super) fn rename_file_by_id(
    server: &CentralServer,
    file_id: u64,
    target_path: &str,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let now = now_ms();
    let (target_dir, target_name) = fs0_core::utils::split_fs0_path_dir_and_name(target_path)?;
    let file = tx.rename_file_by_id(file_id, target_path, now)?;
    tx.insert_file_change_log(
        FileChangeLogKind::Moved,
        Some((target_dir.as_str(), target_name.as_str())),
        Some(file_id),
        now,
    )?;
    let record = tx.file_record(&file)?;
    tx.commit()?;
    Ok(ControlResponse::RenameFileById(record))
}

fn file_id_by_path(server: &CentralServer, path: &str) -> Fs0Result<u64> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let file = tx.get_file_by_path(path)?;
    Ok(file.file_id)
}

pub(super) fn get_file_change_logs(
    server: &CentralServer,
    after_event_id: u64,
    limit: u32,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let logs = tx.get_file_change_logs(after_event_id, limit)?;
    Ok(ControlResponse::GetFileChangeLogs(logs))
}
