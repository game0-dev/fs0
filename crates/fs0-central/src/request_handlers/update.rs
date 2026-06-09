use crate::{
    Fs0Error, Fs0Result,
    db::{CentralTx, CreateUpdateLease},
    server::CentralServer,
};
use fs0_core::{
    UPDATE_LEASE_TTL_MS, UPDATE_VOLUME_USAGE_THRESHOLD, VOLUME_BUNDLE_RAW_SIZE,
    protocol::{
        BeginUpdateRequest, CommitUpdateRequest, CommittedBundle, ControlRequest, ControlResponse,
        FileBundleRef, FileChangeLogKind, FileReadPlan, GrantUploadLeaseRequest, ProtocolRequest,
        ProtocolResponse, StorageVolumeInfo, UpdateLease,
    },
    utils::now_ms,
};
use std::collections::HashMap;

pub(super) async fn begin_update(
    server: &CentralServer,
    request: BeginUpdateRequest,
) -> Fs0Result<ControlResponse> {
    let volume_id = select_update_volume(
        server,
        request.prefer_volume_name.as_deref(),
        request.update_size_hint,
    )?;
    let storage_id = server
        .online_volumes
        .read()
        .get(&volume_id)
        .copied()
        .ok_or(Fs0Error::NotFound)?;
    let lease = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let now = now_ms();
        let expires_at_ms = now + UPDATE_LEASE_TTL_MS;
        let (file, base_size) = match tx.get_file_by_path(&request.path) {
            Ok(file) => {
                if request.offset > file.size_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
                let base_size = file.size_bytes;
                (file, base_size)
            }
            Err(Fs0Error::NotFound) => {
                if request.offset != 0 {
                    return Err(Fs0Error::NotFound);
                }
                let file = tx.create_file(&request.path, now)?;
                (file, 0)
            }
            Err(err) => return Err(err),
        };
        tx.delete_expired_update_leases(now)?;
        if tx.file_has_active_update_lease(file.file_id)? {
            return Err(Fs0Error::AlreadyExists { path: request.path });
        }
        let lease = tx.create_update_lease(CreateUpdateLease {
            file_id: file.file_id,
            volume_id,
            base_size_bytes: base_size,
            offset_bytes: request.offset,
            prefer_volume_name: request.prefer_volume_name,
            expires_at_ms,
            created_at_ms: now,
        })?;
        tx.commit()?;
        lease
    };

    match grant_upload_lease_to_specific_storage(server, storage_id, &lease).await {
        Ok(()) => Ok(ControlResponse::BeginUpdate(lease)),
        Err(err) => {
            let _ = abort_update_db_only(server, lease.lease_id, lease.file_id);
            Err(err)
        }
    }
}

pub(super) async fn commit_update(
    server: &CentralServer,
    request: CommitUpdateRequest,
) -> Fs0Result<ControlResponse> {
    let lease_id = request.lease_id;
    let file_id = request.file_id;
    let storage_id = storage_id_for_update_lease(server, lease_id, file_id).ok();
    let plan = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let plan = commit_update_tx(&tx, request)?;
        tx.commit()?;
        plan
    };
    let result = super::client::hydrate_read_plan_replicas(server, plan);

    if let Some(storage_id) = storage_id {
        revoke_storage_upload_lease(server, storage_id, lease_id).await;
    }

    result.map(ControlResponse::CommitUpdate)
}

pub(super) async fn abort_update(
    server: &CentralServer,
    lease_id: u64,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let storage_id = storage_id_for_update_lease(server, lease_id, file_id).ok();
    abort_update_db_only(server, lease_id, file_id)?;
    if let Some(storage_id) = storage_id {
        revoke_storage_upload_lease(server, storage_id, lease_id).await;
    }

    Ok(ControlResponse::AbortUpdate)
}

fn select_update_volume(
    server: &CentralServer,
    prefer_volume_name: Option<&str>,
    update_size_hint: Option<u64>,
) -> Fs0Result<u64> {
    let storages = server.storage_peers_snapshot();

    if let Some(name) = prefer_volume_name {
        for peer in &storages {
            if let Some(volume) = peer.volumes.iter().find(|volume| volume.name == name)
                && volume_accepts_update(volume, update_size_hint)
            {
                return Ok(volume.volume_id);
            }
        }

        return Err(Fs0Error::InvalidRequest);
    }

    storages
        .iter()
        .flat_map(|peer| peer.volumes.iter())
        .filter(|volume| volume_accepts_update(volume, update_size_hint))
        .min_by(|left, right| {
            let left_used = u128::from(left.max_volume_offset) * u128::from(right.max_bytes);
            let right_used = u128::from(right.max_volume_offset) * u128::from(left.max_bytes);

            left_used
                .cmp(&right_used)
                .then_with(|| left.volume_id.cmp(&right.volume_id))
        })
        .map(|volume| volume.volume_id)
        .ok_or(Fs0Error::NotFound)
}

fn volume_accepts_update(volume: &StorageVolumeInfo, update_size_hint: Option<u64>) -> bool {
    if volume.read_only || volume.max_bytes == 0 {
        return false;
    }

    if volume.max_volume_offset as f64 / volume.max_bytes as f64 >= UPDATE_VOLUME_USAGE_THRESHOLD {
        return false;
    }

    if let Some(size) = update_size_hint
        && volume.max_bytes.saturating_sub(volume.max_volume_offset) < size
    {
        return false;
    }

    true
}

fn abort_update_db_only(server: &CentralServer, lease_id: u64, file_id: u64) -> Fs0Result<()> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    tx.load_active_update_lease(lease_id, file_id)?;
    tx.delete_update_lease(lease_id)?;
    tx.commit()
}

async fn grant_upload_lease_to_specific_storage(
    server: &CentralServer,
    storage_id: u64,
    lease: &UpdateLease,
) -> Fs0Result<()> {
    let connection = server
        .storages
        .read()
        .get(&storage_id)
        .map(|storage| storage.connection.clone())
        .ok_or(Fs0Error::NotFound)?;
    let request = ControlRequest::GrantUploadLease(GrantUploadLeaseRequest {
        lease_id: lease.lease_id,
        file_id: lease.file_id,
        volume_id: lease.volume_id,
        base_size: lease.base_size,
        expires_at_ms: lease.expires_at_ms,
        prefer_volume_name: lease.prefer_volume_name.clone(),
    });

    match connection.rpc(ProtocolRequest::Control(request)).await? {
        ProtocolResponse::Control(ControlResponse::GrantUploadLease { lease_id })
            if lease_id == lease.lease_id =>
        {
            Ok(())
        }
        ProtocolResponse::Control(ControlResponse::Error(err)) | ProtocolResponse::Error(err) => {
            Err(err)
        }
        response => Err(Fs0Error::InvalidFrame {
            message: format!("unexpected grant upload lease response: {response:?}"),
        }),
    }
}

async fn revoke_storage_upload_lease(server: &CentralServer, storage_id: u64, lease_id: u64) {
    let connection = server
        .storages
        .read()
        .get(&storage_id)
        .map(|storage| storage.connection.clone());
    let Some(connection) = connection else {
        return;
    };

    let _: Fs0Result<ProtocolResponse> = connection
        .rpc(ProtocolRequest::Control(
            ControlRequest::RevokeUploadLease { lease_id },
        ))
        .await;
}

fn storage_id_for_update_lease(
    server: &CentralServer,
    lease_id: u64,
    file_id: u64,
) -> Fs0Result<u64> {
    let volume_id = active_update_lease_volume_db(server, lease_id, file_id)?;
    server
        .online_volumes
        .read()
        .get(&volume_id)
        .copied()
        .ok_or(Fs0Error::NotFound)
}

fn active_update_lease_volume_db(
    server: &CentralServer,
    lease_id: u64,
    file_id: u64,
) -> Fs0Result<u64> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let volume_id = tx.active_update_lease_volume(lease_id, file_id)?;
    tx.commit()?;
    Ok(volume_id)
}

fn commit_update_tx(tx: &CentralTx<'_>, request: CommitUpdateRequest) -> Fs0Result<FileReadPlan> {
    let now = now_ms();
    let lease = tx.load_active_update_lease(request.lease_id, request.file_id)?;
    validate_update_base(&lease, request.base_size, request.new_size)?;

    let file = tx.get_file_by_id(lease.file_id)?;
    if file.size_bytes != request.base_size {
        return Err(file_version_conflict());
    }

    let first_bundle_index = lease.offset_bytes / VOLUME_BUNDLE_RAW_SIZE;
    let first_bundle_index_usize =
        usize::try_from(first_bundle_index).map_err(|_| Fs0Error::IntegerConversion {
            message: format!("first_bundle_index {first_bundle_index} exceeds usize"),
        })?;
    let existing_file_bundles = tx.get_file_bundles_by_file_id(lease.file_id)?;
    let prefix_file_bundles = existing_file_bundles
        .get(..first_bundle_index_usize)
        .ok_or(Fs0Error::ChunkNotReady)?;
    let prefix_bundle_ids = prefix_file_bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let prefix_bundle_lengths = tx
        .get_uniq_bundles_by_ids(&prefix_bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    let mut prefix_bundles = Vec::with_capacity(prefix_file_bundles.len());
    for file_bundle in prefix_file_bundles {
        let bundle = prefix_bundle_lengths
            .get(&file_bundle.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        prefix_bundles.push(bundle.clone());
    }
    let (prefix_raw_size_bytes, prefix_compressed_size_bytes) =
        submitted_bundle_totals(&prefix_bundles)?;
    let (submitted_raw_size_bytes, _) = submitted_bundle_totals(&request.bundles)?;
    let bundles_to_insert = if submitted_raw_size_bytes == request.new_size {
        let submitted_prefix = request
            .bundles
            .get(..first_bundle_index_usize)
            .ok_or(Fs0Error::InvalidRequest)?;
        let (submitted_prefix_raw, submitted_prefix_compressed) =
            submitted_bundle_totals(submitted_prefix)?;
        if submitted_prefix_raw != prefix_raw_size_bytes
            || submitted_prefix_compressed != prefix_compressed_size_bytes
        {
            return Err(Fs0Error::InvalidRequest);
        }

        request
            .bundles
            .get(first_bundle_index_usize..)
            .ok_or(Fs0Error::InvalidRequest)?
    } else {
        let suffix_size_bytes = prefix_raw_size_bytes
            .checked_add(submitted_raw_size_bytes)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "committed bundle raw size overflow".to_owned(),
            })?;
        if suffix_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        request.bundles.as_slice()
    };

    validate_submitted_bundles_ready(tx, bundles_to_insert)?;
    let mut new_file_bundles = prefix_bundles;
    new_file_bundles.extend(bundles_to_insert.iter().cloned());
    tx.upsert_file_bundles_by_file_id(lease.file_id, &new_file_bundles)?;

    let (final_raw_size_bytes, final_compressed_size_bytes) =
        tx.calculate_size_by_file_id(lease.file_id)?;
    if final_raw_size_bytes != request.new_size {
        return Err(Fs0Error::InvalidRequest);
    }

    tx.update_file_after_update(
        lease.file_id,
        request.new_size,
        final_compressed_size_bytes,
        now,
    )?;
    tx.delete_update_lease(request.lease_id)?;
    let file_dir = tx.get_dir_path_by_id(file.dir_id)?;
    tx.insert_file_change_log(
        if file.size_bytes == 0 {
            FileChangeLogKind::Created
        } else {
            FileChangeLogKind::Updated
        },
        None,
        Some((file_dir.as_str(), file.name.as_str())),
        Some(lease.file_id),
        now,
    )?;
    get_file_read_plan_tx(tx, lease.file_id)
}

pub(super) fn get_file_read_plan_tx(tx: &CentralTx<'_>, file_id: u64) -> Fs0Result<FileReadPlan> {
    let file = tx.get_file_by_id(file_id)?;
    let record = tx.file_record(&file)?;
    let file_bundles = tx.get_file_bundles_by_file_id(file_id)?;
    let bundle_ids = file_bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let ready_bundles = tx
        .get_uniq_bundles_by_ids(&bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    let mut bundles = Vec::with_capacity(file_bundles.len());
    for file_bundle in file_bundles {
        let bundle = ready_bundles
            .get(&file_bundle.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        bundles.push(FileBundleRef {
            bundle_index: file_bundle.bundle_index,
            raw_len: bundle.raw_len,
            compressed_len: bundle.compressed_len,
            bundle_id: file_bundle.bundle_id,
            replicas: Vec::new(),
        });
    }

    Ok(FileReadPlan {
        file_id: record.file_id,
        path: record.path,
        size: record.size_bytes,
        bundles,
    })
}

fn validate_submitted_bundles_ready(
    tx: &CentralTx<'_>,
    bundles: &[CommittedBundle],
) -> Fs0Result<()> {
    let bundle_ids = bundles
        .iter()
        .map(|bundle| bundle.bundle_id)
        .collect::<Vec<_>>();
    let ready_bundles = tx
        .get_uniq_bundles_by_ids(&bundle_ids)?
        .into_iter()
        .map(|bundle| (bundle.bundle_id, bundle))
        .collect::<HashMap<_, _>>();
    for submitted in bundles {
        let ready = ready_bundles
            .get(&submitted.bundle_id)
            .ok_or(Fs0Error::ChunkNotReady)?;
        if ready.raw_len != submitted.raw_len || ready.compressed_len != submitted.compressed_len {
            return Err(Fs0Error::InvalidRequest);
        }
    }

    Ok(())
}

fn submitted_bundle_totals(bundles: &[CommittedBundle]) -> Fs0Result<(u64, u64)> {
    let mut raw_size_bytes = 0u64;
    let mut compressed_size_bytes = 0u64;
    for bundle in bundles {
        raw_size_bytes = raw_size_bytes.checked_add(bundle.raw_len).ok_or_else(|| {
            Fs0Error::IntegerConversion {
                message: "submitted bundle raw size overflow".to_owned(),
            }
        })?;
        compressed_size_bytes = compressed_size_bytes
            .checked_add(bundle.compressed_len)
            .ok_or_else(|| Fs0Error::IntegerConversion {
                message: "submitted bundle compressed size overflow".to_owned(),
            })?;
    }

    Ok((raw_size_bytes, compressed_size_bytes))
}

fn validate_update_base(
    lease: &crate::db::LeaseRecord,
    base_size: u64,
    new_size: u64,
) -> Fs0Result<()> {
    if lease.base_size_bytes != base_size {
        return Err(file_version_conflict());
    }
    if new_size < lease.offset_bytes {
        return Err(Fs0Error::InvalidRequest);
    }

    Ok(())
}

fn file_version_conflict() -> Fs0Error {
    Fs0Error::VersionConflict {
        message: "file changed while update lease was active".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::CentralDb;
    use fs0_core::{HashId, protocol::CommittedBundle};

    fn open_test_db() -> CentralDb {
        CentralDb::open(":memory:").unwrap()
    }

    fn bundle_id(byte: u8) -> HashId {
        HashId::new([byte; 32])
    }

    fn committed_bundle(byte: u8, raw_len: u64, compressed_len: u64) -> CommittedBundle {
        CommittedBundle {
            bundle_id: bundle_id(byte),
            raw_len,
            compressed_len,
        }
    }

    fn create_volume(db: &mut CentralDb, name: &str) -> u64 {
        let tx = db.tx().unwrap();
        let volume_id = tx
            .create_volume(name.to_owned(), i64::MAX as u64)
            .unwrap()
            .volume_id;
        tx.commit().unwrap();
        volume_id
    }

    fn begin_update(db: &mut CentralDb, volume_id: u64, path: &str, offset: u64) -> UpdateLease {
        let tx = db.tx().unwrap();
        let now = now_ms();
        let expires_at_ms = now + UPDATE_LEASE_TTL_MS;
        let (file, base_size) = match tx.get_file_by_path(path) {
            Ok(file) => {
                let base_size = file.size_bytes;
                (file, base_size)
            }
            Err(Fs0Error::NotFound) => {
                assert_eq!(offset, 0);
                (tx.create_file(path, now).unwrap(), 0)
            }
            Err(err) => panic!("unexpected begin update error {err:?}"),
        };
        tx.delete_expired_update_leases(now).unwrap();
        assert!(!tx.file_has_active_update_lease(file.file_id).unwrap());
        let lease = tx
            .create_update_lease(CreateUpdateLease {
                file_id: file.file_id,
                volume_id,
                base_size_bytes: base_size,
                offset_bytes: offset,
                prefer_volume_name: None,
                expires_at_ms,
                created_at_ms: now,
            })
            .unwrap();
        tx.commit().unwrap();
        lease
    }

    fn commit_update(
        db: &mut CentralDb,
        lease: &UpdateLease,
        new_size: u64,
        bundles: Vec<CommittedBundle>,
    ) -> Fs0Result<FileReadPlan> {
        let tx = db.tx()?;
        let plan = commit_update_tx(
            &tx,
            CommitUpdateRequest {
                lease_id: lease.lease_id,
                file_id: lease.file_id,
                base_size: lease.base_size,
                new_size,
                bundles,
            },
        )?;
        tx.commit()?;
        Ok(plan)
    }

    fn record_bundle(
        db: &mut CentralDb,
        volume_id: u64,
        byte: u8,
        raw_len: u64,
        compressed_len: u64,
    ) {
        let tx = db.tx().unwrap();
        tx.insert_bundle_replica(bundle_id(byte), volume_id, raw_len, compressed_len)
            .unwrap();
        tx.commit().unwrap();
    }

    fn file_by_path(db: &mut CentralDb, path: &str) -> fs0_core::protocol::FileRecord {
        let tx = db.tx().unwrap();
        let file = tx.get_file_by_path(path).unwrap();
        let record = tx.file_record(&file).unwrap();
        tx.commit().unwrap();
        record
    }

    fn remove_bundle_replicas(db: &mut CentralDb, volume_id: u64, byte: u8) {
        let tx = db.tx().unwrap();
        tx.delete_bundle_replica(bundle_id(byte), volume_id)
            .unwrap();
        tx.commit().unwrap();
    }

    fn insert_conflicting_replica(db: &mut CentralDb, volume_id: u64, byte: u8) {
        let tx = db.tx().unwrap();
        tx.insert_bundle_replica(bundle_id(byte), volume_id, 11, 5)
            .unwrap();
        tx.commit().unwrap();
    }

    fn assert_error<T>(result: Fs0Result<T>, expected: Fs0Error) {
        match result {
            Ok(_) => panic!("expected error {expected:?}"),
            Err(err) => assert_eq!(err, expected),
        }
    }

    fn assert_plan_bundles(plan: &FileReadPlan, expected: &[(u64, u8, u64, u64)]) {
        let actual = plan
            .bundles
            .iter()
            .map(|bundle| {
                (
                    bundle.bundle_index,
                    bundle.bundle_id.as_bytes()[0],
                    bundle.raw_len,
                    bundle.compressed_len,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    fn seed_two_bundle_file(db: &mut CentralDb, volume_id: u64) -> FileReadPlan {
        record_bundle(db, volume_id, 1, VOLUME_BUNDLE_RAW_SIZE, 11);
        record_bundle(db, volume_id, 2, 40, 7);
        let lease = begin_update(db, volume_id, "/file.bin", 0);

        commit_update(
            db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 40,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(2, 40, 7),
            ],
        )
        .unwrap()
    }

    #[test]
    fn commit_update_accepts_suffix_bundles_from_first_bundle_index() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        let original = seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_update(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_update(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![committed_bundle(3, 50, 9)],
        )
        .unwrap();
        let file = file_by_path(&mut db, "/file.bin");

        assert_eq!(lease.base_size, original.size);
        assert_eq!(plan.size, VOLUME_BUNDLE_RAW_SIZE + 50);
        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_update_accepts_full_file_bundles_and_skips_existing_prefix() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_update(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let plan = commit_update(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(3, 50, 9),
            ],
        )
        .unwrap();
        let file = file_by_path(&mut db, "/file.bin");

        assert_eq!(file.compressed_size_bytes, 20);
        assert_plan_bundles(&plan, &[(0, 1, VOLUME_BUNDLE_RAW_SIZE, 11), (1, 3, 50, 9)]);
    }

    #[test]
    fn commit_update_rejects_raw_total_that_does_not_match_new_size() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_update(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_update(&mut db, &lease, 11, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_update_rejects_compressed_len_that_does_not_match_replica_metadata() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_update(&mut db, volume_id, "/file.bin", 0);

        assert_error(
            commit_update(&mut db, &lease, 10, vec![committed_bundle(1, 10, 6)]),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_update_rejects_missing_replica_in_preserved_prefix() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        remove_bundle_replicas(&mut db, volume_id, 1);
        let lease = begin_update(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        assert_error(
            commit_update(
                &mut db,
                &lease,
                VOLUME_BUNDLE_RAW_SIZE + 50,
                vec![committed_bundle(3, 50, 9)],
            ),
            Fs0Error::ChunkNotReady,
        );
    }

    #[test]
    fn commit_update_rejects_conflicting_replica_metadata() {
        let mut db = open_test_db();
        let primary_volume_id = create_volume(&mut db, "primary");
        let replica_volume_id = create_volume(&mut db, "replica");
        record_bundle(&mut db, primary_volume_id, 1, 10, 5);
        insert_conflicting_replica(&mut db, replica_volume_id, 1);
        let lease = begin_update(&mut db, primary_volume_id, "/file.bin", 0);

        assert_error(
            commit_update(&mut db, &lease, 10, vec![committed_bundle(1, 10, 5)]),
            Fs0Error::InvalidData {
                message: "bundle replica metadata conflict".to_owned(),
            },
        );
    }
}
