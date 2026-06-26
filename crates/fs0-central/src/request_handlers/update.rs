use crate::{Fs0Error, Fs0Result, db::CreateUpdateLease, server::CentralServer};
use fs0_core::{
    UPDATE_LEASE_TTL_MS, UPDATE_VOLUME_USAGE_THRESHOLD,
    protocol::{
        BeginUpdateRequest, CommitUpdateRequest, ControlRequest, ControlResponse,
        FileChangeLogKind, GrantUploadLeaseRequest, ProtocolRequest, ProtocolResponse, UpdateLease,
    },
    utils::now_ms,
};
use std::time::Duration;

const STORAGE_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

pub(super) async fn begin_update(
    server: &CentralServer,
    request: BeginUpdateRequest,
) -> Fs0Result<ControlResponse> {
    let mut candidate_volume = None;
    let prefer_volume_name = request.prefer_volume_name.as_deref();
    'storages: for storage in server.storage_peers_snapshot() {
        for volume in storage.volumes {
            if volume.read_only || volume.max_bytes == 0 {
                continue;
            }
            if volume.max_volume_offset as f64 / volume.max_bytes as f64
                >= UPDATE_VOLUME_USAGE_THRESHOLD
            {
                continue;
            }
            if let Some(size) = request.update_size_hint
                && volume.max_bytes.saturating_sub(volume.max_volume_offset) < size
            {
                continue;
            }

            if candidate_volume.is_none() {
                candidate_volume = Some((storage.storage_id, volume.volume_id));
            }
            if prefer_volume_name.is_some_and(|name| volume.name == name) {
                candidate_volume = Some((storage.storage_id, volume.volume_id));
                break 'storages;
            }
        }
    }

    let Some((storage_id, volume_id)) = candidate_volume else {
        return Err(Fs0Error::NoAvailableVolume {
            prefer_volume_name: request.prefer_volume_name.clone(),
            update_size_hint: request.update_size_hint,
        });
    };

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
            let mut db = server.db.lock();
            let tx = db.tx()?;
            tx.delete_update_lease(lease.lease_id)?;
            tx.commit()?;
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
    let storage_id = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let volume_id = tx
            .active_update_lease_volume(lease_id, file_id)
            .map_err(|err| {
                if err == Fs0Error::NotFound {
                    Fs0Error::VersionConflict {
                        message: format!(
                            "update lease {lease_id} for file {file_id} expired before commit"
                        ),
                    }
                } else {
                    err
                }
            })?;
        tx.commit()?;
        server.online_volumes.read().get(&volume_id).copied()
    };
    let file = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let now = now_ms();
        let lease = tx
            .load_active_update_lease(request.lease_id, request.file_id)
            .map_err(|err| {
                if err == Fs0Error::NotFound {
                    Fs0Error::VersionConflict {
                        message: format!(
                            "update lease {} for file {} expired before commit",
                            request.lease_id, request.file_id
                        ),
                    }
                } else {
                    err
                }
            })?;
        if lease.base_size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict {
                message: "file changed while update lease was active".to_owned(),
            });
        }
        if request.new_size < lease.offset_bytes {
            return Err(Fs0Error::InvalidRequest);
        }

        let file = tx.get_file_by_id(lease.file_id)?;
        if file.size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict {
                message: "file changed while update lease was active".to_owned(),
            });
        }

        let bundle_ids = request
            .bundles
            .iter()
            .map(|bundle| bundle.bundle_id)
            .collect::<Vec<_>>();
        let mut ready_bundles = std::collections::HashMap::new();
        for replica in tx.get_bundle_replicas_by_id(&bundle_ids)? {
            ready_bundles
                .entry(replica.bundle_id)
                .or_insert(replica.raw_len);
        }

        let mut submitted_raw_size_bytes = 0u64;
        for bundle in &request.bundles {
            let Some(raw_len) = ready_bundles.get(&bundle.bundle_id) else {
                return Err(Fs0Error::ChunkNotReady);
            };
            if *raw_len != bundle.raw_len {
                return Err(Fs0Error::InvalidRequest);
            }

            submitted_raw_size_bytes = submitted_raw_size_bytes
                .checked_add(bundle.raw_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "submitted bundle raw size overflow".to_owned(),
                })?;
        }
        if submitted_raw_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        tx.upsert_file_bundles_by_file_id(lease.file_id, &request.bundles)?;
        let (final_raw_size_bytes, final_compressed_size_bytes) =
            tx.calculate_size_by_file_id(lease.file_id)?;
        if final_raw_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        let updated_file = tx.update_file_after_update(
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
            Some((file_dir.as_str(), file.name.as_str())),
            Some(lease.file_id),
            now,
        )?;
        let file = tx.file_record(&updated_file)?;
        tx.commit()?;
        file
    };

    if let Some(storage_id) = storage_id {
        revoke_storage_upload_lease(server, storage_id, lease_id).await;
    }

    Ok(ControlResponse::CommitUpdate(file))
}

pub(super) async fn abort_update(
    server: &CentralServer,
    lease_id: u64,
    file_id: u64,
) -> Fs0Result<ControlResponse> {
    let storage_id = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let volume_id = tx.active_update_lease_volume(lease_id, file_id)?;
        tx.delete_update_lease(lease_id)?;
        tx.commit()?;
        server.online_volumes.read().get(&volume_id).copied()
    };
    if let Some(storage_id) = storage_id {
        revoke_storage_upload_lease(server, storage_id, lease_id).await;
    }

    Ok(ControlResponse::AbortUpdate)
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

    match connection
        .rpc(
            ProtocolRequest::Control(request),
            Some(STORAGE_CONTROL_REQUEST_TIMEOUT),
        )
        .await?
    {
        ProtocolResponse::Control(ControlResponse::GrantUploadLease { lease_id })
            if lease_id == lease.lease_id =>
        {
            Ok(())
        }
        ProtocolResponse::Error(err) => Err(err),
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
        .rpc(
            ProtocolRequest::Control(ControlRequest::RevokeUploadLease { lease_id }),
            Some(STORAGE_CONTROL_REQUEST_TIMEOUT),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::CentralDb;
    use fs0_core::{
        HashId, VOLUME_BUNDLE_RAW_SIZE,
        protocol::{CommittedBundle, FileBundleRef, FileReadPlan, FileRecord},
    };
    use std::collections::HashMap;

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
    ) -> Fs0Result<FileRecord> {
        let tx = db.tx()?;
        let request = CommitUpdateRequest {
            lease_id: lease.lease_id,
            file_id: lease.file_id,
            base_size: lease.base_size,
            new_size,
            bundles,
        };
        let now = now_ms();
        let lease = tx.load_active_update_lease(request.lease_id, request.file_id)?;
        if lease.base_size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict {
                message: "file changed while update lease was active".to_owned(),
            });
        }
        if request.new_size < lease.offset_bytes {
            return Err(Fs0Error::InvalidRequest);
        }

        let current_file = tx.get_file_by_id(lease.file_id)?;
        if current_file.size_bytes != request.base_size {
            return Err(Fs0Error::VersionConflict {
                message: "file changed while update lease was active".to_owned(),
            });
        }

        let bundle_ids = request
            .bundles
            .iter()
            .map(|bundle| bundle.bundle_id)
            .collect::<Vec<_>>();
        let mut ready_bundles = HashMap::new();
        for replica in tx.get_bundle_replicas_by_id(&bundle_ids)? {
            ready_bundles
                .entry(replica.bundle_id)
                .or_insert(replica.raw_len);
        }

        let mut submitted_raw_size_bytes = 0u64;
        for bundle in &request.bundles {
            let Some(raw_len) = ready_bundles.get(&bundle.bundle_id) else {
                return Err(Fs0Error::ChunkNotReady);
            };
            if *raw_len != bundle.raw_len {
                return Err(Fs0Error::InvalidRequest);
            }

            submitted_raw_size_bytes = submitted_raw_size_bytes
                .checked_add(bundle.raw_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "submitted bundle raw size overflow".to_owned(),
                })?;
        }
        if submitted_raw_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        tx.upsert_file_bundles_by_file_id(lease.file_id, &request.bundles)?;
        let (final_raw_size_bytes, final_compressed_size_bytes) =
            tx.calculate_size_by_file_id(lease.file_id)?;
        if final_raw_size_bytes != request.new_size {
            return Err(Fs0Error::InvalidRequest);
        }

        let updated_file = tx.update_file_after_update(
            lease.file_id,
            request.new_size,
            final_compressed_size_bytes,
            now,
        )?;
        tx.delete_update_lease(request.lease_id)?;
        let file_dir = tx.get_dir_path_by_id(current_file.dir_id)?;
        tx.insert_file_change_log(
            if current_file.size_bytes == 0 {
                FileChangeLogKind::Created
            } else {
                FileChangeLogKind::Updated
            },
            Some((file_dir.as_str(), current_file.name.as_str())),
            Some(lease.file_id),
            now,
        )?;
        let file = tx.file_record(&updated_file)?;
        tx.commit()?;
        Ok(file)
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

    fn read_plan_by_file_id(db: &mut CentralDb, file_id: u64) -> FileReadPlan {
        let tx = db.tx().unwrap();
        let file = tx.get_file_by_id(file_id).unwrap();
        let record = tx.file_record(&file).unwrap();
        let file_bundles = tx.get_file_bundles_by_file_id(file_id).unwrap();
        let bundle_ids = file_bundles
            .iter()
            .map(|bundle| bundle.bundle_id)
            .collect::<Vec<_>>();
        let mut ready_bundles = HashMap::new();
        for replica in tx.get_bundle_replicas_by_id(&bundle_ids).unwrap() {
            ready_bundles
                .entry(replica.bundle_id)
                .or_insert((replica.raw_len, replica.compressed_len));
        }
        let bundles = file_bundles
            .into_iter()
            .map(|file_bundle| {
                let (raw_len, compressed_len) =
                    ready_bundles.get(&file_bundle.bundle_id).copied().unwrap();
                FileBundleRef {
                    bundle_index: file_bundle.bundle_index,
                    raw_len,
                    compressed_len,
                    bundle_id: file_bundle.bundle_id,
                    replicas: Vec::new(),
                }
            })
            .collect();
        let plan = FileReadPlan {
            file_id: record.file_id,
            path: record.path,
            size: record.size_bytes,
            bundles,
        };
        tx.commit().unwrap();
        plan
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

    fn seed_two_bundle_file(db: &mut CentralDb, volume_id: u64) -> FileRecord {
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
    fn commit_update_rejects_suffix_only_bundle_list() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_update(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        assert_error(
            commit_update(
                &mut db,
                &lease,
                VOLUME_BUNDLE_RAW_SIZE + 50,
                vec![committed_bundle(3, 50, 9)],
            ),
            Fs0Error::InvalidRequest,
        );
    }

    #[test]
    fn commit_update_accepts_full_file_bundles_and_skips_existing_prefix() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        seed_two_bundle_file(&mut db, volume_id);
        record_bundle(&mut db, volume_id, 3, 50, 9);
        let lease = begin_update(&mut db, volume_id, "/file.bin", VOLUME_BUNDLE_RAW_SIZE);

        let file = commit_update(
            &mut db,
            &lease,
            VOLUME_BUNDLE_RAW_SIZE + 50,
            vec![
                committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                committed_bundle(3, 50, 9),
            ],
        )
        .unwrap();
        let plan = read_plan_by_file_id(&mut db, file.file_id);

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
    fn commit_update_ignores_submitted_compressed_len() {
        let mut db = open_test_db();
        let volume_id = create_volume(&mut db, "primary");
        record_bundle(&mut db, volume_id, 1, 10, 5);
        let lease = begin_update(&mut db, volume_id, "/file.bin", 0);

        let file = commit_update(&mut db, &lease, 10, vec![committed_bundle(1, 10, 6)]).unwrap();

        assert_eq!(file.compressed_size_bytes, 5);
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
                vec![
                    committed_bundle(1, VOLUME_BUNDLE_RAW_SIZE, 11),
                    committed_bundle(3, 50, 9),
                ],
            ),
            Fs0Error::ChunkNotReady,
        );
    }

    #[test]
    fn commit_update_uses_first_replica_metadata_for_duplicate_bundle() {
        let mut db = open_test_db();
        let primary_volume_id = create_volume(&mut db, "primary");
        let replica_volume_id = create_volume(&mut db, "replica");
        record_bundle(&mut db, primary_volume_id, 1, 10, 5);
        insert_conflicting_replica(&mut db, replica_volume_id, 1);
        let lease = begin_update(&mut db, primary_volume_id, "/file.bin", 0);

        let file = commit_update(&mut db, &lease, 10, vec![committed_bundle(1, 10, 5)]).unwrap();
        let plan = read_plan_by_file_id(&mut db, file.file_id);

        assert_plan_bundles(&plan, &[(0, 1, 10, 5)]);
    }
}
