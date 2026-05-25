use fs0_core::{
    AppendLease, BeginAppendRequest, BundleChunkRef, BundleReplicaEvent, BundleReplicaEventKind,
    BundleReplicaReport, CentralStatus, CentralStorageStatus, CentralVolumeStatus,
    CommitAppendRequest, CommittedBundle, ControlRequest, ControlResponse, DEFAULT_ZSTD_LEVEL,
    DataRequest, DataResponse, DirectoryEntries, FileBundleRef, FileChangeLog, FileChangeLogKind,
    FileChangeLogs, FileReadPlan, Fs0Error, HashId, MAX_FRAME_BODY_LEN, RegisterStorageRequest,
    ReplicaLocation, SessionMessage, StoragePeerInfo, StorageVolumeInfo, UploadLease, blake3_hash,
    bundle_hash_from_chunks, decode_frame, decode_frame_body, encode_frame, encode_frame_body,
    zstd_compress, zstd_decompress,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestPayload {
    object_id: u64,
    client_id: u64,
    chunk_id: HashId,
    name: String,
}

#[test]
fn u64_ids_postcard_roundtrip() {
    let payload = TestPayload {
        object_id: 7,
        client_id: 9,
        chunk_id: HashId([42; 32]),
        name: "payload".to_owned(),
    };

    let encoded = postcard::to_allocvec(&payload).unwrap();
    let decoded: TestPayload = postcard::from_bytes(&encoded).unwrap();

    assert_eq!(decoded, payload);
}

#[test]
fn blake3_hash_returns_hash_id() {
    let hash_id = blake3_hash(b"hello fs0");
    let expected = *blake3::hash(b"hello fs0").as_bytes();

    assert_eq!(hash_id, HashId(expected));
}

#[test]
fn bundle_hash_uses_ordered_chunk_ids() {
    let chunks = vec![
        BundleChunkRef {
            chunk_index: 0,
            chunk_id: blake3_hash(b"chunk-a"),
        },
        BundleChunkRef {
            chunk_index: 1,
            chunk_id: blake3_hash(b"chunk-b"),
        },
    ];
    let mut hasher = blake3::Hasher::new();
    hasher.update(chunks[0].chunk_id.as_bytes());
    hasher.update(chunks[1].chunk_id.as_bytes());

    assert_eq!(
        bundle_hash_from_chunks(&chunks),
        HashId(*hasher.finalize().as_bytes())
    );

    let reversed = vec![
        BundleChunkRef {
            chunk_index: 0,
            chunk_id: chunks[1].chunk_id,
        },
        BundleChunkRef {
            chunk_index: 1,
            chunk_id: chunks[0].chunk_id,
        },
    ];
    assert_ne!(
        bundle_hash_from_chunks(&chunks),
        bundle_hash_from_chunks(&reversed)
    );
}

#[test]
fn zstd_roundtrip() {
    let raw = b"orderbook:".repeat(4096);
    let compressed = zstd_compress(&raw, DEFAULT_ZSTD_LEVEL).unwrap();
    let decoded = zstd_decompress(&compressed, raw.len()).unwrap();

    assert_eq!(decoded, raw);
}

#[test]
fn frame_encode_decode_postcard_payload() {
    let payload = TestPayload {
        object_id: 11,
        client_id: 13,
        chunk_id: blake3_hash(b"/fs0/test"),
        name: "frame".to_owned(),
    };

    let encoded = encode_frame(&payload).unwrap();
    let decoded: TestPayload = decode_frame(&encoded).unwrap();

    assert_eq!(decoded, payload);
}

#[test]
fn frame_rejects_truncated_body() {
    let payload = TestPayload {
        object_id: 11,
        client_id: 13,
        chunk_id: blake3_hash(b"/fs0/test"),
        name: "frame".to_owned(),
    };

    let mut encoded = encode_frame(&payload).unwrap();
    encoded.pop();

    let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
    assert!(matches!(err, Fs0Error::InvalidFrame { .. }));
    assert!(err.to_string().contains("frame length mismatch"));
}

#[test]
fn frame_rejects_declared_body_over_limit() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&((MAX_FRAME_BODY_LEN as u32) + 1).to_le_bytes());

    let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
    assert_eq!(
        err,
        Fs0Error::FrameTooLarge {
            actual: MAX_FRAME_BODY_LEN + 1,
            max: MAX_FRAME_BODY_LEN,
        }
    );
}

#[test]
fn frame_body_roundtrip_preserves_raw_bytes() {
    let body = b"raw frame body";
    let encoded = encode_frame_body(body).unwrap();
    let decoded = decode_frame_body(&encoded).unwrap();

    assert_eq!(decoded, body);
}

#[test]
fn frame_rejects_body_over_limit_before_encoding() {
    let body = vec![0; MAX_FRAME_BODY_LEN + 1];
    let err = encode_frame_body(&body).unwrap_err();

    assert_eq!(
        err,
        Fs0Error::FrameTooLarge {
            actual: MAX_FRAME_BODY_LEN + 1,
            max: MAX_FRAME_BODY_LEN,
        }
    );
}

#[test]
fn fs0_error_roundtrips_over_postcard() {
    let chunk_id = HashId([7; 32]);
    let error = Fs0Error::ChunkNotFound { chunk_id };

    assert_postcard_roundtrip(&error);
    assert!(error.to_string().contains("was not found"));
}

#[test]
fn fs0_error_conversions_keep_messages() {
    let io_error = Fs0Error::from(std::io::Error::other("disk unavailable"));
    assert_eq!(
        io_error,
        Fs0Error::Io {
            message: "disk unavailable".to_owned(),
        }
    );

    let postcard_error = postcard::from_bytes::<TestPayload>(&[0xff]).unwrap_err();
    let fs0_error = Fs0Error::from(postcard_error);
    assert!(matches!(fs0_error, Fs0Error::Postcard { .. }));
    assert!(!fs0_error.to_string().is_empty());
}

#[test]
fn session_messages_roundtrip() {
    let storage = storage_peer();
    assert_postcard_roundtrip(&SessionMessage::Ping);
    assert_postcard_roundtrip(&SessionMessage::Pong);
    assert_postcard_roundtrip(&SessionMessage::RegisterClient {
        name: Some("client-a".to_owned()),
    });
    assert_postcard_roundtrip(&SessionMessage::ClientRegistered {
        client_id: 42,
        storages: vec![storage.clone()],
    });
    assert_postcard_roundtrip(&SessionMessage::RegisterStorage {
        request: register_storage_request(),
    });
    assert_postcard_roundtrip(&SessionMessage::StorageRegistered {
        storage_id: 7,
        storages: vec![storage.clone()],
    });
    assert_postcard_roundtrip(&SessionMessage::StorageChanged(storage));
    assert_postcard_roundtrip(&SessionMessage::StorageRemoved { storage_id: 7 });
    assert_postcard_roundtrip(&SessionMessage::Error(Fs0Error::Unsupported));
}

#[test]
fn control_requests_roundtrip() {
    assert_postcard_roundtrip(&ControlRequest::CreateVolume {
        name: "hot".to_owned(),
        max_bytes: 1024,
    });
    assert_postcard_roundtrip(&ControlRequest::GrantUploadLease(upload_lease()));
    assert_postcard_roundtrip(&ControlRequest::RevokeUploadLease { lease_id: 9 });
    assert_postcard_roundtrip(&ControlRequest::CentralStatus);
    assert_postcard_roundtrip(&ControlRequest::ListDirectory {
        dir: "/trades".to_owned(),
        limit: 100,
        cursor: Some(10),
    });
    assert_postcard_roundtrip(&ControlRequest::GetFileReadPlan {
        path: "/trades/a.parquet".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::GetFileReadPlanById { file_id: 3 });
    assert_postcard_roundtrip(&ControlRequest::DeleteFile {
        path: "/old".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::DeleteFileById { file_id: 4 });
    assert_postcard_roundtrip(&ControlRequest::CopyFile {
        source_path: "/a".to_owned(),
        target_path: "/b".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::CopyFileById {
        source_file_id: 5,
        target_path: "/copy".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::RenameFile {
        source_path: "/old".to_owned(),
        target_path: "/new".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::RenameFileById {
        file_id: 6,
        target_path: "/renamed".to_owned(),
    });
    assert_postcard_roundtrip(&ControlRequest::GetFileChangeLogs {
        after_event_id: 11,
        limit: 200,
    });
    assert_postcard_roundtrip(&ControlRequest::BeginAppend(BeginAppendRequest {
        path: "/a.txt".to_owned(),
        expected_size: 0,
        create: true,
        prefer_volume_name: Some("hot".to_owned()),
        idempotency_key: Some("append-1".to_owned()),
    }));
    assert_postcard_roundtrip(&ControlRequest::CommitAppend(CommitAppendRequest {
        lease_id: 9,
        base_size: 0,
        new_size: 512,
        bundles: vec![committed_bundle()],
    }));
    assert_postcard_roundtrip(&ControlRequest::AbortAppend { lease_id: 9 });
    assert_postcard_roundtrip(&ControlRequest::ReportBundleReplica(BundleReplicaReport {
        events: vec![bundle_replica_event()],
    }));
}

#[test]
fn control_responses_roundtrip() {
    assert_postcard_roundtrip(&ControlResponse::CreateVolume(2));
    assert_postcard_roundtrip(&ControlResponse::UploadLeaseGranted { lease_id: 9 });
    assert_postcard_roundtrip(&ControlResponse::UploadLeaseRevoked { lease_id: 9 });
    assert_postcard_roundtrip(&ControlResponse::CentralStatus(central_status()));
    assert_postcard_roundtrip(&ControlResponse::ListDirectory(DirectoryEntries {
        entries: Vec::new(),
        next_cursor: None,
    }));
    assert_postcard_roundtrip(&ControlResponse::GetFileChangeLogs(FileChangeLogs {
        operations: vec![FileChangeLog {
            event_id: 1,
            kind: FileChangeLogKind::Created,
            file_id: Some(2),
            old_path: None,
            new_path: Some("/a".to_owned()),
            created_at_ms: 1000,
        }],
        next_event_id: Some(2),
    }));
    assert_postcard_roundtrip(&ControlResponse::BeginAppend(AppendLease {
        lease_id: 9,
        file_id: 3,
        volume_id: 4,
        base_size: 0,
        expires_at_ms: 2000,
        prefer_volume_name: Some("hot".to_owned()),
    }));
    assert_postcard_roundtrip(&ControlResponse::CommitAppend(file_read_plan()));
    assert_postcard_roundtrip(&ControlResponse::AbortAppend);
    assert_postcard_roundtrip(&ControlResponse::GetFileReadPlan(file_read_plan()));
    assert_postcard_roundtrip(&ControlResponse::GetFileReadPlanById(file_read_plan()));
    assert_postcard_roundtrip(&ControlResponse::DeleteFile);
    assert_postcard_roundtrip(&ControlResponse::DeleteFileById);
    assert_postcard_roundtrip(&ControlResponse::CopyFile(file_record("/copy")));
    assert_postcard_roundtrip(&ControlResponse::CopyFileById(file_record("/copy-id")));
    assert_postcard_roundtrip(&ControlResponse::RenameFile(file_record("/renamed")));
    assert_postcard_roundtrip(&ControlResponse::RenameFileById(file_record("/renamed-id")));
    assert_postcard_roundtrip(&ControlResponse::ReportBundleReplica);
    assert_postcard_roundtrip(&ControlResponse::Error(Fs0Error::VersionConflict));
}

#[test]
fn data_protocol_roundtrip() {
    let chunk_id = HashId([1; 32]);
    let bundle_id = HashId([2; 32]);
    let chunk = BundleChunkRef {
        chunk_index: 0,
        chunk_id,
    };

    assert_postcard_roundtrip(&DataRequest::HasChunk {
        volume_id: 4,
        chunk_id,
    });
    assert_postcard_roundtrip(&DataRequest::UploadChunk {
        volume_id: 4,
        chunk_id,
        raw_len: 12,
        compressed_bytes: vec![1, 2, 3],
    });
    assert_postcard_roundtrip(&DataRequest::DownloadChunk {
        volume_id: 4,
        chunk_id,
    });
    assert_postcard_roundtrip(&DataRequest::HasBundle {
        volume_id: 4,
        bundle_id,
    });
    assert_postcard_roundtrip(&DataRequest::CommitBundle {
        volume_id: 4,
        bundle_id,
        chunks: vec![chunk.clone()],
    });
    assert_postcard_roundtrip(&DataRequest::DownloadBundle {
        volume_id: 4,
        bundle_id,
    });
    assert_postcard_roundtrip(&DataRequest::ListBundleChunks {
        volume_id: 4,
        bundle_id,
    });

    assert_postcard_roundtrip(&DataResponse::HasChunk {
        exists: true,
        raw_len: Some(12),
        compressed_len: Some(3),
    });
    assert_postcard_roundtrip(&DataResponse::UploadChunk {
        chunk_id,
        raw_len: 12,
        compressed_len: 3,
    });
    assert_postcard_roundtrip(&DataResponse::DownloadChunk {
        compressed_bytes: vec![1, 2, 3],
    });
    assert_postcard_roundtrip(&DataResponse::HasBundle {
        exists: false,
        raw_len: None,
        compressed_len: None,
    });
    assert_postcard_roundtrip(&DataResponse::CommitBundle {
        bundle_id,
        raw_len: 12,
        compressed_len: 3,
    });
    assert_postcard_roundtrip(&DataResponse::DownloadBundle {
        compressed_bytes: vec![1, 2, 3],
    });
    assert_postcard_roundtrip(&DataResponse::ListBundleChunks {
        chunks: vec![chunk],
    });
    assert_postcard_roundtrip(&DataResponse::Error(Fs0Error::UnknownVolume));
}

fn assert_postcard_roundtrip<T>(value: &T)
where
    T: Clone + Eq + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    let encoded = postcard::to_allocvec(value).unwrap();
    let decoded: T = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(&decoded, value);
}

fn storage_peer() -> StoragePeerInfo {
    StoragePeerInfo {
        storage_id: 7,
        name: "storage-a".to_owned(),
        volumes: vec![StorageVolumeInfo {
            volume_id: 4,
            name: "hot".to_owned(),
            max_bytes: 1024 * 1024,
            max_volume_offset: 512,
        }],
        iroh_endpoint: vec![1, 2, 3],
    }
}

fn register_storage_request() -> RegisterStorageRequest {
    RegisterStorageRequest {
        storage_id: 7,
        name: "storage-a".to_owned(),
        volumes: storage_peer().volumes,
        iroh_endpoint: vec![1, 2, 3],
    }
}

fn upload_lease() -> UploadLease {
    UploadLease {
        lease_id: 9,
        client_id: 10,
        file_id: 11,
        volume_id: 4,
        base_size: 0,
        expires_at_ms: 2000,
        prefer_volume_name: Some("hot".to_owned()),
    }
}

fn committed_bundle() -> CommittedBundle {
    CommittedBundle {
        bundle_index: 0,
        bundle_id: HashId([2; 32]),
        raw_len: 12,
        compressed_len: 3,
    }
}

fn bundle_replica_event() -> BundleReplicaEvent {
    BundleReplicaEvent {
        event_id: 1,
        kind: BundleReplicaEventKind::Stored,
        volume_id: 4,
        bundle_id: HashId([2; 32]),
        raw_len: Some(12),
        compressed_len: Some(3),
    }
}

fn file_read_plan() -> FileReadPlan {
    FileReadPlan {
        file_id: 3,
        path: "/a".to_owned(),
        size: 12,
        bundles: vec![FileBundleRef {
            bundle_index: 0,
            raw_len: 12,
            compressed_len: 3,
            bundle_id: HashId([2; 32]),
            replicas: vec![ReplicaLocation {
                storage_id: 7,
                volume_id: 4,
            }],
        }],
    }
}

fn file_record(path: &str) -> fs0_core::FileRecord {
    fs0_core::FileRecord {
        file_id: 3,
        path: path.to_owned(),
        size_bytes: 12,
        compressed_size_bytes: 3,
        created_at_ms: 1000,
        updated_at_ms: 2000,
    }
}

fn central_status() -> CentralStatus {
    CentralStatus {
        storages: vec![CentralStorageStatus {
            storage_id: 7,
            name: "storage-a".to_owned(),
            volumes: vec![CentralVolumeStatus {
                volume_id: 4,
                name: "hot".to_owned(),
                max_bytes: 1024,
                used_bytes: 512,
                raw_bytes: 900,
                compressed_bytes: 300,
            }],
        }],
    }
}
