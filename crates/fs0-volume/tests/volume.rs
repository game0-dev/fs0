use fs0_core::{
    DEFAULT_ZSTD_LEVEL, Fs0Error, HashId, VOLUME_DATA_FILE_PREFIX, VOLUME_DB_FILE,
    VOLUME_DEFAULT_DATA_FILE_SIZE, VOLUME_RAW_CHUNK_SIZE, VOLUME_READ_CONCURRENCY,
    VOLUME_WRITE_CONCURRENCY, blake3_hash, bundle_hash_from_chunks,
    protocol::{BundleChunkRef, BundleReplicaEventKind},
    zstd_compress, zstd_decompress,
};
use fs0_volume::Volume;
use rusqlite::{Connection, params};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

fn test_volume(path: &Path, max_bytes: u64) -> Volume {
    Volume::init_fs(path, max_bytes).unwrap();
    create_sparse_data_file(path);
    Volume::open(
        path,
        VOLUME_READ_CONCURRENCY as u32,
        VOLUME_WRITE_CONCURRENCY as u32,
    )
    .unwrap()
}

fn create_sparse_data_file(path: &Path) {
    let file = File::create(path.join(format!("{VOLUME_DATA_FILE_PREFIX}0"))).unwrap();
    file.set_len(VOLUME_DEFAULT_DATA_FILE_SIZE).unwrap();
}

async fn put(volume: &Volume, raw: &[u8]) -> (HashId, u64) {
    let compressed_bytes = zstd_compress(raw, DEFAULT_ZSTD_LEVEL).unwrap();
    let chunk_id = blake3_hash(raw);
    let compressed_len = compressed_bytes.len() as u64;
    volume
        .put_chunk(chunk_id, raw.len() as u64, compressed_bytes)
        .await
        .unwrap();
    (chunk_id, compressed_len)
}

async fn read_raw(volume: &Volume, chunk_id: HashId) -> Vec<u8> {
    let (_meta, compressed_bytes) = volume.read_chunk(chunk_id).await.unwrap();
    zstd_decompress(&compressed_bytes, VOLUME_RAW_CHUNK_SIZE as usize).unwrap()
}

#[tokio::test]
async fn init_and_open_volume() {
    let temp = tempfile::tempdir().unwrap();
    Volume::init_fs(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE).unwrap();

    let volume = Volume::open(
        temp.path(),
        VOLUME_READ_CONCURRENCY as u32,
        VOLUME_WRITE_CONCURRENCY as u32,
    )
    .unwrap();

    assert_eq!(volume.meta().volume_id, 0);
    assert_eq!(volume.meta().active_volume_offset, 0);

    drop(volume);

    Volume::init_volume_id(temp.path(), 42).unwrap();

    let reopened = Volume::open(
        temp.path(),
        VOLUME_READ_CONCURRENCY as u32,
        VOLUME_WRITE_CONCURRENCY as u32,
    )
    .unwrap();
    assert_eq!(reopened.meta().volume_id, 42);
}

#[tokio::test]
async fn constants_and_options_use_expected_sizes() {
    let max_bytes = 2 * VOLUME_DEFAULT_DATA_FILE_SIZE;

    assert_eq!(VOLUME_RAW_CHUNK_SIZE, 1024 * 1024);
    assert_eq!(VOLUME_DEFAULT_DATA_FILE_SIZE, 4 * 1024 * 1024 * 1024);
    assert_eq!(max_bytes, 2 * VOLUME_DEFAULT_DATA_FILE_SIZE);
}

#[tokio::test]
async fn put_chunk_and_read_it_back() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let raw = b"abcdefghijklmnopqrstuvwxyz012345";
    let (chunk_id, compressed_len) = put(&volume, raw).await;

    let meta = volume.chunk_meta(chunk_id).await.unwrap();
    assert_eq!(meta.chunk_id, chunk_id);
    assert_eq!(meta.raw_len, raw.len() as u64);
    assert_eq!(meta.compressed_len, compressed_len);
    assert_eq!(read_raw(&volume, chunk_id).await, raw);
}

#[tokio::test]
async fn read_chunk_returns_metadata_and_compressed_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let raw = b"read with metadata";
    let (chunk_id, compressed_len) = put(&volume, raw).await;

    let (meta, compressed_bytes) = volume.read_chunk(chunk_id).await.unwrap();

    assert_eq!(meta.chunk_id, chunk_id);
    assert_eq!(meta.raw_len, raw.len() as u64);
    assert_eq!(meta.compressed_len, compressed_len);
    assert_eq!(
        zstd_decompress(&compressed_bytes, VOLUME_RAW_CHUNK_SIZE as usize).unwrap(),
        raw
    );
}

#[tokio::test]
async fn chunk_meta_returns_requested_chunk_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let raw_a = vec![b'a'; VOLUME_RAW_CHUNK_SIZE as usize];
    let raw_b = vec![b'b'; VOLUME_RAW_CHUNK_SIZE as usize];

    let (_a, _) = put(&volume, &raw_a).await;
    let (b, _) = put(&volume, &raw_b).await;

    let chunk = volume.chunk_meta(b).await.unwrap();
    assert_eq!(chunk.chunk_id, b);
    assert_eq!(read_raw(&volume, b).await, raw_b);
}

#[tokio::test]
async fn duplicate_chunk_reuses_existing_storage() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);

    let (chunk_id, _) = put(&volume, b"hello").await;
    let offset = volume.meta().active_volume_offset;
    let (same_chunk_id, _) = put(&volume, b"hello").await;

    assert_eq!(chunk_id, same_chunk_id);
    assert_eq!(volume.meta().active_volume_offset, offset);
}

#[tokio::test]
async fn init_rejects_volume_smaller_than_one_data_file() {
    let temp = tempfile::tempdir().unwrap();

    let result = Volume::init_fs(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE - 1);

    assert!(matches!(result, Err(Fs0Error::InvalidConfig { .. })));
}

#[tokio::test]
async fn same_raw_chunk_with_different_compression_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let raw = b"same raw chunk across compression levels".repeat(4096);
    let chunk_id = blake3_hash(&raw);
    let fast = zstd_compress(&raw, 1).unwrap();
    let mut different_compressed_bytes = fast.clone();
    *different_compressed_bytes.last_mut().unwrap() ^= 1;

    let first = volume
        .put_chunk(chunk_id, raw.len() as u64, fast)
        .await
        .unwrap();
    let offset = volume.meta().active_volume_offset;
    let result = volume
        .put_chunk(chunk_id, raw.len() as u64, different_compressed_bytes)
        .await;

    assert!(matches!(
        result,
        Err(Fs0Error::HashMismatch { volume_offset }) if volume_offset == first.volume_offset
    ));
    assert_eq!(volume.meta().active_volume_offset, offset);
}

#[tokio::test]
async fn delete_removes_chunk_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);

    let (chunk_id, _) = put(&volume, b"delete me").await;
    volume.delete_chunk(chunk_id).await.unwrap();

    assert!(matches!(
        volume.chunk_meta(chunk_id).await,
        Err(Fs0Error::ChunkNotFound { chunk_id: missing }) if missing == chunk_id
    ));
}

#[tokio::test]
async fn read_chunk_does_not_hash_check_stored_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);

    let (chunk_id, _) = put(&volume, b"detect corruption").await;

    let mut file = OpenOptions::new()
        .write(true)
        .open(temp.path().join(format!("{VOLUME_DATA_FILE_PREFIX}0")))
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[9]).unwrap();
    file.sync_data().unwrap();

    let (_meta, read) = volume.read_chunk(chunk_id).await.unwrap();
    assert_eq!(read[0], 9);
}

#[tokio::test]
async fn commit_bundle_rejects_bundle_hash_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let (chunk_id, _) = put(&volume, b"bundle hash").await;
    let chunks = vec![BundleChunkRef {
        chunk_index: 0,
        chunk_id,
    }];

    let result = volume.commit_bundle(HashId([9; 32]), chunks).await;

    assert!(matches!(result, Err(Fs0Error::InvalidData { .. })));
}

#[tokio::test]
async fn commit_bundle_rejects_missing_chunk() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let missing = HashId([7; 32]);
    let chunks = vec![BundleChunkRef {
        chunk_index: 0,
        chunk_id: missing,
    }];
    let bundle_id = bundle_hash_from_chunks(&chunks);

    let result = volume.commit_bundle(bundle_id, chunks).await;

    assert!(matches!(
        result,
        Err(Fs0Error::ChunkNotFound { chunk_id }) if chunk_id == missing
    ));
}

#[tokio::test]
async fn bundle_change_records_are_inserted_and_removed() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let (chunk_id, compressed_len) = put(&volume, b"bundle changes").await;
    let chunks = vec![BundleChunkRef {
        chunk_index: 0,
        chunk_id,
    }];
    let bundle_id = bundle_hash_from_chunks(&chunks);
    volume.commit_bundle(bundle_id, chunks).await.unwrap();

    let records = volume.get_bundle_change_records(10).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, BundleReplicaEventKind::Stored);
    assert_eq!(records[0].bundle_id, bundle_id);
    assert_eq!(records[0].raw_len, Some(b"bundle changes".len() as u64));
    assert_eq!(records[0].compressed_len, Some(compressed_len));

    volume
        .remove_bundle_change_records(records[0].event_id)
        .await
        .unwrap();
    assert!(
        volume
            .get_bundle_change_records(10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn commit_bundle_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let (chunk_id, _) = put(&volume, b"idempotent bundle").await;
    let chunks = vec![BundleChunkRef {
        chunk_index: 0,
        chunk_id,
    }];
    let bundle_id = bundle_hash_from_chunks(&chunks);

    let first = volume
        .commit_bundle(bundle_id, chunks.clone())
        .await
        .unwrap();
    let second = volume.commit_bundle(bundle_id, chunks).await.unwrap();
    let records = volume.get_bundle_change_records(10).await.unwrap();

    assert_eq!(second, first);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, BundleReplicaEventKind::Stored);
    assert_eq!(records[0].bundle_id, bundle_id);
}

#[tokio::test]
async fn db_row_conversion_errors_are_sqlite_errors() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let (chunk_id, _) = put(&volume, b"bad db row").await;

    let conn = Connection::open(temp.path().join(VOLUME_DB_FILE)).unwrap();
    conn.execute(
        "UPDATE chunks SET raw_len = -1 WHERE chunk_id = ?1",
        params![chunk_id.as_bytes().as_slice()],
    )
    .unwrap();

    assert!(matches!(
        volume.chunk_meta(chunk_id).await,
        Err(Fs0Error::Sqlite { .. })
    ));

    conn.execute("PRAGMA ignore_check_constraints = ON", [])
        .unwrap();
    conn.execute(
        "UPDATE chunks SET raw_len = 1, compressed_hash = x'00' WHERE chunk_id = ?1",
        params![chunk_id.as_bytes().as_slice()],
    )
    .unwrap();

    assert!(matches!(
        volume.chunk_meta(chunk_id).await,
        Err(Fs0Error::Sqlite { .. })
    ));
}
