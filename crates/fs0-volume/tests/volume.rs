use fs0_core::{DEFAULT_ZSTD_LEVEL, zstd_compress, zstd_decompress};
use fs0_volume::{DATA_FILE_SIZE, RAW_CHUNK_SIZE, Volume, VolumeError};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

async fn put(volume: &Volume, file_id: u64, chunk_index: u64, raw: &[u8]) -> u64 {
    let compressed_bytes = zstd_compress(raw, DEFAULT_ZSTD_LEVEL).unwrap();
    let compressed_len = compressed_bytes.len() as u64;
    volume
        .put_chunk(file_id, chunk_index, raw.len() as u64, compressed_bytes)
        .await
        .unwrap();
    compressed_len
}

async fn upsert(volume: &Volume, file_id: u64, version: u64, size: u64, compressed: u64) {
    volume
        .commit_file(file_id, version, size, compressed)
        .await
        .unwrap();
}

async fn read_raw(volume: &Volume, file_id: u64, chunk_index: u64) -> Vec<u8> {
    let compressed_bytes = volume.read_chunk(file_id, chunk_index).await.unwrap();
    zstd_decompress(&compressed_bytes, RAW_CHUNK_SIZE as usize).unwrap()
}

#[tokio::test]
async fn init_and_open_volume() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();

    assert_eq!(volume.meta().volume_id, 42);
    assert_eq!(volume.meta().active_volume_offset, 0);

    drop(volume);

    let reopened = Volume::open(temp.path()).unwrap();
    assert_eq!(reopened.meta().volume_id, 42);
}

#[tokio::test]
async fn constants_and_options_use_expected_sizes() {
    let max_bytes = 2 * DATA_FILE_SIZE;

    assert_eq!(RAW_CHUNK_SIZE, 512 * 1024);
    assert_eq!(DATA_FILE_SIZE, 512 * 1024 * 1024);
    assert_eq!(max_bytes, 2 * DATA_FILE_SIZE);
}

#[tokio::test]
async fn put_chunk_and_read_it_back() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();
    let file_id = 100;
    let raw = b"abcdefghijklmnopqrstuvwxyz012345";
    let compressed_len = put(&volume, file_id, 0, raw).await;
    upsert(&volume, file_id, 1, raw.len() as u64, compressed_len).await;

    let file = volume.file_meta(file_id).await.unwrap();
    assert_eq!(file.version, 1);
    assert_eq!(file.size_bytes, raw.len() as u64);
    assert_eq!(file.compressed_size_bytes, compressed_len);
    assert_eq!(read_raw(&volume, file_id, 0).await, raw);
}

#[tokio::test]
async fn get_chunks_meta_returns_requested_chunk_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();
    let file_id = 101;
    let raw_a = vec![b'a'; RAW_CHUNK_SIZE as usize];
    let raw_b = vec![b'b'; RAW_CHUNK_SIZE as usize];
    let raw_c = vec![b'c'; RAW_CHUNK_SIZE as usize];

    let c1 = put(&volume, file_id, 0, &raw_a).await;
    let c2 = put(&volume, file_id, 1, &raw_b).await;
    let c3 = put(&volume, file_id, 2, &raw_c).await;
    upsert(&volume, file_id, 1, 3 * RAW_CHUNK_SIZE, c1 + c2 + c3).await;

    let chunks = volume.get_chunks_meta(file_id, vec![1]).await.unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].chunk_index, 1);
    assert_eq!(read_raw(&volume, file_id, 1).await, raw_b);
}

#[tokio::test]
async fn storage_can_replace_chunk_and_commit_new_file_meta() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();
    let file_id = 102;

    let c1 = put(&volume, file_id, 0, b"hello").await;
    upsert(&volume, file_id, 1, 5, c1).await;

    let c2 = put(&volume, file_id, 0, b"hello world").await;
    upsert(&volume, file_id, 2, 11, c2).await;

    let file = volume.file_meta(file_id).await.unwrap();
    assert_eq!(file.version, 2);
    assert_eq!(file.size_bytes, 11);
    assert_eq!(read_raw(&volume, file_id, 0).await, b"hello world");

    assert_eq!(file.compressed_size_bytes, c2);
}

#[tokio::test]
async fn capacity_limit_is_enforced_without_metadata_update() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 64).unwrap();

    let result = volume.put_chunk(104, 0, 1, vec![1; 65]).await;

    assert!(matches!(result, Err(VolumeError::CapacityExceeded { .. })));
    assert_eq!(volume.meta().active_volume_offset, 0);
    assert!(matches!(
        volume.file_meta(104).await,
        Err(VolumeError::FileNotFound(104))
    ));
}

#[tokio::test]
async fn delete_removes_file_and_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();
    let file_id = 105;

    let compressed_len = put(&volume, file_id, 0, b"delete me").await;
    upsert(&volume, file_id, 1, 9, compressed_len).await;
    volume.delete_file(file_id).await.unwrap();

    assert!(matches!(
        volume.file_meta(file_id).await,
        Err(VolumeError::FileNotFound(105))
    ));

    let read = volume.get_chunks_meta(file_id, vec![0]).await;
    assert!(read.is_err());
}

#[tokio::test]
async fn hash_mismatch_is_detected() {
    let temp = tempfile::tempdir().unwrap();
    let volume = Volume::init(temp.path(), 42, 1024 * 1024).unwrap();
    let file_id = 106;

    volume.put_chunk(file_id, 0, 1, vec![7; 16]).await.unwrap();
    upsert(&volume, file_id, 1, 1, 16).await;

    let mut file = OpenOptions::new()
        .write(true)
        .open(temp.path().join(".data.0"))
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[9]).unwrap();
    file.sync_data().unwrap();

    let read = volume.read_chunk(file_id, 0).await;
    assert!(matches!(read, Err(VolumeError::HashMismatch { .. })));
}
