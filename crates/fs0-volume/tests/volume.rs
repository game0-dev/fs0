use fs0_core::{
    DEFAULT_ZSTD_LEVEL, Fs0Error, HashId, VOLUME_DATA_FILE_PREFIX, VOLUME_DEFAULT_DATA_FILE_SIZE,
    VOLUME_RAW_CHUNK_SIZE, VOLUME_READ_CONCURRENCY, VOLUME_WRITE_CONCURRENCY, blake3_hash,
    zstd_compress, zstd_decompress,
};
use fs0_volume::Volume;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

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
    let compressed_bytes = volume.read_chunk(chunk_id).await.unwrap();
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
async fn same_raw_chunk_reuses_storage_across_compression_levels() {
    let temp = tempfile::tempdir().unwrap();
    let volume = test_volume(temp.path(), VOLUME_DEFAULT_DATA_FILE_SIZE);
    let raw = b"same raw chunk across compression levels".repeat(4096);
    let chunk_id = blake3_hash(&raw);
    let fast = zstd_compress(&raw, 1).unwrap();
    let dense = zstd_compress(&raw, 9).unwrap();

    let first = volume
        .put_chunk(chunk_id, raw.len() as u64, fast)
        .await
        .unwrap();
    let offset = volume.meta().active_volume_offset;
    let second = volume
        .put_chunk(chunk_id, raw.len() as u64, dense)
        .await
        .unwrap();

    assert_eq!(first.volume_offset, second.volume_offset);
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

    let read = volume.read_chunk(chunk_id).await.unwrap();
    assert_eq!(read[0], 9);
}
