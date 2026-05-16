use fs0_core::{ChunkId, DEFAULT_ZSTD_LEVEL, blake3_hash, zstd_compress};
use fs0_storage::{
    StorageConfig, StorageError, StorageNode, StorageP2pRelayConfig, StorageVolumeConfig,
};
use fs0_volume::Volume;
use std::path::Path;

fn init_volume(path: &Path, volume_id: u64, max_bytes: u64) {
    Volume::init(path, volume_id, max_bytes).unwrap();
}

fn config(volumes: Vec<StorageVolumeConfig>) -> StorageConfig {
    StorageConfig {
        storage_id: 1,
        name: "storage-a".to_owned(),
        central_endpoint: Vec::new(),
        cert: "/tmp/fs0-cert.pem".into(),
        p2p_relay: StorageP2pRelayConfig {
            port: 3340,
            quic_port: 7824,
            public_url: "http://127.0.0.1:3340".to_owned(),
        },
        volumes,
    }
}

fn compressed(raw: &[u8]) -> Vec<u8> {
    zstd_compress(raw, DEFAULT_ZSTD_LEVEL).unwrap()
}

async fn put(node: &StorageNode, volume_id: u64, raw: &[u8]) -> (ChunkId, u64) {
    let bytes = compressed(raw);
    let chunk_id = blake3_hash(&bytes);
    let meta = node
        .put_chunk(volume_id, chunk_id, raw.len() as u64, bytes)
        .await
        .unwrap();
    (chunk_id, meta.volume_offset)
}

#[tokio::test]
async fn opens_multiple_volumes_and_routes_by_id() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    let v2 = temp.path().join("v2");
    init_volume(&v1, 1, 1024 * 1024);
    init_volume(&v2, 2, 1024 * 1024);

    let node = StorageNode::open(config(vec![
        StorageVolumeConfig {
            path: v1,
            volume_id: 1,
        },
        StorageVolumeConfig {
            path: v2,
            volume_id: 2,
        },
    ]))
    .unwrap();

    let volumes = node.volumes();
    assert_eq!(volumes.len(), 2);
    assert_eq!(volumes[0].volume_id, 1);
    assert_eq!(volumes[1].volume_id, 2);

    let raw = b"hello";
    let bytes = compressed(raw);
    let (chunk_id, _) = put(&node, 2, raw).await;
    assert_eq!(node.read_chunk(2, chunk_id).await.unwrap(), bytes);
}

#[tokio::test]
async fn unknown_volume_returns_storage_error() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    init_volume(&v1, 1, 1024 * 1024);
    let node = StorageNode::open(config(vec![StorageVolumeConfig {
        path: v1,
        volume_id: 1,
    }]))
    .unwrap();

    let err = node
        .read_chunk(2, blake3_hash(b"missing"))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::UnknownVolume(2)));
}

#[tokio::test]
async fn rejects_duplicate_config_volume_ids() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    let v2 = temp.path().join("v2");
    init_volume(&v1, 1, 1024 * 1024);
    init_volume(&v2, 2, 1024 * 1024);

    let err = StorageNode::open(config(vec![
        StorageVolumeConfig {
            path: v1,
            volume_id: 1,
        },
        StorageVolumeConfig {
            path: v2,
            volume_id: 1,
        },
    ]))
    .unwrap_err();
    assert!(matches!(err, StorageError::DuplicateVolumeId(1)));
}

#[tokio::test]
async fn rejects_config_volume_id_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    init_volume(&v1, 1, 1024 * 1024);

    let err = StorageNode::open(config(vec![StorageVolumeConfig {
        path: v1,
        volume_id: 2,
    }]))
    .unwrap_err();
    assert!(matches!(
        err,
        StorageError::VolumeIdMismatch {
            configured: 2,
            actual: 1,
            ..
        }
    ));
}

#[tokio::test]
async fn can_delete_chunk_through_storage_node() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    init_volume(&v1, 1, 1024 * 1024);
    let node = StorageNode::open(config(vec![StorageVolumeConfig {
        path: v1,
        volume_id: 1,
    }]))
    .unwrap();

    let (chunk_id, _) = put(&node, 1, b"delete me").await;
    node.volume(1)
        .unwrap()
        .delete_chunk(chunk_id)
        .await
        .unwrap();
    let err = node.read_chunk(1, chunk_id).await.unwrap_err();
    assert!(matches!(
        err,
        StorageError::Volume(fs0_volume::VolumeError::ChunkNotFound(id)) if id == chunk_id
    ));
}

#[tokio::test]
async fn concurrent_puts_to_same_volume_get_distinct_offsets() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    init_volume(&v1, 1, 1024 * 1024);
    let node = StorageNode::open(config(vec![StorageVolumeConfig {
        path: v1,
        volume_id: 1,
    }]))
    .unwrap();

    let mut tasks = Vec::new();
    for index in 0..16 {
        let node = node.clone();
        tasks.push(tokio::spawn(async move {
            let raw = vec![index as u8; 128];
            let bytes = compressed(&raw);
            let chunk_id = blake3_hash(&bytes);
            node.put_chunk(1, chunk_id, raw.len() as u64, bytes)
                .await
                .unwrap()
        }));
    }

    let mut offsets = Vec::new();
    for task in tasks {
        offsets.push(task.await.unwrap().volume_offset);
    }
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), 16);
}

#[tokio::test]
async fn capacity_error_does_not_insert_chunk_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    init_volume(&v1, 1, 64);
    let node = StorageNode::open(config(vec![StorageVolumeConfig {
        path: v1,
        volume_id: 1,
    }]))
    .unwrap();

    let bytes = vec![1; 65];
    let chunk_id = blake3_hash(&bytes);
    let err = node.put_chunk(1, chunk_id, 1, bytes).await.unwrap_err();
    assert!(matches!(
        err,
        StorageError::Volume(fs0_volume::VolumeError::CapacityExceeded { .. })
    ));
    let err = node.chunk_meta(1, chunk_id).await.unwrap_err();
    assert!(matches!(
        err,
        StorageError::Volume(fs0_volume::VolumeError::ChunkNotFound(id)) if id == chunk_id
    ));
}

#[tokio::test]
async fn loads_storage_config_from_toml() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("storage.toml");
    std::fs::write(
        &config_path,
        r#"
name = "storage-a"
storage_id = 1
central_endpoint = [1, 2, 3]
cert = "/tmp/fs0-cert.pem"

[p2p_relay]
port = 3340
quic_port = 7824
public_url = "http://127.0.0.1:3340"

[[volumes]]
path = "/tmp/fs0-v1"
volume_id = 1
"#,
    )
    .unwrap();

    let loaded = StorageConfig::load_from(config_path).unwrap();
    assert_eq!(loaded.name, "storage-a");
    assert_eq!(loaded.storage_id, 1);
    assert_eq!(loaded.central_endpoint, vec![1, 2, 3]);
    assert_eq!(loaded.p2p_relay.port, 3340);
    assert_eq!(loaded.p2p_relay.quic_port, 7824);
    assert_eq!(loaded.p2p_relay.public_url, "http://127.0.0.1:3340");
    assert_eq!(loaded.volumes.len(), 1);
    assert_eq!(loaded.volumes[0].volume_id, 1);
}
