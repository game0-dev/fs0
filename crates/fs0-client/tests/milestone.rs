use fs0_central::{CentralConfig, CentralP2pRelayConfig, CentralServer, CommitFileLocation};
use fs0_client::Fs0Client;
use fs0_core::{CreateStorageRequest, CreateVolumeRequest, Fs0Path};
use fs0_storage::{StorageConfig, StorageDaemon, StorageP2pRelayConfig, StorageVolumeConfig};
use fs0_volume::Volume;
use std::net::{SocketAddr, TcpListener as StdTcpListener, UdpSocket};
use std::path::PathBuf;
use tokio::time::{Duration, sleep, timeout};

fn central_test_config(db_path: impl Into<PathBuf>) -> CentralConfig {
    let tcp_port = free_tcp_port();
    let relay_port = free_tcp_port();
    let relay_quic_port = free_udp_port();
    CentralConfig {
        tcp_port,
        db_path: db_path.into(),
        p2p_relay: CentralP2pRelayConfig {
            port: relay_port,
            quic_port: relay_quic_port,
            public_url: format!("http://127.0.0.1:{relay_port}"),
        },
    }
}

fn free_tcp_port() -> u16 {
    StdTcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn free_udp_port() -> u16 {
    UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_control(addr: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    TcpStream::connect(addr).await.unwrap();
}

use tokio::net::TcpStream;

#[tokio::test]
async fn storage_mounts_two_volumes_client_connects_and_pings_p2p() {
    let temp = tempfile::tempdir().unwrap();
    let v1 = temp.path().join("v1");
    let v2 = temp.path().join("v2");
    Volume::init(&v1, 1, 1024 * 1024).unwrap();
    Volume::init(&v2, 2, 1024 * 1024).unwrap();

    let config = central_test_config(temp.path().join("central.sqlite"));
    let central_addr = SocketAddr::from(([127, 0, 0, 1], config.tcp_port));
    let relay_public_url = config.p2p_relay.public_url.clone();
    let relay_port = config.p2p_relay.port;
    let relay_quic_port = config.p2p_relay.quic_port;
    let central = CentralServer::new(config).unwrap();
    let storage_record = central
        .create_storage(CreateStorageRequest {
            name: "storage-a".to_owned(),
        })
        .await
        .unwrap();
    let volume_1 = central
        .create_volume(CreateVolumeRequest {
            name: Some("v1".to_owned()),
            max_bytes: 1024 * 1024,
        })
        .await
        .unwrap();
    let volume_2 = central
        .create_volume(CreateVolumeRequest {
            name: Some("v2".to_owned()),
            max_bytes: 1024 * 1024,
        })
        .await
        .unwrap();
    let server = central.clone();
    let central_task = tokio::spawn(async move { server.run().await });
    wait_for_control(central_addr).await;

    let storage = StorageDaemon::start(StorageConfig {
        storage_id: storage_record.storage_id,
        name: "storage-a".to_owned(),
        central: central_addr.to_string(),
        cert: temp.path().join("cert.pem"),
        p2p_relay: StorageP2pRelayConfig {
            port: relay_port,
            quic_port: relay_quic_port,
            public_url: relay_public_url.clone(),
        },
        volumes: vec![
            StorageVolumeConfig {
                path: v1,
                volume_id: volume_1.volume_id,
            },
            StorageVolumeConfig {
                path: v2,
                volume_id: volume_2.volume_id,
            },
        ],
    })
    .await
    .unwrap();
    storage.ping_central().await.unwrap();

    let client = Fs0Client::connect(
        central_addr,
        Some("client-a".to_owned()),
        &relay_public_url,
        relay_quic_port,
    )
    .await
    .unwrap();
    let peers = client.storage_peers().await.unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].volumes.len(), 2);

    central
        .commit_file_location(CommitFileLocation {
            path: Fs0Path::new("/binance/depth/BTCUSDT.jsonl").unwrap(),
            base_version: 0,
            base_size_bytes: 0,
            new_version: 1,
            new_size_bytes: 100,
            compressed_size_bytes: 80,
            volume_ids: vec![volume_1.volume_id],
        })
        .await
        .unwrap();
    let files = client.list_files().await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].volume_ids, vec![volume_1.volume_id]);
    let file = client
        .get_file_record(Fs0Path::new("/binance/depth/BTCUSDT.jsonl").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.version, 1);

    timeout(Duration::from_secs(10), client.ping_storage_peer(&peers[0]))
        .await
        .unwrap()
        .unwrap();

    drop(storage);
    central_task.abort();
}
