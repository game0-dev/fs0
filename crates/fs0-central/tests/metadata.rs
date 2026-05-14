use fs0_central::{
    CentralConfig, CentralError, CentralP2pRelayConfig, CentralServer, CommitFileLocation,
};
use fs0_core::{
    ControlErrorCode, ControlRequest, ControlResponse, CreateStorageRequest, CreateVolumeRequest,
    Fs0Path, ListDirectoryRequest, RegisterStorageRequest, StorageVolumeInfo,
};
use fs0_transport::{read_frame, write_frame};
use std::net::{SocketAddr, TcpListener as StdTcpListener, UdpSocket};
use std::path::PathBuf;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

fn db_config(path: impl Into<std::path::PathBuf>) -> CentralConfig {
    CentralConfig {
        tcp_port: 0,
        db_path: path.into(),
        p2p_relay: CentralP2pRelayConfig {
            port: 0,
            quic_port: 0,
            public_url: "http://127.0.0.1:0".to_owned(),
        },
    }
}

fn run_config() -> (CentralConfig, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let tcp_port = free_tcp_port();
    let relay_port = free_tcp_port();
    let relay_quic_port = free_udp_port();
    (
        CentralConfig {
            tcp_port,
            db_path: temp.path().join("central.sqlite"),
            p2p_relay: CentralP2pRelayConfig {
                port: relay_port,
                quic_port: relay_quic_port,
                public_url: format!("http://127.0.0.1:{relay_port}"),
            },
        },
        temp,
    )
}

fn control_addr(config: &CentralConfig) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], config.tcp_port))
}

fn relay_addr(config: &CentralConfig) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], config.p2p_relay.port))
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

async fn connect_with_retry(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            return stream;
        }
        sleep(Duration::from_millis(10)).await;
    }
    TcpStream::connect(addr).await.unwrap()
}

#[test]
fn central_config_loads_from_toml() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("central.toml");
    std::fs::write(
        &config_path,
        r#"
tcp_port = 7000
db_path = "central.sqlite"

[p2p_relay]
port = 3340
quic_port = 7824
public_url = "http://127.0.0.1:3340"
"#,
    )
    .unwrap();

    let config = CentralConfig::load_from(config_path).unwrap();
    assert_eq!(config.tcp_port, 7000);
    assert_eq!(config.db_path, PathBuf::from("central.sqlite"));
    assert_eq!(config.p2p_relay.port, 3340);
    assert_eq!(config.p2p_relay.quic_port, 7824);
    assert_eq!(config.p2p_relay.public_url, "http://127.0.0.1:3340");
}

#[test]
fn central_config_requires_db_path() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("central.toml");
    std::fs::write(
        &config_path,
        r#"
tcp_port = 7000

[p2p_relay]
port = 3340
quic_port = 7824
public_url = "http://127.0.0.1:3340"
"#,
    )
    .unwrap();

    assert!(CentralConfig::load_from(config_path).is_err());
}

#[test]
fn central_config_requires_p2p_relay() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("central.toml");
    std::fs::write(
        &config_path,
        r#"
tcp_port = 7000
db_path = "central.sqlite"
"#,
    )
    .unwrap();

    assert!(CentralConfig::load_from(config_path).is_err());
}

#[tokio::test]
async fn central_persists_files_and_volume_locations() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("central.sqlite");

    let central = CentralServer::new(db_config(&db_path)).unwrap();
    let volume_1 = central
        .create_volume(CreateVolumeRequest {
            name: Some("v1".to_owned()),
            max_bytes: 1024,
        })
        .await
        .unwrap();
    let volume_2 = central
        .create_volume(CreateVolumeRequest {
            name: Some("v2".to_owned()),
            max_bytes: 1024,
        })
        .await
        .unwrap();

    let file = central
        .commit_file_location(CommitFileLocation {
            path: Fs0Path::new("/binance/BTCUSDT.jsonl").unwrap(),
            base_version: 0,
            base_size_bytes: 0,
            new_version: 1,
            new_size_bytes: 1024,
            compressed_size_bytes: 512,
            volume_ids: vec![volume_2.volume_id, volume_1.volume_id, volume_1.volume_id],
        })
        .await
        .unwrap();

    assert_eq!(file.file_id, 1);
    assert_eq!(file.version, 1);
    assert_eq!(
        file.volume_ids,
        vec![volume_1.volume_id, volume_2.volume_id]
    );

    let file = central
        .commit_file_location(CommitFileLocation {
            path: Fs0Path::new("/binance/BTCUSDT.jsonl").unwrap(),
            base_version: 1,
            base_size_bytes: 1024,
            new_version: 2,
            new_size_bytes: 2048,
            compressed_size_bytes: 900,
            volume_ids: vec![volume_2.volume_id],
        })
        .await
        .unwrap();

    assert_eq!(file.file_id, 1);
    assert_eq!(file.version, 2);
    assert_eq!(file.size_bytes, 2048);
    assert_eq!(file.volume_ids, vec![volume_2.volume_id]);

    let conflict = central
        .commit_file_location(CommitFileLocation {
            path: Fs0Path::new("/binance/BTCUSDT.jsonl").unwrap(),
            base_version: 1,
            base_size_bytes: 1024,
            new_version: 3,
            new_size_bytes: 4096,
            compressed_size_bytes: 1800,
            volume_ids: vec![volume_2.volume_id],
        })
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        CentralError::Control(err) if err.code == ControlErrorCode::VersionConflict
    ));

    drop(central);

    let reopened = CentralServer::new(db_config(&db_path)).unwrap();
    let files = reopened.list_files().await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path.as_str(), "/binance/BTCUSDT.jsonl");
    assert_eq!(files[0].volume_ids, vec![volume_2.volume_id]);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let storage_table_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'storages'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(storage_table_count, 0);
}

#[tokio::test]
async fn central_creates_storage_and_rejects_duplicate_online_volume_mounts() {
    let (config, _temp) = run_config();
    let addr = control_addr(&config);
    let central = CentralServer::new(config).unwrap();
    let server = central.clone();
    let server_task = tokio::spawn(async move { server.run().await });

    let mut stream = connect_with_retry(addr).await;
    write_frame(
        &mut stream,
        &ControlRequest::CreateStorage(CreateStorageRequest {
            name: "storage-a".to_owned(),
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::StorageCreated { storage_id } = response else {
        panic!("expected storage created response");
    };
    assert_eq!(storage_id, 1);

    write_frame(
        &mut stream,
        &ControlRequest::CreateVolume(CreateVolumeRequest {
            name: Some("volume-a".to_owned()),
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::VolumeCreated { volume_id } = response else {
        panic!("expected volume created response");
    };

    write_frame(
        &mut stream,
        &ControlRequest::RegisterStorage(RegisterStorageRequest {
            storage_id,
            name: "storage-a".to_owned(),
            volumes: vec![StorageVolumeInfo {
                volume_id,
                max_bytes: 1024,
                active_volume_offset: 0,
            }],
            data_endpoint: vec![1, 2, 3],
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    assert!(matches!(
        response,
        ControlResponse::StorageRegistered { storage_id: 1 }
    ));

    let mut stream_2 = connect_with_retry(addr).await;
    write_frame(
        &mut stream_2,
        &ControlRequest::CreateStorage(CreateStorageRequest {
            name: "storage-b".to_owned(),
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream_2).await.unwrap();
    let ControlResponse::StorageCreated {
        storage_id: storage_id_2,
    } = response
    else {
        panic!("expected storage created response");
    };

    write_frame(
        &mut stream_2,
        &ControlRequest::RegisterStorage(RegisterStorageRequest {
            storage_id: storage_id_2,
            name: "storage-b".to_owned(),
            volumes: vec![StorageVolumeInfo {
                volume_id,
                max_bytes: 1024,
                active_volume_offset: 0,
            }],
            data_endpoint: vec![4, 5, 6],
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream_2).await.unwrap();
    let ControlResponse::Error(err) = response else {
        panic!("expected volume mounted error response");
    };
    assert_eq!(err.code, ControlErrorCode::VolumeAlreadyMounted);

    server_task.abort();
}

#[tokio::test]
async fn central_control_protocol_can_list_and_get_files() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("central.sqlite");
    let (mut config, _temp) = run_config();
    config.db_path = db_path.clone();
    let addr = control_addr(&config);
    let central = CentralServer::new(config).unwrap();
    let volume = central
        .create_volume(CreateVolumeRequest {
            name: Some("trades".to_owned()),
            max_bytes: 1024,
        })
        .await
        .unwrap();

    central
        .commit_file_location(CommitFileLocation {
            path: Fs0Path::new("/polymarket/trades.jsonl").unwrap(),
            base_version: 0,
            base_size_bytes: 0,
            new_version: 7,
            new_size_bytes: 123,
            compressed_size_bytes: 88,
            volume_ids: vec![volume.volume_id],
        })
        .await
        .unwrap();
    let server = central.clone();
    let server_task = tokio::spawn(async move { server.run().await });

    let mut stream = connect_with_retry(addr).await;
    write_frame(&mut stream, &ControlRequest::ListFiles)
        .await
        .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::Files(files) = response else {
        panic!("expected files response");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].volume_ids, vec![volume.volume_id]);

    write_frame(
        &mut stream,
        &ControlRequest::ListDirectory(ListDirectoryRequest {
            parent_path: Fs0Path::new("/polymarket").unwrap(),
            limit: 100,
            cursor: None,
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::DirectoryEntries(entries) = response else {
        panic!("expected directory entries response");
    };
    assert_eq!(entries.entries.len(), 1);
    assert_eq!(entries.entries[0].name, "trades.jsonl");

    write_frame(
        &mut stream,
        &ControlRequest::GetFileRecord {
            path: Fs0Path::new("/polymarket/trades.jsonl").unwrap(),
        },
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::FileRecord(Some(file)) = response else {
        panic!("expected file record response");
    };
    assert_eq!(file.version, 7);

    write_frame(
        &mut stream,
        &ControlRequest::BeginAppend(fs0_core::BeginAppendRequest {
            path: Fs0Path::new("/unsupported").unwrap(),
            expected_version: 0,
            expected_size: 0,
        }),
    )
    .await
    .unwrap();
    let response: ControlResponse = read_frame(&mut stream).await.unwrap();
    let ControlResponse::Error(err) = response else {
        panic!("expected error response");
    };
    assert_eq!(err.code, ControlErrorCode::Unsupported);

    server_task.abort();
}

#[tokio::test]
async fn central_run_starts_control_listener_and_relay_when_enabled() {
    let (config, _temp) = run_config();
    let control_addr = control_addr(&config);
    let relay_addr = relay_addr(&config);

    let central = CentralServer::new(config).unwrap();
    let server = central.clone();
    let server_task = tokio::spawn(async move { server.run().await });

    let _control = connect_with_retry(control_addr).await;
    let _relay = connect_with_retry(relay_addr).await;

    server_task.abort();
}
