use crate::{
    commands::connect_client,
    output::{
        json_error, print_directory_entries, print_file_change_logs, print_file_read_plan,
        print_file_record,
    },
};
use fs0_core::Fs0Result;
use std::path::PathBuf;

pub(super) async fn ls(
    config: &Option<PathBuf>,
    json: bool,
    dir: String,
    limit: u32,
    cursor: Option<u64>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let entries = client.list_directory(&dir, limit, cursor).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&entries).map_err(json_error)?
        );
    } else {
        print_directory_entries(entries);
    }
    client.shutdown().await
}

pub(super) async fn stat(
    config: &Option<PathBuf>,
    json: bool,
    remote_path: String,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let plan = client.get_file_read_plan(&remote_path).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&plan).map_err(json_error)?
        );
    } else {
        print_file_read_plan(plan);
    }
    client.shutdown().await
}

pub(super) async fn get(
    config: &Option<PathBuf>,
    remote_path: String,
    local_path: PathBuf,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    client.download_file(&remote_path, local_path).await?;
    client.shutdown().await
}

pub(super) async fn put(
    config: &Option<PathBuf>,
    json: bool,
    remote_path: String,
    local_path: String,
    prefer_volume: Option<String>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let plan = if local_path == "-" {
        client
            .upload(&remote_path, tokio::io::stdin(), prefer_volume)
            .await?
    } else {
        client
            .upload_file(&remote_path, local_path, prefer_volume)
            .await?
    };
    print_write_result(json, &plan)?;
    client.shutdown().await
}

pub(super) async fn rm(config: &Option<PathBuf>, remote_path: String) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    client.delete_file(&remote_path).await?;
    client.shutdown().await
}

pub(super) async fn cp(
    config: &Option<PathBuf>,
    json: bool,
    source_path: String,
    target_path: String,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let file = client.copy_file(&source_path, &target_path).await?;
    print_file_result(json, file)?;
    client.shutdown().await
}

pub(super) async fn mv(
    config: &Option<PathBuf>,
    json: bool,
    source_path: String,
    target_path: String,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let file = client.rename_file(&source_path, &target_path).await?;
    print_file_result(json, file)?;
    client.shutdown().await
}

pub(super) async fn changes(
    config: &Option<PathBuf>,
    json: bool,
    cursor: u64,
    limit: u32,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let logs = client.get_file_change_logs(cursor, limit).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&logs).map_err(json_error)?
        );
    } else {
        print_file_change_logs(logs);
    }
    client.shutdown().await
}

pub(super) async fn peers(config: &Option<PathBuf>, json: bool) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let peers = client.storage_peers();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&peers).map_err(json_error)?
        );
    } else {
        for peer in peers {
            println!("storage {} {}", peer.storage_id, peer.name);
            for volume in peer.volumes {
                println!(
                    "  volume {} {} max={} used={}",
                    volume.volume_id, volume.name, volume.max_bytes, volume.max_volume_offset
                );
            }
        }
    }
    client.shutdown().await
}

fn print_write_result(json: bool, file: &fs0_core::protocol::FileRecord) -> Fs0Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(file).map_err(json_error)?
        );
    } else {
        println!("{} {} bytes", file.path, file.size_bytes);
    }

    Ok(())
}

fn print_file_result(json: bool, file: fs0_core::protocol::FileRecord) -> Fs0Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&file).map_err(json_error)?
        );
    } else {
        print_file_record(file);
    }

    Ok(())
}
