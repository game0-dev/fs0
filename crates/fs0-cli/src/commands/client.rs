use crate::{
    commands::connect_client,
    output::{
        json_error, local_file_name, print_central_status, print_directory_entries,
        print_file_change_logs, print_file_read_plan, print_file_record,
    },
};
use fs0_client::{ListOptions, ReadRange, WriteOptions};
use fs0_core::Fs0Result;
use std::path::{Path, PathBuf};

pub(super) async fn ls(
    config: &Option<PathBuf>,
    json: bool,
    dir: String,
    limit: u32,
    cursor: Option<u64>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let entries = client
        .list_directory(&dir, ListOptions { limit, cursor })
        .await?;
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

pub(super) async fn cat(
    config: &Option<PathBuf>,
    remote_path: String,
    offset: u64,
    len: Option<u64>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let stdout = tokio::io::stdout();
    client
        .download_to_writer(&remote_path, stdout, ReadRange { offset, len })
        .await?;
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
    local_path: Option<PathBuf>,
    offset: u64,
    len: Option<u64>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    if local_path.as_deref() == Some(Path::new("-")) {
        let stdout = tokio::io::stdout();
        client
            .download_to_writer(&remote_path, stdout, ReadRange { offset, len })
            .await?;
    } else {
        let local_path = local_path.unwrap_or_else(|| local_file_name(&remote_path));
        client
            .download_to_path(&remote_path, local_path, ReadRange { offset, len })
            .await?;
    }
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
    let options = WriteOptions {
        prefer_volume_name: prefer_volume,
        offset: None,
    };
    let plan = if local_path == "-" {
        client
            .put_from_reader(&remote_path, tokio::io::stdin(), options)
            .await?
    } else {
        client.put_path(&remote_path, local_path, options).await?
    };
    print_write_result(json, &plan)?;
    client.shutdown().await
}

pub(super) async fn append(
    config: &Option<PathBuf>,
    json: bool,
    remote_path: String,
    local_path: String,
    prefer_volume: Option<String>,
    offset: Option<u64>,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let offset = match offset {
        Some(offset) => Some(offset),
        None => Some(client.get_file_read_plan(&remote_path).await?.size),
    };
    let options = WriteOptions {
        prefer_volume_name: prefer_volume,
        offset,
    };
    let plan = if local_path == "-" {
        client
            .append_from_reader(&remote_path, tokio::io::stdin(), options)
            .await?
    } else {
        client
            .append_path(&remote_path, local_path, options)
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

pub(super) async fn rm_id(config: &Option<PathBuf>, file_id: u64) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    client.delete_file_by_id(file_id).await?;
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

pub(super) async fn cp_by_id(
    config: &Option<PathBuf>,
    json: bool,
    source_file_id: u64,
    target_path: String,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let file = client.copy_file_by_id(source_file_id, &target_path).await?;
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

pub(super) async fn mv_by_id(
    config: &Option<PathBuf>,
    json: bool,
    file_id: u64,
    target_path: String,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let file = client.rename_file_by_id(file_id, &target_path).await?;
    print_file_result(json, file)?;
    client.shutdown().await
}

pub(super) async fn changes(
    config: &Option<PathBuf>,
    json: bool,
    after_event_id: u64,
    limit: u32,
) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let logs = client.get_file_change_logs(after_event_id, limit).await?;
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

pub(super) async fn central_status(config: &Option<PathBuf>, json: bool) -> Fs0Result<()> {
    let client = connect_client(config).await?;
    let status = client.central_status().await?;
    if json {
        let status = serde_json::json!({
            "clients_count": status.clients_count,
            "storages": status.storages,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&status).map_err(json_error)?
        );
    } else {
        print_central_status(status);
    }
    client.shutdown().await
}

fn print_write_result(json: bool, plan: &fs0_core::protocol::FileReadPlan) -> Fs0Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(plan).map_err(json_error)?
        );
    } else {
        println!("{} {} bytes", plan.path, plan.size);
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
