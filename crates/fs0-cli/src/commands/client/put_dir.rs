use crate::{commands::connect_client, output::json_error};
use fs0_client::Fs0Client;
use fs0_core::{Fs0Error, Fs0Result};
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct UploadFile {
    local_path: PathBuf,
    remote_path: String,
    bytes: u64,
}

#[derive(Default)]
struct PutDirSummary {
    uploaded_files: u64,
    uploaded_bytes: u64,
    skipped_files: u64,
    skipped_bytes: u64,
}

pub(in crate::commands) async fn put_dir(
    config: &Option<PathBuf>,
    json: bool,
    remote_dir: String,
    local_dir: PathBuf,
    prefer_volume: Option<String>,
    dry_run: bool,
) -> Fs0Result<()> {
    let remote_dir = normalize_remote_dir(&remote_dir);
    let files = collect_upload_files(&local_dir, &remote_dir)?;

    if dry_run {
        print_dry_run(json, &files)?;
        return Ok(());
    }

    let client = connect_client(config).await?;
    let result = upload_files(&client, json, &files, prefer_volume).await;
    let shutdown = client.shutdown().await;
    let summary = result?;
    shutdown?;

    print_summary(json, &summary)
}

async fn upload_files(
    client: &Fs0Client,
    json: bool,
    files: &[UploadFile],
    prefer_volume: Option<String>,
) -> Fs0Result<PutDirSummary> {
    let mut summary = PutDirSummary::default();

    for (index, file) in files.iter().enumerate() {
        if !json {
            eprintln!(
                "checking {}/{} {}",
                index + 1,
                files.len(),
                file.remote_path
            );
        }
        let remote_exists_with_same_size = match client.get_file_read_plan(&file.remote_path).await
        {
            Ok(plan) => plan.size == file.bytes,
            Err(Fs0Error::NotFound) => false,
            Err(err) => return Err(err),
        };
        if remote_exists_with_same_size {
            summary.skipped_files += 1;
            summary.skipped_bytes += file.bytes;
            if !json {
                println!("skip {} {} bytes", file.remote_path, file.bytes);
            }
            continue;
        }

        if !json {
            eprintln!(
                "uploading {}/{} {} {} bytes",
                index + 1,
                files.len(),
                file.remote_path,
                file.bytes
            );
        }
        let uploaded = client
            .upload_file(
                &file.remote_path,
                &file.local_path,
                prefer_volume.as_ref().cloned(),
            )
            .await?;
        summary.uploaded_files += 1;
        summary.uploaded_bytes += uploaded.size_bytes;
        if !json {
            println!("put {} {} bytes", uploaded.path, uploaded.size_bytes);
        }
    }

    Ok(summary)
}

fn collect_upload_files(local_dir: &Path, remote_dir: &str) -> Fs0Result<Vec<UploadFile>> {
    let local_root = local_dir.canonicalize().map_err(Fs0Error::from)?;
    if !local_root.is_dir() {
        return Err(Fs0Error::InvalidPath {
            path: local_dir.display().to_string(),
        });
    }

    let mut files = Vec::new();
    collect_upload_files_from(&local_root, &local_root, remote_dir, &mut files)?;
    files.sort_by(|left, right| left.remote_path.cmp(&right.remote_path));
    Ok(files)
}

fn collect_upload_files_from(
    root: &Path,
    dir: &Path,
    remote_dir: &str,
    files: &mut Vec<UploadFile>,
) -> Fs0Result<()> {
    let mut entries = std::fs::read_dir(dir)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Fs0Error::from)?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let file_type = entry.file_type().map_err(Fs0Error::from)?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_upload_files_from(root, &path, remote_dir, files)?;
        } else if file_type.is_file() {
            let relative_path = path.strip_prefix(root).map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })?;
            files.push(UploadFile {
                remote_path: upload_remote_path(remote_dir, relative_path)?,
                bytes: entry.metadata().map_err(Fs0Error::from)?.len(),
                local_path: path,
            });
        }
    }

    Ok(())
}

fn upload_remote_path(remote_dir: &str, relative_path: &Path) -> Fs0Result<String> {
    let mut remote_path = remote_dir.trim_end_matches('/').to_owned();
    for component in relative_path.components() {
        let component = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| Fs0Error::InvalidPath {
                path: relative_path.display().to_string(),
            })?;
        remote_path.push('/');
        remote_path.push_str(component);
    }

    Ok(remote_path)
}

fn normalize_remote_dir(remote_dir: &str) -> String {
    let trimmed = remote_dir.trim();
    if trimmed.is_empty() || trimmed == "/" {
        "/".to_owned()
    } else {
        let trimmed = trimmed.trim_end_matches('/');
        if trimmed.starts_with('/') {
            trimmed.to_owned()
        } else {
            format!("/{trimmed}")
        }
    }
}

fn print_dry_run(json: bool, files: &[UploadFile]) -> Fs0Result<()> {
    if json {
        let files = files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "local_path": file.local_path,
                    "remote_path": file.remote_path,
                    "bytes": file.bytes,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "files": files }))
                .map_err(json_error)?
        );
    } else {
        for file in files {
            println!(
                "{} -> {} {} bytes",
                file.local_path.display(),
                file.remote_path,
                file.bytes
            );
        }
    }

    Ok(())
}

fn print_summary(json: bool, summary: &PutDirSummary) -> Fs0Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "uploaded_files": summary.uploaded_files,
                "uploaded_bytes": summary.uploaded_bytes,
                "skipped_files": summary.skipped_files,
                "skipped_bytes": summary.skipped_bytes,
            }))
            .map_err(json_error)?
        );
    } else {
        println!(
            "uploaded {} files {} bytes, skipped {} files {} bytes",
            summary.uploaded_files,
            summary.uploaded_bytes,
            summary.skipped_files,
            summary.skipped_bytes
        );
    }

    Ok(())
}
