use fs0_client::CentralStatus;
use fs0_core::{Fs0Error, protocol::DirectoryEntries};
use std::path::PathBuf;

pub(crate) fn local_file_name(remote_path: &str) -> PathBuf {
    remote_path
        .rsplit('/')
        .find(|name| !name.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fs0.out"))
}

pub(crate) fn print_directory_entries(entries: DirectoryEntries) {
    for entry in entries.entries {
        println!(
            "{}\t{}\t{}\t{}",
            entry.file_id, entry.size_bytes, entry.compressed_size_bytes, entry.path
        );
    }
    if let Some(cursor) = entries.next_cursor {
        println!("next_cursor: {cursor}");
    }
}

pub(crate) fn print_central_status(status: CentralStatus) {
    for storage in status.storages {
        println!("storage {} {}", storage.storage_id, storage.name);
        for volume in storage.volumes {
            println!(
                "  volume {} {} capacity={} used={} read_only={}",
                volume.volume_id,
                volume.name,
                volume.max_bytes,
                volume.max_volume_offset,
                volume.read_only,
            );
        }
    }
}

pub(crate) fn print_volume_meta(meta: fs0_volume::VolumeMeta) {
    println!("volume_id: {}", meta.volume_id);
    println!("format_version: {}", meta.format_version);
    println!("max_bytes: {}", meta.max_bytes);
    println!("active_volume_offset: {}", meta.active_volume_offset);
    println!("created_at_ms: {}", meta.created_at_ms);
    println!("updated_at_ms: {}", meta.updated_at_ms);
}

pub(crate) fn json_error(err: serde_json::Error) -> Fs0Error {
    Fs0Error::InvalidData {
        message: err.to_string(),
    }
}
