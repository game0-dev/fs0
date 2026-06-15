use fs0_core::{
    Fs0Error,
    protocol::{DirectoryEntries, FileChangeLogs, FileReadPlan, FileRecord},
};

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

pub(crate) fn print_file_record(file: FileRecord) {
    println!(
        "{}\t{}\t{}\t{}",
        file.file_id, file.size_bytes, file.compressed_size_bytes, file.path
    );
}

pub(crate) fn print_file_read_plan(plan: FileReadPlan) {
    println!("file {}\t{}\t{} bytes", plan.file_id, plan.path, plan.size);
    for bundle in plan.bundles {
        println!(
            "  bundle {}\t{} raw\t{} compressed\t{} replicas",
            bundle.bundle_index,
            bundle.raw_len,
            bundle.compressed_len,
            bundle.replicas.len()
        );
    }
}

pub(crate) fn print_file_change_logs(logs: FileChangeLogs) {
    for operation in logs.operations {
        println!(
            "{}\t{:?}\tfile={}\tnew={}",
            operation.event_id,
            operation.kind,
            operation
                .file_id
                .map(|file_id| file_id.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            operation.new_path.unwrap_or_else(|| "-".to_owned())
        );
    }
    if let Some(next_event_id) = logs.next_event_id {
        println!("next_event_id: {next_event_id}");
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
