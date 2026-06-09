use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "fs0", version, about = "distributed storage")]
pub(crate) struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Path to the fs0 config file; defaults to ~/.fs0/config.toml"
    )]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    #[command(about = "List a remote directory")]
    Ls {
        #[arg(default_value = "/")]
        dir: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        cursor: Option<u64>,
    },
    #[command(about = "Print remote file bytes to stdout")]
    Cat {
        remote_path: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        len: Option<u64>,
    },
    #[command(about = "Show remote file metadata and read plan")]
    Stat { remote_path: String },
    #[command(about = "Download a remote file")]
    Get {
        remote_path: String,
        local_path: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        len: Option<u64>,
    },
    #[command(about = "Upload a remote file")]
    Put {
        remote_path: String,
        local_path: String,
        #[arg(long)]
        prefer_volume: Option<String>,
    },
    #[command(about = "Update remote file data")]
    Update {
        remote_path: String,
        local_path: String,
        #[arg(long)]
        prefer_volume: Option<String>,
        #[arg(long)]
        offset: Option<u64>,
    },
    #[command(about = "Delete a remote file")]
    Rm { remote_path: String },
    #[command(about = "Copy a remote file")]
    Cp {
        source_path: String,
        target_path: String,
    },
    #[command(about = "Move or rename a remote file")]
    Mv {
        source_path: String,
        target_path: String,
    },
    #[command(about = "List central file change events")]
    Changes {
        #[arg(long = "cursor", default_value_t = 0)]
        cursor: u64,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    #[command(about = "Show known storage peers")]
    Peers,
    #[command(about = "Run or inspect the central server")]
    Central {
        #[command(subcommand)]
        command: CentralCommand,
    },
    #[command(about = "Run storage nodes and manage local volumes")]
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CentralCommand {
    #[command(about = "Run a central metadata server")]
    Run,
    #[command(about = "Show central server status")]
    Status,
}

#[derive(Debug, Subcommand)]
pub(crate) enum StorageCommand {
    #[command(about = "Run a storage node")]
    Run,
    #[command(about = "Create and register a local volume")]
    CreateVolume {
        path: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        max_bytes: String,
    },
    #[command(about = "Inspect local volume metadata")]
    InspectVolume { path: PathBuf },
}
