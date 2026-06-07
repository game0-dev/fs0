use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "fs0", version, about = "append-only distributed storage")]
pub(crate) struct Cli {
    #[arg(long, global = true)]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    Ls {
        #[arg(default_value = "/")]
        dir: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
        #[arg(long)]
        cursor: Option<u64>,
    },
    Cat {
        remote_path: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        len: Option<u64>,
    },
    Get {
        remote_path: String,
        local_path: Option<PathBuf>,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        len: Option<u64>,
    },
    Put {
        remote_path: String,
        local_path: String,
        #[arg(long)]
        prefer_volume: Option<String>,
    },
    Append {
        remote_path: String,
        local_path: String,
        #[arg(long)]
        prefer_volume: Option<String>,
        #[arg(long)]
        offset: Option<u64>,
    },
    Rm {
        remote_path: String,
    },
    Peers,
    Central {
        #[command(subcommand)]
        command: CentralCommand,
    },
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CentralCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Status,
}

#[derive(Debug, Subcommand)]
pub(crate) enum StorageCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum VolumeCommand {
    Create {
        path: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        max_bytes: String,
        #[arg(long)]
        central: Option<PathBuf>,
    },
    Meta {
        path: PathBuf,
    },
}
