use clap::{Parser, Subcommand};
use fs0_client::{ClientOptions, Fs0Client, ListOptions, ReadRange, WriteOptions};
use fs0_config::{CentralConfig, Fs0Config, StorageConfig};
use fs0_core::{CentralStatus, DirectoryEntries, Fs0Error};
use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Debug, Parser)]
#[command(name = "fs0", version, about = "append-only distributed storage")]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    Append {
        remote_path: String,
        local_path: String,
        #[arg(long)]
        prefer_volume: Option<String>,
        #[arg(long)]
        idempotency_key: Option<String>,
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
enum CentralCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Status,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum VolumeCommand {
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

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fs0: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> fs0_client::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Ls { dir, limit, cursor } => {
            let client = client(&cli.config).await?;
            let entries = client
                .list_directory(&dir, ListOptions { limit, cursor })
                .await?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&entries).map_err(json_error)?
                );
            } else {
                print_directory_entries(entries);
            }
            client.shutdown().await
        }
        Command::Cat {
            remote_path,
            offset,
            len,
        } => {
            let client = client(&cli.config).await?;
            let stdout = tokio::io::stdout();
            client
                .download_to_writer(&remote_path, stdout, ReadRange { offset, len })
                .await?;
            client.shutdown().await
        }
        Command::Get {
            remote_path,
            local_path,
            offset,
            len,
        } => {
            let client = client(&cli.config).await?;
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
        Command::Put {
            remote_path,
            local_path,
            prefer_volume,
            idempotency_key,
        } => {
            let client = client(&cli.config).await?;
            let options = WriteOptions {
                prefer_volume_name: prefer_volume,
                idempotency_key,
            };
            let plan = if local_path == "-" {
                client
                    .put_from_reader(&remote_path, tokio::io::stdin(), options)
                    .await?
            } else {
                client.put_path(&remote_path, local_path, options).await?
            };
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&plan).map_err(json_error)?
                );
            } else {
                println!("{} {} bytes", plan.path, plan.size);
            }
            client.shutdown().await
        }
        Command::Append {
            remote_path,
            local_path,
            prefer_volume,
            idempotency_key,
        } => {
            let client = client(&cli.config).await?;
            let options = WriteOptions {
                prefer_volume_name: prefer_volume,
                idempotency_key,
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
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&plan).map_err(json_error)?
                );
            } else {
                println!("{} {} bytes", plan.path, plan.size);
            }
            client.shutdown().await
        }
        Command::Rm { remote_path } => {
            let client = client(&cli.config).await?;
            client.delete_file(&remote_path).await?;
            client.shutdown().await
        }
        Command::Peers => {
            let client = client(&cli.config).await?;
            let peers = client.storage_peers();
            if cli.json {
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
                            volume.volume_id,
                            volume.name,
                            volume.max_bytes,
                            volume.max_volume_offset
                        );
                    }
                }
            }
            client.shutdown().await
        }
        Command::Central { command } => match command {
            CentralCommand::Run { config } => {
                let server =
                    fs0_central::CentralServer::run(CentralConfig::load_from(config)?).await?;
                println!("central endpoint: {:?}", server.control_endpoint());
                tokio::signal::ctrl_c().await?;
                server.shutdown().await;
                Ok(())
            }
            CentralCommand::Status => {
                let client = client(&cli.config).await?;
                let status = client.central_status().await?;
                if cli.json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&status).map_err(json_error)?
                    );
                } else {
                    print_central_status(status);
                }
                client.shutdown().await
            }
        },
        Command::Storage { command } => match command {
            StorageCommand::Run { config } => {
                let server =
                    fs0_storage::StorageServer::run(StorageConfig::load_from(config)?).await?;
                tokio::signal::ctrl_c().await?;
                server.shutdown().await;
                Ok(())
            }
        },
        Command::Volume { command } => match command {
            VolumeCommand::Create {
                path,
                name,
                max_bytes,
                central,
            } => {
                let max_bytes = parse_bytes(&max_bytes)?;
                let volume = fs0_volume::Volume::init(path, max_bytes)?;
                let config = central.or_else(|| cli.config.clone());
                let client = client(&config).await?;
                let volume_id = client.create_volume(name, max_bytes).await?;
                client.shutdown().await?;
                let meta = volume.assign_volume_id(volume_id)?;
                print_volume_meta(meta);
                Ok(())
            }
            VolumeCommand::Meta { path } => {
                let volume = fs0_volume::Volume::open(path)?;
                print_volume_meta(volume.meta());
                Ok(())
            }
        },
    }
}

async fn client(config: &Option<PathBuf>) -> fs0_client::Result<Fs0Client> {
    Fs0Client::connect(
        Fs0Config::load_from(config_path(config))?.client()?,
        ClientOptions::default(),
    )
    .await
}

fn config_path(config: &Option<PathBuf>) -> PathBuf {
    config.clone().unwrap_or_else(default_config_path)
}

fn default_config_path() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".fs0rc")
}

fn local_file_name(remote_path: &str) -> PathBuf {
    remote_path
        .rsplit('/')
        .find(|name| !name.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fs0.out"))
}

fn parse_bytes(value: &str) -> fs0_client::Result<u64> {
    let value = value.trim();
    let (number, multiplier) = match value.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&value[..value.len() - 1], 1024u64),
        Some(b'm' | b'M') => (&value[..value.len() - 1], 1024u64.pow(2)),
        Some(b'g' | b'G') => (&value[..value.len() - 1], 1024u64.pow(3)),
        Some(b't' | b'T') => (&value[..value.len() - 1], 1024u64.pow(4)),
        _ => (value, 1),
    };
    let number = number
        .parse::<u64>()
        .map_err(|_| Fs0Error::InvalidRequest)?;
    number
        .checked_mul(multiplier)
        .ok_or(Fs0Error::InvalidRequest)
}

fn print_directory_entries(entries: DirectoryEntries) {
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

fn print_central_status(status: CentralStatus) {
    for storage in status.storages {
        println!("storage {} {}", storage.storage_id, storage.name);
        for volume in storage.volumes {
            println!(
                "  volume {} {} capacity={} used={} raw={} compressed={}",
                volume.volume_id,
                volume.name,
                volume.max_bytes,
                volume.used_bytes,
                volume.raw_bytes,
                volume.compressed_bytes,
            );
        }
    }
}

fn print_volume_meta(meta: fs0_volume::VolumeMeta) {
    println!("volume_id: {}", meta.volume_id);
    println!("format_version: {}", meta.format_version);
    println!("max_bytes: {}", meta.max_bytes);
    println!("active_volume_offset: {}", meta.active_volume_offset);
    println!("created_at_ms: {}", meta.created_at_ms);
    println!("updated_at_ms: {}", meta.updated_at_ms);
}

fn json_error(err: serde_json::Error) -> Fs0Error {
    Fs0Error::InvalidData {
        message: err.to_string(),
    }
}
