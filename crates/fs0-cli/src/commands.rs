mod client;
mod server;
mod volume;

use crate::cli::{CentralCommand, Cli, Command, StorageCommand};
use fs0_client::Fs0Client;
use fs0_config::Fs0Config;
use fs0_core::Fs0Result;
use std::{env, path::PathBuf};

pub(crate) async fn run(cli: Cli) -> Fs0Result<()> {
    match cli.command {
        Command::Ls { dir, limit, cursor } => {
            client::ls(&cli.config, cli.json, dir, limit, cursor).await
        }
        Command::Stat { remote_path } => client::stat(&cli.config, cli.json, remote_path).await,
        Command::Get {
            remote_path,
            local_path,
        } => client::get(&cli.config, remote_path, local_path).await,
        Command::Put {
            remote_path,
            local_path,
            prefer_volume,
        } => {
            client::put(
                &cli.config,
                cli.json,
                remote_path,
                local_path,
                prefer_volume,
            )
            .await
        }
        Command::Rm { remote_path } => client::rm(&cli.config, remote_path).await,
        Command::Cp {
            source_path,
            target_path,
        } => client::cp(&cli.config, cli.json, source_path, target_path).await,
        Command::Mv {
            source_path,
            target_path,
        } => client::mv(&cli.config, cli.json, source_path, target_path).await,
        Command::Changes { cursor, limit } => {
            client::changes(&cli.config, cli.json, cursor, limit).await
        }
        Command::Peers => client::peers(&cli.config, cli.json).await,
        Command::Central { command } => match command {
            CentralCommand::Run => server::run_central(&cli.config).await,
        },
        Command::Storage { command } => match command {
            StorageCommand::Run => server::run_storage(&cli.config).await,
            StorageCommand::CreateVolume {
                path,
                name,
                max_bytes,
            } => volume::create(&cli.config, path, name, max_bytes).await,
            StorageCommand::InspectVolume { path } => volume::inspect(path),
        },
    }
}

pub(super) async fn connect_client(config: &Option<PathBuf>) -> Fs0Result<Fs0Client> {
    Fs0Client::connect(Fs0Config::load_client_from(config_path(config))?).await
}

pub(super) fn config_path(config: &Option<PathBuf>) -> PathBuf {
    config.clone().unwrap_or_else(default_config_path)
}

fn default_config_path() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".fs0")
        .join("config.toml")
}
