mod client;
mod server;
mod volume;

use crate::cli::{CentralCommand, Cli, Command, StorageCommand, VolumeCommand};
use fs0_client::{ClientOptions, Fs0Client};
use fs0_config::Fs0Config;
use fs0_core::Fs0Result;
use std::{env, path::PathBuf};

pub(crate) async fn run(cli: Cli) -> Fs0Result<()> {
    match cli.command {
        Command::Ls { dir, limit, cursor } => {
            client::ls(&cli.config, cli.json, dir, limit, cursor).await
        }
        Command::Cat {
            remote_path,
            offset,
            len,
        } => client::cat(&cli.config, remote_path, offset, len).await,
        Command::Get {
            remote_path,
            local_path,
            offset,
            len,
        } => client::get(&cli.config, remote_path, local_path, offset, len).await,
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
        Command::Append {
            remote_path,
            local_path,
            prefer_volume,
            offset,
        } => {
            client::append(
                &cli.config,
                cli.json,
                remote_path,
                local_path,
                prefer_volume,
                offset,
            )
            .await
        }
        Command::Rm { remote_path } => client::rm(&cli.config, remote_path).await,
        Command::Peers => client::peers(&cli.config, cli.json).await,
        Command::Central { command } => match command {
            CentralCommand::Run { config } => server::run_central(config).await,
            CentralCommand::Status => client::central_status(&cli.config, cli.json).await,
        },
        Command::Storage { command } => match command {
            StorageCommand::Run { config } => server::run_storage(config).await,
        },
        Command::Volume { command } => match command {
            VolumeCommand::Create {
                path,
                name,
                max_bytes,
                central,
            } => volume::create(&cli.config, path, name, max_bytes, central).await,
            VolumeCommand::Meta { path } => volume::meta(path),
        },
    }
}

pub(super) async fn connect_client(config: &Option<PathBuf>) -> Fs0Result<Fs0Client> {
    Fs0Client::connect(
        Fs0Config::load_from(config_path(config))?.client()?,
        ClientOptions::default(),
    )
    .await
}

pub(super) fn config_path(config: &Option<PathBuf>) -> PathBuf {
    config.clone().unwrap_or_else(default_config_path)
}

fn default_config_path() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".fs0rc")
}
