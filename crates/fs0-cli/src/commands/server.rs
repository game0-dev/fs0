use crate::commands::config_path;
use fs0_config::Fs0Config;
use fs0_core::Fs0Result;
use std::path::PathBuf;

pub(super) async fn run_central(config: &Option<PathBuf>) -> Fs0Result<()> {
    let server =
        fs0_central::CentralServer::run(Fs0Config::load_central_from(config_path(config))?).await?;
    println!("central endpoint: {:?}", server.control_endpoint());
    wait_for_shutdown_signal().await?;
    server.shutdown().await;
    Ok(())
}

pub(super) async fn run_storage(config: &Option<PathBuf>) -> Fs0Result<()> {
    let server =
        fs0_storage::StorageServer::run(Fs0Config::load_storage_from(config_path(config))?).await?;
    wait_for_shutdown_signal().await?;
    server.shutdown().await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Fs0Result<()> {
    use tokio::signal::{
        ctrl_c,
        unix::{SignalKind, signal},
    };

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = ctrl_c() => result?,
        _ = terminate.recv() => {}
    }

    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Fs0Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}
