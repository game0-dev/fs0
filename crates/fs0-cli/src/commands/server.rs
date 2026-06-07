use fs0_config::{CentralConfig, StorageConfig};
use fs0_core::Fs0Result;
use std::path::PathBuf;

pub(super) async fn run_central(config: PathBuf) -> Fs0Result<()> {
    let server = fs0_central::CentralServer::run(CentralConfig::load_from(config)?).await?;
    println!("central endpoint: {:?}", server.control_endpoint());
    tokio::signal::ctrl_c().await?;
    server.shutdown().await;
    Ok(())
}

pub(super) async fn run_storage(config: PathBuf) -> Fs0Result<()> {
    let server = fs0_storage::StorageServer::run(StorageConfig::load_from(config)?).await?;
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
