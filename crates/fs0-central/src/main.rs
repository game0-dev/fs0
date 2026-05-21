use clap::Parser;
use fs0_central::{CentralConfig, CentralServer};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "fs0-central",
    version,
    about = "fs0 central metadata and relay server"
)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = CentralConfig::load_from(args.config)?;
    eprintln!(
        "starting central: relay={}, relay_quic=127.0.0.1:{}",
        config.p2p_relay.public_url, config.p2p_relay.quic_port
    );
    let _server = CentralServer::run(config).await?;
    std::future::pending::<()>().await;
    Ok(())
}
