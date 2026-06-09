use crate::{commands::config_path, output::print_volume_meta};
use fs0_client::{ClientOptions, Fs0Client};
use fs0_config::{ClientConfig, Fs0Config};
use fs0_core::{Fs0Error, Fs0Result, VOLUME_READ_CONCURRENCY, VOLUME_WRITE_CONCURRENCY};
use std::path::PathBuf;

pub(super) async fn create(
    config: &Option<PathBuf>,
    path: PathBuf,
    name: String,
    max_bytes: String,
) -> Fs0Result<()> {
    let max_bytes = parse_bytes(&max_bytes)?;
    fs0_volume::Volume::init_fs(&path, max_bytes)?;
    let client = connect_storage_control(config_path(config)).await?;
    let volume_id = client.create_volume(name, max_bytes).await?;
    client.shutdown().await?;
    let meta = fs0_volume::Volume::init_volume_id(path, volume_id)?;
    print_volume_meta(meta);
    Ok(())
}

pub(super) fn inspect(path: PathBuf) -> Fs0Result<()> {
    let volume = fs0_volume::Volume::open(
        path,
        VOLUME_READ_CONCURRENCY as u32,
        VOLUME_WRITE_CONCURRENCY as u32,
    )?;
    print_volume_meta(volume.meta());
    Ok(())
}

async fn connect_storage_control(config: PathBuf) -> Fs0Result<Fs0Client> {
    let config = Fs0Config::load_from(config)?.storage()?;
    Fs0Client::connect(
        ClientConfig {
            token: config.token,
            central_endpoint_id: config.central_endpoint_id,
            central_addr: config.central_addr,
            relay: config.relay,
        },
        ClientOptions::default(),
    )
    .await
}

fn parse_bytes(value: &str) -> Fs0Result<u64> {
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
