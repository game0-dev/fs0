use crate::{Fs0Error, Fs0Result, StorageConfig};
use fs0_core::protocol::StorageVolumeInfo;
use fs0_transport::{EndpointAddr, EndpointId};
use fs0_volume::Volume;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

pub(super) fn open_volumes(config: &StorageConfig) -> Fs0Result<HashMap<u64, Arc<Volume>>> {
    let mut seen_ids = HashSet::with_capacity(config.volumes.len());
    let mut seen_names = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        if !seen_names.insert(volume_config.name.clone()) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume name {}", volume_config.name),
            });
        }

        let read_concurrency = u32::try_from(volume_config.read_concurrency).map_err(|_| {
            Fs0Error::IntegerConversion {
                message: format!(
                    "read_concurrency {} exceeds u32",
                    volume_config.read_concurrency
                ),
            }
        })?;
        let write_concurrency = u32::try_from(volume_config.write_concurrency).map_err(|_| {
            Fs0Error::IntegerConversion {
                message: format!(
                    "write_concurrency {} exceeds u32",
                    volume_config.write_concurrency
                ),
            }
        })?;
        let volume = Volume::open(&volume_config.path, read_concurrency, write_concurrency)?;
        let meta = volume.meta();
        if meta.volume_id == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: format!(
                    "volume {} has not been assigned a central volume id",
                    volume_config.path.display()
                ),
            });
        }
        if !seen_ids.insert(meta.volume_id) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume id {}", meta.volume_id),
            });
        }

        volumes.insert(meta.volume_id, Arc::new(volume));
    }

    Ok(volumes)
}

pub(super) fn volume_infos(
    config: &StorageConfig,
    volumes: &HashMap<u64, Arc<Volume>>,
) -> Fs0Result<Vec<StorageVolumeInfo>> {
    let mut infos = Vec::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        let volume = volumes
            .values()
            .find(|volume| volume.root() == volume_config.path)
            .ok_or_else(|| Fs0Error::VolumeNotFound {
                path: volume_config.path.display().to_string(),
            })?;
        let meta = volume.meta();
        infos.push(StorageVolumeInfo {
            volume_id: meta.volume_id,
            name: volume_config.name.clone(),
            max_bytes: meta.max_bytes,
            max_volume_offset: meta.active_volume_offset,
            read_only: volume_config.read_only,
        });
    }

    Ok(infos)
}

pub(super) fn central_endpoint_addr(config: &StorageConfig) -> Fs0Result<EndpointAddr> {
    let endpoint_id =
        parse_endpoint_id(&config.central_endpoint_id, "storage.central_endpoint_id")?;
    let socket_addr = parse_socket_addr(&config.central_addr, "storage.central_addr")?;

    Ok(EndpointAddr::new(endpoint_id).with_ip_addr(socket_addr))
}

fn parse_endpoint_id(value: &str, field: &str) -> Fs0Result<EndpointId> {
    value
        .parse::<EndpointId>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field}: {err}"),
        })
}

fn parse_socket_addr(value: &str, field: &str) -> Fs0Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field} {value}: {err}"),
        })
}
