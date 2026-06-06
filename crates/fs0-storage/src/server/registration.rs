use crate::{Fs0Error, Fs0Result, StorageConfig};
use fs0_core::protocol::{
    ControlRequest, ControlResponse, ProtocolRequest, ProtocolResponse, StoragePeerInfo,
    StorageVolumeInfo,
};
use fs0_transport::{Connection, EndpointAddr, EndpointId};
use fs0_volume::Volume;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

#[derive(Debug)]
pub(super) struct OpenedVolumes {
    pub(super) volumes: HashMap<u64, Arc<Volume>>,
    pub(super) read_only_volume_ids: HashSet<u64>,
    pub(super) infos: Vec<StorageVolumeInfo>,
}

pub(super) fn open_volumes(config: &StorageConfig) -> Fs0Result<OpenedVolumes> {
    let mut seen_ids = HashSet::with_capacity(config.volumes.len());
    let mut seen_names = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());
    let mut read_only_volume_ids = HashSet::new();
    let mut infos = Vec::with_capacity(config.volumes.len());

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

        infos.push(StorageVolumeInfo {
            volume_id: meta.volume_id,
            name: volume_config.name.clone(),
            max_bytes: meta.max_bytes,
            max_volume_offset: meta.active_volume_offset,
            read_only: volume_config.read_only,
        });
        if volume_config.read_only {
            read_only_volume_ids.insert(meta.volume_id);
        }
        volumes.insert(meta.volume_id, Arc::new(volume));
    }
    infos.sort_by_key(|volume| volume.volume_id);

    Ok(OpenedVolumes {
        volumes,
        read_only_volume_ids,
        infos,
    })
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

pub(super) async fn register_storage(
    control: &Connection,
    config: &StorageConfig,
    volumes: Vec<StorageVolumeInfo>,
    data_endpoint: Vec<u8>,
) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
    match control
        .rpc(ProtocolRequest::Control(ControlRequest::RegisterStorage {
            name: config.name.clone(),
            token: config.token.clone(),
            volumes,
            iroh_endpoint: data_endpoint,
        }))
        .await?
    {
        ProtocolResponse::Control(ControlResponse::RegisterStorage {
            storage_id,
            storages,
        }) => Ok((storage_id, storages)),
        ProtocolResponse::Control(ControlResponse::Error(err)) | ProtocolResponse::Error(err) => {
            Err(err)
        }
        response => Err(Fs0Error::InvalidFrame {
            message: format!("unexpected storage registration response: {response:?}"),
        }),
    }
}
