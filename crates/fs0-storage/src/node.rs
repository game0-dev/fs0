use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use fs0_core::{
    ChunkId, ControlRequest, ControlResponse, DataRequest, DataResponse, RegisterStorageRequest,
    SessionMessage, StorageVolumeInfo, blake3_hash,
};
use fs0_transport::{
    bind_endpoint, connect_control, control_rpc, encode_endpoint_addr, read_frame, write_frame,
};
use fs0_volume::{ChunkMeta, Volume, VolumeMeta};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct StorageNode {
    config: StorageConfig,
    volumes: Arc<HashMap<u64, VolumeHandle>>,
}

#[derive(Debug, Clone)]
pub struct VolumeHandle {
    volume: Volume,
}

#[derive(Debug)]
pub struct StorageDaemon {
    storage_id: u64,
    node: StorageNode,
    control: Connection,
    _session: Arc<Mutex<SendStream>>,
    endpoint: Endpoint,
    data_task: JoinHandle<()>,
}

impl StorageNode {
    pub fn open(config: StorageConfig) -> Result<Self> {
        let mut seen = HashSet::with_capacity(config.volumes.len());
        let mut volumes = HashMap::with_capacity(config.volumes.len());

        for volume_config in &config.volumes {
            if !seen.insert(volume_config.volume_id) {
                return Err(StorageError::DuplicateVolumeId(volume_config.volume_id));
            }

            let volume = Volume::open(&volume_config.path)?;
            let meta = volume.meta();
            if meta.volume_id != volume_config.volume_id {
                return Err(StorageError::VolumeIdMismatch {
                    path: volume_config.path.clone(),
                    configured: volume_config.volume_id,
                    actual: meta.volume_id,
                });
            }

            volumes.insert(volume_config.volume_id, VolumeHandle { volume });
        }

        Ok(Self {
            config,
            volumes: Arc::new(volumes),
        })
    }

    pub fn open_config(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(StorageConfig::load_from(path)?)
    }

    #[must_use]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    pub fn volumes(&self) -> Vec<VolumeMeta> {
        let mut volumes = self
            .volumes
            .values()
            .map(VolumeHandle::meta)
            .collect::<Vec<_>>();
        volumes.sort_by_key(|volume| volume.volume_id);
        volumes
    }

    pub fn volume(&self, volume_id: u64) -> Result<VolumeHandle> {
        self.volumes
            .get(&volume_id)
            .cloned()
            .ok_or(StorageError::UnknownVolume(volume_id))
    }

    pub async fn put_chunk(
        &self,
        volume_id: u64,
        chunk_id: ChunkId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        self.volume(volume_id)?
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, volume_id: u64, chunk_id: ChunkId) -> Result<Vec<u8>> {
        self.volume(volume_id)?.read_chunk(chunk_id).await
    }

    pub async fn chunk_meta(&self, volume_id: u64, chunk_id: ChunkId) -> Result<ChunkMeta> {
        self.volume(volume_id)?.chunk_meta(chunk_id).await
    }
}

impl StorageDaemon {
    pub async fn start(config: StorageConfig) -> Result<Self> {
        let node = StorageNode::open(config)?;
        let endpoint = bind_endpoint(
            &node.config.p2p_relay.public_url,
            node.config.p2p_relay.quic_port,
            vec![fs0_core::DATA_ALPN.to_vec()],
        )
        .await?;
        let data_endpoint = encode_endpoint_addr(&endpoint)?;
        let data_task = spawn_data_accept_loop(endpoint.clone(), node.clone());

        let control = connect_control(&endpoint, &node.config.central_endpoint).await?;
        let (session_send, response) = register_storage(&control, &node, data_endpoint).await?;
        let storage_id = match response {
            SessionMessage::StorageRegistered { storage_id } => storage_id,
            SessionMessage::Error(err) => {
                return Err(StorageError::UnexpectedControlResponse(
                    ControlResponse::Error(err),
                ));
            }
            response => {
                return Err(StorageError::UnexpectedControlResponse(
                    ControlResponse::Error(fs0_core::ControlError {
                        code: fs0_core::ControlErrorCode::InvalidRequest,
                        message: format!("unexpected session response: {response:?}"),
                    }),
                ));
            }
        };

        Ok(Self {
            storage_id,
            node,
            control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
            data_task,
        })
    }

    pub async fn start_config(path: impl AsRef<Path>) -> Result<Self> {
        Self::start(StorageConfig::load_from(path)?).await
    }

    #[must_use]
    pub fn storage_id(&self) -> u64 {
        self.storage_id
    }

    #[must_use]
    pub fn node(&self) -> &StorageNode {
        &self.node
    }

    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub async fn ping_central(&self) -> Result<()> {
        match control_rpc(&self.control, ControlRequest::Ping).await? {
            ControlResponse::Pong => Ok(()),
            response => Err(StorageError::UnexpectedControlResponse(response)),
        }
    }
}

impl Drop for StorageDaemon {
    fn drop(&mut self) {
        self.data_task.abort();
    }
}

impl VolumeHandle {
    pub fn meta(&self) -> VolumeMeta {
        self.volume.meta()
    }

    pub async fn put_chunk(
        &self,
        chunk_id: ChunkId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        Ok(self
            .volume
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await?)
    }

    pub async fn read_chunk(&self, chunk_id: ChunkId) -> Result<Vec<u8>> {
        Ok(self.volume.read_chunk(chunk_id).await?)
    }

    pub async fn chunk_meta(&self, chunk_id: ChunkId) -> Result<ChunkMeta> {
        Ok(self.volume.chunk_meta(chunk_id).await?)
    }

    pub async fn delete_chunk(&self, chunk_id: ChunkId) -> Result<()> {
        Ok(self.volume.delete_chunk(chunk_id).await?)
    }
}

async fn register_storage(
    control: &Connection,
    node: &StorageNode,
    data_endpoint: Vec<u8>,
) -> Result<(SendStream, SessionMessage)> {
    let request = SessionMessage::RegisterStorage(RegisterStorageRequest {
        storage_id: node.config.storage_id,
        name: node.config.name.clone(),
        volumes: node
            .volumes()
            .into_iter()
            .map(|volume| StorageVolumeInfo {
                volume_id: volume.volume_id,
                name: None,
                max_bytes: volume.max_bytes,
                active_volume_offset: volume.active_volume_offset,
            })
            .collect(),
        data_endpoint,
    });
    let (mut send, mut recv) = control.open_bi().await.map_err(|err| {
        StorageError::Transport(fs0_transport::TransportError::Iroh(err.to_string()))
    })?;
    write_frame(&mut send, &request).await?;
    let response = read_frame(&mut recv).await?;
    Ok((send, response))
}

fn spawn_data_accept_loop(endpoint: Endpoint, node: StorageNode) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let node = node.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.await else {
                    return;
                };
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    return;
                };
                let Ok(request) = read_frame::<DataRequest, _>(&mut recv).await else {
                    return;
                };
                let response = match request {
                    DataRequest::Ping => DataResponse::Pong,
                    DataRequest::UploadChunk {
                        volume_id,
                        chunk_id,
                        raw_len,
                        compressed_bytes,
                        upload_token: _,
                    } => {
                        let compressed_len = compressed_bytes.len() as u64;
                        if blake3_hash(&compressed_bytes) != chunk_id {
                            DataResponse::Bytes(Vec::new())
                        } else if node
                            .put_chunk(volume_id, chunk_id, raw_len, compressed_bytes)
                            .await
                            .is_err()
                        {
                            DataResponse::Bytes(Vec::new())
                        } else {
                            DataResponse::ChunkStored {
                                chunk_id,
                                raw_len,
                                compressed_len,
                            }
                        }
                    }
                    DataRequest::GetChunk {
                        volume_id,
                        chunk_id,
                    } => match node.read_chunk(volume_id, chunk_id).await {
                        Ok(bytes) => DataResponse::Bytes(bytes),
                        Err(_) => DataResponse::Bytes(Vec::new()),
                    },
                    DataRequest::GetRange {
                        volume_id,
                        chunk_id,
                        offset,
                        len,
                    } => match node.read_chunk(volume_id, chunk_id).await {
                        Ok(bytes) => {
                            let start = usize::try_from(offset).unwrap_or(usize::MAX);
                            let len = usize::try_from(len).unwrap_or(usize::MAX);
                            if start >= bytes.len() {
                                DataResponse::Bytes(Vec::new())
                            } else {
                                let end = start.saturating_add(len).min(bytes.len());
                                DataResponse::Bytes(bytes[start..end].to_vec())
                            }
                        }
                        Err(_) => DataResponse::Bytes(Vec::new()),
                    },
                    DataRequest::RepairCopy { .. } => DataResponse::RepairStarted,
                };
                let _ = write_frame(&mut send, &response).await;
                let _ = send.finish();
                connection.closed().await;
            });
        }
    })
}
