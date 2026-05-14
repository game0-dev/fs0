use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use fs0_core::{
    ControlRequest, ControlResponse, DataRequest, DataResponse, RegisterStorageRequest,
    StorageVolumeInfo,
};
use fs0_transport::{bind_data_endpoint_accepting, encode_endpoint_addr, read_frame, write_frame};
use fs0_volume::{ChunkMeta, FileMeta, Volume, VolumeMeta};
use iroh::Endpoint;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpStream;
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
    control: Arc<Mutex<TcpStream>>,
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
        file_id: u64,
        chunk_index: u64,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        self.volume(volume_id)?
            .put_chunk(file_id, chunk_index, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(
        &self,
        volume_id: u64,
        file_id: u64,
        chunk_index: u64,
    ) -> Result<Vec<u8>> {
        self.volume(volume_id)?
            .read_chunk(file_id, chunk_index)
            .await
    }

    pub async fn get_chunks_meta(
        &self,
        volume_id: u64,
        file_id: u64,
        indexes: Vec<u64>,
    ) -> Result<Vec<ChunkMeta>> {
        self.volume(volume_id)?
            .get_chunks_meta(file_id, indexes)
            .await
    }

    pub async fn commit_file(
        &self,
        volume_id: u64,
        file_id: u64,
        version: u64,
        size_bytes: u64,
        compressed_size_bytes: u64,
    ) -> Result<FileMeta> {
        self.volume(volume_id)?
            .commit_file(file_id, version, size_bytes, compressed_size_bytes)
            .await
    }

    pub async fn delete_file(&self, volume_id: u64, file_id: u64) -> Result<()> {
        self.volume(volume_id)?.delete_file(file_id).await
    }
}

impl StorageDaemon {
    pub async fn start(config: StorageConfig) -> Result<Self> {
        let node = StorageNode::open(config)?;
        let endpoint = bind_data_endpoint_accepting(
            &node.config.p2p_relay.public_url,
            node.config.p2p_relay.quic_port,
        )
        .await?;
        let data_endpoint = encode_endpoint_addr(&endpoint)?;
        let data_task = spawn_data_accept_loop(endpoint.clone());

        let mut control = TcpStream::connect(&node.config.central).await?;
        let response = register_storage(&mut control, &node, data_endpoint).await?;
        let storage_id = match response {
            ControlResponse::StorageRegistered { storage_id } => storage_id,
            response => return Err(StorageError::UnexpectedControlResponse(response)),
        };

        Ok(Self {
            storage_id,
            node,
            control: Arc::new(Mutex::new(control)),
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
        let mut control = self.control.lock().await;
        write_frame(&mut *control, &ControlRequest::Ping).await?;
        match read_frame(&mut *control).await? {
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

    pub async fn file_meta(&self, file_id: u64) -> Result<FileMeta> {
        Ok(self.volume.file_meta(file_id).await?)
    }

    pub async fn put_chunk(
        &self,
        file_id: u64,
        chunk_index: u64,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        Ok(self
            .volume
            .put_chunk(file_id, chunk_index, raw_len, compressed_bytes)
            .await?)
    }

    pub async fn read_chunk(&self, file_id: u64, chunk_index: u64) -> Result<Vec<u8>> {
        Ok(self.volume.read_chunk(file_id, chunk_index).await?)
    }

    pub async fn get_chunks_meta(&self, file_id: u64, indexes: Vec<u64>) -> Result<Vec<ChunkMeta>> {
        Ok(self.volume.get_chunks_meta(file_id, indexes).await?)
    }

    pub async fn commit_file(
        &self,
        file_id: u64,
        version: u64,
        size_bytes: u64,
        compressed_size_bytes: u64,
    ) -> Result<FileMeta> {
        Ok(self
            .volume
            .commit_file(file_id, version, size_bytes, compressed_size_bytes)
            .await?)
    }

    pub async fn delete_file(&self, file_id: u64) -> Result<()> {
        Ok(self.volume.delete_file(file_id).await?)
    }
}

async fn register_storage(
    control: &mut TcpStream,
    node: &StorageNode,
    data_endpoint: Vec<u8>,
) -> Result<ControlResponse> {
    let request = ControlRequest::RegisterStorage(RegisterStorageRequest {
        storage_id: node.config.storage_id,
        name: node.config.name.clone(),
        volumes: node
            .volumes()
            .into_iter()
            .map(|volume| StorageVolumeInfo {
                volume_id: volume.volume_id,
                max_bytes: volume.max_bytes,
                active_volume_offset: volume.active_volume_offset,
            })
            .collect(),
        data_endpoint,
    });
    write_frame(control, &request).await?;
    Ok(read_frame(control).await?)
}

fn spawn_data_accept_loop(endpoint: Endpoint) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
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
                    _ => DataResponse::Pong,
                };
                let _ = write_frame(&mut send, &response).await;
                let _ = send.finish();
                connection.closed().await;
            });
        }
    })
}
