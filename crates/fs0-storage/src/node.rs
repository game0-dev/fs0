use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use fs0_core::{
    ChunkId, ControlRequest, ControlResponse, DataRequest, DataResponse, RegisterStorageRequest,
    SessionMessage, StorageChunkEvent, StorageChunkEvents, StorageVolumeInfo, blake3_hash,
};
use fs0_transport::{
    bind_endpoint, connect_control, control_rpc, encode_endpoint_addr, read_frame, write_frame,
};
use fs0_volume::{ChunkMeta, Volume, VolumeMeta};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, SendStream},
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
    event_task: JoinHandle<()>,
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

    pub async fn has_chunk(&self, volume_id: u64, chunk_id: ChunkId) -> Result<Option<ChunkMeta>> {
        match self.volume(volume_id)?.chunk_meta(chunk_id).await {
            Ok(meta) => Ok(Some(meta)),
            Err(StorageError::Volume(fs0_volume::VolumeError::ChunkNotFound(_))) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<StorageChunkEvent>> {
        let mut per_volume = self.volumes.iter().collect::<Vec<(&u64, &VolumeHandle)>>();
        per_volume.sort_by_key(|(volume_id, _)| **volume_id);

        let mut selected_volume_id = None;
        let mut events = Vec::new();
        for (volume_id, handle) in per_volume {
            let volume_events = handle.pending_central_events(limit).await?;
            if volume_events.is_empty() {
                continue;
            }
            selected_volume_id = Some(*volume_id);
            events = volume_events;
            break;
        }
        if let Some(volume_id) = selected_volume_id {
            events.sort_by_key(|event| event.event_id);
            for event in &events {
                debug_assert_eq!(event.volume_id, volume_id);
            }
        }
        Ok(events)
    }

    pub async fn ack_pending_central_events(&self, events: &[StorageChunkEvent]) -> Result<()> {
        let mut by_volume: HashMap<u64, Vec<u64>> = HashMap::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .or_default()
                .push(event.event_id);
        }
        for (volume_id, event_ids) in by_volume {
            self.volume(volume_id)?
                .ack_pending_central_events(&event_ids)
                .await?;
        }
        Ok(())
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        events: &[StorageChunkEvent],
        failed_at_ms: u64,
    ) -> Result<()> {
        let mut by_volume: HashMap<u64, Vec<u64>> = HashMap::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .or_default()
                .push(event.event_id);
        }
        for (volume_id, event_ids) in by_volume {
            self.volume(volume_id)?
                .mark_pending_central_events_failed(&event_ids, failed_at_ms)
                .await?;
        }
        Ok(())
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
        let control = connect_control(&endpoint, &node.config.central_endpoint).await?;
        let (session_send, response) = register_storage(&control, &node, data_endpoint).await?;
        let storage_id = match response {
            SessionMessage::StorageRegistered { storage_id, .. } => storage_id,
            SessionMessage::Error(err) => {
                return Err(StorageError::UnexpectedControlResponse(
                    ControlResponse::Error(err),
                ));
            }
            _response => {
                return Err(StorageError::UnexpectedControlResponse(
                    ControlResponse::Error(fs0_core::Fs0ProtocolError::InvalidRequest),
                ));
            }
        };

        let data_task = spawn_data_accept_loop(endpoint.clone(), node.clone());
        let event_task = spawn_central_event_sync(node.clone(), control.clone());

        Ok(Self {
            storage_id,
            node,
            control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
            data_task,
            event_task,
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
            ControlResponse::Ping => Ok(()),
            response => Err(StorageError::UnexpectedControlResponse(response)),
        }
    }
}

impl Drop for StorageDaemon {
    fn drop(&mut self) {
        self.data_task.abort();
        self.event_task.abort();
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

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<StorageChunkEvent>> {
        Ok(self.volume.pending_central_events(limit).await?)
    }

    pub async fn ack_pending_central_events(&self, event_ids: &[u64]) -> Result<()> {
        Ok(self.volume.ack_pending_central_events(event_ids).await?)
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        event_ids: &[u64],
        failed_at_ms: u64,
    ) -> Result<()> {
        Ok(self
            .volume
            .mark_pending_central_events_failed(event_ids, failed_at_ms)
            .await?)
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
    let request = SessionMessage::RegisterStorage {
        request: RegisterStorageRequest {
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
        },
    };
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
                loop {
                    let Ok((send, recv)) = connection.accept_bi().await else {
                        break;
                    };
                    let node = node.clone();
                    tokio::spawn(async move {
                        handle_data_stream(node, send, recv).await;
                    });
                }
            });
        }
    })
}

async fn handle_data_stream(node: StorageNode, mut send: SendStream, mut recv: RecvStream) {
    let Ok(request) = read_frame::<DataRequest, _>(&mut recv).await else {
        return;
    };
    let response = match request {
        DataRequest::Ping => DataResponse::Pong,
        DataRequest::HasChunk {
            volume_id,
            chunk_id,
        } => match node.has_chunk(volume_id, chunk_id).await {
            Ok(Some(meta)) => DataResponse::ChunkPresence {
                exists: true,
                raw_len: Some(meta.raw_len),
                compressed_len: Some(meta.compressed_len),
            },
            Ok(None) => DataResponse::ChunkPresence {
                exists: false,
                raw_len: None,
                compressed_len: None,
            },
            Err(err) => DataResponse::Error(storage_error_to_protocol_error(&err)),
        },
        DataRequest::UploadChunk {
            volume_id,
            chunk_id,
            raw_len,
            compressed_bytes,
        } => {
            let compressed_len = compressed_bytes.len() as u64;
            if blake3_hash(&compressed_bytes) != chunk_id {
                DataResponse::Error(fs0_core::Fs0ProtocolError::HashMismatch)
            } else {
                match node
                    .put_chunk(volume_id, chunk_id, raw_len, compressed_bytes)
                    .await
                {
                    Ok(_) => DataResponse::ChunkStored {
                        chunk_id,
                        raw_len,
                        compressed_len,
                    },
                    Err(err) => DataResponse::Error(storage_error_to_protocol_error(&err)),
                }
            }
        }
        DataRequest::GetChunk {
            volume_id,
            chunk_id,
        } => match node.read_chunk(volume_id, chunk_id).await {
            Ok(bytes) => DataResponse::Bytes(bytes),
            Err(err) => DataResponse::Error(storage_error_to_protocol_error(&err)),
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
            Err(err) => DataResponse::Error(storage_error_to_protocol_error(&err)),
        },
        DataRequest::RepairCopy { .. } => DataResponse::RepairStarted,
    };
    let _ = write_frame(&mut send, &response).await;
    let _ = send.finish();
}

fn storage_error_to_protocol_error(err: &StorageError) -> fs0_core::Fs0ProtocolError {
    match err {
        StorageError::UnknownVolume(_) => fs0_core::Fs0ProtocolError::UnknownVolume,
        StorageError::Volume(fs0_volume::VolumeError::ChunkNotFound(_)) => {
            fs0_core::Fs0ProtocolError::NotFound
        }
        StorageError::Volume(fs0_volume::VolumeError::CapacityExceeded { .. }) => {
            fs0_core::Fs0ProtocolError::CapacityExceeded
        }
        StorageError::Volume(fs0_volume::VolumeError::HashMismatch { .. }) => {
            fs0_core::Fs0ProtocolError::HashMismatch
        }
        StorageError::Volume(fs0_volume::VolumeError::InvalidChunk(_)) => {
            fs0_core::Fs0ProtocolError::InvalidRequest
        }
        StorageError::DuplicateVolumeId(_)
        | StorageError::UnexpectedControlResponse(_)
        | StorageError::VolumeIdMismatch { .. }
        | StorageError::Io(_)
        | StorageError::TomlDecode(_)
        | StorageError::Transport(_)
        | StorageError::Volume(_) => fs0_core::Fs0ProtocolError::Internal,
    }
}

fn spawn_central_event_sync(node: StorageNode, control: Connection) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        loop {
            interval.tick().await;
            let Ok(events) = node.pending_central_events(128).await else {
                continue;
            };
            if events.is_empty() {
                continue;
            }
            let request = ControlRequest::RecordChunkEvents(StorageChunkEvents {
                events: events.clone(),
            });
            match control_rpc(&control, request).await {
                Ok(ControlResponse::RecordChunkEvents) => {
                    let _ = node.ack_pending_central_events(&events).await;
                }
                Ok(_) | Err(_) => {
                    let _ = node
                        .mark_pending_central_events_failed(&events, now_ms())
                        .await;
                }
            }
        }
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
