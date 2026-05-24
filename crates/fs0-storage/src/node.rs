use crate::Result;
use crate::config::StorageConfig;
use fs0_core::{
    BundleChunkRef, BundleReplicaEvent, BundleReplicaReport, ControlRequest, ControlResponse,
    DataRequest, DataResponse, Fs0Error, HashId, RegisterStorageRequest, SessionMessage,
    StorageVolumeInfo, blake3_hash,
};
use fs0_transport::{
    bind_endpoint, connect_control, control_rpc, encode_endpoint_addr, read_frame, write_frame,
};
use fs0_volume::{BundleMeta, ChunkMeta, Volume, VolumeMeta, VolumeOptions};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, SendStream},
};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct StorageNode {
    config: StorageConfig,
    volumes: Arc<HashMap<u64, VolumeHandle>>,
}

#[derive(Debug, Clone)]
pub struct VolumeHandle {
    volume: Arc<Volume>,
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
                return Err(Fs0Error::InvalidConfig {
                    message: format!("duplicate volume id {}", volume_config.volume_id),
                });
            }

            let volume = Volume::open_with_options(
                &volume_config.path,
                VolumeOptions {
                    read_concurrency: config.volume_io.read_concurrency,
                    write_concurrency: config.volume_io.write_concurrency,
                },
            )?;
            let meta = volume.meta();
            if meta.volume_id != volume_config.volume_id {
                return Err(Fs0Error::InvalidConfig {
                    message: format!(
                        "configured volume id {} does not match volume metadata id {}: {}",
                        volume_config.volume_id,
                        meta.volume_id,
                        volume_config.path.display()
                    ),
                });
            }

            volumes.insert(
                volume_config.volume_id,
                VolumeHandle {
                    volume: Arc::new(volume),
                },
            );
        }

        Ok(Self {
            config,
            volumes: Arc::new(volumes),
        })
    }

    pub fn open_config(path: impl AsRef<Path>) -> Result<Self> {
        Self::open(fs0_config::Fs0Config::load_from(path)?.storage()?)
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
            .ok_or(Fs0Error::UnknownVolume)
    }

    pub async fn put_chunk(
        &self,
        volume_id: u64,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        self.volume(volume_id)?
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, volume_id: u64, chunk_id: HashId) -> Result<Vec<u8>> {
        self.volume(volume_id)?.read_chunk(chunk_id).await
    }

    pub async fn chunk_meta(&self, volume_id: u64, chunk_id: HashId) -> Result<ChunkMeta> {
        self.volume(volume_id)?.chunk_meta(chunk_id).await
    }

    pub async fn has_chunk(&self, volume_id: u64, chunk_id: HashId) -> Result<Option<ChunkMeta>> {
        match self.volume(volume_id)?.chunk_meta(chunk_id).await {
            Ok(meta) => Ok(Some(meta)),
            Err(Fs0Error::ChunkNotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn commit_bundle(
        &self,
        volume_id: u64,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Result<BundleMeta> {
        self.volume(volume_id)?
            .commit_bundle(bundle_id, chunks)
            .await
    }

    pub async fn list_bundle_chunks(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Result<Vec<BundleChunkRef>> {
        self.volume(volume_id)?.list_bundle_chunks(bundle_id).await
    }

    pub async fn bundle_meta(
        &self,
        volume_id: u64,
        bundle_id: HashId,
    ) -> Result<Option<(u64, u64)>> {
        let chunks = match self.list_bundle_chunks(volume_id, bundle_id).await {
            Ok(chunks) => chunks,
            Err(Fs0Error::BundleNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut raw_len = 0u64;
        let mut compressed_len = 0u64;
        for chunk in chunks {
            let meta = self.chunk_meta(volume_id, chunk.chunk_id).await?;
            raw_len =
                raw_len
                    .checked_add(meta.raw_len)
                    .ok_or_else(|| Fs0Error::IntegerConversion {
                        message: "bundle raw_len overflow".to_owned(),
                    })?;
            compressed_len = compressed_len
                .checked_add(meta.compressed_len)
                .ok_or_else(|| Fs0Error::IntegerConversion {
                    message: "bundle compressed_len overflow".to_owned(),
                })?;
        }

        Ok(Some((raw_len, compressed_len)))
    }

    pub async fn read_bundle(&self, volume_id: u64, bundle_id: HashId) -> Result<Vec<u8>> {
        let chunks = self.list_bundle_chunks(volume_id, bundle_id).await?;
        let mut bytes = Vec::new();
        for chunk in chunks {
            bytes.extend(self.read_chunk(volume_id, chunk.chunk_id).await?);
        }
        Ok(bytes)
    }

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
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

    pub async fn ack_pending_central_events(&self, events: &[BundleReplicaEvent]) -> Result<()> {
        let mut by_volume: HashMap<u64, u64> = HashMap::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .and_modify(|max_event_id| *max_event_id = (*max_event_id).max(event.event_id))
                .or_insert(event.event_id);
        }
        for (volume_id, max_event_id) in by_volume {
            self.volume(volume_id)?
                .ack_pending_central_events(max_event_id)
                .await?;
        }
        Ok(())
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        events: &[BundleReplicaEvent],
        failed_at_ms: u64,
    ) -> Result<()> {
        let mut by_volume: HashMap<u64, u64> = HashMap::new();
        for event in events {
            by_volume
                .entry(event.volume_id)
                .and_modify(|max_event_id| *max_event_id = (*max_event_id).max(event.event_id))
                .or_insert(event.event_id);
        }
        for (volume_id, max_event_id) in by_volume {
            self.volume(volume_id)?
                .mark_pending_central_events_failed(max_event_id, failed_at_ms)
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
        .await
        .map_err(transport_error)?;
        let data_endpoint = encode_endpoint_addr(&endpoint).map_err(transport_error)?;
        let control = connect_control(&endpoint, &node.config.central_endpoint)
            .await
            .map_err(transport_error)?;
        let (session_send, response) = register_storage(&control, &node, data_endpoint).await?;
        let storage_id = match response {
            SessionMessage::StorageRegistered { storage_id, .. } => storage_id,
            SessionMessage::Error(err) => return Err(err),
            response => {
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected storage registration response: {response:?}"),
                });
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
        Self::start(fs0_config::Fs0Config::load_from(path)?.storage()?).await
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

    #[must_use]
    pub fn control_connection(&self) -> &Connection {
        &self.control
    }

    pub async fn ping_central(&self) -> Result<()> {
        Err(Fs0Error::Unsupported)
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
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<ChunkMeta> {
        self.volume
            .put_chunk(chunk_id, raw_len, compressed_bytes)
            .await
    }

    pub async fn read_chunk(&self, chunk_id: HashId) -> Result<Vec<u8>> {
        self.volume.read_chunk(chunk_id).await
    }

    pub async fn chunk_meta(&self, chunk_id: HashId) -> Result<ChunkMeta> {
        self.volume.chunk_meta(chunk_id).await
    }

    pub async fn commit_bundle(
        &self,
        bundle_id: HashId,
        chunks: Vec<BundleChunkRef>,
    ) -> Result<BundleMeta> {
        self.volume.commit_bundle(bundle_id, chunks).await
    }

    pub async fn list_bundle_chunks(&self, bundle_id: HashId) -> Result<Vec<BundleChunkRef>> {
        self.volume.list_bundle_chunks(bundle_id).await
    }

    pub async fn pending_central_events(&self, limit: usize) -> Result<Vec<BundleReplicaEvent>> {
        self.volume.pending_central_events(limit).await
    }

    pub async fn ack_pending_central_events(&self, max_event_id: u64) -> Result<()> {
        self.volume.ack_pending_central_events(max_event_id).await
    }

    pub async fn mark_pending_central_events_failed(
        &self,
        max_event_id: u64,
        failed_at_ms: u64,
    ) -> Result<()> {
        self.volume
            .mark_pending_central_events_failed(max_event_id, failed_at_ms)
            .await
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
                    name: format!("volume-{}", volume.volume_id),
                    max_bytes: volume.max_bytes,
                    max_volume_offset: volume.active_volume_offset,
                })
                .collect(),
            iroh_endpoint: data_endpoint,
        },
    };
    let (mut send, mut recv) = control
        .open_bi()
        .await
        .map_err(|err| transport_error(fs0_transport::TransportError::Iroh(err.to_string())))?;
    write_frame(&mut send, &request)
        .await
        .map_err(transport_error)?;
    let response = read_frame(&mut recv).await.map_err(transport_error)?;
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
        DataRequest::HasChunk {
            volume_id,
            chunk_id,
        } => match node.has_chunk(volume_id, chunk_id).await {
            Ok(Some(meta)) => DataResponse::HasChunk {
                exists: true,
                raw_len: Some(meta.raw_len),
                compressed_len: Some(meta.compressed_len),
            },
            Ok(None) => DataResponse::HasChunk {
                exists: false,
                raw_len: None,
                compressed_len: None,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::UploadChunk {
            volume_id,
            chunk_id,
            raw_len,
            compressed_bytes,
        } => {
            let compressed_len = compressed_bytes.len() as u64;
            if blake3_hash(&compressed_bytes) != chunk_id {
                DataResponse::Error(Fs0Error::HashMismatch { volume_offset: 0 })
            } else {
                match node
                    .put_chunk(volume_id, chunk_id, raw_len, compressed_bytes)
                    .await
                {
                    Ok(_) => DataResponse::UploadChunk {
                        chunk_id,
                        raw_len,
                        compressed_len,
                    },
                    Err(err) => DataResponse::Error(err),
                }
            }
        }
        DataRequest::DownloadChunk {
            volume_id,
            chunk_id,
        } => match node.read_chunk(volume_id, chunk_id).await {
            Ok(bytes) => DataResponse::DownloadChunk {
                compressed_bytes: bytes,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::HasBundle {
            volume_id,
            bundle_id,
        } => match node.bundle_meta(volume_id, bundle_id).await {
            Ok(Some((raw_len, compressed_len))) => DataResponse::HasBundle {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            },
            Ok(None) => DataResponse::HasBundle {
                exists: false,
                raw_len: None,
                compressed_len: None,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::CommitBundle {
            volume_id,
            bundle_id,
            chunks,
        } => match node.commit_bundle(volume_id, bundle_id, chunks).await {
            Ok(bundle) => DataResponse::CommitBundle {
                bundle_id,
                raw_len: bundle.raw_len,
                compressed_len: bundle.compressed_len,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::DownloadBundle {
            volume_id,
            bundle_id,
        } => match node.read_bundle(volume_id, bundle_id).await {
            Ok(bytes) => DataResponse::DownloadBundle {
                compressed_bytes: bytes,
            },
            Err(err) => DataResponse::Error(err),
        },
        DataRequest::ListBundleChunks {
            volume_id,
            bundle_id,
        } => match node.list_bundle_chunks(volume_id, bundle_id).await {
            Ok(chunks) => DataResponse::ListBundleChunks { chunks },
            Err(err) => DataResponse::Error(err),
        },
    };
    let _ = write_frame(&mut send, &response).await;
    let _ = send.finish();
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
            let request = ControlRequest::ReportBundleReplica(BundleReplicaReport {
                events: events.clone(),
            });
            match control_rpc(&control, request).await {
                Ok(ControlResponse::ReportBundleReplica) => {
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

fn transport_error(err: fs0_transport::TransportError) -> Fs0Error {
    Fs0Error::Internal {
        message: err.to_string(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time is before unix epoch")
        .as_millis() as u64
}
