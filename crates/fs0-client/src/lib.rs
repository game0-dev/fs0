use fs0_core::{
    AppendLease, BeginAppendRequest, ChunkId, ChunkPlanInput, ChunkPlans, CommitAppendRequest,
    ControlRequest, ControlResponse, DataRequest, DataResponse, DirectoryEntries, FileEvents,
    FileManifest, FileRecord, Fs0ProtocolError, SessionMessage, StoragePeerInfo, UploadTarget,
};
use fs0_transport::{
    TransportError, bind_endpoint, connect_control, connect_data, control_rpc, data_rpc,
    data_rpc_on_connection, ping_data_peer, read_frame, write_frame,
};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type Result<T> = std::result::Result<T, ClientError>;

pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no storage peers are registered with central")]
    NoStoragePeers,

    #[error("unexpected control response: {0:?}")]
    UnexpectedControlResponse(ControlResponse),

    #[error("unexpected data response: {0:?}")]
    UnexpectedDataResponse(DataResponse),

    #[error("protocol error: {0:?}")]
    Protocol(Fs0ProtocolError),

    #[error("upload task failed: {0}")]
    UploadTask(String),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("transport error")]
    Transport(#[from] TransportError),
}

#[derive(Debug, Clone)]
pub struct ChunkUpload {
    pub chunk_id: ChunkId,
    pub raw_len: u64,
    pub compressed_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkUploadResult {
    pub chunk_id: ChunkId,
    pub uploaded: bool,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    client_id: u64,
    control: Connection,
    _session: Arc<Mutex<SendStream>>,
    endpoint: Endpoint,
}

impl Fs0Client {
    pub async fn connect(
        central_endpoint: &[u8],
        name: Option<String>,
        relay_url: &str,
        relay_quic_port: u16,
    ) -> Result<Self> {
        let endpoint = bind_endpoint(relay_url, relay_quic_port, Vec::new()).await?;
        let control = connect_control(&endpoint, central_endpoint).await?;
        let (mut session_send, mut session_recv) = control
            .open_bi()
            .await
            .map_err(|err| TransportError::Iroh(err.to_string()))?;
        write_frame(&mut session_send, &SessionMessage::RegisterClient { name }).await?;
        let response = read_frame(&mut session_recv).await?;
        let client_id = match response {
            SessionMessage::ClientRegistered { client_id, .. } => client_id,
            SessionMessage::Error(err) => return Err(ClientError::Protocol(err)),
            response => {
                return Err(ClientError::Transport(TransportError::InvalidFrame(
                    format!("unexpected session response: {response:?}"),
                )));
            }
        };

        Ok(Self {
            client_id,
            control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
        })
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub async fn storage_peers(&self) -> Result<Vec<StoragePeerInfo>> {
        match self.request(ControlRequest::ListStoragePeers).await? {
            ControlResponse::ListStoragePeers(peers) => Ok(peers),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_files(&self) -> Result<Vec<FileRecord>> {
        match self.request(ControlRequest::ListFiles).await? {
            ControlResponse::ListFiles(files) => Ok(files),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn get_file_record(&self, path: String) -> Result<Option<FileRecord>> {
        match self.request(ControlRequest::GetFileRecord { path }).await? {
            ControlResponse::GetFileRecord(file) => Ok(file),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_directory(
        &self,
        dir: String,
        limit: u32,
        cursor: Option<u64>,
    ) -> Result<DirectoryEntries> {
        match self
            .request(ControlRequest::ListDirectory { dir, limit, cursor })
            .await?
        {
            ControlResponse::ListDirectory(entries) => Ok(entries),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::BeginAppend(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn plan_chunks(
        &self,
        lease_id: u64,
        chunks: Vec<ChunkPlanInput>,
    ) -> Result<ChunkPlans> {
        match self
            .request(ControlRequest::PlanChunks { lease_id, chunks })
            .await?
        {
            ControlResponse::PlanChunks(plans) => Ok(plans),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileManifest> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::CommitAppend(file_manifest) => Ok(file_manifest),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn abort_append(&self, lease_id: u64) -> Result<()> {
        match self
            .request(ControlRequest::AbortAppend { lease_id })
            .await?
        {
            ControlResponse::AbortAppend => Ok(()),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_file_events(&self, after_event_id: u64, limit: u32) -> Result<FileEvents> {
        match self
            .request(ControlRequest::ListFileEvents {
                after_event_id,
                limit,
            })
            .await?
        {
            ControlResponse::ListFileEvents(events) => Ok(events),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn get_file_manifest(&self, path: String) -> Result<FileManifest> {
        match self
            .request(ControlRequest::GetFileManifest { path })
            .await?
        {
            ControlResponse::GetFileManifest(manifest) => Ok(manifest),
            ControlResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Result<()> {
        Ok(ping_data_peer(&self.endpoint, &peer.data_endpoint).await?)
    }

    pub async fn storage_has_chunk(
        &self,
        target: &UploadTarget,
        chunk_id: ChunkId,
    ) -> Result<Option<(u64, u64)>> {
        match data_rpc(
            &self.endpoint,
            &target.data_endpoint,
            DataRequest::HasChunk {
                volume_id: target.volume_id,
                chunk_id,
            },
        )
        .await?
        {
            DataResponse::ChunkPresence {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            } => Ok(Some((raw_len, compressed_len))),
            DataResponse::ChunkPresence { exists: false, .. } => Ok(None),
            DataResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedDataResponse(response)),
        }
    }

    pub async fn upload_chunk_if_missing(
        &self,
        target: &UploadTarget,
        chunk_id: ChunkId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<bool> {
        if self.storage_has_chunk(target, chunk_id).await?.is_some() {
            return Ok(false);
        }

        match data_rpc(
            &self.endpoint,
            &target.data_endpoint,
            DataRequest::UploadChunk {
                volume_id: target.volume_id,
                chunk_id,
                raw_len,
                compressed_bytes,
            },
        )
        .await?
        {
            DataResponse::ChunkStored { .. } => Ok(true),
            DataResponse::Error(err) => Err(ClientError::Protocol(err)),
            response => Err(ClientError::UnexpectedDataResponse(response)),
        }
    }

    pub async fn upload_chunks_if_missing(
        &self,
        target: &UploadTarget,
        chunks: Vec<ChunkUpload>,
    ) -> Result<Vec<ChunkUploadResult>> {
        self.upload_chunks_if_missing_with_concurrency(target, chunks, DEFAULT_UPLOAD_CONCURRENCY)
            .await
    }

    pub async fn upload_chunks_if_missing_with_concurrency(
        &self,
        target: &UploadTarget,
        chunks: Vec<ChunkUpload>,
        concurrency: usize,
    ) -> Result<Vec<ChunkUploadResult>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = concurrency.max(1);
        let connection = Arc::new(connect_data(&self.endpoint, &target.data_endpoint).await?);
        let mut chunk_iter = chunks.into_iter().enumerate();
        let mut upload_tasks = tokio::task::JoinSet::new();
        let mut results = Vec::new();

        loop {
            while upload_tasks.len() < concurrency {
                let Some((index, chunk)) = chunk_iter.next() else {
                    break;
                };
                let connection = connection.clone();
                let volume_id = target.volume_id;
                upload_tasks.spawn(async move {
                    upload_chunk_if_missing_on_connection(index, connection, volume_id, chunk).await
                });
            }

            if upload_tasks.is_empty() {
                break;
            }

            match upload_tasks.join_next().await {
                Some(Ok(Ok(result))) => results.push(result),
                Some(Ok(Err(err))) => {
                    upload_tasks.abort_all();
                    connection.close(0u32.into(), b"fs0 upload failed");
                    return Err(err);
                }
                Some(Err(err)) => {
                    upload_tasks.abort_all();
                    connection.close(0u32.into(), b"fs0 upload task failed");
                    return Err(ClientError::UploadTask(err.to_string()));
                }
                None => break,
            }
        }

        connection.close(0u32.into(), b"fs0 upload complete");
        results.sort_by_key(|(index, _)| *index);
        Ok(results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>())
    }

    pub async fn ping_first_storage_peer(&self) -> Result<StoragePeerInfo> {
        let mut peers = self.storage_peers().await?;
        if peers.is_empty() {
            return Err(ClientError::NoStoragePeers);
        }
        let peer = peers.remove(0);
        self.ping_storage_peer(&peer).await?;
        Ok(peer)
    }

    async fn request(&self, request: ControlRequest) -> Result<ControlResponse> {
        Ok(control_rpc(&self.control, request).await?)
    }
}

async fn upload_chunk_if_missing_on_connection(
    index: usize,
    connection: Arc<Connection>,
    volume_id: u64,
    chunk: ChunkUpload,
) -> Result<(usize, ChunkUploadResult)> {
    match data_rpc_on_connection(
        &connection,
        DataRequest::HasChunk {
            volume_id,
            chunk_id: chunk.chunk_id,
        },
    )
    .await?
    {
        DataResponse::ChunkPresence { exists: true, .. } => {
            return Ok((
                index,
                ChunkUploadResult {
                    chunk_id: chunk.chunk_id,
                    uploaded: false,
                },
            ));
        }
        DataResponse::ChunkPresence { exists: false, .. } => {}
        DataResponse::Error(err) => return Err(ClientError::Protocol(err)),
        response => return Err(ClientError::UnexpectedDataResponse(response)),
    }

    match data_rpc_on_connection(
        &connection,
        DataRequest::UploadChunk {
            volume_id,
            chunk_id: chunk.chunk_id,
            raw_len: chunk.raw_len,
            compressed_bytes: chunk.compressed_bytes,
        },
    )
    .await?
    {
        DataResponse::ChunkStored { .. } => Ok((
            index,
            ChunkUploadResult {
                chunk_id: chunk.chunk_id,
                uploaded: true,
            },
        )),
        DataResponse::Error(err) => Err(ClientError::Protocol(err)),
        response => Err(ClientError::UnexpectedDataResponse(response)),
    }
}
