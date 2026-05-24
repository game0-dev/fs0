pub use fs0_config::{ClientConfig, ClientP2pRelayConfig};
use fs0_core::{
    AppendLease, BeginAppendRequest, CommitAppendRequest, ControlRequest, ControlResponse,
    DataRequest, DataResponse, DirectoryEntries, FileChangeLogs, FileReadPlan, Fs0Error, HashId,
    SessionMessage, StoragePeerInfo, UploadTarget,
};
use fs0_transport::{
    bind_endpoint, connect_control, connect_data, control_rpc, data_rpc, data_rpc_on_connection,
    ping_data_peer, read_frame, write_frame,
};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
};
use parking_lot::Mutex;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Fs0Error>;

pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 32;

#[derive(Debug, Clone)]
pub struct ChunkUpload {
    pub chunk_id: HashId,
    pub raw_len: u64,
    pub compressed_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkUploadResult {
    pub chunk_id: HashId,
    pub uploaded: bool,
}

#[derive(Debug, Clone)]
pub struct Fs0Client {
    client_id: u64,
    control: Connection,
    _session: Arc<Mutex<SendStream>>,
    endpoint: Endpoint,
    storages: Vec<StoragePeerInfo>,
}

impl Fs0Client {
    pub async fn connect(
        central_endpoint: &[u8],
        name: Option<String>,
        relay_url: &str,
        relay_quic_port: u16,
    ) -> Result<Self> {
        let endpoint = bind_endpoint(relay_url, relay_quic_port, Vec::new())
            .await
            .map_err(transport_error)?;
        let control = connect_control(&endpoint, central_endpoint)
            .await
            .map_err(transport_error)?;
        let (mut session_send, mut session_recv) = control
            .open_bi()
            .await
            .map_err(|err| internal_error(err.to_string()))?;
        write_frame(&mut session_send, &SessionMessage::RegisterClient { name })
            .await
            .map_err(transport_error)?;
        let response = read_frame(&mut session_recv)
            .await
            .map_err(transport_error)?;
        let (client_id, storages) = match response {
            SessionMessage::ClientRegistered {
                client_id,
                storages,
            } => (client_id, storages),
            SessionMessage::Error(err) => return Err(err),
            response => {
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected session response: {response:?}"),
                });
            }
        };

        Ok(Self {
            client_id,
            control,
            _session: Arc::new(Mutex::new(session_send)),
            endpoint,
            storages,
        })
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    pub fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storages.clone()
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
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan(&self, path: String) -> Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlan { path })
            .await?
        {
            ControlResponse::GetFileReadPlan(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_read_plan_by_id(&self, file_id: u64) -> Result<FileReadPlan> {
        match self
            .request(ControlRequest::GetFileReadPlanById { file_id })
            .await?
        {
            ControlResponse::GetFileReadPlanById(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::BeginAppend(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileReadPlan> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::CommitAppend(plan) => Ok(plan),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn abort_append(&self, lease_id: u64) -> Result<()> {
        match self
            .request(ControlRequest::AbortAppend { lease_id })
            .await?
        {
            ControlResponse::AbortAppend => Ok(()),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn get_file_change_logs(
        &self,
        after_event_id: u64,
        limit: u32,
    ) -> Result<FileChangeLogs> {
        match self
            .request(ControlRequest::GetFileChangeLogs {
                after_event_id,
                limit,
            })
            .await?
        {
            ControlResponse::GetFileChangeLogs(logs) => Ok(logs),
            ControlResponse::Error(err) => Err(err),
            response => unexpected_control_response(response),
        }
    }

    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Result<()> {
        ping_data_peer(&self.endpoint, &peer.iroh_endpoint)
            .await
            .map_err(transport_error)
    }

    pub async fn storage_has_chunk(
        &self,
        target: &UploadTarget,
        chunk_id: HashId,
    ) -> Result<Option<(u64, u64)>> {
        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::HasChunk {
                volume_id: target.volume_id,
                chunk_id,
            },
        )
        .await
        .map_err(transport_error)?
        {
            DataResponse::HasChunk {
                exists: true,
                raw_len: Some(raw_len),
                compressed_len: Some(compressed_len),
            } => Ok(Some((raw_len, compressed_len))),
            DataResponse::HasChunk { exists: false, .. } => Ok(None),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
        }
    }

    pub async fn upload_chunk_if_missing(
        &self,
        target: &UploadTarget,
        chunk_id: HashId,
        raw_len: u64,
        compressed_bytes: Vec<u8>,
    ) -> Result<bool> {
        if self.storage_has_chunk(target, chunk_id).await?.is_some() {
            return Ok(false);
        }

        match data_rpc(
            &self.endpoint,
            &target.iroh_endpoint,
            DataRequest::UploadChunk {
                volume_id: target.volume_id,
                chunk_id,
                raw_len,
                compressed_bytes,
            },
        )
        .await
        .map_err(transport_error)?
        {
            DataResponse::UploadChunk { .. } => Ok(true),
            DataResponse::Error(err) => Err(err),
            response => unexpected_data_response(response),
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
        let connection = Arc::new(
            connect_data(&self.endpoint, &target.iroh_endpoint)
                .await
                .map_err(transport_error)?,
        );
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
                    return Err(internal_error(err.to_string()));
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
        let mut peers = self.storage_peers();
        if peers.is_empty() {
            return Err(Fs0Error::NotFound);
        }
        let peer = peers.remove(0);
        self.ping_storage_peer(&peer).await?;
        Ok(peer)
    }

    async fn request(&self, request: ControlRequest) -> Result<ControlResponse> {
        control_rpc(&self.control, request)
            .await
            .map_err(transport_error)
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
    .await
    .map_err(transport_error)?
    {
        DataResponse::HasChunk { exists: true, .. } => {
            return Ok((
                index,
                ChunkUploadResult {
                    chunk_id: chunk.chunk_id,
                    uploaded: false,
                },
            ));
        }
        DataResponse::HasChunk { exists: false, .. } => {}
        DataResponse::Error(err) => return Err(err),
        response => return unexpected_data_response(response),
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
    .await
    .map_err(transport_error)?
    {
        DataResponse::UploadChunk { .. } => Ok((
            index,
            ChunkUploadResult {
                chunk_id: chunk.chunk_id,
                uploaded: true,
            },
        )),
        DataResponse::Error(err) => Err(err),
        response => unexpected_data_response(response),
    }
}

fn unexpected_control_response<T>(response: ControlResponse) -> Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected control response: {response:?}"),
    })
}

fn unexpected_data_response<T>(response: DataResponse) -> Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected data response: {response:?}"),
    })
}

fn transport_error(err: fs0_transport::TransportError) -> Fs0Error {
    internal_error(err.to_string())
}

fn internal_error(message: String) -> Fs0Error {
    Fs0Error::Internal { message }
}
