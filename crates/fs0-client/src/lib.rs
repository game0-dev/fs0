use fs0_core::{
    AbortAppendRequest, AppendLease, BeginAppendRequest, ChunkPlans, CommitAppendRequest,
    ControlError, ControlRequest, ControlResponse, DirectoryEntries, FileEvents, FileManifest,
    FileRecord, Fs0Path, ListDirectoryRequest, ListFileEventsRequest, PlanChunksRequest,
    SessionMessage, StoragePeerInfo,
};
use fs0_transport::{
    TransportError, bind_endpoint, connect_control, control_rpc, ping_data_peer, read_frame,
    write_frame,
};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no storage peers are registered with central")]
    NoStoragePeers,

    #[error("unexpected control response: {0:?}")]
    UnexpectedControlResponse(ControlResponse),

    #[error("control error: {0:?}")]
    Control(ControlError),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("transport error")]
    Transport(#[from] TransportError),
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
            SessionMessage::ClientRegistered { client_id } => client_id,
            SessionMessage::Error(err) => return Err(ClientError::Control(err)),
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
            ControlResponse::StoragePeers(peers) => Ok(peers),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_files(&self) -> Result<Vec<FileRecord>> {
        match self.request(ControlRequest::ListFiles).await? {
            ControlResponse::Files(files) => Ok(files),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn get_file_record(&self, path: Fs0Path) -> Result<Option<FileRecord>> {
        match self.request(ControlRequest::GetFileRecord { path }).await? {
            ControlResponse::FileRecord(file) => Ok(file),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_directory(&self, request: ListDirectoryRequest) -> Result<DirectoryEntries> {
        match self.request(ControlRequest::ListDirectory(request)).await? {
            ControlResponse::DirectoryEntries(entries) => Ok(entries),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn begin_append(&self, request: BeginAppendRequest) -> Result<AppendLease> {
        match self.request(ControlRequest::BeginAppend(request)).await? {
            ControlResponse::AppendLease(lease) => Ok(lease),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn plan_chunks(&self, request: PlanChunksRequest) -> Result<ChunkPlans> {
        match self.request(ControlRequest::PlanChunks(request)).await? {
            ControlResponse::ChunkPlans(plans) => Ok(plans),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn commit_append(&self, request: CommitAppendRequest) -> Result<FileManifest> {
        match self.request(ControlRequest::CommitAppend(request)).await? {
            ControlResponse::AppendCommitted { file_manifest } => Ok(file_manifest),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn abort_append(&self, request: AbortAppendRequest) -> Result<()> {
        match self.request(ControlRequest::AbortAppend(request)).await? {
            ControlResponse::AppendAborted => Ok(()),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn list_file_events(&self, request: ListFileEventsRequest) -> Result<FileEvents> {
        match self
            .request(ControlRequest::ListFileEvents(request))
            .await?
        {
            ControlResponse::FileEvents(events) => Ok(events),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn get_file_manifest(&self, path: Fs0Path) -> Result<FileManifest> {
        match self
            .request(ControlRequest::GetFileManifest { path })
            .await?
        {
            ControlResponse::FileManifest(manifest) => Ok(manifest),
            ControlResponse::Error(err) => Err(ClientError::Control(err)),
            response => Err(ClientError::UnexpectedControlResponse(response)),
        }
    }

    pub async fn ping_storage_peer(&self, peer: &StoragePeerInfo) -> Result<()> {
        Ok(ping_data_peer(&self.endpoint, &peer.data_endpoint).await?)
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
