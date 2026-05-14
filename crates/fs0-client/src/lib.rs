use fs0_core::{
    ControlError, ControlRequest, ControlResponse, DirectoryEntries, FileRecord, Fs0Path,
    ListDirectoryRequest, StoragePeerInfo,
};
use fs0_transport::{TransportError, bind_data_endpoint, ping_data_peer, read_frame, write_frame};
use iroh::Endpoint;
use std::sync::Arc;
use tokio::net::{TcpStream, ToSocketAddrs};
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
    control: Arc<Mutex<TcpStream>>,
    endpoint: Endpoint,
}

impl Fs0Client {
    pub async fn connect(
        central_addr: impl ToSocketAddrs,
        name: Option<String>,
        relay_url: &str,
        relay_quic_port: u16,
    ) -> Result<Self> {
        let stream = TcpStream::connect(central_addr).await?;
        let endpoint = bind_data_endpoint(relay_url, relay_quic_port).await?;
        let client = Self {
            client_id: 0,
            control: Arc::new(Mutex::new(stream)),
            endpoint,
        };
        let response = client
            .request(ControlRequest::RegisterClient { name })
            .await?;
        let client_id = match response {
            ControlResponse::ClientRegistered { client_id } => client_id,
            response => return Err(ClientError::UnexpectedControlResponse(response)),
        };

        Ok(Self {
            client_id,
            control: client.control,
            endpoint: client.endpoint,
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
        let mut control = self.control.lock().await;
        write_frame(&mut *control, &request).await?;
        Ok(read_frame(&mut *control).await?)
    }
}
