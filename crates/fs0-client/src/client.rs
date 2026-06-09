mod control;
mod data;
mod download;
mod endpoint;
mod upload;

pub use fs0_config::ClientConfig;
use fs0_config::Fs0Config;
use fs0_core::{
    DEFAULT_CLIENT_DATA_CONCURRENCY, FS0_VERSION, Fs0Error, Fs0Result, HashId,
    TRANSPORT_CONTROL_ALPN,
    protocol::{
        ControlRequest, ControlResponse, ProtocolRequest, ProtocolResponse, StoragePeerInfo,
    },
};
use fs0_transport::{Connection, EndpointAddr, Transport};
use parking_lot::RwLock;
use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOptions {
    pub name: Option<String>,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
    pub download_cache_enabled: bool,
    pub download_cache_dir: Option<PathBuf>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            name: None,
            upload_concurrency: DEFAULT_CLIENT_DATA_CONCURRENCY,
            download_concurrency: DEFAULT_CLIENT_DATA_CONCURRENCY,
            download_cache_enabled: true,
            download_cache_dir: default_download_cache_dir(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOptions {
    pub limit: u32,
    pub cursor: Option<u64>,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteOptions {
    pub prefer_volume_name: Option<String>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadRange {
    pub offset: u64,
    pub len: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub chunks: u64,
    pub bundles: u64,
    pub downloaded_compressed_bytes: u64,
    pub cached_compressed_bytes: u64,
    pub downloaded_chunks: u64,
    pub cached_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentralStatus {
    pub clients_count: u32,
    pub storages: Vec<StoragePeerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageTarget {
    pub storage_id: u64,
    pub volume_id: u64,
    pub iroh_endpoint: Vec<u8>,
}

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
    pub(super) options: ClientOptions,
    pub(super) token: String,
    pub(super) client_id: Arc<RwLock<u64>>,
    pub(super) central_endpoint: EndpointAddr,
    pub(super) control: Arc<Mutex<Connection>>,
    pub(super) endpoint: Transport,
    pub(super) storages: Arc<RwLock<Vec<StoragePeerInfo>>>,
}

impl Fs0Client {
    pub async fn connect(config: ClientConfig, options: ClientOptions) -> Fs0Result<Self> {
        let endpoint = Transport::bind(Vec::new(), None, None, config.relay.clone()).await?;
        let central_endpoint = endpoint::central_endpoint_addr(&config)?;
        let token = config.token;
        let (control, client_id, storages) =
            connect_registered_control(&endpoint, central_endpoint.clone(), &options, &token)
                .await?;

        Ok(Self {
            options,
            token,
            client_id: Arc::new(RwLock::new(client_id)),
            central_endpoint,
            control: Arc::new(Mutex::new(control)),
            endpoint,
            storages: Arc::new(RwLock::new(storages)),
        })
    }

    pub async fn connect_from_config(
        path: impl AsRef<Path>,
        options: ClientOptions,
    ) -> Fs0Result<Self> {
        Self::connect(Fs0Config::load_from(path)?.client()?, options).await
    }

    pub async fn shutdown(&self) -> Fs0Result<()> {
        self.control.lock().await.close(b"fs0 client shutdown");
        self.endpoint.close().await;

        Ok(())
    }

    #[must_use]
    pub fn client_id(&self) -> u64 {
        *self.client_id.read()
    }

    pub fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storages.read().clone()
    }

    pub(super) fn set_storage_peers(&self, storages: Vec<StoragePeerInfo>) {
        *self.storages.write() = storages;
    }

    pub(super) async fn reconnect_control(&self, control: &mut Connection) -> Fs0Result<()> {
        let (new_control, client_id, storages) = connect_registered_control(
            &self.endpoint,
            self.central_endpoint.clone(),
            &self.options,
            &self.token,
        )
        .await?;

        control.close(b"fs0 client reconnect");
        *control = new_control;
        *self.client_id.write() = client_id;
        self.set_storage_peers(storages);

        Ok(())
    }
}

fn default_download_cache_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .map(|home| home.join(".fs0").join("cache"))
}

pub(super) async fn request_control(
    connection: &Connection,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
    request_control_result(connection, request)
        .await
        .map_err(ControlRequestError::into_error)
}

pub(super) async fn request_control_result(
    connection: &Connection,
    request: ControlRequest,
) -> Result<ControlResponse, ControlRequestError> {
    match connection
        .rpc(ProtocolRequest::Control(request))
        .await
        .map_err(ControlRequestError::Rpc)?
    {
        ProtocolResponse::Control(ControlResponse::Error(err)) | ProtocolResponse::Error(err) => {
            Err(ControlRequestError::Response(err))
        }
        ProtocolResponse::Control(response) => Ok(response),
        response => {
            unexpected_protocol_control_response(response).map_err(ControlRequestError::Rpc)
        }
    }
}

pub(super) enum ControlRequestError {
    Rpc(Fs0Error),
    Response(Fs0Error),
}

impl ControlRequestError {
    pub(super) fn into_error(self) -> Fs0Error {
        match self {
            Self::Rpc(err) | Self::Response(err) => err,
        }
    }
}

pub(super) fn unexpected_control_response<T>(response: ControlResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected control response: {response:?}"),
    })
}

pub(super) fn unexpected_protocol_control_response<T>(response: ProtocolResponse) -> Fs0Result<T> {
    Err(Fs0Error::InvalidFrame {
        message: format!("unexpected control response: {response:?}"),
    })
}

async fn connect_registered_control(
    endpoint: &Transport,
    central_endpoint: EndpointAddr,
    options: &ClientOptions,
    token: &str,
) -> Fs0Result<(Connection, u64, Vec<StoragePeerInfo>)> {
    let control = endpoint
        .connect(central_endpoint, TRANSPORT_CONTROL_ALPN)
        .await?;
    let response = request_control(
        &control,
        ControlRequest::RegisterClient {
            name: options.name.clone(),
            token: token.to_owned(),
            version: FS0_VERSION.to_owned(),
        },
    )
    .await?;

    match response {
        ControlResponse::RegisterClient {
            client_id,
            storages,
        } => Ok((control, client_id, storages)),
        response => unexpected_control_response(response),
    }
}
