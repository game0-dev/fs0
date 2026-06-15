pub(crate) mod data;
mod request_scheduler;

use crate::{Fs0Error, Fs0Result};
use fs0_config::ClientConfig;
use fs0_core::{
    TRANSPORT_DATA_ALPN,
    protocol::{
        DataRequest, DataResponse, DownloadChunkRequest, ProtocolRequest, ProtocolResponse,
        UploadChunkRequest, UploadChunkResponse,
    },
};
use fs0_transport::{Connection, Transport};
use std::sync::Arc;
use tokio::sync::Mutex;

use self::request_scheduler::HashRequestScheduler;

#[derive(Debug)]
pub(crate) struct StorageSession {
    pub(crate) inner: Arc<StorageSessionInner>,
    upload_scheduler: HashRequestScheduler<(), UploadChunkRequest, UploadChunkResponse>,
    download_scheduler: HashRequestScheduler<(), DownloadChunkRequest, Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct StorageSessionInner {
    config: ClientConfig,
    transport: Transport,
    client_id: u64,
    iroh_endpoint: Vec<u8>,
    connection: Mutex<Option<Connection>>,
}

impl StorageSession {
    pub(crate) fn new(
        config: ClientConfig,
        transport: Transport,
        client_id: u64,
        iroh_endpoint: Vec<u8>,
    ) -> Self {
        let upload_concurrency = config.upload_concurrency;
        let download_concurrency = config.download_concurrency;
        let inner = Arc::new(StorageSessionInner {
            config,
            transport,
            client_id,
            iroh_endpoint,
            connection: Mutex::new(None),
        });
        let upload_scheduler = HashRequestScheduler::new(upload_concurrency, {
            let inner = Arc::clone(&inner);
            move |(), request| {
                let inner = Arc::clone(&inner);
                Box::pin(async move { inner.upload_chunk(request).await })
            }
        });
        let download_scheduler = HashRequestScheduler::new(download_concurrency, {
            let inner = Arc::clone(&inner);
            move |(), request| {
                let inner = Arc::clone(&inner);
                Box::pin(async move { inner.download_chunk(request).await })
            }
        });

        Self {
            inner,
            upload_scheduler,
            download_scheduler,
        }
    }

    pub(crate) async fn upload_chunk(
        &self,
        request: UploadChunkRequest,
    ) -> Fs0Result<Arc<UploadChunkResponse>> {
        let chunk_id = request.chunk_id;
        self.upload_scheduler.request(chunk_id, (), request).await
    }

    pub(crate) async fn download_chunk(
        &self,
        request: DownloadChunkRequest,
    ) -> Fs0Result<Arc<Vec<u8>>> {
        let chunk_id = request.chunk_id;
        self.download_scheduler.request(chunk_id, (), request).await
    }

    pub(crate) async fn close(&self, reason: &[u8]) {
        self.inner.close(reason).await;
    }
}

impl StorageSessionInner {
    pub(crate) async fn ensure_connected(&self) -> Fs0Result<Connection> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref()
            && !connection.is_closed()
        {
            return Ok(connection.clone());
        }

        if let Some(closed) = current.take() {
            closed.close(b"fs0 storage reconnect");
        }

        let data_endpoint = postcard::from_bytes(&self.iroh_endpoint).map_err(Fs0Error::from)?;
        let connection = self
            .transport
            .connect(data_endpoint, TRANSPORT_DATA_ALPN)
            .await?;
        match connection
            .rpc(ProtocolRequest::Data(DataRequest::Authenticate {
                client_id: self.client_id,
                client_token: self.config.token.clone(),
            }))
            .await
        {
            Ok(ProtocolResponse::Data(DataResponse::Authenticate {
                client_id: authenticated_client_id,
            })) if authenticated_client_id == self.client_id => {}
            Ok(ProtocolResponse::Error(err)) => {
                connection.close(b"storage authentication failed");
                return Err(err);
            }
            Ok(response) => {
                connection.close(b"storage authentication failed");
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected data response: {response:?}"),
                });
            }
            Err(err) => {
                connection.close(b"storage authentication failed");
                return Err(err);
            }
        }

        *current = Some(connection.clone());

        Ok(connection)
    }

    pub(crate) async fn request(&self, request: DataRequest) -> Fs0Result<DataResponse> {
        let connection = self.ensure_connected().await?;
        let response = match connection.rpc(ProtocolRequest::Data(request)).await? {
            ProtocolResponse::Error(err) => Err(err),
            ProtocolResponse::Data(response) => Ok(response),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected data response: {response:?}"),
            }),
        };
        if response.is_err() && connection.is_closed() {
            *self.connection.lock().await = None;
        }

        response
    }

    async fn close(&self, reason: &[u8]) {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close(reason);
        }
    }
}
