mod data;
mod download;
mod upload;

use crate::client::StorageTarget;
use fs0_config::ClientConfig;
use fs0_core::{
    Fs0Error, Fs0Result, TRANSPORT_DATA_ALPN,
    protocol::{DataRequest, DataResponse, ProtocolRequest, ProtocolResponse},
};
use fs0_transport::{Connection, Transport};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(crate) struct StorageSession {
    config: ClientConfig,
    transport: Transport,
    storage_id: u64,
    connection: Mutex<Option<Connection>>,
    upload_permits: Arc<Semaphore>,
    download_permits: Arc<Semaphore>,
}

impl StorageSession {
    pub(crate) fn new(
        config: ClientConfig,
        transport: Transport,
        storage_id: u64,
        upload_concurrency: usize,
        download_concurrency: usize,
    ) -> Self {
        Self {
            config,
            transport,
            storage_id,
            connection: Mutex::new(None),
            upload_permits: Arc::new(Semaphore::new(upload_concurrency.max(1))),
            download_permits: Arc::new(Semaphore::new(download_concurrency.max(1))),
        }
    }

    pub(crate) async fn ensure_connected(
        &self,
        client_id: u64,
        target: &StorageTarget,
    ) -> Fs0Result<Connection> {
        if target.storage_id != self.storage_id {
            return Err(Fs0Error::InvalidRequest);
        }

        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref()
            && !connection.is_closed()
        {
            return Ok(connection.clone());
        }

        if let Some(closed) = current.take() {
            closed.close(b"fs0 storage reconnect");
        }

        let data_endpoint = postcard::from_bytes(&target.iroh_endpoint).map_err(Fs0Error::from)?;
        let connection = self
            .transport
            .connect(data_endpoint, TRANSPORT_DATA_ALPN)
            .await?;
        match connection
            .rpc(ProtocolRequest::Data(DataRequest::Authenticate {
                client_id,
                client_token: self.config.token.clone(),
            }))
            .await
        {
            Ok(ProtocolResponse::Data(DataResponse::Authenticate {
                client_id: authenticated_client_id,
            })) if authenticated_client_id == client_id => {}
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

    pub(crate) async fn request(
        &self,
        client_id: u64,
        target: &StorageTarget,
        request: DataRequest,
    ) -> Fs0Result<DataResponse> {
        let connection = self.ensure_connected(client_id, target).await?;
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

    pub(crate) async fn close(&self, reason: &[u8]) {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close(reason);
        }
    }

    pub(crate) fn upload_available_permits(&self) -> usize {
        self.upload_permits.available_permits()
    }

    pub(crate) async fn acquire_upload_permit(&self) -> Fs0Result<OwnedSemaphorePermit> {
        Arc::clone(&self.upload_permits)
            .acquire_owned()
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })
    }

    pub(crate) async fn acquire_download_permit(&self) -> Fs0Result<OwnedSemaphorePermit> {
        Arc::clone(&self.download_permits)
            .acquire_owned()
            .await
            .map_err(|err| Fs0Error::Internal {
                message: err.to_string(),
            })
    }
}
