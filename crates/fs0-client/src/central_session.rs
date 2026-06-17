mod control;
use fs0_config::ClientConfig;
use fs0_core::{
    FS0_VERSION, Fs0Error, Fs0Result, TRANSPORT_CONTROL_ALPN,
    protocol::{
        ControlRequest, ControlResponse, ProtocolRequest, ProtocolResponse, StoragePeerInfo,
    },
};
use fs0_transport::{Connection, Transport};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tracing::{info, warn};

pub(crate) struct CentralSession {
    config: ClientConfig,
    transport: Transport,
    name: Option<String>,
    client_id: AtomicU64,
    connection: Mutex<Option<Connection>>,
    storages: RwLock<Vec<StoragePeerInfo>>,
}

impl std::fmt::Debug for CentralSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CentralSession")
            .field("name", &self.name)
            .field("client_id", &self.client_id())
            .field("storages", &self.storage_peers())
            .finish_non_exhaustive()
    }
}

impl CentralSession {
    pub(crate) fn new(config: ClientConfig, transport: Transport, name: Option<String>) -> Self {
        Self {
            config,
            transport,
            name,
            client_id: AtomicU64::new(0),
            connection: Mutex::new(None),
            storages: RwLock::new(Vec::new()),
        }
    }

    pub(crate) async fn request(&self, request: ControlRequest) -> Fs0Result<ControlResponse> {
        let connection = self.ensure_connected().await?;
        match connection.rpc(ProtocolRequest::Control(request)).await? {
            ProtocolResponse::Error(err) => Err(err),
            ProtocolResponse::Control(response) => Ok(response),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            }),
        }
    }

    pub(crate) fn client_id(&self) -> u64 {
        self.client_id.load(Ordering::Acquire)
    }

    pub(crate) fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storages.read().clone()
    }

    pub(crate) fn set_storage_peers(&self, storages: Vec<StoragePeerInfo>) {
        *self.storages.write() = storages;
    }

    pub(crate) async fn close(&self, reason: &[u8]) {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close(reason);
        }
        self.client_id.store(0, Ordering::Release);
    }

    pub(crate) async fn ensure_connected(&self) -> Fs0Result<Connection> {
        let mut connection = self.connection.lock().await;
        if let Some(existing) = connection.as_ref()
            && !existing.is_closed()
        {
            return Ok(existing.clone());
        }
        if let Some(closed) = connection.take() {
            closed.close(b"fs0 central reconnect");
        }

        let central_endpoint = self.config.central_endpoint.into();
        info!(endpoint = ?central_endpoint, "client connecting to central");
        let new_connection = self
            .transport
            .connect(central_endpoint, TRANSPORT_CONTROL_ALPN)
            .await?;
        let response = match new_connection
            .rpc(ProtocolRequest::Control(ControlRequest::RegisterClient {
                name: self.name.clone(),
                token: self.config.token.clone(),
                version: FS0_VERSION.to_owned(),
            }))
            .await?
        {
            ProtocolResponse::Error(err) => {
                warn!(error = %err, "client central registration failed");
                new_connection.close(b"client registration failed");
                return Err(err);
            }
            ProtocolResponse::Control(response) => response,
            response => {
                new_connection.close(b"client registration failed");
                return Err(Fs0Error::InvalidFrame {
                    message: format!("unexpected control response: {response:?}"),
                });
            }
        };
        let ControlResponse::RegisterClient {
            client_id,
            storages,
        } = response
        else {
            new_connection.close(b"client registration failed");
            return Err(Fs0Error::InvalidFrame {
                message: format!("unexpected control response: {response:?}"),
            });
        };

        info!(
            client_id,
            storages = storages.len(),
            "client registered with central"
        );
        self.client_id.store(client_id, Ordering::Release);
        *self.storages.write() = storages;
        *connection = Some(new_connection.clone());

        Ok(new_connection)
    }
}
