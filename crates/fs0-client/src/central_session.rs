mod control;
use fs0_config::ClientConfig;
use fs0_core::{
    FS0_VERSION, Fs0Error, Fs0Result, TRANSPORT_CONTROL_ALPN,
    protocol::{
        ControlRequest, ControlResponse, ProtocolEvent, ProtocolRequest, ProtocolResponse,
        StoragePeerInfo,
    },
};
use fs0_transport::{ConnectOptions, ConnectRetry, Connection, Transport};
use parking_lot::RwLock;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{info, warn};

const CENTRAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const CENTRAL_CONNECT_RETRY_ATTEMPTS: usize = 3;
const CENTRAL_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(250);
const CENTRAL_CONNECT_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);
const CENTRAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

pub(crate) struct CentralSession {
    config: ClientConfig,
    transport: Transport,
    name: Option<String>,
    client_id: AtomicU64,
    event_listener_stopping: Arc<AtomicBool>,
    connection: Mutex<Option<Connection>>,
    storages: Arc<RwLock<Vec<StoragePeerInfo>>>,
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
            event_listener_stopping: Arc::new(AtomicBool::new(false)),
            connection: Mutex::new(None),
            storages: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) async fn request(&self, request: ControlRequest) -> Fs0Result<ControlResponse> {
        let connection = self.ensure_connected().await?;
        match connection
            .rpc(
                ProtocolRequest::Control(request),
                Some(CENTRAL_REQUEST_TIMEOUT),
            )
            .await?
        {
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

    pub(crate) fn storage_peer(&self, storage_id: u64) -> Option<StoragePeerInfo> {
        self.storages
            .read()
            .iter()
            .find(|storage| storage.storage_id == storage_id)
            .cloned()
    }

    pub(crate) fn set_storage_peers(&self, storages: Vec<StoragePeerInfo>) {
        *self.storages.write() = storages;
    }

    pub(crate) async fn close(&self, reason: &[u8]) {
        self.event_listener_stopping.store(true, Ordering::Release);
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
        self.event_listener_stopping.store(false, Ordering::Release);
        let connect_options = ConnectOptions::new()
            .with_timeout(CENTRAL_CONNECT_TIMEOUT)
            .with_retry(ConnectRetry::new(
                CENTRAL_CONNECT_RETRY_ATTEMPTS,
                CENTRAL_CONNECT_RETRY_DELAY,
                CENTRAL_CONNECT_RETRY_MAX_DELAY,
            ));
        let new_connection = self
            .transport
            .connect(
                central_endpoint,
                TRANSPORT_CONTROL_ALPN,
                Some(connect_options),
            )
            .await?;
        let response = match new_connection
            .rpc(
                ProtocolRequest::Control(ControlRequest::RegisterClient {
                    name: self.name.clone(),
                    token: self.config.token.clone(),
                    version: FS0_VERSION.to_owned(),
                }),
                Some(CENTRAL_REQUEST_TIMEOUT),
            )
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
        spawn_event_listener(
            new_connection.clone(),
            Arc::clone(&self.storages),
            Arc::clone(&self.event_listener_stopping),
        );
        *connection = Some(new_connection.clone());

        Ok(new_connection)
    }
}

fn spawn_event_listener(
    connection: Connection,
    storages: Arc<RwLock<Vec<StoragePeerInfo>>>,
    stopping: Arc<AtomicBool>,
) {
    let accept_task = connection.spawn_accept(
        move |request| {
            let storages = Arc::clone(&storages);
            async move {
                match request {
                    ProtocolRequest::Event(ProtocolEvent::StorageChanged(peer)) => {
                        let mut storages = storages.write();
                        match storages
                            .iter_mut()
                            .find(|storage| storage.storage_id == peer.storage_id)
                        {
                            Some(storage) => *storage = peer,
                            None => storages.push(peer),
                        }
                        storages.sort_by_key(|storage| storage.storage_id);
                        Ok(None)
                    }
                    ProtocolRequest::Event(ProtocolEvent::StorageRemoved { storage_id }) => {
                        storages
                            .write()
                            .retain(|storage| storage.storage_id != storage_id);
                        Ok(None)
                    }
                    _ => Err(Fs0Error::InvalidRequest),
                }
            }
        },
        std::future::pending(),
    );
    tokio::spawn(async move {
        match accept_task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) if !stopping.load(Ordering::Acquire) => {
                warn!(error = %err, "client central event listener stopped");
            }
            Err(err) if !stopping.load(Ordering::Acquire) => {
                warn!(error = %err, "client central event listener task failed");
            }
            Ok(Err(_)) | Err(_) => {}
        }
    });
}
