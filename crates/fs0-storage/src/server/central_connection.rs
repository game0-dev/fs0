use super::{StorageServer, registration};
use crate::request_handlers::handle_control_request;
use fs0_core::{
    FS0_VERSION, Fs0Error, Fs0Result, TRANSPORT_CONTROL_ALPN,
    protocol::{
        BundleReplicaEvent, ControlRequest, ControlResponse, ProtocolRequest, ProtocolResponse,
        StoragePeerInfo,
    },
};
use fs0_transport::Connection;
use parking_lot::RwLock;
use std::{
    sync::{
        Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, task::JoinHandle, time::sleep};

#[derive(Debug)]
pub(crate) struct CentralConnection {
    storage_id: AtomicU64,
    connection: RwLock<Option<Connection>>,
}

impl CentralConnection {
    pub(crate) fn new() -> Self {
        Self {
            storage_id: AtomicU64::new(0),
            connection: RwLock::new(None),
        }
    }

    pub(crate) fn storage_id(&self) -> u64 {
        self.storage_id.load(Ordering::Acquire)
    }

    pub(crate) fn spawn(
        server: Weak<StorageServer>,
        shutdown_notify: std::sync::Arc<Notify>,
        initial_connection: Option<Connection>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut next_connection = initial_connection;
            loop {
                let Some(server) = server.upgrade() else {
                    return;
                };
                if server.is_exiting() {
                    return;
                }

                let connection = match next_connection.take() {
                    Some(connection) => connection,
                    None => match server
                        .central_connection
                        .connect_and_register(&server)
                        .await
                    {
                        Ok(connection) => connection,
                        Err(_) => {
                            wait_before_reconnect(&shutdown_notify).await;
                            continue;
                        }
                    },
                };

                tokio::select! {
                    _ = shutdown_notify.notified() => {
                        server.central_connection.close(b"storage shutdown");
                        return;
                    }
                    _ = connection.serve({
                        let server = server.clone();
                        move |request| {
                            let server = server.clone();
                            async move {
                                let response = match request {
                                    ProtocolRequest::Control(request) => {
                                        match handle_control_request(&server, request) {
                                            Ok(response) => ProtocolResponse::Control(response),
                                            Err(err) => ProtocolResponse::Error(err),
                                        }
                                    }
                                    _ => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                                };
                                Ok(Some(response))
                            }
                        }
                    }) => {}
                }

                connection.close(b"storage central connection closed");
                server.central_connection.clear();
                wait_before_reconnect(&shutdown_notify).await;
            }
        })
    }

    pub(crate) async fn connect_and_register(
        &self,
        server: &StorageServer,
    ) -> Fs0Result<Connection> {
        let central_endpoint = registration::central_endpoint_addr(&server.config)?;
        let connection = server
            .endpoint()
            .connect(central_endpoint, TRANSPORT_CONTROL_ALPN)
            .await?;
        let registered = self.register_storage(server, &connection).await;
        if registered.is_err() {
            connection.close(b"storage registration failed");
        }
        let (storage_id, _storages) = registered?;
        self.set_registered(storage_id, connection.clone());
        Ok(connection)
    }

    async fn register_storage(
        &self,
        server: &StorageServer,
        connection: &Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        let volumes = registration::volume_infos(&server.config, &server.volumes)?;
        let data_endpoint = postcard::to_allocvec(&server.endpoint().addr()).map_err(|err| {
            Fs0Error::InvalidFrame {
                message: format!("failed to encode storage endpoint: {err}"),
            }
        })?;

        match connection
            .rpc(ProtocolRequest::Control(ControlRequest::RegisterStorage {
                name: server.config.name.clone(),
                token: server.config.token.clone(),
                version: FS0_VERSION.to_owned(),
                volumes,
                iroh_endpoint: data_endpoint,
            }))
            .await?
        {
            ProtocolResponse::Control(ControlResponse::RegisterStorage {
                storage_id,
                storages,
            }) => Ok((storage_id, storages)),
            ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected storage registration response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn validate_client_auth(
        &self,
        client_id: u64,
        client_token: String,
    ) -> Fs0Result<()> {
        match self
            .connection()?
            .rpc(ProtocolRequest::Control(
                ControlRequest::ValidateClientAuth {
                    client_id,
                    client_token,
                },
            ))
            .await?
        {
            ProtocolResponse::Control(ControlResponse::ValidateClientAuth { client_id: _ }) => {
                Ok(())
            }
            ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected validate client auth response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn report_bundle_replica(
        &self,
        events: Vec<BundleReplicaEvent>,
    ) -> Fs0Result<()> {
        match self
            .connection()?
            .rpc(ProtocolRequest::Control(
                ControlRequest::ReportBundleReplica { events },
            ))
            .await?
        {
            ProtocolResponse::Control(ControlResponse::ReportBundleReplica) => Ok(()),
            ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected report bundle replica response: {response:?}"),
            }),
        }
    }

    pub(crate) async fn update_storage_volume_offset(
        &self,
        volume_id: u64,
        max_volume_offset: u64,
    ) -> Fs0Result<()> {
        match self
            .connection()?
            .rpc(ProtocolRequest::Control(
                ControlRequest::UpdateStorageVolumeOffset {
                    volume_id,
                    max_volume_offset,
                },
            ))
            .await?
        {
            ProtocolResponse::Control(ControlResponse::UpdateStorageVolumeOffset) => Ok(()),
            ProtocolResponse::Error(err) => Err(err),
            response => Err(Fs0Error::InvalidFrame {
                message: format!("unexpected update storage volume offset response: {response:?}"),
            }),
        }
    }

    pub(crate) fn close(&self, reason: &[u8]) {
        if let Some(connection) = self.connection.write().take() {
            connection.close(reason);
        }
        self.storage_id.store(0, Ordering::Release);
    }

    fn set_registered(&self, storage_id: u64, connection: Connection) {
        self.storage_id.store(storage_id, Ordering::Release);
        *self.connection.write() = Some(connection);
    }

    fn clear(&self) {
        *self.connection.write() = None;
        self.storage_id.store(0, Ordering::Release);
    }

    fn connection(&self) -> Fs0Result<Connection> {
        self.connection
            .read()
            .clone()
            .ok_or(Fs0Error::CentralUnavailable)
    }
}

async fn wait_before_reconnect(shutdown_notify: &Notify) {
    tokio::select! {
        _ = shutdown_notify.notified() => {}
        _ = sleep(Duration::from_secs(1)) => {}
    }
}
