use super::{StorageServer, storage_volume_infos};
use crate::request_handlers::handle_control_request;
use fs0_config::StorageConfig;
use fs0_core::{
    FS0_VERSION, Fs0Error, Fs0Result, TRANSPORT_CONTROL_ALPN,
    protocol::{
        BundleReplicaEvent, ControlRequest, ControlResponse, ProtocolRequest, ProtocolResponse,
        StoragePeerInfo,
    },
};
use fs0_transport::{Connection, Transport};
use fs0_volume::Volume;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::sleep;
use tracing::{info, warn};

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

    pub(crate) async fn connect_and_register(
        &self,
        config: &StorageConfig,
        transport: &Transport,
        volumes: &HashMap<u64, Arc<Volume>>,
    ) -> Fs0Result<Connection> {
        let central_endpoint = config.central_endpoint.into();
        info!(endpoint = ?central_endpoint, "connecting storage to central");
        let connection = transport
            .connect(central_endpoint, TRANSPORT_CONTROL_ALPN)
            .await?;
        let (storage_id, _storages) = match self
            .register_storage(config, transport, volumes, &connection)
            .await
        {
            Ok(registered) => registered,
            Err(err) => {
                connection.close(b"storage registration failed");
                return Err(err);
            }
        };

        self.storage_id.store(storage_id, Ordering::Release);
        *self.connection.write() = Some(connection.clone());
        info!(storage_id, "storage central connection registered");
        Ok(connection)
    }
    pub(crate) fn spawn(&self, server: Weak<StorageServer>) -> Fs0Result<()> {
        let initial_connection = self
            .connection
            .read()
            .clone()
            .ok_or(Fs0Error::CentralUnavailable)?;

        tokio::spawn(async move {
            let mut next_connection = Some(initial_connection);

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
                        .connect_and_register(&server.config, server.transport(), &server.volumes)
                        .await
                    {
                        Ok(connection) => connection,
                        Err(err) => {
                            warn!(error = %err, "storage failed to reconnect to central");
                            tokio::select! {
                                _ = server.shutdown_notify.notified() => {}
                                _ = sleep(Duration::from_secs(1)) => {}
                            }
                            continue;
                        }
                    },
                };

                if server.is_exiting() {
                    return;
                }

                tokio::select! {
                    _ = server.shutdown_notify.notified() => {
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
                                        info!("storage received central control request");
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
                warn!("storage central connection closed; reconnecting");
                *server.central_connection.connection.write() = None;
                server
                    .central_connection
                    .storage_id
                    .store(0, Ordering::Release);
                tokio::select! {
                    _ = server.shutdown_notify.notified() => {}
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        });
        Ok(())
    }

    pub(crate) fn storage_id(&self) -> u64 {
        self.storage_id.load(Ordering::Acquire)
    }

    async fn register_storage(
        &self,
        config: &StorageConfig,
        transport: &Transport,
        volumes: &HashMap<u64, Arc<Volume>>,
        connection: &Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        let volumes = storage_volume_infos(config, volumes)?;
        let data_endpoint_addr = transport.addr();
        info!(endpoint = ?data_endpoint_addr, "registering storage data endpoint");
        let data_endpoint =
            postcard::to_allocvec(&data_endpoint_addr).map_err(|err| Fs0Error::InvalidFrame {
                message: format!("failed to encode storage endpoint: {err}"),
            })?;

        match connection
            .rpc(ProtocolRequest::Control(ControlRequest::RegisterStorage {
                name: config.name.clone(),
                token: config.token.clone(),
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
        let connection = self
            .connection
            .read()
            .clone()
            .ok_or(Fs0Error::CentralUnavailable)?;

        match connection
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
        let connection = self
            .connection
            .read()
            .clone()
            .ok_or(Fs0Error::CentralUnavailable)?;

        match connection
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
        let connection = self
            .connection
            .read()
            .clone()
            .ok_or(Fs0Error::CentralUnavailable)?;

        match connection
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
}
