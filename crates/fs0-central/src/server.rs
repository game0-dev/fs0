mod relay;
mod spawn;

use crate::{CentralConfig, Fs0Result, db::CentralDb};
use fs0_core::{
    Fs0Error, TRANSPORT_CONTROL_ALPN,
    protocol::{ProtocolEvent, StoragePeerInfo, StorageVolumeInfo},
};
use fs0_transport::{Connection, EndpointAddr, SecretKey, Transport};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{info, warn};

#[derive(Debug)]
pub struct CentralServer {
    config: Arc<CentralConfig>,
    transport: Transport,
    next_id: AtomicU64,
    pub(crate) clients: RwLock<HashMap<u64, ClientControlConnection>>,
    pub(crate) storages: RwLock<HashMap<u64, StorageControlConnection>>,
    pub(crate) online_volumes: RwLock<HashMap<u64, u64>>,
    pub(crate) db: Mutex<CentralDb>,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    _join_handles: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ClientControlConnection {
    pub(crate) token: String,
    pub(crate) connection: Connection,
}

#[derive(Debug, Clone)]
pub(crate) struct StorageControlConnection {
    pub(crate) peer: StoragePeerInfo,
    pub(crate) connection: Connection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ControlConnectionIdentity {
    #[default]
    Anonymous,
    Client(u64),
    Storage(u64),
}

impl CentralServer {
    pub async fn run(config: CentralConfig) -> Fs0Result<Arc<Self>> {
        if config.replication_factor == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: "replication_factor must be greater than zero".to_owned(),
            });
        }

        let relay = relay::spawn_relay(&config.relay).await?;
        let secret_key =
            config
                .secret_key
                .parse::<SecretKey>()
                .map_err(|err| Fs0Error::InvalidConfig {
                    message: format!("invalid central.secret_key: {err}"),
                })?;
        let transport = Transport::bind(
            vec![TRANSPORT_CONTROL_ALPN],
            Some(secret_key),
            Some(SocketAddr::from(([0, 0, 0, 0], config.bind_port))),
            None,
        )
        .await?;
        info!(endpoint = ?transport.addr(), "central control transport bound");
        let db = CentralDb::open(&config.db_path)?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            transport,
            next_id: AtomicU64::new(1),
            clients: RwLock::new(HashMap::new()),
            storages: RwLock::new(HashMap::new()),
            online_volumes: RwLock::new(HashMap::new()),
            db: Mutex::new(db),
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            _join_handles: Mutex::new(None),
        });

        *server._join_handles.lock() = Some(spawn::spawn_central_tasks(
            server.transport.clone(),
            relay,
            Arc::downgrade(&server),
        ));

        Ok(server)
    }

    #[must_use]
    pub fn config(&self) -> &CentralConfig {
        &self.config
    }

    #[must_use]
    pub fn control_endpoint(&self) -> EndpointAddr {
        self.transport.addr()
    }

    pub async fn storage_peers(&self) -> Vec<StoragePeerInfo> {
        self.storage_peers_snapshot()
    }

    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.transport.close().await;

        let join_handle = self._join_handles.lock().take();
        if let Some(task) = join_handle {
            let _ = task.await;
        }
    }

    pub(crate) fn is_exiting(&self) -> bool {
        self.exit.load(Ordering::Acquire)
    }

    pub(crate) fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::AcqRel)
    }

    pub(crate) fn token_allowed(&self, token: &str) -> bool {
        self.config
            .auth_tokens
            .iter()
            .any(|allowed| allowed == token)
    }

    pub(crate) fn storage_peers_snapshot(&self) -> Vec<StoragePeerInfo> {
        let mut peers = self
            .storages
            .read()
            .values()
            .map(|storage| storage.peer.clone())
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| peer.storage_id);
        peers
    }

    pub(crate) fn storage_peer(&self, storage_id: u64) -> Option<StoragePeerInfo> {
        self.storages
            .read()
            .get(&storage_id)
            .map(|storage| storage.peer.clone())
    }

    pub(crate) async fn broadcast_event(&self, event: ProtocolEvent) {
        let connections = {
            let client_connections = self
                .clients
                .read()
                .values()
                .map(|client| client.connection.clone())
                .collect::<Vec<_>>();
            let storage_connections = self
                .storages
                .read()
                .values()
                .map(|storage| storage.connection.clone())
                .collect::<Vec<_>>();
            client_connections
                .into_iter()
                .chain(storage_connections.into_iter())
                .collect::<Vec<_>>()
        };

        for connection in connections {
            if let Err(err) = connection.send_event(&event).await {
                warn!(error = %err, event = ?event, "central failed to broadcast event");
            }
        }
    }

    pub(crate) fn register_client(
        &self,
        token: String,
        connection: Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        if !self.token_allowed(&token) {
            return Err(Fs0Error::Unauthorized);
        }

        let client_id = self.next_id();
        self.clients
            .write()
            .insert(client_id, ClientControlConnection { token, connection });

        Ok((client_id, self.storage_peers_snapshot()))
    }

    pub(crate) fn unregister_client(&self, client_id: u64) {
        if let Some(client) = self.clients.write().remove(&client_id) {
            client.connection.close(b"central client unregistered");
        }
    }

    pub(crate) fn register_storage(
        &self,
        name: String,
        token: String,
        mut volumes: Vec<StorageVolumeInfo>,
        iroh_endpoint: Vec<u8>,
        connection: Connection,
    ) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
        if !self.token_allowed(&token) {
            return Err(Fs0Error::Unauthorized);
        }

        {
            let mut db = self.db.lock();
            let tx = db.tx()?;
            for volume in &mut volumes {
                let registered = tx.get_volume(volume.volume_id)?;
                volume.name = registered.name;
                volume.max_volume_offset =
                    registered.max_volume_offset.max(volume.max_volume_offset);
                if volume.max_volume_offset != registered.max_volume_offset {
                    tx.update_volume_offset(volume.volume_id, volume.max_volume_offset)?;
                }

                if registered.max_bytes != volume.max_bytes {
                    return Err(Fs0Error::InvalidRequest);
                }
            }
            tx.commit()?;
        }

        let storage_id = self.next_id();
        let peer = StoragePeerInfo {
            storage_id,
            name,
            volumes,
            iroh_endpoint,
        };

        {
            let mut online_volumes = self.online_volumes.write();
            for volume in &peer.volumes {
                if online_volumes.contains_key(&volume.volume_id) {
                    return Err(Fs0Error::VolumeAlreadyMounted);
                }
            }

            for volume in &peer.volumes {
                online_volumes.insert(volume.volume_id, storage_id);
            }
        }

        self.storages.write().insert(
            peer.storage_id,
            StorageControlConnection { peer, connection },
        );

        Ok((storage_id, self.storage_peers_snapshot()))
    }

    pub(crate) fn unregister_storage(&self, storage_id: u64) -> Option<ProtocolEvent> {
        let removed = if let Some(storage) = self.storages.write().remove(&storage_id) {
            storage.connection.close(b"central storage unregistered");
            true
        } else {
            false
        };
        self.online_volumes
            .write()
            .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);

        removed.then_some(ProtocolEvent::StorageRemoved { storage_id })
    }

    pub(crate) fn unregister_identity(
        &self,
        identity: ControlConnectionIdentity,
    ) -> Option<ProtocolEvent> {
        match identity {
            ControlConnectionIdentity::Anonymous => None,
            ControlConnectionIdentity::Client(client_id) => {
                self.unregister_client(client_id);
                None
            }
            ControlConnectionIdentity::Storage(storage_id) => self.unregister_storage(storage_id),
        }
    }
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }
}
