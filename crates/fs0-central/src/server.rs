mod relay;
mod spawn;

use crate::{CentralConfig, Fs0Result, db::CentralDb};
use fs0_core::{Fs0Error, TRANSPORT_CONTROL_ALPN, protocol::StoragePeerInfo};
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
            server.shutdown_notify.clone(),
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
}

impl Drop for CentralServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }
}
