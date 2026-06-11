pub(crate) mod bundle_reporter;
mod central_connection;
mod client_connection;
mod registration;
mod spawn;
mod tasks;

use crate::{Fs0Result, StorageConfig};
use fs0_core::Fs0Error;
use fs0_transport::Transport;
use fs0_volume::{Volume, VolumeMeta};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Notify;

pub(crate) use bundle_reporter::BundleReporter;
pub(crate) use central_connection::CentralConnection;

#[derive(Debug)]
pub struct StorageServer {
    pub(crate) config: Arc<StorageConfig>,
    pub(crate) central_connection: CentralConnection,
    pub(crate) bundle_reporter: BundleReporter,
    pub(crate) volumes: Arc<HashMap<u64, Arc<Volume>>>,
    pub(crate) upload_leases: RwLock<HashMap<u64, UploadLeaseState>>,
    endpoint: Transport,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    tasks: tasks::ServerTasks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadLeaseState {
    pub(crate) file_id: u64,
    pub(crate) volume_id: u64,
    pub(crate) expires_at_ms: u64,
}

impl StorageServer {
    pub async fn run(config: StorageConfig) -> Fs0Result<Arc<Self>> {
        let volumes = registration::open_volumes(&config)?;
        let volumes = Arc::new(volumes);
        let bind_addr = config
            .bind_port
            .map(|port| SocketAddr::from(([0, 0, 0, 0], port)));
        let endpoint = Transport::bind(
            vec![fs0_core::TRANSPORT_DATA_ALPN],
            None,
            bind_addr,
            config.relay.clone(),
        )
        .await?;

        let server = Arc::new(Self {
            config: Arc::new(config),
            central_connection: CentralConnection::new(),
            bundle_reporter: BundleReporter::new(&volumes),
            volumes,
            upload_leases: RwLock::new(HashMap::new()),
            endpoint,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            tasks: tasks::ServerTasks::new(),
        });

        let central_connection = server
            .central_connection
            .connect_and_register(&server)
            .await?;

        server.tasks.push(CentralConnection::spawn(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
            Some(central_connection),
        ));
        server.tasks.push(spawn::spawn_connection_accept_loop(
            server.endpoint.clone(),
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        server.tasks.push(tasks::spawn_bundle_reporter_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));
        server.tasks.push(tasks::spawn_idle_file_close_loop(
            Arc::downgrade(&server),
            server.shutdown_notify.clone(),
        ));

        Ok(server)
    }

    pub async fn run_config(path: impl AsRef<Path>) -> Fs0Result<Arc<Self>> {
        Self::run(fs0_config::Fs0Config::load_from(path)?.storage()?).await
    }

    #[must_use]
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    #[must_use]
    pub fn storage_id(&self) -> u64 {
        self.central_connection.storage_id()
    }

    #[must_use]
    pub fn endpoint(&self) -> &Transport {
        &self.endpoint
    }
    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.central_connection.close(b"storage shutdown");
        self.endpoint.close().await;

        self.tasks.join_all().await;
    }

    #[must_use]
    pub(crate) fn is_exiting(&self) -> bool {
        self.exit.load(Ordering::Acquire)
    }

    pub fn volumes_meta(&self) -> Vec<VolumeMeta> {
        let mut volumes = self
            .volumes
            .values()
            .map(|volume| volume.meta())
            .collect::<Vec<_>>();
        volumes.sort_by_key(|volume| volume.volume_id);
        volumes
    }

    pub fn volume(&self, volume_id: u64) -> Fs0Result<Arc<Volume>> {
        self.volumes
            .get(&volume_id)
            .cloned()
            .ok_or(Fs0Error::UnknownVolume)
    }

    pub(crate) fn close_idle_data_files(&self) {
        for volume in self.volumes.values() {
            volume.close_idle_data_files();
        }
    }
}

impl Drop for StorageServer {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.central_connection.close(b"storage dropped");
    }
}
