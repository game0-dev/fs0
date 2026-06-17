pub(crate) mod bundle_reporter;
mod central_connection;
mod tasks;

use crate::{Fs0Result, StorageConfig};
use fs0_core::{Fs0Error, protocol::StorageVolumeInfo};
use fs0_transport::Transport;
use fs0_volume::{Volume, VolumeMeta};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::info;

pub(crate) use bundle_reporter::BundleReporter;
pub(crate) use central_connection::CentralConnection;

#[derive(Debug)]
pub struct StorageServer {
    pub(crate) config: Arc<StorageConfig>,
    pub(crate) central_connection: CentralConnection,
    pub(crate) bundle_reporter: BundleReporter,
    pub(crate) volumes: Arc<HashMap<u64, Arc<Volume>>>,
    pub(crate) upload_leases: RwLock<HashMap<u64, UploadLeaseState>>,
    transport: Transport,
    exit: AtomicBool,
    shutdown_notify: Arc<Notify>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadLeaseState {
    pub(crate) file_id: u64,
    pub(crate) volume_id: u64,
    pub(crate) expires_at_ms: u64,
}

impl StorageServer {
    pub async fn run(config: StorageConfig) -> Fs0Result<Arc<Self>> {
        let volumes = open_volumes(&config)?;
        let volumes = Arc::new(volumes);
        let bind_addr = config
            .bind_port
            .map(|port| SocketAddr::from(([0, 0, 0, 0], port)));
        let transport = Transport::bind(
            vec![fs0_core::TRANSPORT_DATA_ALPN],
            None,
            bind_addr,
            config.relay.clone(),
        )
        .await?;
        info!(endpoint = ?transport.addr(), "storage data transport bound");
        let central_connection = CentralConnection::new();
        central_connection
            .connect_and_register(&config, &transport, &volumes)
            .await?;
        info!(
            storage_id = central_connection.storage_id(),
            "storage registered with central"
        );

        let server = Arc::new(Self {
            config: Arc::new(config),
            central_connection,
            bundle_reporter: BundleReporter::new(&volumes),
            volumes,
            upload_leases: RwLock::new(HashMap::new()),
            transport,
            exit: AtomicBool::new(false),
            shutdown_notify: Arc::new(Notify::new()),
            tasks: Mutex::new(Vec::new()),
        });

        server.central_connection.spawn(Arc::downgrade(&server))?;

        server.tasks.lock().extend([
            tasks::spawn_connection_accept_loop(
                server.transport.clone(),
                Arc::downgrade(&server),
                server.shutdown_notify.clone(),
            ),
            tasks::spawn_bundle_reporter_loop(
                Arc::downgrade(&server),
                server.shutdown_notify.clone(),
            ),
            tasks::spawn_idle_file_close_loop(
                Arc::downgrade(&server),
                server.shutdown_notify.clone(),
            ),
        ]);

        Ok(server)
    }

    pub async fn run_config(path: impl AsRef<Path>) -> Fs0Result<Arc<Self>> {
        Self::run(fs0_config::Fs0Config::load_storage_from(path)?).await
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
    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    pub async fn shutdown(&self) {
        if self.exit.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shutdown_notify.notify_waiters();
        self.central_connection.close(b"storage shutdown");
        self.transport.close().await;

        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
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

fn open_volumes(config: &StorageConfig) -> Fs0Result<HashMap<u64, Arc<Volume>>> {
    let mut seen_ids = HashSet::with_capacity(config.volumes.len());
    let mut seen_names = HashSet::with_capacity(config.volumes.len());
    let mut volumes = HashMap::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        if !seen_names.insert(volume_config.name.clone()) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume name {}", volume_config.name),
            });
        }

        let volume = Volume::open(
            &volume_config.path,
            volume_config.read_concurrency,
            volume_config.write_concurrency,
        )?;
        let meta = volume.meta();
        if meta.volume_id == 0 {
            return Err(Fs0Error::InvalidConfig {
                message: format!(
                    "volume {} has not been assigned a central volume id",
                    volume_config.path.display()
                ),
            });
        }
        if !seen_ids.insert(meta.volume_id) {
            return Err(Fs0Error::InvalidConfig {
                message: format!("duplicate volume id {}", meta.volume_id),
            });
        }

        volumes.insert(meta.volume_id, Arc::new(volume));
    }

    Ok(volumes)
}

pub(super) fn storage_volume_infos(
    config: &StorageConfig,
    volumes: &HashMap<u64, Arc<Volume>>,
) -> Fs0Result<Vec<StorageVolumeInfo>> {
    let mut infos = Vec::with_capacity(config.volumes.len());

    for volume_config in &config.volumes {
        let volume = volumes
            .values()
            .find(|volume| volume.root() == volume_config.path)
            .ok_or_else(|| Fs0Error::VolumeNotFound {
                path: volume_config.path.display().to_string(),
            })?;
        let meta = volume.meta();
        infos.push(StorageVolumeInfo {
            volume_id: meta.volume_id,
            name: volume_config.name.clone(),
            max_bytes: meta.max_bytes,
            max_volume_offset: meta.active_volume_offset,
            read_only: volume_config.read_only,
        });
    }

    Ok(infos)
}
