use crate::server::StorageServer;
use fs0_core::VOLUME_DATA_FILE_IDLE_TTL_MS;
use parking_lot::Mutex;
use std::sync::{Arc, Weak};
use tokio::{
    sync::Notify,
    task::JoinHandle,
    time::{Duration, interval},
};

#[derive(Debug, Default)]
pub(super) struct ServerTasks {
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ServerTasks {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn push(&self, handle: JoinHandle<()>) {
        self.handles.lock().push(handle);
    }

    pub(super) async fn join_all(&self) {
        let handles = std::mem::take(&mut *self.handles.lock());
        for handle in handles {
            let _ = handle.await;
        }
    }
}

pub(super) fn spawn_bundle_reporter_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            let _ = server.sync_bundle_change_records().await;
        }
    })
}

pub(super) fn spawn_idle_file_close_loop(
    server: Weak<StorageServer>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(VOLUME_DATA_FILE_IDLE_TTL_MS));
        loop {
            tokio::select! {
                _ = shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }

            let Some(server) = server.upgrade() else {
                break;
            };
            if server.is_exiting() {
                break;
            }

            server.close_idle_data_files();
        }
    })
}
