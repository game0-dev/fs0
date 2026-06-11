use super::CentralConnection;
use crate::Fs0Result;
use fs0_core::Fs0Error;
use fs0_volume::Volume;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct BundleReporter {
    volume_sync_locks: HashMap<u64, Mutex<()>>,
}

impl BundleReporter {
    pub(crate) fn new(volumes: &HashMap<u64, Arc<Volume>>) -> Self {
        let volume_sync_locks = volumes
            .keys()
            .copied()
            .map(|volume_id| (volume_id, Mutex::new(())))
            .collect();
        Self { volume_sync_locks }
    }

    pub(crate) async fn sync_all(
        &self,
        central: &CentralConnection,
        volumes: &HashMap<u64, Arc<Volume>>,
    ) -> Fs0Result<()> {
        let mut per_volume = volumes.iter().collect::<Vec<_>>();
        per_volume.sort_by_key(|(volume_id, _)| **volume_id);

        for (_, volume) in per_volume {
            self.sync_volume(central, volume).await?;
        }

        Ok(())
    }

    pub(crate) async fn sync_volume(
        &self,
        central: &CentralConnection,
        volume: &Volume,
    ) -> Fs0Result<()> {
        let volume_id = volume.meta().volume_id;
        let lock = self
            .volume_sync_locks
            .get(&volume_id)
            .ok_or(Fs0Error::UnknownVolume)?;
        let _guard = lock.lock().await;

        loop {
            let mut events = volume.get_bundle_change_records(128).await?;
            if events.is_empty() {
                break;
            }

            events.sort_by_key(|event| event.event_id);
            let max_event_id = events.last().map(|event| event.event_id).unwrap_or(0);
            central.report_bundle_replica(events).await?;
            volume.remove_bundle_change_records(max_event_id).await?;
        }

        let max_volume_offset = volume.meta().active_volume_offset;
        central
            .update_storage_volume_offset(volume_id, max_volume_offset)
            .await
    }
}
