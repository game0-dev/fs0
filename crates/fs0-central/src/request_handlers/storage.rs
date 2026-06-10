use crate::{Fs0Error, Fs0Result, server::CentralServer};
use fs0_core::protocol::{BundleReplicaEvent, BundleReplicaEventKind, ControlResponse};

pub(super) fn validate_client_auth(
    server: &CentralServer,
    client_id: u64,
    client_token: String,
) -> Fs0Result<ControlResponse> {
    let clients = server.clients.read();
    let Some(client) = clients.get(&client_id) else {
        return Err(Fs0Error::Unauthorized);
    };

    if client.token == client_token {
        Ok(ControlResponse::ValidateClientAuth { client_id })
    } else {
        Err(Fs0Error::Unauthorized)
    }
}

pub(super) fn create_volume(
    server: &CentralServer,
    name: String,
    max_bytes: u64,
) -> Fs0Result<ControlResponse> {
    let mut db = server.db.lock();
    let tx = db.tx()?;
    let volume = tx.create_volume(name, max_bytes)?;
    tx.commit()?;
    Ok(ControlResponse::CreateVolume {
        volume_id: volume.volume_id,
    })
}

pub(super) fn report_bundle_replica(
    server: &CentralServer,
    storage_id: u64,
    events: Vec<BundleReplicaEvent>,
) -> Fs0Result<ControlResponse> {
    let online_volumes = server.online_volumes.read();
    for event in &events {
        if online_volumes.get(&event.volume_id) != Some(&storage_id) {
            return Err(Fs0Error::InvalidRequest);
        }
    }

    let mut db = server.db.lock();
    let tx = db.tx()?;
    for event in events {
        match event.kind {
            BundleReplicaEventKind::Stored => {
                tx.insert_bundle_replica(
                    event.bundle_id,
                    event.volume_id,
                    event.raw_len.ok_or(Fs0Error::InvalidRequest)?,
                    event.compressed_len.ok_or(Fs0Error::InvalidRequest)?,
                )?;
            }
            BundleReplicaEventKind::Deleted => {
                tx.delete_bundle_replica(event.bundle_id, event.volume_id)?;
            }
        }
    }
    tx.commit()?;

    Ok(ControlResponse::ReportBundleReplica)
}

pub(super) fn update_storage_volume_offset(
    server: &CentralServer,
    storage_id: u64,
    volume_id: u64,
    max_volume_offset: u64,
) -> Fs0Result<ControlResponse> {
    let registered = {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        let registered = tx.update_volume_offset(volume_id, max_volume_offset)?;
        tx.commit()?;
        registered
    };
    let mut storages = server.storages.write();
    let storage = storages.get_mut(&storage_id).ok_or(Fs0Error::NotFound)?;
    let volume = storage
        .peer
        .volumes
        .iter_mut()
        .find(|volume| volume.volume_id == volume_id)
        .ok_or(Fs0Error::NotFound)?;

    volume.name = registered.name;
    volume.max_bytes = registered.max_bytes;
    volume.max_volume_offset = registered.max_volume_offset;

    Ok(ControlResponse::UpdateStorageVolumeOffset)
}
