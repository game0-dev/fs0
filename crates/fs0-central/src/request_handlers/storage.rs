use crate::{
    Fs0Error, Fs0Result,
    server::{CentralServer, StorageControlConnection},
};
use fs0_core::protocol::{
    BundleReplicaEvent, BundleReplicaEventKind, ControlResponse, StoragePeerInfo, StorageVolumeInfo,
};
use fs0_transport::Connection;

pub(super) fn register_storage(
    server: &CentralServer,
    name: String,
    token: String,
    mut volumes: Vec<StorageVolumeInfo>,
    iroh_endpoint: Vec<u8>,
    connection: Connection,
) -> Fs0Result<(u64, Vec<StoragePeerInfo>)> {
    if !server.token_allowed(&token) {
        return Err(Fs0Error::Unauthorized);
    }

    {
        let mut db = server.db.lock();
        let tx = db.tx()?;
        for volume in &mut volumes {
            let registered = tx.get_volume(volume.volume_id)?;
            volume.name = registered.name;
            volume.max_volume_offset = registered.max_volume_offset.max(volume.max_volume_offset);
            if volume.max_volume_offset != registered.max_volume_offset {
                tx.update_volume_offset(volume.volume_id, volume.max_volume_offset)?;
            }

            if registered.max_bytes != volume.max_bytes {
                return Err(Fs0Error::InvalidRequest);
            }
        }
        tx.commit()?;
    }

    let storage_id = server.next_id();
    let peer = StoragePeerInfo {
        storage_id,
        name,
        volumes,
        iroh_endpoint,
    };

    {
        let mut online_volumes = server.online_volumes.write();
        for volume in &peer.volumes {
            if online_volumes.contains_key(&volume.volume_id) {
                return Err(Fs0Error::VolumeAlreadyMounted);
            }
        }

        for volume in &peer.volumes {
            online_volumes.insert(volume.volume_id, storage_id);
        }
    }

    server.storages.write().insert(
        peer.storage_id,
        StorageControlConnection { peer, connection },
    );

    Ok((storage_id, server.storage_peers_snapshot()))
}

pub(super) fn unregister_storage(server: &CentralServer, storage_id: u64) {
    if let Some(storage) = server.storages.write().remove(&storage_id) {
        storage.connection.close(b"central storage unregistered");
    }
    server
        .online_volumes
        .write()
        .retain(|_, mounted_storage_id| *mounted_storage_id != storage_id);
}

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

pub(super) fn central_status(server: &CentralServer) -> Fs0Result<ControlResponse> {
    Ok(ControlResponse::CentralStatus {
        clients_count: server.clients.read().len() as u32,
        storages: server.storage_peers_snapshot(),
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
