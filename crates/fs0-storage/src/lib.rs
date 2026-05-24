mod config;
mod node;

pub use config::{
    StorageConfig, StorageP2pRelayConfig, StorageVolumeConfig, StorageVolumeIoConfig,
};
pub use fs0_core::Fs0Error;
pub use node::{StorageDaemon, StorageNode, VolumeHandle};

pub type Result<T> = std::result::Result<T, Fs0Error>;
