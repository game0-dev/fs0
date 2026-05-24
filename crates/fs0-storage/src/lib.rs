mod data_server;
mod server;

pub use fs0_config::{
    StorageConfig, StorageP2pRelayConfig, StorageVolumeConfig, StorageVolumeIoConfig,
};
pub use fs0_core::Fs0Error;
pub use server::StorageServer;

pub type Result<T> = std::result::Result<T, Fs0Error>;
