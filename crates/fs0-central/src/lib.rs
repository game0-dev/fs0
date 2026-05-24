mod db;
mod server;

pub use fs0_config::{CentralConfig, CentralP2pRelayConfig};
pub use fs0_core::Fs0Error;
pub use server::CentralServer;

pub type Result<T> = std::result::Result<T, Fs0Error>;
