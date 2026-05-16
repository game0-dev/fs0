pub mod codec;
pub mod compression;
pub mod error;
pub mod hash;
pub mod id;
pub mod manifest;
pub mod protocol;

pub use codec::*;
pub use compression::*;
pub use error::{Fs0Error, Result};
pub use hash::*;
pub use id::*;
pub use manifest::*;
pub use protocol::*;

pub const DEFAULT_ZSTD_LEVEL: i32 = 9;
pub const CONTROL_ALPN: &[u8] = b"/fs0/control/1";
pub const DATA_ALPN: &[u8] = b"/fs0/data/1";
