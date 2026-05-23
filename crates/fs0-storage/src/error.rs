use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("duplicate volume id in storage config: {0}")]
    DuplicateVolumeId(u64),

    #[error("volume {0} is not mounted by this storage node")]
    UnknownVolume(u64),

    #[error("unexpected control response: {0:?}")]
    UnexpectedControlResponse(fs0_core::ControlResponse),

    #[error(
        "configured volume id {configured} does not match volume metadata id {actual}: {}",
        .path.display()
    )]
    VolumeIdMismatch {
        path: PathBuf,
        configured: u64,
        actual: u64,
    },

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("toml decode error")]
    TomlDecode(#[from] toml::de::Error),

    #[error("transport error")]
    Transport(#[from] fs0_transport::TransportError),

    #[error("volume error: {0}")]
    Volume(#[from] fs0_core::Fs0Error),
}
