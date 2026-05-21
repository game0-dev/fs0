use fs0_core::Fs0ProtocolError;
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, CentralError>;

#[derive(Debug, thiserror::Error)]
pub enum CentralError {
    #[error("protocol error: {0:?}")]
    Protocol(Fs0ProtocolError),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("transport error")]
    Transport(#[from] fs0_transport::TransportError),

    #[error("sqlite error")]
    Sqlite(#[from] rusqlite::Error),

    #[error("toml decode error")]
    TomlDecode(#[from] toml::de::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("relay error: {0}")]
    Relay(String),

    #[error("core error")]
    Core(#[from] fs0_core::Fs0Error),

    #[error("integer conversion failed: {0}")]
    IntegerConversion(String),
}

impl CentralError {
    #[must_use]
    pub fn control(error: Fs0ProtocolError, _message: impl Into<String>) -> Self {
        Self::Protocol(error)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::control(Fs0ProtocolError::NotFound, message)
    }

    #[must_use]
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::control(Fs0ProtocolError::InvalidRequest, message)
    }

    #[must_use]
    pub fn version_conflict() -> Self {
        Self::control(Fs0ProtocolError::VersionConflict, "file version conflict")
    }

    #[must_use]
    pub fn volume_already_mounted(
        volume_id: impl Display,
        mounted_by_storage_id: impl Display,
    ) -> Self {
        Self::control(
            Fs0ProtocolError::VolumeAlreadyMounted,
            format!("volume {volume_id} is already mounted by storage {mounted_by_storage_id}"),
        )
    }

    #[must_use]
    pub fn to_protocol_error(&self) -> Fs0ProtocolError {
        match self {
            Self::Protocol(err) => err.clone(),
            Self::Sqlite(err)
                if matches!(
                    err.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::ConstraintViolation)
                ) =>
            {
                Fs0ProtocolError::AlreadyExists
            }
            Self::Core(_) => Fs0ProtocolError::InvalidRequest,
            Self::Io(_)
            | Self::Transport(_)
            | Self::Sqlite(_)
            | Self::TomlDecode(_)
            | Self::Config(_)
            | Self::Relay(_)
            | Self::IntegerConversion(_) => Fs0ProtocolError::Internal,
        }
    }
}

impl From<Fs0ProtocolError> for CentralError {
    fn from(value: Fs0ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<CentralError> for Fs0ProtocolError {
    fn from(value: CentralError) -> Self {
        value.to_protocol_error()
    }
}
