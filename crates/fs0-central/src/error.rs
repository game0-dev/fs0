use fs0_core::{ControlError, ControlErrorCode};
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, CentralError>;

#[derive(Debug, thiserror::Error)]
pub enum CentralError {
    #[error("control error: {0:?}")]
    Control(ControlError),

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
    pub fn control(code: ControlErrorCode, message: impl Into<String>) -> Self {
        Self::Control(ControlError {
            code,
            message: message.into(),
        })
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::control(ControlErrorCode::NotFound, message)
    }

    #[must_use]
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::control(ControlErrorCode::InvalidRequest, message)
    }

    #[must_use]
    pub fn version_conflict() -> Self {
        Self::control(ControlErrorCode::VersionConflict, "file version conflict")
    }

    #[must_use]
    pub fn volume_already_mounted(
        volume_id: impl Display,
        mounted_by_storage_id: impl Display,
    ) -> Self {
        Self::control(
            ControlErrorCode::VolumeAlreadyMounted,
            format!("volume {volume_id} is already mounted by storage {mounted_by_storage_id}"),
        )
    }

    #[must_use]
    pub fn to_control_error(&self) -> ControlError {
        match self {
            Self::Control(err) => err.clone(),
            Self::Sqlite(err)
                if matches!(
                    err.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::ConstraintViolation)
                ) =>
            {
                ControlError {
                    code: ControlErrorCode::AlreadyExists,
                    message: "central metadata already exists".to_owned(),
                }
            }
            Self::Core(err) => ControlError {
                code: ControlErrorCode::InvalidRequest,
                message: err.to_string(),
            },
            Self::Io(_)
            | Self::Transport(_)
            | Self::Sqlite(_)
            | Self::TomlDecode(_)
            | Self::Config(_)
            | Self::Relay(_)
            | Self::IntegerConversion(_) => ControlError {
                code: ControlErrorCode::Internal,
                message: "central internal error".to_owned(),
            },
        }
    }
}

impl From<ControlError> for CentralError {
    fn from(value: ControlError) -> Self {
        Self::Control(value)
    }
}

impl From<CentralError> for ControlError {
    fn from(value: CentralError) -> Self {
        value.to_control_error()
    }
}
