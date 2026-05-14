use fs0_core::{DataRequest, DataResponse, FRAME_LEN_BYTES, MAX_FRAME_BODY_LEN};
use iroh::{Endpoint, EndpointAddr, RelayConfig, RelayMap, RelayMode, RelayUrl, endpoint::presets};
use iroh_relay::RelayQuicConfig;
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type Result<T> = std::result::Result<T, TransportError>;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("frame body length {actual} exceeds maximum {max}")]
    FrameTooLarge { actual: usize, max: usize },

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("io error")]
    Io(#[from] std::io::Error),

    #[error("postcard error")]
    Postcard(#[from] postcard::Error),

    #[error("iroh error: {0}")]
    Iroh(String),

    #[error("invalid relay url {url}: {message}")]
    InvalidRelayUrl { url: String, message: String },
}

pub async fn read_frame<T, R>(reader: &mut R) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; FRAME_LEN_BYTES];
    reader.read_exact(&mut len).await?;
    let body_len = u32::from_le_bytes(len) as usize;
    if body_len > MAX_FRAME_BODY_LEN {
        return Err(TransportError::FrameTooLarge {
            actual: body_len,
            max: MAX_FRAME_BODY_LEN,
        });
    }

    let mut body = vec![0; body_len];
    reader.read_exact(&mut body).await?;
    Ok(postcard::from_bytes(&body)?)
}

pub async fn write_frame<T, W>(writer: &mut W, value: &T) -> Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let body = postcard::to_allocvec(value)?;
    if body.len() > MAX_FRAME_BODY_LEN {
        return Err(TransportError::FrameTooLarge {
            actual: body.len(),
            max: MAX_FRAME_BODY_LEN,
        });
    }

    writer.write_all(&(body.len() as u32).to_le_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn bind_data_endpoint_accepting(
    relay_url: &str,
    relay_quic_port: u16,
) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .relay_mode(relay_mode(relay_url, relay_quic_port)?)
        .alpns(vec![fs0_core::DATA_ALPN.to_vec()])
        .bind()
        .await
        .map_err(|err| TransportError::Iroh(err.to_string()))
}

pub async fn bind_data_endpoint(relay_url: &str, relay_quic_port: u16) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .relay_mode(relay_mode(relay_url, relay_quic_port)?)
        .bind()
        .await
        .map_err(|err| TransportError::Iroh(err.to_string()))
}

pub fn encode_endpoint_addr(endpoint: &Endpoint) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&endpoint.addr())?)
}

pub fn decode_endpoint_addr(bytes: &[u8]) -> Result<EndpointAddr> {
    Ok(postcard::from_bytes(bytes)?)
}

pub async fn ping_data_peer(endpoint: &Endpoint, data_endpoint: &[u8]) -> Result<()> {
    let addr = decode_endpoint_addr(data_endpoint)?;
    let conn = endpoint
        .connect(addr, fs0_core::DATA_ALPN)
        .await
        .map_err(|err| TransportError::Iroh(err.to_string()))?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|err| TransportError::Iroh(err.to_string()))?;

    write_frame(&mut send, &DataRequest::Ping).await?;

    match read_frame::<DataResponse, _>(&mut recv).await? {
        DataResponse::Pong => {
            send.finish()
                .map_err(|err| TransportError::Iroh(err.to_string()))?;
            conn.close(0u32.into(), b"fs0 ping complete");
            Ok(())
        }
        response => Err(TransportError::InvalidFrame(format!(
            "expected data pong, got {response:?}"
        ))),
    }
}

fn relay_mode(relay_url: &str, relay_quic_port: u16) -> Result<RelayMode> {
    let relay_url = relay_url
        .parse::<RelayUrl>()
        .map_err(
            |err: iroh::RelayUrlParseError| TransportError::InvalidRelayUrl {
                url: relay_url.to_owned(),
                message: err.to_string(),
            },
        )?;
    let relay = RelayConfig::new(relay_url, Some(RelayQuicConfig::new(relay_quic_port)));
    Ok(RelayMode::Custom(RelayMap::from(relay)))
}
