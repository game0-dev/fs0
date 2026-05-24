use fs0_core::{
    ControlRequest, ControlResponse, DataRequest, DataResponse, FRAME_LEN_BYTES, Fs0Error,
    MAX_FRAME_BODY_LEN,
};
use iroh::{
    Endpoint, EndpointAddr, RelayConfig, RelayMap, RelayMode, RelayUrl,
    endpoint::{Connection, presets},
};
use iroh_relay::RelayQuicConfig;
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type Result<T> = std::result::Result<T, Fs0Error>;

pub async fn read_frame<T, R>(reader: &mut R) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; FRAME_LEN_BYTES];
    reader.read_exact(&mut len).await?;
    let body_len = u32::from_le_bytes(len) as usize;
    if body_len > MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
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
        return Err(Fs0Error::FrameTooLarge {
            actual: body.len(),
            max: MAX_FRAME_BODY_LEN,
        });
    }

    writer.write_all(&(body.len() as u32).to_le_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn bind_endpoint(
    relay_url: &str,
    relay_quic_port: u16,
    alpns: Vec<Vec<u8>>,
) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .relay_mode(relay_mode(relay_url, relay_quic_port)?)
        .alpns(alpns)
        .bind()
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub fn encode_endpoint_addr(endpoint: &Endpoint) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&endpoint.addr())?)
}

pub fn decode_endpoint_addr(bytes: &[u8]) -> Result<EndpointAddr> {
    Ok(postcard::from_bytes(bytes)?)
}

pub async fn connect_control(endpoint: &Endpoint, control_endpoint: &[u8]) -> Result<Connection> {
    let addr = decode_endpoint_addr(control_endpoint)?;
    endpoint
        .connect(addr, fs0_core::CONTROL_ALPN)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub async fn connect_data(endpoint: &Endpoint, data_endpoint: &[u8]) -> Result<Connection> {
    let addr = decode_endpoint_addr(data_endpoint)?;
    endpoint
        .connect(addr, fs0_core::DATA_ALPN)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub async fn control_rpc(
    connection: &Connection,
    request: ControlRequest,
) -> Result<ControlResponse> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
    write_frame(&mut send, &request).await?;
    send.finish().map_err(|err| Fs0Error::Internal {
        message: err.to_string(),
    })?;
    read_frame(&mut recv).await
}

pub async fn data_rpc(
    endpoint: &Endpoint,
    data_endpoint: &[u8],
    request: DataRequest,
) -> Result<DataResponse> {
    let conn = connect_data(endpoint, data_endpoint).await?;
    let response = data_rpc_on_connection(&conn, request).await;
    conn.close(0u32.into(), b"fs0 data rpc complete");
    response
}

pub async fn data_rpc_on_connection(
    connection: &Connection,
    request: DataRequest,
) -> Result<DataResponse> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
    write_frame(&mut send, &request).await?;
    send.finish().map_err(|err| Fs0Error::Internal {
        message: err.to_string(),
    })?;
    read_frame(&mut recv).await
}

pub async fn ping_data_peer(endpoint: &Endpoint, data_endpoint: &[u8]) -> Result<()> {
    let conn = connect_data(endpoint, data_endpoint).await?;
    conn.close(0u32.into(), b"fs0 data ping complete");
    Ok(())
}

fn relay_mode(relay_url: &str, relay_quic_port: u16) -> Result<RelayMode> {
    let relay_url = relay_url
        .parse::<RelayUrl>()
        .map_err(|err: iroh::RelayUrlParseError| Fs0Error::InvalidConfig {
            message: format!("invalid relay url {relay_url}: {err}"),
        })?;
    let relay = RelayConfig::new(relay_url, Some(RelayQuicConfig::new(relay_quic_port)));
    Ok(RelayMode::Custom(RelayMap::from(relay)))
}
