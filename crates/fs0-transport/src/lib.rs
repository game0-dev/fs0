use fs0_core::{
    ControlRequest, ControlResponse, DataRequest, DataResponse, Fs0Error, Fs0Result,
    TRANSPORT_CONTROL_ALPN, TRANSPORT_DATA_ALPN, TRANSPORT_FRAME_LEN_BYTES,
    TRANSPORT_MAX_FRAME_BODY_LEN,
};
use iroh::{
    Endpoint, EndpointAddr, RelayConfig, RelayMap, RelayMode, RelayUrl,
    endpoint::{Connection, presets},
};
use iroh_relay::RelayQuicConfig;
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub fn encode_frame<T: Serialize>(value: &T) -> Fs0Result<Vec<u8>> {
    let body = postcard::to_allocvec(value)?;
    encode_frame_body(&body)
}

pub fn encode_frame_body(body: &[u8]) -> Fs0Result<Vec<u8>> {
    if body.len() > TRANSPORT_MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body.len(),
            max: TRANSPORT_MAX_FRAME_BODY_LEN,
        });
    }

    let mut out = Vec::with_capacity(TRANSPORT_FRAME_LEN_BYTES + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Fs0Result<T> {
    let body = decode_frame_body(frame)?;
    Ok(postcard::from_bytes(body)?)
}

pub fn decode_frame_body(frame: &[u8]) -> Fs0Result<&[u8]> {
    if frame.len() < TRANSPORT_FRAME_LEN_BYTES {
        return Err(Fs0Error::InvalidFrame {
            message: format!("frame too short: {} bytes", frame.len()),
        });
    }

    let body_len = u32::from_le_bytes(
        frame[..TRANSPORT_FRAME_LEN_BYTES]
            .try_into()
            .expect("slice length is fixed"),
    ) as usize;

    if body_len > TRANSPORT_MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body_len,
            max: TRANSPORT_MAX_FRAME_BODY_LEN,
        });
    }

    let expected_len = TRANSPORT_FRAME_LEN_BYTES
        .checked_add(body_len)
        .ok_or_else(|| Fs0Error::InvalidFrame {
            message: "frame length overflow".to_owned(),
        })?;

    if frame.len() != expected_len {
        return Err(Fs0Error::InvalidFrame {
            message: format!(
                "frame length mismatch: expected {expected_len}, actual {}",
                frame.len()
            ),
        });
    }

    Ok(&frame[TRANSPORT_FRAME_LEN_BYTES..])
}

pub async fn read_frame<T, R>(reader: &mut R) -> Fs0Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; TRANSPORT_FRAME_LEN_BYTES];
    reader.read_exact(&mut len).await?;
    let body_len = u32::from_le_bytes(len) as usize;
    if body_len > TRANSPORT_MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body_len,
            max: TRANSPORT_MAX_FRAME_BODY_LEN,
        });
    }

    let mut body = vec![0; body_len];
    reader.read_exact(&mut body).await?;
    Ok(postcard::from_bytes(&body)?)
}

pub async fn write_frame<T, W>(writer: &mut W, value: &T) -> Fs0Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let body = postcard::to_allocvec(value)?;
    if body.len() > TRANSPORT_MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body.len(),
            max: TRANSPORT_MAX_FRAME_BODY_LEN,
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
) -> Fs0Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .relay_mode(relay_mode(relay_url, relay_quic_port)?)
        .alpns(alpns)
        .bind()
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub fn encode_endpoint_addr(endpoint: &Endpoint) -> Fs0Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&endpoint.addr())?)
}

pub fn decode_endpoint_addr(bytes: &[u8]) -> Fs0Result<EndpointAddr> {
    Ok(postcard::from_bytes(bytes)?)
}

pub async fn connect_control(endpoint: &Endpoint, control_endpoint: &[u8]) -> Fs0Result<Connection> {
    let addr = decode_endpoint_addr(control_endpoint)?;
    endpoint
        .connect(addr, TRANSPORT_CONTROL_ALPN)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub async fn connect_data(endpoint: &Endpoint, data_endpoint: &[u8]) -> Fs0Result<Connection> {
    let addr = decode_endpoint_addr(data_endpoint)?;
    endpoint
        .connect(addr, TRANSPORT_DATA_ALPN)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })
}

pub async fn control_rpc(
    connection: &Connection,
    request: ControlRequest,
) -> Fs0Result<ControlResponse> {
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
) -> Fs0Result<DataResponse> {
    let conn = connect_data(endpoint, data_endpoint).await?;
    let response = data_rpc_on_connection(&conn, request).await;
    conn.close(0u32.into(), b"fs0 data rpc complete");
    response
}

pub async fn data_rpc_on_connection(
    connection: &Connection,
    request: DataRequest,
) -> Fs0Result<DataResponse> {
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

pub async fn ping_data_peer(endpoint: &Endpoint, data_endpoint: &[u8]) -> Fs0Result<()> {
    let conn = connect_data(endpoint, data_endpoint).await?;
    conn.close(0u32.into(), b"fs0 data ping complete");
    Ok(())
}

fn relay_mode(relay_url: &str, relay_quic_port: u16) -> Fs0Result<RelayMode> {
    let relay_url = relay_url
        .parse::<RelayUrl>()
        .map_err(|err: iroh::RelayUrlParseError| Fs0Error::InvalidConfig {
            message: format!("invalid relay url {relay_url}: {err}"),
        })?;
    let relay = RelayConfig::new(relay_url, Some(RelayQuicConfig::new(relay_quic_port)));
    Ok(RelayMode::Custom(RelayMap::from(relay)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs0_core::{HashId, blake3_hash};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestPayload {
        object_id: u64,
        client_id: u64,
        chunk_id: HashId,
        name: String,
    }

    #[test]
    fn frame_encode_decode_postcard_payload() {
        let payload = TestPayload {
            object_id: 11,
            client_id: 13,
            chunk_id: blake3_hash(b"/fs0/test"),
            name: "frame".to_owned(),
        };

        let encoded = encode_frame(&payload).unwrap();
        let decoded: TestPayload = decode_frame(&encoded).unwrap();

        assert_eq!(decoded, payload);
    }

    #[test]
    fn frame_rejects_truncated_body() {
        let payload = TestPayload {
            object_id: 11,
            client_id: 13,
            chunk_id: blake3_hash(b"/fs0/test"),
            name: "frame".to_owned(),
        };

        let mut encoded = encode_frame(&payload).unwrap();
        encoded.pop();

        let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
        assert!(matches!(err, Fs0Error::InvalidFrame { .. }));
        assert!(err.to_string().contains("frame length mismatch"));
    }

    #[test]
    fn frame_rejects_declared_body_over_limit() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&((TRANSPORT_MAX_FRAME_BODY_LEN as u32) + 1).to_le_bytes());

        let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
        assert_eq!(
            err,
            Fs0Error::FrameTooLarge {
                actual: TRANSPORT_MAX_FRAME_BODY_LEN + 1,
                max: TRANSPORT_MAX_FRAME_BODY_LEN,
            }
        );
    }

    #[test]
    fn frame_body_roundtrip_preserves_raw_bytes() {
        let body = b"raw frame body";
        let encoded = encode_frame_body(body).unwrap();
        let decoded = decode_frame_body(&encoded).unwrap();

        assert_eq!(decoded, body);
    }

    #[test]
    fn frame_rejects_body_over_limit_before_encoding() {
        let body = vec![0; TRANSPORT_MAX_FRAME_BODY_LEN + 1];
        let err = encode_frame_body(&body).unwrap_err();

        assert_eq!(
            err,
            Fs0Error::FrameTooLarge {
                actual: TRANSPORT_MAX_FRAME_BODY_LEN + 1,
                max: TRANSPORT_MAX_FRAME_BODY_LEN,
            }
        );
    }
}
