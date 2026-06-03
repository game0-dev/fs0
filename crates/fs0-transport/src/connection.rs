use fs0_core::{
    Fs0Error, Fs0Result, TRANSPORT_FRAME_LEN_BYTES, TRANSPORT_MAX_FRAME_BODY_LEN,
    protocol::{ProtocolEvent, ProtocolRequest, ProtocolResponse},
};
use iroh::endpoint::{Connection as IrohConnection, SendStream};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("alpn", &self.alpn())
            .field("remote_id", &self.inner.iroh_connection.remote_id())
            .finish_non_exhaustive()
    }
}

struct ConnectionInner {
    alpn: Vec<u8>,
    iroh_connection: IrohConnection,
    uni_stream: tokio::sync::Mutex<Option<SendStream>>,
}

impl Connection {
    pub(crate) fn new(alpn: Vec<u8>, iroh_connection: IrohConnection) -> Self {
        Self {
            inner: Arc::new(ConnectionInner {
                alpn,
                iroh_connection,
                uni_stream: tokio::sync::Mutex::new(None),
            }),
        }
    }

    #[must_use]
    pub fn alpn(&self) -> &[u8] {
        &self.inner.alpn
    }

    pub async fn rpc(&self, request: ProtocolRequest) -> Fs0Result<ProtocolResponse> {
        let (mut send, mut recv) =
            self.inner
                .iroh_connection
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

    pub async fn serve<F, Fut>(&self, handler: F) -> Fs0Result<()>
    where
        F: Fn(ProtocolRequest) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Fs0Result<Option<ProtocolResponse>>> + Send + 'static,
    {
        loop {
            tokio::select! {
                stream = self.inner.iroh_connection.accept_bi() => {
                    let (mut send, mut recv) = stream.map_err(|err| Fs0Error::Internal {
                        message: err.to_string(),
                    })?;
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let response = match read_frame(&mut recv).await {
                            Ok(request) => match handler(request).await {
                                Ok(Some(response)) => response,
                                Ok(None) => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                                Err(err) => ProtocolResponse::Error(err),
                            },
                            Err(err) => ProtocolResponse::Error(err),
                        };

                        let _ = write_frame(&mut send, &response).await;
                        let _ = send.finish();
                    });
                }
                stream = self.inner.iroh_connection.accept_uni() => {
                    let mut stream = stream.map_err(|err| Fs0Error::Internal {
                        message: err.to_string(),
                    })?;
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        loop {
                            let event = match read_frame(&mut stream).await {
                                Ok(event) => event,
                                Err(_) => break,
                            };

                            let _ = handler(ProtocolRequest::Event(event)).await;
                        }
                    });
                }
            };
        }
    }

    pub async fn send_event(&self, event: &ProtocolEvent) -> Fs0Result<()> {
        let mut uni_stream = self.inner.uni_stream.lock().await;
        if uni_stream.is_none() {
            *uni_stream = Some(self.inner.iroh_connection.open_uni().await.map_err(|err| {
                Fs0Error::Internal {
                    message: err.to_string(),
                }
            })?);
        }

        let stream = uni_stream.as_mut().ok_or_else(|| Fs0Error::Internal {
            message: "uni stream is not open".to_owned(),
        })?;
        if let Err(err) = write_frame(stream, event).await {
            *uni_stream = None;
            return Err(err);
        }

        Ok(())
    }

    pub fn close(&self, reason: &[u8]) {
        self.inner.iroh_connection.close(0u32.into(), reason);
    }
}

async fn read_frame<T, R>(reader: &mut R) -> Fs0Result<T>
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

async fn write_frame<T, W>(writer: &mut W, value: &T) -> Fs0Result<()>
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

    #[tokio::test]
    async fn frame_read_write_postcard_payload() {
        let payload = TestPayload {
            object_id: 11,
            client_id: 13,
            chunk_id: blake3_hash(b"/fs0/test"),
            name: "frame".to_owned(),
        };

        let (mut client, mut server) = tokio::io::duplex(1024);
        write_frame(&mut client, &payload).await.unwrap();
        let decoded: TestPayload = read_frame(&mut server).await.unwrap();

        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn frame_rejects_truncated_body() {
        let payload = TestPayload {
            object_id: 11,
            client_id: 13,
            chunk_id: blake3_hash(b"/fs0/test"),
            name: "frame".to_owned(),
        };

        let body = postcard::to_allocvec(&payload).unwrap();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(body.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&body[..body.len() - 1]);
        let mut reader = std::io::Cursor::new(encoded);

        let err = read_frame::<TestPayload, _>(&mut reader).await.unwrap_err();
        assert!(matches!(err, Fs0Error::Io { .. }));
    }

    #[tokio::test]
    async fn frame_rejects_declared_body_over_limit() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&((TRANSPORT_MAX_FRAME_BODY_LEN as u32) + 1).to_le_bytes());
        let mut reader = std::io::Cursor::new(encoded);

        let err = read_frame::<TestPayload, _>(&mut reader).await.unwrap_err();
        assert_eq!(
            err,
            Fs0Error::FrameTooLarge {
                actual: TRANSPORT_MAX_FRAME_BODY_LEN + 1,
                max: TRANSPORT_MAX_FRAME_BODY_LEN,
            }
        );
    }

    #[tokio::test]
    async fn frame_rejects_body_over_limit_before_writing() {
        let body = vec![0; TRANSPORT_MAX_FRAME_BODY_LEN + 1];
        let payload = TestPayload {
            object_id: 11,
            client_id: 13,
            chunk_id: blake3_hash(&body),
            name: String::from_utf8_lossy(&body).into_owned(),
        };
        let mut writer = tokio::io::sink();
        let err = write_frame(&mut writer, &payload).await.unwrap_err();

        assert!(matches!(err, Fs0Error::FrameTooLarge { .. }));
    }
}
