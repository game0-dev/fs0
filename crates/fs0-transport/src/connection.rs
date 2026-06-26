use fs0_core::{
    Fs0Error, Fs0Result, TRANSPORT_FRAME_LEN_BYTES, TRANSPORT_MAX_FRAME_BODY_LEN,
    protocol::{ProtocolEvent, ProtocolRequest, ProtocolResponse},
};
use iroh::endpoint::{Connection as IrohConnection, PathEvent};
use n0_future::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::{JoinHandle, JoinSet},
};
use tracing::{info, warn};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct Connection {
    iroh_connection: IrohConnection,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("remote_id", &self.iroh_connection.remote_id())
            .finish_non_exhaustive()
    }
}

impl Connection {
    pub fn new(iroh_connection: IrohConnection) -> Self {
        let connection = Self { iroh_connection };
        connection.start_watch_connection_path();
        connection
    }

    pub async fn rpc(
        &self,
        request: ProtocolRequest,
        timeout: Option<Duration>,
    ) -> Fs0Result<ProtocolResponse> {
        let started = Instant::now();
        let timeout = timeout.unwrap_or(DEFAULT_RPC_TIMEOUT);
        let rpc = async {
            let (mut send, mut recv) =
                self.iroh_connection
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
        };

        tokio::time::timeout(timeout, rpc).await.map_err(|_| {
            warn!(
                remote_id = ?self.iroh_connection.remote_id(),
                timeout = ?timeout,
                elapsed_ms = started.elapsed().as_millis(),
                "iroh rpc timed out"
            );
            Fs0Error::Internal {
                message: format!("transport rpc timed out after {timeout:?}"),
            }
        })?
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.iroh_connection.close_reason().is_some()
    }

    pub fn spawn_accept<F, Fut, Exit>(&self, handler: F, exit: Exit) -> JoinHandle<Fs0Result<()>>
    where
        F: Fn(ProtocolRequest) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Fs0Result<Option<ProtocolResponse>>> + Send + 'static,
        Exit: Future<Output = ()> + Send + 'static,
    {
        let connection = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                result = connection.accept_loop(handler) => result,
                _ = exit => Ok(()),
            }
        })
    }

    pub async fn send_event(&self, event: &ProtocolEvent) -> Fs0Result<()> {
        let mut stream =
            self.iroh_connection
                .open_uni()
                .await
                .map_err(|err| Fs0Error::Internal {
                    message: err.to_string(),
                })?;

        if let Err(err) = write_frame(&mut stream, event).await {
            warn!(
                remote_id = ?self.iroh_connection.remote_id(),
                error = %err,
                "iroh failed to send event"
            );
            return Err(err);
        }
        stream.finish().map_err(|err| {
            let err = Fs0Error::Internal {
                message: err.to_string(),
            };
            warn!(
                remote_id = ?self.iroh_connection.remote_id(),
                error = %err,
                "iroh failed to finish event stream"
            );
            err
        })?;

        Ok(())
    }

    pub fn close(&self, reason: &[u8]) {
        info!(
            remote_id = ?self.iroh_connection.remote_id(),
            reason = %String::from_utf8_lossy(reason),
            "iroh connection closing"
        );
        self.iroh_connection.close(0u32.into(), reason);
    }

    fn start_watch_connection_path(&self) {
        let iroh_connection = self.iroh_connection.clone();
        tokio::spawn(async move {
            let remote_id = iroh_connection.remote_id();
            let mut events = iroh_connection.path_events();

            while let Some(event) = events.next().await {
                match event {
                    PathEvent::Selected { id, remote_addr } => {
                        let path_kind = if remote_addr.is_relay() {
                            "relay"
                        } else if remote_addr.is_ip() {
                            "direct"
                        } else {
                            "other"
                        };
                        info!(
                            remote_id = ?remote_id,
                            path_id = ?id,
                            path_kind,
                            remote_addr = %remote_addr,
                            relay_proxy = remote_addr.is_relay(),
                            "iroh path selected"
                        );
                    }
                    PathEvent::Lagged { missed } => {
                        warn!(
                            remote_id = ?remote_id,
                            missed,
                            "iroh path event listener lagged"
                        );
                    }
                    PathEvent::Opened { .. } | PathEvent::Closed { .. } => {}
                    _ => {}
                }
            }
        });
    }

    async fn accept_loop<F, Fut>(self, handler: F) -> Fs0Result<()>
    where
        F: Fn(ProtocolRequest) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Fs0Result<Option<ProtocolResponse>>> + Send + 'static,
    {
        let connection = self;
        let mut stream_tasks = JoinSet::new();
        info!(
            remote_id = ?connection.iroh_connection.remote_id(),
            "iroh connection accept started"
        );

        loop {
            tokio::select! {
                result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                    if let Some(Err(err)) = result {
                        if err.is_panic() {
                            return Err(Fs0Error::Internal {
                                message: format!("transport stream task panicked: {err}"),
                            });
                        }
                    }
                }
                stream = connection.iroh_connection.accept_bi() => {
                    let (mut send, mut recv) = stream.map_err(|err| {
                        let err = Fs0Error::Internal {
                            message: err.to_string(),
                        };
                        warn!(
                            remote_id = ?connection.iroh_connection.remote_id(),
                            error = %err,
                            "iroh failed to accept bidirectional stream"
                        );
                        err
                    })?;
                    let handler = handler.clone();
                    let remote_id = connection.iroh_connection.remote_id();
                    stream_tasks.spawn(async move {
                        let response = match read_frame(&mut recv).await {
                            Ok(request) => {
                                info!(
                                    remote_id = ?remote_id,
                                    request_kind = protocol_request_kind(&request),
                                    "iroh rpc request received"
                                );
                                match handler(request).await {
                                    Ok(Some(response)) => response,
                                    Ok(None) => ProtocolResponse::Error(Fs0Error::InvalidRequest),
                                    Err(err) => ProtocolResponse::Error(err),
                                }
                            }
                            Err(err) => ProtocolResponse::Error(err),
                        };
                        let response_kind = protocol_response_kind(&response);

                        if let Err(err) = write_frame(&mut send, &response).await {
                            warn!(
                                remote_id = ?remote_id,
                                error = %err,
                                "iroh failed to write response"
                            );
                            return;
                        }
                        if let Err(err) = send.finish() {
                            warn!(
                                remote_id = ?remote_id,
                                error = %err,
                                "iroh failed to finish response stream"
                            );
                        }
                        info!(
                            remote_id = ?remote_id,
                            response_kind,
                            "iroh rpc response sent"
                        );
                    });
                }
                stream = connection.iroh_connection.accept_uni() => {
                    let mut stream = stream.map_err(|err| {
                        let err = Fs0Error::Internal {
                            message: err.to_string(),
                        };
                        warn!(
                            remote_id = ?connection.iroh_connection.remote_id(),
                            error = %err,
                            "iroh failed to accept unidirectional stream"
                        );
                        err
                    })?;
                    let handler = handler.clone();
                    stream_tasks.spawn(async move {
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

fn protocol_request_kind(request: &ProtocolRequest) -> &'static str {
    match request {
        ProtocolRequest::Control(_) => "control",
        ProtocolRequest::Data(_) => "data",
        ProtocolRequest::Event(_) => "event",
        ProtocolRequest::CentralAdmin(_) => "central_admin",
        ProtocolRequest::StorageAdmin(_) => "storage_admin",
    }
}

fn protocol_response_kind(response: &ProtocolResponse) -> &'static str {
    match response {
        ProtocolResponse::Error(_) => "error",
        ProtocolResponse::Control(_) => "control",
        ProtocolResponse::Data(_) => "data",
        ProtocolResponse::CentralAdmin(_) => "central_admin",
        ProtocolResponse::StorageAdmin(_) => "storage_admin",
    }
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
