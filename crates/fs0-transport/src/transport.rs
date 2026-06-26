use crate::Connection;
use fs0_config::RelayClientConfig;
use fs0_core::{Fs0Error, Fs0Result};
use iroh::{
    Endpoint, EndpointAddr, RelayConfig as IrohRelayConfig, RelayMap, RelayMode, RelayUrl,
    SecretKey, Watcher, endpoint::presets,
};
use iroh_relay::{RelayQuicConfig, tls::CaRootsConfig};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    timeout: Duration,
    retry: ConnectRetry,
}

impl ConnectOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_retry(mut self, retry: ConnectRetry) -> Self {
        self.retry = retry;
        self
    }

    #[must_use]
    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    #[must_use]
    pub(crate) fn retry(&self) -> ConnectRetry {
        self.retry
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(15),
            retry: ConnectRetry::disabled(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectRetry {
    max_attempts: usize,
    initial_delay: Duration,
    max_delay: Duration,
}

impl ConnectRetry {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    #[must_use]
    pub fn new(max_attempts: usize, initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            initial_delay,
            max_delay: max_delay.max(initial_delay),
        }
    }

    #[must_use]
    pub(crate) fn max_attempts(self) -> usize {
        self.max_attempts
    }

    #[must_use]
    pub(crate) fn delay_after_attempt(self, attempt: usize) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31) as u32;
        let factor = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        self.initial_delay
            .checked_mul(factor)
            .unwrap_or(self.max_delay)
            .min(self.max_delay)
    }
}

#[derive(Debug, Clone)]
pub struct TransportOptions {
    alpns: Vec<Vec<u8>>,
    secret_key: Option<SecretKey>,
    bind_addr: Option<SocketAddr>,
    relay: Option<RelayClientConfig>,
}

impl TransportOptions {
    #[must_use]
    pub fn new(alpns: Vec<&[u8]>) -> Self {
        Self {
            alpns: alpns.into_iter().map(<[u8]>::to_vec).collect(),
            secret_key: None,
            bind_addr: None,
            relay: None,
        }
    }

    #[must_use]
    pub fn with_secret_key(mut self, secret_key: SecretKey) -> Self {
        self.secret_key = Some(secret_key);
        self
    }

    #[must_use]
    pub fn with_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.bind_addr = Some(bind_addr);
        self
    }

    #[must_use]
    pub fn with_relay(mut self, relay: Option<RelayClientConfig>) -> Self {
        self.relay = relay;
        self
    }
}

#[derive(Debug, Clone)]
pub struct Transport {
    endpoint: Endpoint,
}

impl Transport {
    pub async fn bind(options: TransportOptions) -> Fs0Result<Self> {
        let ca_roots_config = options
            .relay
            .as_ref()
            .and_then(|relay| relay.ca_cert.as_deref())
            .map(ca_roots_config)
            .transpose()?;
        let mut builder = Endpoint::builder(presets::Minimal)
            .alpns(options.alpns)
            .relay_mode(
                options
                    .relay
                    .map_or(Ok(RelayMode::Disabled), |relay| relay_mode(&relay))?,
            );
        if let Some(ca_roots_config) = ca_roots_config {
            builder = builder.ca_roots_config(ca_roots_config);
        }

        if let Some(secret_key) = options.secret_key {
            builder = builder.secret_key(secret_key);
        }
        if let Some(bind_addr) = options.bind_addr {
            builder = builder
                .bind_addr(bind_addr)
                .map_err(|err| Fs0Error::InvalidConfig {
                    message: format!("invalid transport bind address {bind_addr}: {err}"),
                })?;
        }

        let endpoint = builder.bind().await.map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
        info!(
            endpoint = ?endpoint.addr(),
            "iroh transport endpoint bound"
        );

        Ok(Self { endpoint })
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub fn watch_addr(&self) -> impl Watcher<Value = EndpointAddr> + use<> {
        self.endpoint.watch_addr()
    }

    pub fn router(&self) -> iroh::protocol::RouterBuilder {
        iroh::protocol::Router::builder(self.endpoint.clone())
    }

    pub async fn connect(
        &self,
        remote_endpoint: EndpointAddr,
        alpn: &[u8],
        options: Option<ConnectOptions>,
    ) -> Fs0Result<Connection> {
        let options = options.unwrap_or_default();
        let retry = options.retry();
        let alpn_label = String::from_utf8_lossy(alpn);
        let started = Instant::now();
        let attempts = retry.max_attempts();

        for attempt in 1..=attempts {
            let attempt_started = Instant::now();
            info!(
                endpoint = ?remote_endpoint,
                alpn = %alpn_label,
                attempt,
                attempts,
                timeout = ?options.timeout(),
                "iroh connect attempt"
            );

            let connect = self.endpoint.connect(remote_endpoint.clone(), alpn);
            let timeout = options.timeout();
            let result = tokio::time::timeout(timeout, connect)
                .await
                .map_err(|_| Fs0Error::Internal {
                    message: format!("transport connect timed out after {timeout:?}"),
                })
                .and_then(|result| {
                    result.map_err(|err| Fs0Error::Internal {
                        message: err.to_string(),
                    })
                });

            match result {
                Ok(iroh_connection) => {
                    info!(
                        endpoint = ?remote_endpoint,
                        alpn = %alpn_label,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis(),
                        attempt_elapsed_ms = attempt_started.elapsed().as_millis(),
                        remote_id = ?iroh_connection.remote_id(),
                        "iroh connect succeeded"
                    );
                    return Ok(Connection::new(iroh_connection));
                }
                Err(err) if attempt == attempts => {
                    warn!(
                        endpoint = ?remote_endpoint,
                        alpn = %alpn_label,
                        attempt,
                        attempts,
                        elapsed_ms = started.elapsed().as_millis(),
                        error = %err,
                        "iroh connect failed"
                    );
                    return Err(err);
                }
                Err(err) => {
                    let retry_delay = retry.delay_after_attempt(attempt);
                    warn!(
                        endpoint = ?remote_endpoint,
                        alpn = %alpn_label,
                        attempt,
                        attempts,
                        retry_delay_ms = retry_delay.as_millis(),
                        error = %err,
                        "iroh connect attempt failed; retrying"
                    );
                    sleep(retry_delay).await;
                }
            }
        }

        Err(Fs0Error::Internal {
            message: "transport connect had no attempts".to_owned(),
        })
    }

    pub async fn close(&self) {
        self.endpoint.close().await;
    }
}

fn relay_mode(config: &RelayClientConfig) -> Fs0Result<RelayMode> {
    let relay_url = config
        .url
        .parse::<RelayUrl>()
        .map_err(|err: iroh::RelayUrlParseError| Fs0Error::InvalidConfig {
            message: format!("invalid relay url {}: {err}", config.url),
        })?;
    let relay = IrohRelayConfig::new(relay_url, Some(RelayQuicConfig::new(config.quic_port)))
        .with_auth_token(config.token.clone());
    Ok(RelayMode::Custom(RelayMap::from(relay)))
}

fn ca_roots_config(ca_cert: &str) -> Fs0Result<CaRootsConfig> {
    let certs = CertificateDer::pem_slice_iter(ca_cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid relay ca_cert: {err}"),
        })?;
    if certs.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: "relay ca_cert contains no certificates".to_owned(),
        });
    }

    Ok(CaRootsConfig::custom(certs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs0_core::{
        TRANSPORT_CONTROL_ALPN,
        protocol::{
            ControlRequest, ControlResponse, ProtocolEvent, ProtocolRequest, ProtocolResponse,
        },
    };
    use iroh::protocol::{AcceptError, ProtocolHandler};

    #[derive(Debug, Clone)]
    struct TestHandler {
        event_tx: Option<tokio::sync::mpsc::Sender<ProtocolEvent>>,
    }

    impl ProtocolHandler for TestHandler {
        async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
            let connection = Connection::new(connection);
            let event_tx = self.event_tx.clone();
            connection
                .spawn_accept(
                    move |request| {
                        let event_tx = event_tx.clone();
                        async move {
                            match request {
                                ProtocolRequest::Control(ControlRequest::CentralStatus) => {
                                    Ok(Some(ProtocolResponse::Control(
                                        ControlResponse::CentralStatus {
                                            clients_count: 1,
                                            storages: Vec::new(),
                                        },
                                    )))
                                }
                                ProtocolRequest::Event(event) => {
                                    let Some(event_tx) = event_tx else {
                                        return Ok(Some(ProtocolResponse::Error(
                                            Fs0Error::InvalidRequest,
                                        )));
                                    };
                                    event_tx.send(event).await.unwrap();
                                    Ok(None)
                                }
                                _ => Ok(Some(ProtocolResponse::Error(Fs0Error::InvalidRequest))),
                            }
                        }
                    },
                    std::future::pending(),
                )
                .await
                .map_err(AcceptError::from_err)?
                .map_err(AcceptError::from_err)
        }
    }

    #[tokio::test]
    async fn connection_rpc_roundtrip() {
        let server = Transport::bind(TransportOptions::new(vec![TRANSPORT_CONTROL_ALPN]))
            .await
            .unwrap();
        let router = server
            .router()
            .accept(TRANSPORT_CONTROL_ALPN, TestHandler { event_tx: None })
            .spawn();
        let client = Transport::bind(TransportOptions::new(Vec::new()))
            .await
            .unwrap();
        let server_addr = server.addr();

        let connection = client
            .connect(server_addr, TRANSPORT_CONTROL_ALPN, None)
            .await
            .unwrap();
        let response = connection
            .rpc(
                ProtocolRequest::Control(ControlRequest::CentralStatus),
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            ProtocolResponse::Control(ControlResponse::CentralStatus {
                clients_count: 1,
                storages: Vec::new(),
            })
        );
        connection.close(b"test complete");
        router.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn connection_event_stream_roundtrip() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(2);
        let server = Transport::bind(TransportOptions::new(vec![TRANSPORT_CONTROL_ALPN]))
            .await
            .unwrap();
        let router = server
            .router()
            .accept(
                TRANSPORT_CONTROL_ALPN,
                TestHandler {
                    event_tx: Some(event_tx),
                },
            )
            .spawn();
        let client = Transport::bind(TransportOptions::new(Vec::new()))
            .await
            .unwrap();
        let server_addr = server.addr();

        let connection = client
            .connect(server_addr, TRANSPORT_CONTROL_ALPN, None)
            .await
            .unwrap();
        connection
            .send_event(&ProtocolEvent::StorageRemoved { storage_id: 7 })
            .await
            .unwrap();
        connection
            .send_event(&ProtocolEvent::StorageChanged(
                fs0_core::protocol::StoragePeerInfo {
                    storage_id: 8,
                    name: "storage".to_owned(),
                    volumes: Vec::new(),
                    iroh_endpoint: Vec::new(),
                },
            ))
            .await
            .unwrap();

        assert_eq!(
            event_rx.recv().await,
            Some(ProtocolEvent::StorageRemoved { storage_id: 7 })
        );
        assert_eq!(
            event_rx.recv().await,
            Some(ProtocolEvent::StorageChanged(
                fs0_core::protocol::StoragePeerInfo {
                    storage_id: 8,
                    name: "storage".to_owned(),
                    volumes: Vec::new(),
                    iroh_endpoint: Vec::new(),
                },
            ))
        );

        connection.close(b"test complete");
        router.shutdown().await.unwrap();
    }
}
