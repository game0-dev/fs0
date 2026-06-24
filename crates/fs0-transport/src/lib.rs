mod connection;

pub use connection::{Connection, SelectedPath, SelectedPathKind};
pub use iroh::{EndpointAddr, EndpointId, SecretKey, Watcher};

use fs0_config::RelayClientConfig;
use fs0_core::{Fs0Error, Fs0Result};
use iroh::{
    Endpoint, RelayConfig as IrohRelayConfig, RelayMap, RelayMode, RelayUrl, endpoint::presets,
};
use iroh_relay::{RelayQuicConfig, tls::CaRootsConfig};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Transport {
    endpoint: Endpoint,
}

impl Transport {
    pub async fn bind(
        alpns: Vec<&[u8]>,
        secret_key: Option<SecretKey>,
        bind_addr: Option<SocketAddr>,
        relay: Option<RelayClientConfig>,
    ) -> Fs0Result<Self> {
        let alpns = alpns.into_iter().map(<[u8]>::to_vec).collect();
        let ca_roots_config = relay
            .as_ref()
            .and_then(|relay| relay.ca_cert.as_deref())
            .map(ca_roots_config)
            .transpose()?;
        let mut builder = Endpoint::builder(presets::Minimal)
            .alpns(alpns)
            .relay_mode(relay.map_or(Ok(RelayMode::Disabled), |relay| relay_mode(&relay))?);
        if let Some(ca_roots_config) = ca_roots_config {
            builder = builder.ca_roots_config(ca_roots_config);
        }

        if let Some(secret_key) = secret_key {
            builder = builder.secret_key(secret_key);
        }
        if let Some(bind_addr) = bind_addr {
            builder = builder
                .bind_addr(bind_addr)
                .map_err(|err| Fs0Error::InvalidConfig {
                    message: format!("invalid transport bind address {bind_addr}: {err}"),
                })?;
        }

        let endpoint = builder.bind().await.map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;

        Ok(Self { endpoint })
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub fn watch_addr(&self) -> impl Watcher<Value = EndpointAddr> + use<> {
        self.endpoint.watch_addr()
    }

    pub async fn connect(
        &self,
        remote_endpoint: EndpointAddr,
        alpn: &[u8],
    ) -> Fs0Result<Connection> {
        let iroh_connection =
            self.endpoint
                .connect(remote_endpoint, alpn)
                .await
                .map_err(|err| Fs0Error::Internal {
                    message: err.to_string(),
                })?;

        Ok(Connection::new(alpn.to_vec(), iroh_connection))
    }

    pub async fn accept(&self) -> Fs0Result<Option<Connection>> {
        let Some(incoming) = self.endpoint.accept().await else {
            return Ok(None);
        };

        let mut accepting = incoming.accept().map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
        let alpn = accepting.alpn().await.map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;
        let iroh_connection = accepting.await.map_err(|err| Fs0Error::Internal {
            message: err.to_string(),
        })?;

        Ok(Some(Connection::new(alpn, iroh_connection)))
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

    #[tokio::test]
    async fn connection_rpc_roundtrip() {
        let server = Transport::bind(vec![TRANSPORT_CONTROL_ALPN], None, None, None)
            .await
            .unwrap();
        let client = Transport::bind(Vec::new(), None, None, None).await.unwrap();
        let server_addr = server.addr();

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.unwrap().unwrap();
            let _ = connection
                .serve(|request| async move {
                    assert_eq!(
                        request,
                        ProtocolRequest::Control(ControlRequest::CentralStatus)
                    );
                    Ok(Some(ProtocolResponse::Control(
                        ControlResponse::CentralStatus {
                            clients_count: 1,
                            storages: Vec::new(),
                        },
                    )))
                })
                .await;
        });

        let connection = client
            .connect(server_addr, TRANSPORT_CONTROL_ALPN)
            .await
            .unwrap();
        let response = connection
            .rpc(ProtocolRequest::Control(ControlRequest::CentralStatus))
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
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn connection_event_stream_roundtrip() {
        let server = Transport::bind(vec![TRANSPORT_CONTROL_ALPN], None, None, None)
            .await
            .unwrap();
        let client = Transport::bind(Vec::new(), None, None, None).await.unwrap();
        let server_addr = server.addr();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(2);

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await.unwrap().unwrap();
            let _ = connection
                .serve(move |request| {
                    let event_tx = event_tx.clone();
                    async move {
                        let ProtocolRequest::Event(event) = request else {
                            return Ok(Some(ProtocolResponse::Error(Fs0Error::InvalidRequest)));
                        };
                        event_tx.send(event).await.unwrap();
                        Ok(None)
                    }
                })
                .await;
        });

        let connection = client
            .connect(server_addr, TRANSPORT_CONTROL_ALPN)
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
        server_task.await.unwrap();
    }
}
