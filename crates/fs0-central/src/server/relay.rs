use crate::{Fs0Error, Fs0Result};
use fs0_config::CentralRelayConfig;
use iroh_relay::server::{
    Access as RelayAccess, AccessConfig as RelayAccessConfig, CertConfig as RelayCertConfig,
    QuicConfig as RelayQuicConfig, RelayConfig as RelayServerConfig, Server as RelayServer,
    ServerConfig as RelayRootConfig, TlsConfig as RelayTlsConfig,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{net::SocketAddr, sync::Arc};

pub(super) async fn spawn_relay(config: &CentralRelayConfig) -> Fs0Result<RelayServer> {
    if config.token.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: "central.relay.token must not be empty".to_owned(),
        });
    }

    let mut relay_config = RelayServerConfig::new(SocketAddr::from(([127, 0, 0, 1], 0)));
    relay_config.access = relay_access_config(config.token.clone());
    relay_config.tls = Some(relay_tls_config(config)?);

    let mut root_config = RelayRootConfig::default();
    root_config.quic = Some(RelayQuicConfig::new(SocketAddr::from((
        [0, 0, 0, 0],
        config.quic_bind_port,
    ))));
    root_config.relay = Some(relay_config);

    let relay = RelayServer::spawn(root_config)
        .await
        .map_err(|err| Fs0Error::Internal {
            message: format!("failed to start relay: {err}"),
        })?;

    Ok(relay)
}

fn relay_access_config(token: String) -> RelayAccessConfig {
    let token = Arc::new(token);
    RelayAccessConfig::Restricted(Box::new(move |request| {
        let token = token.clone();
        Box::pin(async move {
            if request.auth_token().as_deref() == Some(token.as_str()) {
                RelayAccess::Allow
            } else {
                RelayAccess::Deny
            }
        })
    }))
}

fn relay_tls_config(config: &CentralRelayConfig) -> Fs0Result<RelayTlsConfig> {
    let certs = CertificateDer::pem_file_iter(&config.cert_path)
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to open central.relay cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay cert_path {}: {err}",
                config.cert_path.display()
            ),
        })?;
    if certs.is_empty() {
        return Err(Fs0Error::InvalidConfig {
            message: format!(
                "central.relay cert_path {} contains no certificates",
                config.cert_path.display()
            ),
        });
    }

    let private_key =
        PrivateKeyDer::from_pem_file(&config.key_path).map_err(|err| Fs0Error::InvalidConfig {
            message: format!(
                "failed to read central.relay key_path {}: {err}",
                config.key_path.display()
            ),
        })?;
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("failed to configure central.relay TLS protocols: {err}"),
    })?
    .with_no_client_auth()
    .with_single_cert(certs, private_key)
    .map_err(|err| Fs0Error::InvalidConfig {
        message: format!("invalid central.relay certificate or key: {err}"),
    })?;

    Ok(RelayTlsConfig::new(
        SocketAddr::from(([0, 0, 0, 0], config.https_bind_port)),
        RelayCertConfig::Manual { server_config },
    ))
}
