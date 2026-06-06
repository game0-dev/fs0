use crate::{Fs0Error, Fs0Result};
use fs0_config::ClientConfig;
use fs0_transport::{EndpointAddr, EndpointId};
use std::net::SocketAddr;

pub(super) fn central_endpoint_addr(config: &ClientConfig) -> Fs0Result<EndpointAddr> {
    let endpoint_id = parse_endpoint_id(&config.central_endpoint_id, "client.central_endpoint_id")?;
    let socket_addr = parse_socket_addr(&config.central_addr, "client.central_addr")?;

    Ok(EndpointAddr::new(endpoint_id).with_ip_addr(socket_addr))
}

pub(super) fn decode_endpoint_addr(bytes: &[u8]) -> Fs0Result<EndpointAddr> {
    postcard::from_bytes(bytes).map_err(Fs0Error::from)
}

fn parse_endpoint_id(value: &str, field: &str) -> Fs0Result<EndpointId> {
    value
        .parse::<EndpointId>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field}: {err}"),
        })
}

fn parse_socket_addr(value: &str, field: &str) -> Fs0Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .map_err(|err| Fs0Error::InvalidConfig {
            message: format!("invalid {field} {value}: {err}"),
        })
}
