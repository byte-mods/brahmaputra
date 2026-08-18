//! Transport selection for the data plane.
//!
//! The wire *format* is the same either way — length-prefixed frames whose
//! bodies are BitPacker structs (plus raw record-batch bytes). What differs
//! is how concurrent requests share a connection:
//!
//! | | TCP | QUIC |
//! |---|---|---|
//! | Multiplexing | one byte stream, correlation ids | one bidirectional stream per request |
//! | Head-of-line blocking | yes: a lost segment stalls every in-flight request | no: loss only delays its own stream |
//! | Encryption | none (M5 adds TLS) | mandatory TLS 1.3 |
//! | Handshake | 1 RTT (TCP) | 1 RTT, 0-RTT possible on resumption |
//!
//! Pick with `--transport` on both broker and client; a broker speaks one
//! transport at a time, so both ends must agree.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use brahmaputra_protocol::gen::{AuthenticateRequest, AuthenticateResponse};
use brahmaputra_protocol::ApiKey;
use bytes::Bytes;

use crate::connection::TcpConnection;
use crate::error::ClientError;
use crate::quic::QuicConnection;

/// Which transport carries data-plane frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    #[default]
    Tcp,
    /// TCP with TLS 1.3. Same multiplexing as `Tcp`, encrypted.
    TcpTls,
    Quic,
}

impl FromStr for Transport {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "tcp" => Ok(Transport::Tcp),
            "tcp-tls" => Ok(Transport::TcpTls),
            "quic" => Ok(Transport::Quic),
            other => Err(format!(
                "unknown transport {other:?}, expected tcp, tcp-tls or quic"
            )),
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Transport::Tcp => "tcp",
            Transport::TcpTls => "tcp-tls",
            Transport::Quic => "quic",
        })
    }
}

/// A connection to one broker over the configured transport.
#[derive(Clone)]
pub enum Connection {
    Tcp(TcpConnection),
    Quic(QuicConnection),
}

impl Connection {
    /// Connect over TCP. Kept as the default so existing callers and tests
    /// read unchanged.
    pub async fn connect(
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<Connection, ClientError> {
        Connection::connect_with(Transport::Tcp, addr, client_id, max_in_flight).await
    }

    pub async fn connect_with(
        transport: Transport,
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<Connection, ClientError> {
        match transport {
            Transport::Tcp => Ok(Connection::Tcp(
                TcpConnection::connect(addr, client_id, max_in_flight).await?,
            )),
            Transport::TcpTls => Ok(Connection::Tcp(
                TcpConnection::connect_tls(addr, client_id, max_in_flight).await?,
            )),
            Transport::Quic => Ok(Connection::Quic(
                QuicConnection::connect(addr, client_id, max_in_flight).await?,
            )),
        }
    }

    /// Send a request without waiting for a response (`acks=0` produce).
    pub async fn send(&self, api_key: ApiKey, body: &[u8]) -> Result<(), ClientError> {
        match self {
            Connection::Tcp(connection) => connection.send(api_key, body).await,
            Connection::Quic(connection) => connection.send(api_key, body).await,
        }
    }

    /// Send a request and await its response body (frame header stripped).
    pub async fn request(&self, api_key: ApiKey, body: &[u8]) -> Result<Bytes, ClientError> {
        match self {
            Connection::Tcp(connection) => connection.request(api_key, body).await,
            Connection::Quic(connection) => connection.request(api_key, body).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_parses_from_its_own_display_form() {
        for transport in [Transport::Tcp, Transport::Quic] {
            assert_eq!(
                transport.to_string().parse::<Transport>().unwrap(),
                transport
            );
        }
        assert!("udp".parse::<Transport>().is_err());
        assert_eq!(Transport::default(), Transport::Tcp);
    }
}

/// Credentials a client presents when the broker requires authentication.
///
/// The password crosses the wire in the clear, exactly as SASL/PLAIN does,
/// so a broker refuses this on a plaintext listener. Use `tcp-tls` or
/// `quic`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

impl Connection {
    /// Bind a principal to this connection. Every later request on it is
    /// authorized as that principal.
    pub async fn authenticate(&self, credentials: &Credentials) -> Result<String, ClientError> {
        let request = AuthenticateRequest {
            username: credentials.username.clone(),
            password: credentials.password.clone(),
        };
        let body = request
            .encode()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let response = self.request(ApiKey::Authenticate, &body).await?;
        let response = AuthenticateResponse::decode(&response)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        ClientError::from_error_code(response.error_code)?;
        Ok(response.principal)
    }
}
