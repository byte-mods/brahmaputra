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
use crate::tls::TlsSettings;

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

/// A transport plus the TLS material it should use.
///
/// Separate from [`Transport`] so every existing caller that passes a bare
/// `Transport` keeps working — the conversion is free and produces the same
/// defaults as before. Only a caller that has certificates to offer, or a
/// CA to check the broker against, needs to name this type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportConfig {
    pub transport: Transport,
    pub tls: TlsSettings,
    /// Credentials to present on every connection this configuration
    /// opens.
    ///
    /// Carried with the transport rather than passed to one call, because
    /// a client reconnects: a producer whose broker restarted opens a new
    /// socket, and a socket that skipped authentication is anonymous no
    /// matter what the first one did.
    pub credentials: Option<Credentials>,
}

impl TransportConfig {
    pub fn new(transport: Transport, tls: TlsSettings) -> Self {
        TransportConfig {
            transport,
            tls,
            credentials: None,
        }
    }

    pub fn with_credentials(mut self, credentials: Option<Credentials>) -> Self {
        self.credentials = credentials;
        self
    }
}

impl From<Transport> for TransportConfig {
    fn from(transport: Transport) -> Self {
        TransportConfig {
            transport,
            tls: TlsSettings::default(),
            credentials: None,
        }
    }
}

impl fmt::Display for TransportConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.transport.fmt(f)
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
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<Connection, ClientError> {
        let config = transport.into();
        match config.transport {
            Transport::Tcp => Ok(Connection::Tcp(
                TcpConnection::connect(addr, client_id, max_in_flight).await?,
            )),
            Transport::TcpTls => Ok(Connection::Tcp(
                TcpConnection::connect_tls(addr, client_id, max_in_flight, &config.tls).await?,
            )),
            Transport::Quic => Ok(Connection::Quic(
                QuicConnection::connect(addr, client_id, max_in_flight, &config.tls).await?,
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

/// How a client proves who it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaslMechanism {
    /// The password crosses the wire in the clear, exactly as SASL/PLAIN
    /// does, so a broker refuses it on a plaintext listener.
    Plain,
    /// A challenge–response in which the password never travels at all
    /// (RFC 5802). Two round trips instead of one, and safe on a plaintext
    /// listener — which is the whole reason to prefer it.
    #[default]
    ScramSha256,
}

impl SaslMechanism {
    fn name(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => brahmaputra_metadata::scram::MECHANISM,
        }
    }

    /// Parse the spelling used on a command line.
    pub fn parse(name: &str) -> Option<SaslMechanism> {
        match name.to_ascii_uppercase().as_str() {
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" | "SCRAM" => Some(SaslMechanism::ScramSha256),
            _ => None,
        }
    }
}

/// Credentials a client presents when the broker requires authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    /// Which mechanism to use. SCRAM by default, because it is safe on
    /// every listener and PLAIN is not.
    pub mechanism: SaslMechanism,
}

impl Credentials {
    /// Credentials using the default mechanism.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Credentials {
        Credentials {
            username: username.into(),
            password: password.into(),
            mechanism: SaslMechanism::default(),
        }
    }

    pub fn with_mechanism(mut self, mechanism: SaslMechanism) -> Credentials {
        self.mechanism = mechanism;
        self
    }
}

impl Connection {
    /// Bind a principal to this connection. Every later request on it is
    /// authorized as that principal.
    pub async fn authenticate(&self, credentials: &Credentials) -> Result<String, ClientError> {
        match credentials.mechanism {
            SaslMechanism::Plain => self.authenticate_plain(credentials).await,
            SaslMechanism::ScramSha256 => self.authenticate_scram(credentials).await,
        }
    }

    async fn authenticate_plain(&self, credentials: &Credentials) -> Result<String, ClientError> {
        let request = AuthenticateRequest {
            username: credentials.username.clone(),
            password: credentials.password.clone(),
            mechanism: SaslMechanism::Plain.name().to_owned(),
            payload: String::new(),
        };
        let response = self.authenticate_step(request).await?;
        Ok(response.principal)
    }

    /// The SCRAM-SHA-256 exchange, client side.
    ///
    /// The server's final message is verified rather than ignored: without
    /// that check a client would authenticate happily to anything that
    /// could relay the first two messages, which is precisely the attack
    /// mutual authentication exists to stop.
    async fn authenticate_scram(&self, credentials: &Credentials) -> Result<String, ClientError> {
        use brahmaputra_metadata::scram;

        let client_nonce = scram::random_nonce();
        let bare = format!("n={},r={}", credentials.username, client_nonce);
        let first = self
            .authenticate_step(AuthenticateRequest {
                username: credentials.username.clone(),
                password: String::new(),
                mechanism: SaslMechanism::ScramSha256.name().to_owned(),
                payload: format!("n,,{bare}"),
            })
            .await?;
        if first.done {
            return Err(ClientError::Configuration(
                "broker ended the SCRAM exchange before it began".into(),
            ));
        }
        let server_first = first.payload;
        let (Some(nonce), Some(salt), Some(iterations)) = (
            scram::field(&server_first, "r"),
            scram::field(&server_first, "s"),
            scram::field(&server_first, "i").and_then(|value| value.parse::<u32>().ok()),
        ) else {
            return Err(ClientError::Configuration(
                "malformed SCRAM server-first message".into(),
            ));
        };
        // The server must have kept the client's nonce, which is what makes
        // this exchange this exchange rather than a replayed one.
        if !nonce.starts_with(&client_nonce) {
            return Err(ClientError::Configuration(
                "SCRAM server nonce does not extend the client nonce".into(),
            ));
        }
        // `biws` is base64 of "n,," — the GS2 header, echoed so the server
        // can see it was not tampered with in flight.
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let proof = scram::ScramCredential::client_proof(
            &credentials.password,
            salt,
            iterations,
            &auth_message,
        );
        let final_response = self
            .authenticate_step(AuthenticateRequest {
                username: credentials.username.clone(),
                password: String::new(),
                mechanism: SaslMechanism::ScramSha256.name().to_owned(),
                payload: format!("{without_proof},p={proof}"),
            })
            .await?;
        Ok(final_response.principal)
    }

    async fn authenticate_step(
        &self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse, ClientError> {
        let body = request
            .encode()
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        let response = self.request(ApiKey::Authenticate, &body).await?;
        let response = AuthenticateResponse::decode(&response)
            .map_err(|error| ClientError::Configuration(error.to_string()))?;
        ClientError::from_error_code(response.error_code)?;
        Ok(response)
    }
}
