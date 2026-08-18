//! QUIC transport for the data plane (DESIGN.md §6, transport option).
//!
//! The frame format is identical to the TCP transport — the same
//! length-prefixed header + BitPacker body — but the multiplexing model is
//! different, and that difference is the point:
//!
//! * **TCP**: one byte stream carries every request and response, matched
//!   by correlation id. A lost segment stalls *every* in-flight request
//!   behind it (head-of-line blocking), because the kernel must deliver the
//!   stream in order.
//! * **QUIC**: each request opens its own bidirectional stream — the
//!   request is written on one direction, the response read from the other.
//!   Streams are independently ordered, so a loss on one only delays that
//!   request. Correlation ids are unnecessary: the stream *is* the
//!   correlation.
//!
//! Connections are established with TLS 1.3 (QUIC requires it). The broker
//! presents a self-signed certificate generated at startup, and this client
//! accepts it: the data plane has no authentication story yet (M5/M6), so
//! verifying a name here would imply a guarantee that does not exist.

use std::net::SocketAddr;
use std::sync::Arc;

use brahmaputra_protocol::{decode_payload, encode_payload, ApiKey, FrameHeader};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use quinn::{ClientConfig, Endpoint, TransportConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use crate::error::ClientError;

/// ALPN protocol identifier for the Brahmaputra data plane over QUIC.
pub const ALPN: &[u8] = b"brahmaputra/1";
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

/// Bytes a single stream may have outstanding before the peer must ack.
/// A record batch can be megabytes, and quinn's default (1.25 MB, sized
/// for a 100 ms internet path) means one large batch fills the window and
/// stalls the sender mid-request.
const STREAM_RECEIVE_WINDOW: u32 = 32 * 1024 * 1024;
/// Bytes in flight across all streams of a connection.
const CONNECTION_WINDOW: u32 = 256 * 1024 * 1024;

/// Retune quinn's defaults, which target a lossy ~100 ms internet path,
/// for the links a broker actually runs on.
///
/// Left alone, the defaults cost most of QUIC's throughput here: a 1 MiB
/// record nearly fills the default per-stream window, and the congestion
/// controller starts from a 333 ms RTT estimate on a sub-millisecond LAN.
/// These are *transport* parameters only — none of them weaken the
/// delivery, ordering or integrity guarantees.
pub fn tune_for_datacenter(transport: &mut TransportConfig) {
    transport.stream_receive_window(STREAM_RECEIVE_WINDOW.into());
    transport.receive_window(CONNECTION_WINDOW.into());
    transport.send_window(CONNECTION_WINDOW as u64);
    // The default 333 ms initial estimate makes the controller ramp as if
    // every peer were a satellite link. 5 ms is still conservative for a
    // datacentre and only seeds the estimate — measurement takes over.
    transport.initial_rtt(std::time::Duration::from_millis(5));
    // Start near a standard Ethernet MTU instead of the 1200 B floor;
    // MTU discovery still probes and backs off if the path is smaller.
    transport.initial_mtu(1350);
    transport.datagram_receive_buffer_size(Some(STREAM_RECEIVE_WINDOW as usize));
}

/// A QUIC connection to one broker. Cheap to clone; every request gets its
/// own bidirectional stream, so clones share the connection without
/// serialising behind each other.
#[derive(Clone)]
pub struct QuicConnection {
    connection: quinn::Connection,
    client_id: Option<String>,
    // Kept alive for the lifetime of the connection: dropping the endpoint
    // closes its UDP socket and with it every connection it created.
    _endpoint: Arc<Endpoint>,
}

impl QuicConnection {
    pub async fn connect(
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<QuicConnection, ClientError> {
        let bind: SocketAddr = if addr.is_ipv4() {
            "0.0.0.0:0".parse().expect("valid bind address")
        } else {
            "[::]:0".parse().expect("valid bind address")
        };
        let mut endpoint = Endpoint::client(bind).map_err(ClientError::Io)?;

        let mut tls = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
            .with_no_client_auth();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let tls = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .map_err(|error| ClientError::Configuration(format!("quic tls config: {error}")))?;
        let mut config = ClientConfig::new(Arc::new(tls));

        let mut transport = TransportConfig::default();
        // One stream per in-flight request, plus headroom so a burst never
        // blocks on stream credit.
        transport.max_concurrent_bidi_streams((max_in_flight.max(1) as u32 * 4).into());
        tune_for_datacenter(&mut transport);
        config.transport_config(Arc::new(transport));
        endpoint.set_default_client_config(config);

        let connection = endpoint
            .connect(addr, "brahmaputra")
            .map_err(|error| ClientError::Configuration(format!("quic connect: {error}")))?
            .await
            .map_err(|error| ClientError::Configuration(format!("quic handshake: {error}")))?;

        Ok(QuicConnection {
            connection,
            client_id,
            _endpoint: Arc::new(endpoint),
        })
    }

    /// One request on its own bidirectional stream: write the framed
    /// request, finish our half so the broker sees a complete request, then
    /// read the response off the other half.
    pub async fn request(&self, api_key: ApiKey, body: &[u8]) -> Result<Bytes, ClientError> {
        let (mut send, mut recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|error| connection_error("open stream", error))?;

        let frame = self.frame(api_key, body);
        send.write_all(&frame)
            .await
            .map_err(|error| ClientError::Io(std::io::Error::other(error)))?;
        send.finish()
            .map_err(|error| ClientError::Io(std::io::Error::other(error)))?;

        let response = recv
            .read_to_end(MAX_FRAME_BYTES)
            .await
            .map_err(|error| ClientError::Io(std::io::Error::other(error)))?;
        decode_response(Bytes::from(response))
    }

    /// Fire and forget (`acks=0`): write the request and close our half
    /// without waiting for anything back.
    pub async fn send(&self, api_key: ApiKey, body: &[u8]) -> Result<(), ClientError> {
        let mut send = self
            .connection
            .open_uni()
            .await
            .map_err(|error| connection_error("open stream", error))?;
        let frame = self.frame(api_key, body);
        send.write_all(&frame)
            .await
            .map_err(|error| ClientError::Io(std::io::Error::other(error)))?;
        send.finish()
            .map_err(|error| ClientError::Io(std::io::Error::other(error)))?;
        Ok(())
    }

    fn frame(&self, api_key: ApiKey, body: &[u8]) -> Bytes {
        // Correlation ids are meaningless per stream, but the header shape
        // is shared with TCP so brokers decode one frame format.
        let header = FrameHeader::new(api_key, 0, self.client_id.clone());
        let payload = encode_payload(&header, body);
        let mut framed = BytesMut::with_capacity(4 + payload.len());
        framed.put_u32(payload.len() as u32);
        framed.extend_from_slice(&payload);
        framed.freeze()
    }
}

fn decode_response(mut response: Bytes) -> Result<Bytes, ClientError> {
    if response.len() < 4 {
        return Err(ClientError::ConnectionClosed);
    }
    let length = response.get_u32() as usize;
    if response.len() < length {
        return Err(ClientError::ConnectionClosed);
    }
    let mut payload = response.split_to(length);
    decode_payload(&mut payload)?;
    Ok(payload)
}

fn connection_error(what: &str, error: quinn::ConnectionError) -> ClientError {
    match error {
        quinn::ConnectionError::ApplicationClosed(_)
        | quinn::ConnectionError::ConnectionClosed(_)
        | quinn::ConnectionError::LocallyClosed => ClientError::ConnectionClosed,
        other => ClientError::Io(std::io::Error::other(format!("quic {what}: {other}"))),
    }
}

/// The data plane has no server authentication yet, so the certificate is
/// accepted as-is. This is deliberately explicit rather than hidden behind
/// a "skip verification" flag: when authn lands (DESIGN.md §9.4), this is
/// the single place that has to change.
#[derive(Debug)]
pub(crate) struct AcceptAnyServerCert;

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}
