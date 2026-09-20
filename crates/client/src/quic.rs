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
use quinn::{ClientConfig, Endpoint, EndpointConfig, MtuDiscoveryConfig, TransportConfig};
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
const MAX_DISCOVERED_UDP_PAYLOAD: u16 = 9000;

/// Advertise capacity for jumbo datagrams without assuming that the path can
/// carry them. Transport discovery starts small and only grows after probes
/// are acknowledged; standard-MTU paths retain their smaller packet size.
pub fn datacenter_endpoint_config() -> EndpointConfig {
    let mut config = EndpointConfig::default();
    config
        .max_udp_payload_size(MAX_DISCOVERED_UDP_PAYLOAD)
        .expect("valid QUIC UDP payload size");
    config
}

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
    let mut discovery = MtuDiscoveryConfig::default();
    discovery.upper_bound(MAX_DISCOVERED_UDP_PAYLOAD);
    transport.mtu_discovery_config(Some(discovery));
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
        tls_settings: &crate::tls::TlsSettings,
    ) -> Result<QuicConnection, ClientError> {
        let bind: SocketAddr = if addr.is_ipv4() {
            "0.0.0.0:0".parse().expect("valid bind address")
        } else {
            "[::]:0".parse().expect("valid bind address")
        };
        let socket = std::net::UdpSocket::bind(bind).map_err(ClientError::Io)?;
        let mut endpoint = Endpoint::new(
            datacenter_endpoint_config(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(ClientError::Io)?;

        let tls = crate::tls::client_config(tls_settings)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn quic_discovers_jumbo_paths_and_survives_dropped_oversized_datagrams() {
        for path_limit in [9000, 1400] {
            let certificate =
                rcgen::generate_simple_self_signed(vec!["brahmaputra".into()]).unwrap();
            let key =
                rustls_pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der());
            let mut tls = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate.cert.der().clone()], key.into())
                .unwrap();
            tls.alpn_protocols = vec![ALPN.to_vec()];
            let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
            ));
            let mut transport = TransportConfig::default();
            tune_for_datacenter(&mut transport);
            server_config.transport_config(Arc::new(transport));
            let endpoint = Endpoint::new(
                datacenter_endpoint_config(),
                Some(server_config),
                std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
                Arc::new(quinn::TokioRuntime),
            )
            .unwrap();
            let server_address = endpoint.local_addr().unwrap();

            // A UDP relay models a path that silently drops oversized packets.
            // Its two directions share the same MTU, including discovery probes.
            let proxy = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let proxy_address = proxy.local_addr().unwrap();
            let dropped = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(AtomicUsize::new(0));
            let dropped_proxy = dropped.clone();
            let seen_proxy = seen.clone();
            let relay = tokio::spawn(async move {
                let mut client_address = None;
                let mut buffer = vec![0_u8; 65536];
                loop {
                    let (length, from) = proxy.recv_from(&mut buffer).await.unwrap();
                    let target = if from == server_address {
                        client_address.expect("client sent the initial packet")
                    } else {
                        client_address = Some(from);
                        server_address
                    };
                    if length > path_limit {
                        dropped_proxy.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    seen_proxy.fetch_max(length, Ordering::Relaxed);
                    proxy.send_to(&buffer[..length], target).await.unwrap();
                }
            });
            let server_endpoint = endpoint.clone();
            let (stop_server, stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let connection = server_endpoint.accept().await.unwrap().await.unwrap();
                for _ in 0..8 {
                    let (mut send, mut recv) = connection.accept_bi().await.unwrap();
                    let frame = recv.read_to_end(MAX_FRAME_BYTES).await.unwrap();
                    send.write_all(&frame).await.unwrap();
                    send.finish().unwrap();
                }
                let _ = stopped.await;
                connection.close(0_u32.into(), b"test complete");
            });

            tokio::time::timeout(Duration::from_secs(20), async {
                let client =
                    QuicConnection::connect(proxy_address, None, 5, &crate::TlsSettings::default())
                        .await
                        .unwrap();
                if path_limit == 9000 {
                    // Check acknowledged discovery before bulk traffic. A busy
                    // UDP relay can lose packets in the kernel receive queue;
                    // Quinn may correctly fall back after that loss even when
                    // every MTU probe succeeded. The final MTU is not a record
                    // of the largest size that was successfully discovered.
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while client.connection.stats().path.current_mtu != 9000 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await
                    .expect("jumbo probes must be acknowledged up to the configured ceiling");
                }
                for sequence in 0..8 {
                    let payload: Vec<u8> = (0..512 * 1024)
                        .map(|index| ((index + sequence) % 251) as u8)
                        .collect();
                    let reply = client.request(ApiKey::Metadata, &payload).await.unwrap();
                    assert_eq!(reply.as_ref(), payload.as_slice());
                }
                let mtu = client.connection.stats().path.current_mtu;
                eprintln!(
                    "path limit={path_limit}, discovered MTU={mtu}, dropped={}, largest forwarded={}, stats={:?}",
                    dropped.load(Ordering::Relaxed),
                    seen.load(Ordering::Relaxed),
                    client.connection.stats().path
                );
                if path_limit == 9000 {
                    assert_eq!(seen.load(Ordering::Relaxed), 9000);
                } else {
                    assert!(dropped.load(Ordering::Relaxed) > 0);
                    assert!(mtu <= 1400, "oversized probes cannot raise the usable MTU");
                }
            })
            .await
            .expect("both paths must deliver every byte without stalling");
            let _ = stop_server.send(());
            server.await.unwrap();
            relay.abort();
            endpoint.close(0_u32.into(), b"test complete");
        }
    }
}
