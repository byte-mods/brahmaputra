//! QUIC listener for the data plane, the alternative to the TCP acceptor in
//! [`crate::server`].
//!
//! Frames are byte-identical to the TCP transport; only the multiplexing
//! changes. Each client request arrives on its own bidirectional stream, is
//! dispatched by the same [`crate::handlers::dispatch`], and its response is
//! written back on that stream's send half. Because streams are ordered
//! independently of one another, a lost packet delays only the request it
//! belonged to instead of every request behind it in a shared byte stream.
//!
//! `acks=0` produce requests arrive on unidirectional streams: there is no
//! response to write, so there is no reason to pay for a return path.
//!
//! TLS is mandatory in QUIC. The broker generates a self-signed certificate
//! at startup; the data plane has no authentication yet (M5/M6), so this
//! provides encryption and integrity, not identity.

use std::net::SocketAddr;
use std::sync::Arc;

use brahmaputra_protocol::decode_payload;
use bytes::{Buf, Bytes};
use quinn::{Endpoint, ServerConfig, TransportConfig};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::error::BrokerError;
use crate::handlers;
use crate::server::Broker;

/// ALPN protocol identifier, shared with the client.
pub const ALPN: &[u8] = b"brahmaputra/1";

/// A self-signed certificate and key, generated at startup.
///
/// Shared by the QUIC listener and the TLS-over-TCP acceptor so both
/// transports present the same kind of identity — which is to say,
/// encryption without authentication until the M6 user store lands.
pub(crate) fn self_signed_identity() -> Result<
    (
        rustls_pki_types::CertificateDer<'static>,
        rustls_pki_types::PrivatePkcs8KeyDer<'static>,
    ),
    BrokerError,
> {
    let certificate = rcgen::generate_simple_self_signed(vec!["brahmaputra".to_owned()])
        .map_err(|error| BrokerError::Meta(format!("cannot generate certificate: {error}")))?;
    let cert_der = certificate.cert.der().clone();
    let key_der = rustls_pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der());
    Ok((cert_der, key_der))
}

/// A bound QUIC endpoint, ready to accept connections.
pub struct QuicListener {
    endpoint: Endpoint,
    local_addr: SocketAddr,
}

impl QuicListener {
    /// Bind a QUIC endpoint on `addr` with a freshly generated self-signed
    /// certificate.
    pub fn bind(addr: SocketAddr, max_frame_bytes: usize) -> Result<QuicListener, BrokerError> {
        let (cert_der, key_der) = self_signed_identity()?;

        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der.into())
            .map_err(|error| BrokerError::Meta(format!("cannot build quic tls config: {error}")))?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let tls = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
            .map_err(|error| BrokerError::Meta(format!("quic tls config: {error}")))?;

        let mut config = ServerConfig::with_crypto(Arc::new(tls));
        let mut transport = TransportConfig::default();
        // Producers and consumers open one stream per request; a busy client
        // with a deep in-flight window needs plenty of stream credit.
        transport.max_concurrent_bidi_streams(1024u32.into());
        transport.max_concurrent_uni_streams(1024u32.into());
        // Both ends must widen their windows: flow control is advertised by
        // the receiver, so a tuned client against a default broker still
        // stalls on the broker's credit.
        brahmaputra_client::tune_quic_transport(&mut transport);
        config.transport_config(Arc::new(transport));

        let endpoint = Endpoint::server(config, addr)
            .map_err(|error| BrokerError::Meta(format!("cannot bind quic endpoint: {error}")))?;
        let local_addr = endpoint
            .local_addr()
            .map_err(|error| BrokerError::Meta(format!("quic endpoint has no address: {error}")))?;
        debug!(%local_addr, max_frame_bytes, "quic endpoint bound");
        Ok(QuicListener {
            endpoint,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept connections until `shutdown` resolves.
    pub async fn serve(self, broker: Arc<Broker>, max_frame_bytes: usize) {
        let mut connections = JoinSet::new();
        let mut shutdown = broker.shutdown_receiver();
        loop {
            tokio::select! {
                biased;
                _ = async {
                    while !*shutdown.borrow_and_update() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                } => break,
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let broker = Arc::clone(&broker);
                    connections.spawn(async move {
                        match incoming.await {
                            Ok(connection) => {
                                serve_connection(broker, connection, max_frame_bytes).await;
                            }
                            Err(error) => debug!(%error, "quic handshake failed"),
                        }
                    });
                }
            }
        }
        self.endpoint.close(0u32.into(), b"shutdown");
        connections.shutdown().await;
    }
}

async fn serve_connection(
    broker: Arc<Broker>,
    connection: quinn::Connection,
    max_frame_bytes: usize,
) {
    let peer = connection.remote_address();
    debug!(%peer, "quic connection accepted");
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            bi = connection.accept_bi() => match bi {
                Ok((send, recv)) => {
                    let broker = Arc::clone(&broker);
                    streams.spawn(async move {
                        if let Err(error) = serve_request(broker, send, recv, max_frame_bytes).await {
                            debug!(%error, "quic request stream failed");
                        }
                    });
                }
                Err(error) => {
                    debug!(%peer, %error, "quic connection closed");
                    break;
                }
            },
            uni = connection.accept_uni() => match uni {
                Ok(recv) => {
                    let broker = Arc::clone(&broker);
                    streams.spawn(async move {
                        if let Err(error) = serve_oneway(broker, recv, max_frame_bytes).await {
                            debug!(%error, "quic oneway stream failed");
                        }
                    });
                }
                Err(error) => {
                    debug!(%peer, %error, "quic connection closed");
                    break;
                }
            },
        }
    }
    streams.shutdown().await;
}

/// Read one framed request off `recv`, dispatch it, write the response.
async fn serve_request(
    broker: Arc<Broker>,
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
    max_frame_bytes: usize,
) -> Result<(), String> {
    let (header, body) = read_frame(recv, max_frame_bytes).await?;
    let Some(response) = handlers::dispatch(&broker, &header, body).await else {
        return Ok(());
    };
    // Write the prefix and then each of the response's own buffers, rather
    // than concatenating them first. A fetch response is mostly record
    // batches straight from the page cache, and joining them into one
    // buffer would copy every byte served for no reason — QUIC frames the
    // stream for us either way.
    let prefix = brahmaputra_protocol::encode_frame_prefix(
        &brahmaputra_protocol::FrameHeader::new(
            header.api_key,
            header.correlation_id,
            header.client_id.clone(),
        ),
        response.len(),
    );
    send.write_all(&prefix)
        .await
        .map_err(|error| format!("write response: {error}"))?;
    for chunk in response.chunks() {
        send.write_all(chunk)
            .await
            .map_err(|error| format!("write response: {error}"))?;
    }
    send.finish().map_err(|error| format!("finish: {error}"))?;
    Ok(())
}

/// A request with no response (`acks=0`).
async fn serve_oneway(
    broker: Arc<Broker>,
    recv: quinn::RecvStream,
    max_frame_bytes: usize,
) -> Result<(), String> {
    let (header, body) = read_frame(recv, max_frame_bytes).await?;
    if let Some(_response) = handlers::dispatch(&broker, &header, body).await {
        warn!(api_key = ?header.api_key, "response dropped: request arrived on a oneway stream");
    }
    Ok(())
}

async fn read_frame(
    mut recv: quinn::RecvStream,
    max_frame_bytes: usize,
) -> Result<(brahmaputra_protocol::FrameHeader, Bytes), String> {
    let raw = recv
        .read_to_end(max_frame_bytes)
        .await
        .map_err(|error| format!("read request: {error}"))?;
    let mut raw = Bytes::from(raw);
    if raw.len() < 4 {
        return Err("truncated frame".to_owned());
    }
    let length = raw.get_u32() as usize;
    if raw.len() < length {
        return Err("frame shorter than its length prefix".to_owned());
    }
    let mut payload = raw.split_to(length);
    let header = decode_payload(&mut payload).map_err(|error| format!("decode header: {error}"))?;
    Ok((header, payload))
}
