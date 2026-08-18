//! One multiplexed TCP connection to a broker.
//!
//! A writer task owns the send half, a reader task the receive half.
//! Requests are stamped with a correlation id and matched to responses by
//! id, so multiple requests can be in flight at once and responses may
//! arrive out of order. In-flight requests are bounded by a semaphore
//! (`max_in_flight`, DESIGN.md §8 backpressure). If either task dies, every
//! pending request fails with [`ClientError::ConnectionClosed`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use brahmaputra_protocol::{decode_payload, encode_payload, ApiKey, FrameHeader};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::debug;

use crate::error::ClientError;

const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

type Pending = Arc<Mutex<HashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>>>;

/// Either a plain or a TLS-wrapped socket: the framing above them is
/// identical, so the rest of the connection does not care which it has.
pub(crate) trait ClientStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> ClientStream for T {}

/// A multiplexed TCP client connection. Cheap to clone.
#[derive(Clone)]
pub struct TcpConnection {
    inner: Arc<Inner>,
}

struct Inner {
    out_tx: mpsc::Sender<Bytes>,
    pending: Pending,
    next_correlation: AtomicI32,
    in_flight: Semaphore,
    client_id: Option<String>,
}

impl TcpConnection {
    /// Connect to `addr`; `max_in_flight` bounds unacknowledged requests.
    pub async fn connect(
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<TcpConnection, ClientError> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        TcpConnection::from_stream(Box::new(socket), client_id, max_in_flight)
    }

    /// Connect with TLS 1.3 over the same TCP framing.
    ///
    /// The broker presents a certificate it generated at startup, which
    /// this client accepts without verifying a name: the data plane has no
    /// identity story yet (M6 adds the user store), so this buys
    /// confidentiality and integrity on the wire, not authentication.
    pub async fn connect_tls(
        addr: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<TcpConnection, ClientError> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(
                crate::quic::AcceptAnyServerCert,
            ))
            .with_no_client_auth();
        config.alpn_protocols = vec![crate::quic::ALPN.to_vec()];
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
        let server_name = rustls_pki_types::ServerName::try_from("brahmaputra")
            .map_err(|error| ClientError::Configuration(format!("tls server name: {error}")))?;
        let stream = connector.connect(server_name, socket).await?;
        TcpConnection::from_stream(Box::new(stream), client_id, max_in_flight)
    }

    fn from_stream(
        stream: Box<dyn ClientStream>,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<TcpConnection, ClientError> {
        let codec = LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(MAX_FRAME_BYTES)
            .new_codec();
        let framed = Framed::new(stream, codec);
        let (mut sink, mut stream) = framed.split();

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(max_in_flight * 2);

        let writer = tokio::spawn(async move {
            let mut result = Ok(());
            while let Some(payload) = out_rx.recv().await {
                if let Err(e) = sink.send(payload).await {
                    result = Err(ClientError::Io(e));
                    break;
                }
            }
            result
        });

        let reader_pending = Arc::clone(&pending);
        let reader = tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(bytes) => {
                        let mut payload = bytes.freeze();
                        match decode_payload(&mut payload) {
                            Ok(header) => {
                                let waiter = reader_pending
                                    .lock()
                                    .expect("pending")
                                    .remove(&header.correlation_id);
                                if let Some(waiter) = waiter {
                                    let _ = waiter.send(Ok(payload));
                                } else {
                                    debug!(
                                        correlation_id = header.correlation_id,
                                        "response with no waiter (late or duplicate)"
                                    );
                                }
                            }
                            Err(e) => {
                                return Err(ClientError::Protocol(e));
                            }
                        }
                    }
                    Err(e) => return Err(ClientError::Io(e)),
                }
            }
            Ok(())
        });

        // Whichever task finishes first tears the connection down: abort
        // the other and fail every pending request.
        let janitor_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut writer = writer;
            let mut reader = reader;
            tokio::select! {
                r = &mut writer => {
                    debug!(?r, "connection writer stopped");
                    reader.abort();
                }
                r = &mut reader => {
                    debug!(?r, "connection reader stopped");
                    writer.abort();
                }
            }
            let waiters: Vec<_> = janitor_pending
                .lock()
                .expect("pending")
                .drain()
                .map(|(_, w)| w)
                .collect();
            for waiter in waiters {
                let _ = waiter.send(Err(ClientError::ConnectionClosed));
            }
        });

        Ok(TcpConnection {
            inner: Arc::new(Inner {
                out_tx,
                pending,
                next_correlation: AtomicI32::new(0),
                in_flight: Semaphore::new(max_in_flight),
                client_id,
            }),
        })
    }

    /// Send a request without waiting for a response (acks=0 produce).
    pub async fn send(&self, api_key: ApiKey, body: &[u8]) -> Result<(), ClientError> {
        let correlation_id = self.inner.next_correlation.fetch_add(1, Ordering::Relaxed);
        let header = FrameHeader::new(api_key, correlation_id, self.inner.client_id.clone());
        let payload = encode_payload(&header, body);
        self.inner
            .out_tx
            .send(payload)
            .await
            .map_err(|_| ClientError::ConnectionClosed)
    }

    /// Send a request and await its response body (frame header stripped).
    pub async fn request(&self, api_key: ApiKey, body: &[u8]) -> Result<Bytes, ClientError> {
        // The permit is held until the response arrives (or the send fails).
        let permit = self
            .inner
            .in_flight
            .acquire()
            .await
            .map_err(|_| ClientError::ConnectionClosed)?;
        let correlation_id = self.inner.next_correlation.fetch_add(1, Ordering::Relaxed);
        let header = FrameHeader::new(api_key, correlation_id, self.inner.client_id.clone());
        let payload = encode_payload(&header, body);

        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .expect("pending")
            .insert(correlation_id, tx);
        if self.inner.out_tx.send(payload).await.is_err() {
            self.inner
                .pending
                .lock()
                .expect("pending")
                .remove(&correlation_id);
            return Err(ClientError::ConnectionClosed);
        }
        let result = rx.await.map_err(|_| ClientError::ConnectionClosed)?;
        drop(permit);
        result
    }
}
