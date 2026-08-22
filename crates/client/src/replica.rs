//! Low-level cluster-internal replication client.
//!
//! Unlike the record-decoding consumer, this client returns leader batch
//! bytes exactly as received so a follower can pass them directly to
//! `PartitionHandle::append_replica_batch`. Non-zero wire error codes remain
//! in the typed response; the replica manager decides whether to truncate,
//! refresh metadata, or re-register its broker incarnation.

use std::net::SocketAddr;

use brahmaputra_protocol::replica::{
    decode_replica_fetch_response, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse,
    ReplicaFetchRequest, ReplicaFetchResponse,
};
use brahmaputra_protocol::ApiKey;
use bytes::Bytes;

use crate::transport::TransportConfig;
use crate::{ClientError, Connection, Transport};

/// Decoded ReplicaFetch response plus validated, byte-identical batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchResult {
    pub response: ReplicaFetchResponse,
    pub batches: Vec<Bytes>,
}

/// Cheap clone around a multiplexed broker connection.
#[derive(Clone)]
pub struct ReplicaClient {
    connection: Connection,
}

impl ReplicaClient {
    pub async fn connect(
        addr: SocketAddr,
        client_id: impl Into<String>,
        max_in_flight: usize,
    ) -> Result<Self, ClientError> {
        ReplicaClient::connect_with(Transport::default(), addr, client_id, max_in_flight).await
    }

    /// Follower fetches ride the same transport the cluster is configured
    /// for, so switching to QUIC moves replication traffic too.
    pub async fn connect_with(
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        client_id: impl Into<String>,
        max_in_flight: usize,
    ) -> Result<Self, ClientError> {
        let connection =
            Connection::connect_with(transport, addr, Some(client_id.into()), max_in_flight)
                .await?;
        Ok(Self { connection })
    }

    pub fn from_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Fetch uncommitted leader batches as raw validated bytes.
    pub async fn fetch_raw(
        &self,
        request: &ReplicaFetchRequest,
    ) -> Result<ReplicaFetchResult, ClientError> {
        let body = request.encode()?;
        let body = self.connection.request(ApiKey::ReplicaFetch, &body).await?;
        let (response, batches) = decode_replica_fetch_response(body)?;
        Ok(ReplicaFetchResult { response, batches })
    }

    /// Query the leader-epoch checkpoint used to find a common prefix.
    pub async fn offsets_for_leader_epoch(
        &self,
        request: &OffsetsForLeaderEpochRequest,
    ) -> Result<OffsetsForLeaderEpochResponse, ClientError> {
        let body = request.encode()?;
        let body = self
            .connection
            .request(ApiKey::OffsetsForLeaderEpoch, &body)
            .await?;
        Ok(OffsetsForLeaderEpochResponse::decode(&body)?)
    }
}
