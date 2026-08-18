//! Frame header encode/decode for the data-plane wire protocol
//! (DESIGN.md §6).
//!
//! Every frame on the wire:
//!
//! ```text
//! length:i32  api_key:i16  api_version:i16  correlation_id:i32  client_id:string  body
//! ```
//!
//! - `length` counts everything after itself (header + body), big-endian.
//! - `client_id` is a nullable string: `i16` length, `-1` = null.
//! - `body` is a BitPacker-encoded struct from [`crate::gen`]; for Produce
//!   requests and Fetch responses raw record-batch bytes trail the struct
//!   (see [`crate::codec`]).
//!
//! `length` is handled by `tokio_util::codec::LengthDelimitedCodec` at the
//! transport edges (broker/client); this module encodes/decodes the payload
//! after the length field. [`encode_frame`] additionally prepends the length
//! prefix itself for tests and any length-prefixed raw writer. This crate
//! stays pure/sync — no tokio here.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;

/// The only api version implemented so far (all M1 APIs).
pub const API_VERSION: i16 = 1;

/// Bytes in a payload header: api_key + api_version + correlation_id.
/// (`client_id` is variable-length and follows.)
const FIXED_HEADER_LEN: usize = 2 + 2 + 4;

/// Data-plane API keys (DESIGN.md §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    /// Cluster-internal leader-to-follower raw batch fetch.
    ReplicaFetch = 4,
    /// Cluster-internal leader epoch common-prefix lookup.
    OffsetsForLeaderEpoch = 5,
    /// Allocate a producer identity or bump its epoch.
    InitProducerId = 6,
    /// Consumer-group membership: join (and trigger/collect a rebalance).
    JoinGroup = 7,
    /// Consumer-group membership: distribute the leader-computed assignment.
    SyncGroup = 8,
    /// Consumer-group member keepalive.
    Heartbeat = 9,
    /// Commit consumed offsets to the group coordinator.
    OffsetCommit = 10,
    /// Read committed offsets from the group coordinator.
    OffsetFetch = 11,
    /// List the groups coordinated by the broker that serves the request.
    ListGroups = 12,
    /// Describe one group's membership and committed offsets.
    DescribeGroup = 13,
    /// Discover which APIs and versions this broker speaks. Answered at
    /// any requested api_version, because it is what a client asks before
    /// it knows which versions are safe to use.
    ApiVersions = 14,
    /// Produce to many partitions in one request. The form clients use.
    ProduceMulti = 15,
    /// Fetch from many partitions in one request. The form clients use.
    FetchMulti = 16,
}

impl ApiKey {
    pub fn from_i16(v: i16) -> Result<Self, ProtocolError> {
        match v {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            2 => Ok(ApiKey::ListOffsets),
            3 => Ok(ApiKey::Metadata),
            4 => Ok(ApiKey::ReplicaFetch),
            5 => Ok(ApiKey::OffsetsForLeaderEpoch),
            6 => Ok(ApiKey::InitProducerId),
            7 => Ok(ApiKey::JoinGroup),
            8 => Ok(ApiKey::SyncGroup),
            9 => Ok(ApiKey::Heartbeat),
            10 => Ok(ApiKey::OffsetCommit),
            11 => Ok(ApiKey::OffsetFetch),
            12 => Ok(ApiKey::ListGroups),
            13 => Ok(ApiKey::DescribeGroup),
            14 => Ok(ApiKey::ApiVersions),
            15 => Ok(ApiKey::ProduceMulti),
            16 => Ok(ApiKey::FetchMulti),
            other => Err(ProtocolError::UnknownApiKey(other)),
        }
    }
}

/// Everything in a frame except the length prefix and the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl FrameHeader {
    pub fn new(api_key: ApiKey, correlation_id: i32, client_id: Option<String>) -> Self {
        FrameHeader {
            api_key,
            api_version: API_VERSION,
            correlation_id,
            client_id,
        }
    }
}

/// Error-code values carried in `*_response.error_code` fields.
pub mod error_code {
    pub const NONE: i32 = 0;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i32 = 1;
    pub const OFFSET_OUT_OF_RANGE: i32 = 2;
    pub const INVALID_REQUEST: i32 = 3;
    pub const UNSUPPORTED_VERSION: i32 = 4;
    pub const INTERNAL: i32 = 5;
    /// The contacted broker is not the current leader (or an assigned
    /// follower for an internal replica operation) for the partition.
    pub const NOT_LEADER_OR_FOLLOWER: i32 = 6;
    /// The follower identity is valid, but its broker incarnation is stale.
    pub const FENCED_BROKER_EPOCH: i32 = 7;
    /// The request names a leader epoch older than the current assignment.
    pub const FENCED_LEADER_EPOCH: i32 = 8;
    /// The request names a leader epoch newer than, or absent from, history.
    pub const UNKNOWN_LEADER_EPOCH: i32 = 9;
    /// The current in-sync replica set cannot satisfy the requested write.
    pub const NOT_ENOUGH_REPLICAS: i32 = 10;
    /// The producer epoch is older than the partition/coordinator state.
    pub const FENCED_PRODUCER_EPOCH: i32 = 11;
    /// The batch sequence is neither the next expected sequence nor a
    /// byte-identical duplicate retained in the deduplication window.
    pub const OUT_OF_ORDER_SEQUENCE: i32 = 12;
    /// The member id is unknown to the group coordinator (evicted or never
    /// joined); the member must rejoin.
    pub const UNKNOWN_MEMBER_ID: i32 = 13;
    /// The group is rebalancing; heartbeats/commits fail and members must
    /// rejoin to learn the new generation.
    pub const REBALANCE_IN_PROGRESS: i32 = 14;
    /// The contacted broker does not coordinate this group (it is not the
    /// leader of the group's `__consumer_offsets` partition).
    pub const NOT_COORDINATOR: i32 = 15;
    /// The request carries a generation older than the group's current one.
    pub const ILLEGAL_GENERATION: i32 = 16;
    /// The coordinator is still replaying the offsets log; retry shortly.
    pub const COORDINATOR_LOAD_IN_PROGRESS: i32 = 17;
}

/// Encode `header` + `body` as a complete frame including the `length:i32`
/// prefix.
pub fn encode_frame(header: &FrameHeader, body: &[u8]) -> Bytes {
    let payload = encode_payload(header, body);
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_i32(payload.len() as i32);
    out.extend_from_slice(&payload);
    out.freeze()
}

/// Encode `header` + `body` as a frame payload WITHOUT the length prefix —
/// the form expected by `LengthDelimitedCodec`, which supplies the prefix.
pub fn encode_payload(header: &FrameHeader, body: &[u8]) -> Bytes {
    let client_len = header.client_id.as_ref().map_or(0, |c| c.len());
    let mut out = BytesMut::with_capacity(FIXED_HEADER_LEN + 2 + client_len + body.len());
    out.put_i16(header.api_key as i16);
    out.put_i16(header.api_version);
    out.put_i32(header.correlation_id);
    match &header.client_id {
        None => out.put_i16(-1),
        Some(client_id) => {
            out.put_i16(client_id.len() as i16);
            out.extend_from_slice(client_id.as_bytes());
        }
    }
    out.extend_from_slice(body);
    out.freeze()
}

/// Decode a frame payload (no length prefix) into its header and the
/// remaining body bytes. Consumes the header from `payload`.
pub fn decode_payload(payload: &mut Bytes) -> Result<FrameHeader, ProtocolError> {
    if payload.remaining() < FIXED_HEADER_LEN + 2 {
        return Err(ProtocolError::Truncated {
            needed: FIXED_HEADER_LEN + 2,
            available: payload.remaining(),
        });
    }
    let api_key = ApiKey::from_i16(payload.get_i16())?;
    let api_version = payload.get_i16();
    let correlation_id = payload.get_i32();
    let client_len = payload.get_i16();
    let client_id = if client_len < 0 {
        None
    } else {
        let len = client_len as usize;
        if payload.remaining() < len {
            return Err(ProtocolError::Truncated {
                needed: len,
                available: payload.remaining(),
            });
        }
        let bytes = payload.copy_to_bytes(len);
        let s = std::str::from_utf8(&bytes)
            .map_err(|_| ProtocolError::Malformed("client_id is not utf-8"))?;
        Some(s.to_owned())
    };
    Ok(FrameHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_with_and_without_client_id() {
        let body = b"\x01\x02\x03body-bytes";
        for client_id in [
            None,
            Some("brahmaputra-cli".to_owned()),
            Some(String::new()),
        ] {
            let header = FrameHeader::new(ApiKey::Fetch, 42, client_id);
            let frame = encode_frame(&header, body);

            // Length prefix covers everything after itself.
            let mut rest = frame.clone();
            let length = rest.get_i32() as usize;
            assert_eq!(length, rest.remaining());

            let decoded_header = decode_payload(&mut rest).unwrap();
            assert_eq!(decoded_header, header);
            assert_eq!(&rest[..], body);
        }
    }

    #[test]
    fn decode_payload_splits_header_and_body() {
        let header = FrameHeader::new(ApiKey::Produce, -7, Some("p".into()));
        let mut payload = encode_payload(&header, &[9, 9, 9]);
        let decoded = decode_payload(&mut payload).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(&payload[..], &[9, 9, 9]);
    }

    #[test]
    fn unknown_api_key_rejected() {
        let mut payload = BytesMut::new();
        payload.put_i16(99);
        payload.put_i16(API_VERSION);
        payload.put_i32(0);
        payload.put_i16(-1);
        let mut payload = payload.freeze();
        assert!(matches!(
            decode_payload(&mut payload),
            Err(ProtocolError::UnknownApiKey(99))
        ));
    }

    #[test]
    fn truncated_header_rejected() {
        let header = FrameHeader::new(ApiKey::Metadata, 1, Some("some-client".into()));
        let payload = encode_payload(&header, &[1, 2, 3]);
        for cut in 0..FIXED_HEADER_LEN + 2 + "some-client".len() {
            let mut buf = payload.slice(..cut);
            assert!(decode_payload(&mut buf).is_err(), "cut {cut}");
        }
    }
}
