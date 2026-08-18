//! Checked codecs for cluster-internal replication APIs.
//!
//! These messages deliberately do not use the generated BitPacker schemas:
//! replication is an internal, fixed-width protocol whose epoch fields must
//! be decoded without unchecked reads. All integers are big-endian and topic
//! strings use a non-null `u16` byte length. ReplicaFetch responses append
//! validated record-batch bytes verbatim after their fixed fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{validate_batch_header, ProtocolError, BATCH_HEADER_LEN};

/// Request sent by an assigned follower to the partition leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchRequest {
    pub topic: String,
    pub partition: i32,
    pub follower_id: i32,
    pub follower_broker_epoch: u64,
    /// Current leader epoch observed by the follower; used as a fence.
    pub leader_epoch: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

/// Leader response to [`ReplicaFetchRequest`]. Raw batches trail this
/// structure on the wire and are returned separately by the decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFetchResponse {
    pub topic: String,
    pub partition: i32,
    pub error_code: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub batches_length: i64,
}

/// Query the leader for the exclusive end offset of an epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsForLeaderEpochRequest {
    pub topic: String,
    pub partition: i32,
    pub follower_id: i32,
    pub follower_broker_epoch: u64,
    /// Current leader epoch observed by the follower; used as a fence.
    pub leader_epoch: i32,
    /// Historical epoch whose exclusive end offset is requested.
    pub query_leader_epoch: i32,
}

/// Common-prefix lookup result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsForLeaderEpochResponse {
    pub topic: String,
    pub partition: i32,
    pub error_code: i32,
    /// The leader's current epoch, even when the request was fenced.
    pub leader_epoch: i32,
    /// Exclusive end offset, or `-1` when unavailable.
    pub end_offset: i64,
}

impl ReplicaFetchRequest {
    pub fn encode(&self) -> Result<Bytes, ProtocolError> {
        let mut out = BytesMut::with_capacity(48 + self.topic.len());
        put_string(&mut out, &self.topic)?;
        out.put_i32(self.partition);
        out.put_i32(self.follower_id);
        out.put_u64(self.follower_broker_epoch);
        out.put_i32(self.leader_epoch);
        out.put_i64(self.fetch_offset);
        out.put_i32(self.max_bytes);
        Ok(out.freeze())
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        let mut input = Bytes::copy_from_slice(body);
        let result = Self {
            topic: take_string(&mut input)?,
            partition: take_i32(&mut input)?,
            follower_id: take_i32(&mut input)?,
            follower_broker_epoch: take_u64(&mut input)?,
            leader_epoch: take_i32(&mut input)?,
            fetch_offset: take_i64(&mut input)?,
            max_bytes: take_i32(&mut input)?,
        };
        require_empty(&input)?;
        Ok(result)
    }
}

impl OffsetsForLeaderEpochRequest {
    pub fn encode(&self) -> Result<Bytes, ProtocolError> {
        let mut out = BytesMut::with_capacity(40 + self.topic.len());
        put_string(&mut out, &self.topic)?;
        out.put_i32(self.partition);
        out.put_i32(self.follower_id);
        out.put_u64(self.follower_broker_epoch);
        out.put_i32(self.leader_epoch);
        out.put_i32(self.query_leader_epoch);
        Ok(out.freeze())
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        let mut input = Bytes::copy_from_slice(body);
        let result = Self {
            topic: take_string(&mut input)?,
            partition: take_i32(&mut input)?,
            follower_id: take_i32(&mut input)?,
            follower_broker_epoch: take_u64(&mut input)?,
            leader_epoch: take_i32(&mut input)?,
            query_leader_epoch: take_i32(&mut input)?,
        };
        require_empty(&input)?;
        Ok(result)
    }
}

impl OffsetsForLeaderEpochResponse {
    pub fn encode(&self) -> Result<Bytes, ProtocolError> {
        let mut out = BytesMut::with_capacity(32 + self.topic.len());
        put_string(&mut out, &self.topic)?;
        out.put_i32(self.partition);
        out.put_i32(self.error_code);
        out.put_i32(self.leader_epoch);
        out.put_i64(self.end_offset);
        Ok(out.freeze())
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        let mut input = Bytes::copy_from_slice(body);
        let result = Self {
            topic: take_string(&mut input)?,
            partition: take_i32(&mut input)?,
            error_code: take_i32(&mut input)?,
            leader_epoch: take_i32(&mut input)?,
            end_offset: take_i64(&mut input)?,
        };
        require_empty(&input)?;
        Ok(result)
    }
}

/// Encode a ReplicaFetch response followed by raw record batches.
pub fn encode_replica_fetch_response(
    response: &ReplicaFetchResponse,
    batches: &[Bytes],
) -> Result<Bytes, ProtocolError> {
    let batches_length = batches.iter().try_fold(0_i64, |total, batch| {
        validate_exact_batch(batch)?;
        total
            .checked_add(batch.len() as i64)
            .ok_or(ProtocolError::Malformed(
                "replica batch region is too large",
            ))
    })?;
    let mut out = BytesMut::with_capacity(50 + response.topic.len() + batches_length as usize);
    put_string(&mut out, &response.topic)?;
    out.put_i32(response.partition);
    out.put_i32(response.error_code);
    out.put_i32(response.leader_epoch);
    out.put_i64(response.high_watermark);
    out.put_i64(response.log_start_offset);
    out.put_i64(response.log_end_offset);
    out.put_i64(batches_length);
    for batch in batches {
        out.extend_from_slice(batch);
    }
    Ok(out.freeze())
}

/// Decode the fixed response and split the trailing region into validated,
/// byte-identical batches.
pub fn decode_replica_fetch_response(
    body: Bytes,
) -> Result<(ReplicaFetchResponse, Vec<Bytes>), ProtocolError> {
    let mut input = body;
    let topic = take_string(&mut input)?;
    let partition = take_i32(&mut input)?;
    let error_code = take_i32(&mut input)?;
    let leader_epoch = take_i32(&mut input)?;
    let high_watermark = take_i64(&mut input)?;
    let log_start_offset = take_i64(&mut input)?;
    let log_end_offset = take_i64(&mut input)?;
    let batches_length = take_i64(&mut input)?;
    if batches_length < 0 || batches_length as usize != input.remaining() {
        return Err(ProtocolError::Malformed(
            "replica batches_length does not match trailing bytes",
        ));
    }
    let batches = take_batches(input)?;
    Ok((
        ReplicaFetchResponse {
            topic,
            partition,
            error_code,
            leader_epoch,
            high_watermark,
            log_start_offset,
            log_end_offset,
            batches_length,
        },
        batches,
    ))
}

fn take_batches(mut input: Bytes) -> Result<Vec<Bytes>, ProtocolError> {
    let mut batches = Vec::new();
    while input.has_remaining() {
        let header = validate_batch_header(&input)?;
        let total = BATCH_HEADER_LEN
            .checked_add(header.batch_length as usize)
            .ok_or(ProtocolError::Malformed("replica batch length overflow"))?;
        if input.remaining() < total {
            return Err(ProtocolError::Truncated {
                needed: total,
                available: input.remaining(),
            });
        }
        batches.push(input.copy_to_bytes(total));
    }
    Ok(batches)
}

fn validate_exact_batch(batch: &Bytes) -> Result<(), ProtocolError> {
    let header = validate_batch_header(batch)?;
    let expected = BATCH_HEADER_LEN
        .checked_add(header.batch_length as usize)
        .ok_or(ProtocolError::Malformed("replica batch length overflow"))?;
    if batch.len() != expected {
        return Err(ProtocolError::Malformed(
            "replica batch contains trailing bytes",
        ));
    }
    Ok(())
}

fn put_string(out: &mut BytesMut, value: &str) -> Result<(), ProtocolError> {
    let len = u16::try_from(value.len())
        .map_err(|_| ProtocolError::Malformed("replica topic exceeds u16 length"))?;
    out.put_u16(len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_string(input: &mut Bytes) -> Result<String, ProtocolError> {
    require(input, 2)?;
    let len = input.get_u16() as usize;
    require(input, len)?;
    let raw = input.copy_to_bytes(len);
    String::from_utf8(raw.to_vec())
        .map_err(|_| ProtocolError::Malformed("replica topic is not utf-8"))
}

fn take_i32(input: &mut Bytes) -> Result<i32, ProtocolError> {
    require(input, 4)?;
    Ok(input.get_i32())
}

fn take_i64(input: &mut Bytes) -> Result<i64, ProtocolError> {
    require(input, 8)?;
    Ok(input.get_i64())
}

fn take_u64(input: &mut Bytes) -> Result<u64, ProtocolError> {
    require(input, 8)?;
    Ok(input.get_u64())
}

fn require(input: &Bytes, needed: usize) -> Result<(), ProtocolError> {
    if input.remaining() < needed {
        return Err(ProtocolError::Truncated {
            needed,
            available: input.remaining(),
        });
    }
    Ok(())
}

fn require_empty(input: &Bytes) -> Result<(), ProtocolError> {
    if input.has_remaining() {
        return Err(ProtocolError::Malformed(
            "replica message contains trailing bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Record, RecordBatch};

    fn raw_batches() -> Vec<Bytes> {
        vec![
            RecordBatch::new(12, 3, 1_000, vec![Record::new("alpha")]).encode(),
            RecordBatch::new(
                13,
                3,
                2_000,
                vec![Record::new("beta"), Record::new("gamma")],
            )
            .encode(),
        ]
    }

    #[test]
    fn replica_fetch_request_round_trip_is_fixed_width_and_checked() {
        let request = ReplicaFetchRequest {
            topic: "orders".into(),
            partition: 7,
            follower_id: 12,
            follower_broker_epoch: u64::MAX - 1,
            leader_epoch: 9,
            fetch_offset: i64::MAX - 2,
            max_bytes: 1_048_576,
        };
        let encoded = request.encode().unwrap();
        assert_eq!(ReplicaFetchRequest::decode(&encoded).unwrap(), request);
        for cut in 0..encoded.len() {
            assert!(ReplicaFetchRequest::decode(&encoded[..cut]).is_err());
        }
        let mut with_trailing = encoded.to_vec();
        with_trailing.push(0);
        assert!(ReplicaFetchRequest::decode(&with_trailing).is_err());
    }

    #[test]
    fn replica_fetch_response_preserves_every_batch_byte() {
        let batches = raw_batches();
        let response = ReplicaFetchResponse {
            topic: "orders".into(),
            partition: 7,
            error_code: 0,
            leader_epoch: 3,
            high_watermark: 13,
            log_start_offset: 4,
            log_end_offset: 15,
            batches_length: 0,
        };
        let encoded = encode_replica_fetch_response(&response, &batches).unwrap();
        let (decoded, decoded_batches) = decode_replica_fetch_response(encoded).unwrap();
        assert_eq!(
            decoded.batches_length,
            batches.iter().map(|b| b.len() as i64).sum::<i64>()
        );
        assert_eq!(decoded_batches, batches);
    }

    #[test]
    fn replica_fetch_rejects_corrupt_or_misframed_trailing_data() {
        let batches = raw_batches();
        let response = ReplicaFetchResponse {
            topic: "t".into(),
            partition: 0,
            error_code: 0,
            leader_epoch: 0,
            high_watermark: 0,
            log_start_offset: 0,
            log_end_offset: 0,
            batches_length: 0,
        };
        let mut encoded = encode_replica_fetch_response(&response, &batches)
            .unwrap()
            .to_vec();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(matches!(
            decode_replica_fetch_response(Bytes::from(encoded)),
            Err(ProtocolError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn offsets_for_leader_epoch_round_trip() {
        let request = OffsetsForLeaderEpochRequest {
            topic: "events".into(),
            partition: 1,
            follower_id: 4,
            follower_broker_epoch: 91,
            leader_epoch: 8,
            query_leader_epoch: 6,
        };
        assert_eq!(
            OffsetsForLeaderEpochRequest::decode(&request.encode().unwrap()).unwrap(),
            request
        );

        let response = OffsetsForLeaderEpochResponse {
            topic: "events".into(),
            partition: 1,
            error_code: 0,
            leader_epoch: 8,
            end_offset: 410,
        };
        assert_eq!(
            OffsetsForLeaderEpochResponse::decode(&response.encode().unwrap()).unwrap(),
            response
        );
    }
}
