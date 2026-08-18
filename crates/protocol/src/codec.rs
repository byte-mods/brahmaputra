//! Request/response body codec helpers (DESIGN.md §6).
//!
//! A frame body is a BitPacker-encoded struct from [`crate::gen`]. For the
//! two messages that carry record batches — [`gen::ProduceRequest`] and
//! [`gen::FetchResponse`] — the raw batch bytes are NOT inside the struct:
//! they are appended after the struct bytes, and the struct's
//! `batches_length` field carries their total length. The helpers here hide
//! that trailing-bytes convention from broker and client code.
//!
//! Splitting the trailing region into individual batches uses each batch's
//! own `batch_length` header field, validated (framing + CRC) by
//! [`crate::validate_batch_header`].

use bytes::{Buf, Bytes, BytesMut};

use crate::batch::validate_batch_header;
use crate::error::ProtocolError;
use crate::gen::{
    FetchMultiResponse, FetchMultiResult, FetchResponse, ProduceMultiPartition,
    ProduceMultiRequest, ProduceRequest,
};
use crate::BATCH_HEADER_LEN;

fn msg_err(e: std::io::Error) -> ProtocolError {
    ProtocolError::Message(e.to_string())
}

/// Encode a Produce request body: struct bytes followed by the raw batches.
/// `req.batches_length` is set from `batches` — callers need not fill it in.
pub fn encode_produce_request(
    req: &ProduceRequest,
    batches: &[Bytes],
) -> Result<Bytes, ProtocolError> {
    let mut req = req.clone();
    req.batches_length = batches.iter().map(|b| b.len() as i64).sum();
    let struct_bytes = req.encode().map_err(msg_err)?;
    let mut out = BytesMut::with_capacity(struct_bytes.len() + req.batches_length as usize);
    out.extend_from_slice(&struct_bytes);
    for batch in batches {
        out.extend_from_slice(batch);
    }
    Ok(out.freeze())
}

/// Decode a Produce request body into its struct and the trailing raw
/// batches (each batch validated, unmodified bytes).
pub fn decode_produce_request(body: Bytes) -> Result<(ProduceRequest, Vec<Bytes>), ProtocolError> {
    if body.is_empty() {
        return Err(ProtocolError::Truncated {
            needed: 1,
            available: 0,
        });
    }
    let req = ProduceRequest::decode(&body).map_err(msg_err)?;
    let batches = take_trailing_batches(&body, req.batches_length)?;
    Ok((req, batches))
}

/// Encode a Fetch response body: struct bytes followed by the raw batches.
/// `resp.batches_length` is set from `batches` — callers need not fill it in.
pub fn encode_fetch_response(
    resp: &FetchResponse,
    batches: &[Bytes],
) -> Result<Bytes, ProtocolError> {
    let mut resp = resp.clone();
    resp.batches_length = batches.iter().map(|b| b.len() as i64).sum();
    let struct_bytes = resp.encode().map_err(msg_err)?;
    let mut out = BytesMut::with_capacity(struct_bytes.len() + resp.batches_length as usize);
    out.extend_from_slice(&struct_bytes);
    for batch in batches {
        out.extend_from_slice(batch);
    }
    Ok(out.freeze())
}

/// Decode a Fetch response body into its struct and the trailing raw
/// batches (each batch validated, unmodified bytes).
pub fn decode_fetch_response(body: Bytes) -> Result<(FetchResponse, Vec<Bytes>), ProtocolError> {
    if body.is_empty() {
        return Err(ProtocolError::Truncated {
            needed: 1,
            available: 0,
        });
    }
    let resp = FetchResponse::decode(&body).map_err(msg_err)?;
    let batches = take_trailing_batches(&body, resp.batches_length)?;
    Ok((resp, batches))
}

/// Split off the last `batches_length` bytes of `body` (the trailing
/// batch region) and cut them into individual validated batches.
fn take_trailing_batches(body: &Bytes, batches_length: i64) -> Result<Vec<Bytes>, ProtocolError> {
    if batches_length < 0 || batches_length as usize > body.len() {
        return Err(ProtocolError::Malformed("batches_length out of bounds"));
    }
    let mut region = body.slice(body.len() - batches_length as usize..);
    let mut batches = Vec::new();
    while !region.is_empty() {
        let header = validate_batch_header(&region)?;
        let total = BATCH_HEADER_LEN + header.batch_length as usize;
        batches.push(region.copy_to_bytes(total));
    }
    Ok(batches)
}

// ---------- multi-partition Produce/Fetch (api_keys 15, 16) ----------
//
// Same trailing-bytes convention, one region per partition, concatenated
// in the order the partitions appear in the struct.

/// Encode a multi-partition Produce request. `partitions` pairs each
/// partition descriptor with its batches; `batches_length` is filled in
/// from the batches themselves.
pub fn encode_produce_multi(
    acks: i32,
    timeout_ms: i32,
    partitions: &[(ProduceMultiPartition, Vec<Bytes>)],
) -> Result<Bytes, ProtocolError> {
    let request = ProduceMultiRequest {
        acks,
        timeout_ms,
        partitions: partitions
            .iter()
            .map(|(partition, batches)| ProduceMultiPartition {
                topic: partition.topic.clone(),
                partition: partition.partition,
                batches_length: batches.iter().map(|batch| batch.len() as i64).sum(),
            })
            .collect(),
    };
    let struct_bytes = request.encode().map_err(msg_err)?;
    let payload: usize = partitions
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .map(|batch| batch.len())
        .sum();
    let mut out = BytesMut::with_capacity(struct_bytes.len() + payload);
    out.extend_from_slice(&struct_bytes);
    for (_, batches) in partitions {
        for batch in batches {
            out.extend_from_slice(batch);
        }
    }
    Ok(out.freeze())
}

/// Decode a multi-partition Produce request into its struct and one batch
/// list per partition, in the same order.
pub fn decode_produce_multi(
    body: Bytes,
) -> Result<(ProduceMultiRequest, Vec<Vec<Bytes>>), ProtocolError> {
    let request = ProduceMultiRequest::decode(&body).map_err(msg_err)?;
    let lengths: Vec<i64> = request
        .partitions
        .iter()
        .map(|partition| partition.batches_length)
        .collect();
    let batches = split_trailing_regions(&body, &lengths)?;
    Ok((request, batches))
}

/// Encode a multi-partition Fetch response. Each result's
/// `batches_length` is filled in from its batches.
pub fn encode_fetch_multi_response(
    results: &[(FetchMultiResult, Vec<Bytes>)],
) -> Result<Bytes, ProtocolError> {
    let response = FetchMultiResponse {
        results: results
            .iter()
            .map(|(result, batches)| FetchMultiResult {
                batches_length: batches.iter().map(|batch| batch.len() as i64).sum(),
                ..result.clone()
            })
            .collect(),
    };
    let struct_bytes = response.encode().map_err(msg_err)?;
    let payload: usize = results
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .map(|batch| batch.len())
        .sum();
    let mut out = BytesMut::with_capacity(struct_bytes.len() + payload);
    out.extend_from_slice(&struct_bytes);
    for (_, batches) in results {
        for batch in batches {
            out.extend_from_slice(batch);
        }
    }
    Ok(out.freeze())
}

/// Decode a multi-partition Fetch response into its struct and one batch
/// list per result.
pub fn decode_fetch_multi_response(
    body: Bytes,
) -> Result<(FetchMultiResponse, Vec<Vec<Bytes>>), ProtocolError> {
    let response = FetchMultiResponse::decode(&body).map_err(msg_err)?;
    let lengths: Vec<i64> = response
        .results
        .iter()
        .map(|result| result.batches_length)
        .collect();
    let batches = split_trailing_regions(&body, &lengths)?;
    Ok((response, batches))
}

/// Split the trailing region into per-partition batch lists.
///
/// The regions are laid out back to back at the end of the body, so their
/// total length locates the start; from there each partition takes exactly
/// its declared byte count. Slicing `Bytes` shares the buffer, so this does
/// not copy record data.
fn split_trailing_regions(
    body: &Bytes,
    lengths: &[i64],
) -> Result<Vec<Vec<Bytes>>, ProtocolError> {
    let total: i64 = lengths.iter().sum();
    if total < 0 || total as usize > body.len() {
        return Err(ProtocolError::Malformed("batches_length out of bounds"));
    }
    let mut region = body.slice(body.len() - total as usize..);
    let mut out = Vec::with_capacity(lengths.len());
    for length in lengths {
        if *length < 0 || *length as usize > region.len() {
            return Err(ProtocolError::Malformed("partition batches_length out of bounds"));
        }
        let mut partition_region = region.split_to(*length as usize);
        let mut batches = Vec::new();
        while !partition_region.is_empty() {
            let header = validate_batch_header(&partition_region)?;
            let total = BATCH_HEADER_LEN + header.batch_length as usize;
            if total > partition_region.len() {
                return Err(ProtocolError::Malformed("batch runs past its partition region"));
            }
            batches.push(partition_region.copy_to_bytes(total));
        }
        out.push(batches);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Record, RecordBatch};

    fn batches() -> Vec<Bytes> {
        vec![
            RecordBatch::new(0, 0, 1_000, vec![Record::new(b"a".to_vec())]).encode(),
            RecordBatch::new(
                1,
                0,
                2_000,
                vec![
                    Record::with_key(b"k".to_vec(), b"bc".to_vec(), 0),
                    Record::new(b"def".to_vec()),
                ],
            )
            .encode(),
        ]
    }

    #[test]
    fn produce_request_round_trip_with_trailing_batches() {
        let req = ProduceRequest {
            topic: "orders".into(),
            partition: 2,
            acks: -1,
            timeout_ms: 10_000,
            ..Default::default()
        };
        let raw = batches();
        let body = encode_produce_request(&req, &raw).unwrap();

        let (decoded, decoded_batches) = decode_produce_request(body).unwrap();
        assert_eq!(decoded.topic, "orders");
        assert_eq!(decoded.partition, 2);
        assert_eq!(decoded.acks, -1);
        assert_eq!(decoded.timeout_ms, 10_000);
        assert_eq!(
            decoded.batches_length,
            raw.iter().map(|b| b.len() as i64).sum::<i64>()
        );
        assert_eq!(decoded_batches, raw);
    }

    #[test]
    fn fetch_response_round_trip_with_trailing_batches() {
        let resp = FetchResponse {
            topic: "orders".into(),
            partition: 0,
            error_code: 0,
            high_watermark: 3,
            last_stable_offset: 3,
            ..Default::default()
        };
        let raw = batches();
        let body = encode_fetch_response(&resp, &raw).unwrap();

        let (decoded, decoded_batches) = decode_fetch_response(body).unwrap();
        assert_eq!(decoded.high_watermark, 3);
        assert_eq!(decoded_batches, raw);
    }

    #[test]
    fn empty_batch_region_round_trip() {
        let req = ProduceRequest {
            topic: "t".into(),
            ..Default::default()
        };
        let body = encode_produce_request(&req, &[]).unwrap();
        let (decoded, batches) = decode_produce_request(body).unwrap();
        assert_eq!(decoded.batches_length, 0);
        assert!(batches.is_empty());

        let resp = FetchResponse {
            topic: "t".into(),
            ..Default::default()
        };
        let body = encode_fetch_response(&resp, &[]).unwrap();
        let (_, batches) = decode_fetch_response(body).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn corrupt_trailing_batch_rejected() {
        let resp = FetchResponse::default();
        let mut raw = batches();
        let last = raw[1].len() - 1;
        let mut corrupted = raw[1].to_vec();
        corrupted[last] ^= 0xff;
        raw[1] = Bytes::from(corrupted);
        let body = encode_fetch_response(&resp, &raw).unwrap();
        assert!(matches!(
            decode_fetch_response(body),
            Err(ProtocolError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn batches_length_beyond_body_rejected() {
        let req = ProduceRequest::default();
        let mut body = encode_produce_request(&req, &[]).unwrap().to_vec();
        // Bump batches_length (last field) far beyond the body size.
        let n = body.len();
        body[n - 1] = 0x7f;
        assert!(matches!(
            decode_produce_request(Bytes::from(body)),
            Err(ProtocolError::Malformed(_)) | Err(ProtocolError::Message(_))
        ));
    }
}

#[cfg(test)]
mod multi_tests {
    use super::*;
    use crate::{Record, RecordBatch};

    fn batch(base: i64, payload: &[u8]) -> Bytes {
        RecordBatch::new(base, 0, 1_000, vec![Record::new(payload.to_vec())]).encode()
    }

    #[test]
    fn produce_multi_round_trips_each_partition_separately() {
        let partitions = vec![
            (
                ProduceMultiPartition {
                    topic: "orders".into(),
                    partition: 0,
                    batches_length: 0,
                },
                vec![batch(0, b"a"), batch(1, b"bb")],
            ),
            (
                ProduceMultiPartition {
                    topic: "orders".into(),
                    partition: 3,
                    batches_length: 0,
                },
                vec![batch(0, b"ccc")],
            ),
            (
                // A partition with nothing to send still reports itself.
                ProduceMultiPartition {
                    topic: "events".into(),
                    partition: 1,
                    batches_length: 0,
                },
                vec![],
            ),
        ];
        let body = encode_produce_multi(-1, 5_000, &partitions).unwrap();
        let (request, decoded) = decode_produce_multi(body).unwrap();

        assert_eq!(request.acks, -1);
        assert_eq!(request.timeout_ms, 5_000);
        assert_eq!(request.partitions.len(), 3);
        assert_eq!(request.partitions[1].topic, "orders");
        assert_eq!(request.partitions[1].partition, 3);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].len(), 2, "partition 0 kept both batches");
        assert_eq!(decoded[1].len(), 1);
        assert!(decoded[2].is_empty(), "an empty partition decodes as empty");

        // Bytes survive untouched, which is the whole point of the layout.
        assert_eq!(decoded[0][0], batch(0, b"a"));
        assert_eq!(decoded[1][0], batch(0, b"ccc"));
    }

    #[test]
    fn fetch_multi_round_trips_results_and_batches() {
        let results = vec![
            (
                FetchMultiResult {
                    topic: "orders".into(),
                    partition: 0,
                    error_code: 0,
                    high_watermark: 42,
                    last_stable_offset: 42,
                    batches_length: 0,
                },
                vec![batch(40, b"x"), batch(41, b"y")],
            ),
            (
                FetchMultiResult {
                    topic: "orders".into(),
                    partition: 1,
                    error_code: 6,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    batches_length: 0,
                },
                vec![],
            ),
        ];
        let body = encode_fetch_multi_response(&results).unwrap();
        let (response, decoded) = decode_fetch_multi_response(body).unwrap();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].high_watermark, 42);
        assert_eq!(
            response.results[1].error_code, 6,
            "a per-partition error rides alongside successful partitions"
        );
        assert_eq!(decoded[0].len(), 2);
        assert!(decoded[1].is_empty());
    }

    #[test]
    fn a_lying_length_is_rejected_rather_than_read_past() {
        let results = vec![(
            FetchMultiResult {
                topic: "t".into(),
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                batches_length: 0,
            },
            vec![batch(0, b"z")],
        )];
        let body = encode_fetch_multi_response(&results).unwrap();
        let (mut response, _) = decode_fetch_multi_response(body).unwrap();

        // Claim more trailing bytes than the frame carries.
        response.results[0].batches_length = 1 << 30;
        let struct_bytes = response.encode().unwrap();
        let forged = Bytes::from(struct_bytes);
        assert!(
            decode_fetch_multi_response(forged).is_err(),
            "an out-of-bounds length must be refused, not trusted"
        );
    }
}
