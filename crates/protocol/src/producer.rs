//! Fixed-width codec for the idempotent producer session API.
//!
//! A request with `producer_id = -1, producer_epoch = -1` allocates a new
//! durable identity. Supplying an existing identity and its current epoch
//! requests the next epoch, fencing older producer instances.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::ProtocolError;

const REQUEST_LEN: usize = 8 + 2;
const RESPONSE_LEN: usize = 4 + 8 + 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdRequest {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// The `transactional.id` this producer is claiming, if any.
    ///
    /// Present turns this into a *transactional* session: the request goes
    /// to that id's coordinator, which fences whatever previous instance
    /// held it and resolves any transaction that instance abandoned.
    /// Absent leaves the plain idempotent behaviour untouched.
    ///
    /// Encoded as a trailing `i32` length plus bytes, so a request with no
    /// transactional id is exactly the ten bytes it always was — an older
    /// client keeps working, and this is why the field is at the end.
    pub transactional_id: Option<String>,
    /// How long the coordinator lets a transaction stay open before it is
    /// eligible to be aborted. Ignored without a transactional id.
    pub transaction_timeout_ms: i32,
}

impl InitProducerIdRequest {
    pub const fn allocate() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
            transactional_id: None,
            transaction_timeout_ms: 0,
        }
    }

    /// Claim a transactional id, fencing any older instance of it.
    pub fn transactional(transactional_id: impl Into<String>, timeout_ms: i32) -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
            transactional_id: Some(transactional_id.into()),
            transaction_timeout_ms: timeout_ms,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(REQUEST_LEN + 8);
        out.put_i64(self.producer_id);
        out.put_i16(self.producer_epoch);
        if let Some(transactional_id) = &self.transactional_id {
            out.put_i32(transactional_id.len() as i32);
            out.extend_from_slice(transactional_id.as_bytes());
            out.put_i32(self.transaction_timeout_ms);
        }
        out.freeze()
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        if body.len() < REQUEST_LEN {
            return Err(ProtocolError::Truncated {
                needed: REQUEST_LEN,
                available: body.len(),
            });
        }
        let mut body = Bytes::copy_from_slice(body);
        let producer_id = body.get_i64();
        let producer_epoch = body.get_i16();
        if body.is_empty() {
            return Ok(Self {
                producer_id,
                producer_epoch,
                transactional_id: None,
                transaction_timeout_ms: 0,
            });
        }

        if body.remaining() < 4 {
            return Err(ProtocolError::Truncated {
                needed: REQUEST_LEN + 4,
                available: REQUEST_LEN + body.remaining(),
            });
        }
        let length = body.get_i32();
        if length < 0 {
            return Err(ProtocolError::Malformed(
                "InitProducerId transactional id has a negative length",
            ));
        }
        let length = length as usize;
        if body.remaining() < length + 4 {
            return Err(ProtocolError::Truncated {
                needed: length + 4,
                available: body.remaining(),
            });
        }
        let transactional_id = String::from_utf8(body.copy_to_bytes(length).to_vec())
            .map_err(|_| ProtocolError::Malformed("InitProducerId transactional id is not UTF-8"))?;
        let transaction_timeout_ms = body.get_i32();
        if !body.is_empty() {
            return Err(ProtocolError::Malformed(
                "InitProducerId request contains trailing bytes",
            ));
        }
        Ok(Self {
            producer_id,
            producer_epoch,
            transactional_id: Some(transactional_id),
            transaction_timeout_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitProducerIdResponse {
    pub error_code: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponse {
    pub fn encode(self) -> Bytes {
        let mut out = BytesMut::with_capacity(RESPONSE_LEN);
        out.put_i32(self.error_code);
        out.put_i64(self.producer_id);
        out.put_i16(self.producer_epoch);
        out.freeze()
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        if body.len() != RESPONSE_LEN {
            return Err(if body.len() < RESPONSE_LEN {
                ProtocolError::Truncated {
                    needed: RESPONSE_LEN,
                    available: body.len(),
                }
            } else {
                ProtocolError::Malformed("InitProducerId response contains trailing bytes")
            });
        }
        let mut body = Bytes::copy_from_slice(body);
        Ok(Self {
            error_code: body.get_i32(),
            producer_id: body.get_i64(),
            producer_epoch: body.get_i16(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_response_round_trip_with_strict_lengths() {
        let request = InitProducerIdRequest {
            producer_id: i64::MAX - 1,
            producer_epoch: i16::MAX - 1,
            transactional_id: None,
            transaction_timeout_ms: 0,
        };
        let encoded = request.encode();
        assert_eq!(InitProducerIdRequest::decode(&encoded).unwrap(), request);
        for cut in 0..encoded.len() {
            assert!(InitProducerIdRequest::decode(&encoded[..cut]).is_err());
        }

        let response = InitProducerIdResponse {
            error_code: 12,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch + 1,
        };
        let encoded = response.encode();
        assert_eq!(InitProducerIdResponse::decode(&encoded).unwrap(), response);
        for cut in 0..encoded.len() {
            assert!(InitProducerIdResponse::decode(&encoded[..cut]).is_err());
        }
    }

    #[test]
    fn allocation_sentinel_is_unambiguous() {
        let request = InitProducerIdRequest::allocate();
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);
        assert_eq!(
            InitProducerIdRequest::decode(&request.encode()).unwrap(),
            request
        );
    }

    #[test]
    fn a_request_without_a_transactional_id_is_byte_identical_to_the_old_one() {
        // The transactional id is appended, not inserted, so a client that
        // has never heard of transactions encodes exactly the ten bytes it
        // always did — and this broker still decodes them.
        let plain = InitProducerIdRequest::allocate();
        assert_eq!(plain.encode().len(), REQUEST_LEN);

        let legacy = {
            let mut out = BytesMut::new();
            out.put_i64(-1);
            out.put_i16(-1);
            out.freeze()
        };
        assert_eq!(InitProducerIdRequest::decode(&legacy).unwrap(), plain);
    }

    #[test]
    fn a_transactional_request_round_trips_and_rejects_a_torn_tail() {
        let request = InitProducerIdRequest::transactional("orders-etl", 60_000);
        let encoded = request.encode();
        let decoded = InitProducerIdRequest::decode(&encoded).unwrap();
        assert_eq!(decoded.transactional_id.as_deref(), Some("orders-etl"));
        assert_eq!(decoded.transaction_timeout_ms, 60_000);
        assert_eq!(decoded, request);

        // Every truncation past the fixed prefix must fail rather than be
        // read as a shorter id: a transactional id that decodes to the
        // wrong string would fence the wrong producer.
        for cut in REQUEST_LEN + 1..encoded.len() {
            assert!(
                InitProducerIdRequest::decode(&encoded[..cut]).is_err(),
                "a request truncated at {cut} was accepted"
            );
        }
    }
}
