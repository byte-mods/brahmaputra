//! Fixed-width codec for the idempotent producer session API.
//!
//! A request with `producer_id = -1, producer_epoch = -1` allocates a new
//! durable identity. Supplying an existing identity and its current epoch
//! requests the next epoch, fencing older producer instances.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::ProtocolError;

const REQUEST_LEN: usize = 8 + 2;
const RESPONSE_LEN: usize = 4 + 8 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitProducerIdRequest {
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    pub const fn allocate() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
        }
    }

    pub fn encode(self) -> Bytes {
        let mut out = BytesMut::with_capacity(REQUEST_LEN);
        out.put_i64(self.producer_id);
        out.put_i16(self.producer_epoch);
        out.freeze()
    }

    pub fn decode(body: &[u8]) -> Result<Self, ProtocolError> {
        if body.len() != REQUEST_LEN {
            return Err(if body.len() < REQUEST_LEN {
                ProtocolError::Truncated {
                    needed: REQUEST_LEN,
                    available: body.len(),
                }
            } else {
                ProtocolError::Malformed("InitProducerId request contains trailing bytes")
            });
        }
        let mut body = Bytes::copy_from_slice(body);
        Ok(Self {
            producer_id: body.get_i64(),
            producer_epoch: body.get_i16(),
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
}
