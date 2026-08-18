//! Unsigned LEB128 varints plus zigzag encoding for signed values,
//! used inside the record payload (DESIGN.md §4.1).

use bytes::{BufMut, BytesMut};

use crate::ProtocolError;

pub(crate) fn put_uvarint(buf: &mut BytesMut, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.put_u8(b);
        if v == 0 {
            break;
        }
    }
}

pub(crate) fn get_uvarint(buf: &mut &[u8]) -> Result<u64, ProtocolError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if buf.is_empty() {
            return Err(ProtocolError::Truncated {
                needed: 1,
                available: 0,
            });
        }
        let b = buf[0];
        *buf = &buf[1..];
        if shift == 63 {
            // 10th byte: only values 0 or 1 keep the u64 in range, and the
            // continuation bit must be clear.
            if b > 1 {
                return Err(ProtocolError::Malformed("varint overflow"));
            }
            result |= (b as u64) << shift;
            return Ok(result);
        }
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

pub(crate) fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub(crate) fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: u64) {
        let mut buf = BytesMut::new();
        put_uvarint(&mut buf, v);
        let mut slice = &buf[..];
        assert_eq!(get_uvarint(&mut slice).unwrap(), v);
        assert!(slice.is_empty());
    }

    #[test]
    fn uvarint_roundtrip_boundaries() {
        for v in [
            0,
            1,
            127,
            128,
            300,
            16_384,
            u32::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            roundtrip(v);
        }
    }

    #[test]
    fn uvarint_truncated_errors() {
        let mut slice: &[u8] = &[0x80]; // continuation bit set, then EOF
        assert!(matches!(
            get_uvarint(&mut slice),
            Err(ProtocolError::Truncated { .. })
        ));
    }

    #[test]
    fn uvarint_overflow_errors() {
        let mut slice: &[u8] = &[0xff; 10]; // 10 continuation bytes
        assert!(matches!(
            get_uvarint(&mut slice),
            Err(ProtocolError::Malformed(_))
        ));
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [
            0,
            1,
            -1,
            63,
            -64,
            i64::MAX,
            i64::MIN,
            1_234_567_890,
            -9_876_543_210,
        ] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }
}
