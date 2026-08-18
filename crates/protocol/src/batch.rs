//! Record batch encode/decode per DESIGN.md §4.1.
//!
//! On-disk / on-wire layout (all integers big-endian):
//!
//! ```text
//! base_offset:       i64
//! batch_length:      i32   (bytes following this field)
//! leader_epoch:      i32
//! magic:             u8    (MAGIC_V1 or MAGIC_V2)
//! crc32c:            u32   (covers everything after this field)
//! attributes:        u16   (bits 0..=2: compression type)
//! last_offset_delta: i32
//! max_timestamp:     i64
//! producer_id:       i64   (magic v2 only)
//! producer_epoch:    i16   (magic v2 only)
//! base_sequence:     i32   (magic v2 only)
//! records:           [Record]  (possibly compressed, per attributes)
//! ```
//!
//! Record layout (magic v1; record headers are not implemented yet — the
//! magic byte allows the format to evolve):
//!
//! ```text
//! record_length:    uvarint  (bytes following this field)
//! key_len_plus_one: uvarint  (0 => null key, else key length + 1)
//! key:              [u8]
//! value_len:        uvarint
//! value:            [u8]
//! timestamp_delta:  uvarint  (zigzag-encoded i64)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::varint::{get_uvarint, put_uvarint, zigzag_decode, zigzag_encode};

/// Current batch format version.
pub const MAGIC_V1: u8 = 1;
/// Idempotent-producer batch format. Its fixed producer extension is placed
/// after `max_timestamp` and before the (possibly compressed) record payload.
pub const MAGIC_V2: u8 = 2;

/// Length of the `base_offset + batch_length` prefix preceding every batch.
pub const BATCH_HEADER_LEN: usize = 8 + 4;

/// Smallest legal `batch_length`: leader_epoch + magic + crc + attributes +
/// last_offset_delta + max_timestamp, with zero record bytes.
pub const MIN_BATCH_LENGTH: usize = 4 + 1 + 4 + 2 + 4 + 8;

/// Bytes in the magic-v2 producer header extension.
pub const PRODUCER_EXTENSION_LEN: usize = 8 + 2 + 4;

const COMPRESSION_MASK: u16 = 0x0007;

/// Compression applied to the records payload inside a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    // Reserved for later: Zstd = 2, Snappy = 3 (see DESIGN.md §9).
}

impl Compression {
    fn to_bits(self) -> u16 {
        self as u16
    }

    fn from_bits(bits: u16) -> Result<Self, ProtocolError> {
        match bits {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            other => Err(ProtocolError::UnsupportedCompression(other)),
        }
    }
}

/// A single record inside a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Bytes,
    /// Milliseconds relative to the batch's `max_timestamp` base.
    pub timestamp_delta: i64,
}

/// Identity and per-partition ordering metadata carried by magic-v2 batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerMetadata {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Record {
            key: None,
            value: value.into(),
            timestamp_delta: 0,
        }
    }

    pub fn with_key(key: impl Into<Bytes>, value: impl Into<Bytes>, timestamp_delta: i64) -> Self {
        Record {
            key: Some(key.into()),
            value: value.into(),
            timestamp_delta,
        }
    }
}

/// The only unit on disk and on the wire (DESIGN.md §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub leader_epoch: i32,
    pub max_timestamp: i64,
    pub records: Vec<Record>,
    pub compression: Compression,
    /// `None` encodes the legacy magic-v1 layout. `Some` encodes magic v2.
    pub producer: Option<ProducerMetadata>,
}

impl RecordBatch {
    pub fn new(
        base_offset: i64,
        leader_epoch: i32,
        max_timestamp: i64,
        records: Vec<Record>,
    ) -> Self {
        RecordBatch {
            base_offset,
            leader_epoch,
            max_timestamp,
            records,
            compression: Compression::None,
            producer: None,
        }
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Encode this batch as magic v2 with idempotent-producer metadata.
    pub fn with_producer(
        mut self,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> Self {
        self.producer = Some(ProducerMetadata {
            producer_id,
            producer_epoch,
            base_sequence,
        });
        self
    }

    /// Offset delta of the last record (0 for an empty batch).
    pub fn last_offset_delta(&self) -> i32 {
        self.records.len().saturating_sub(1) as i32
    }

    /// The offset one past the last record in this batch.
    pub fn next_offset(&self) -> i64 {
        self.base_offset + self.records.len() as i64
    }

    /// Iterate records together with their absolute offsets.
    pub fn iter(&self) -> impl Iterator<Item = (i64, &Record)> {
        self.records
            .iter()
            .enumerate()
            .map(move |(i, r)| (self.base_offset + i as i64, r))
    }

    /// Serialize the batch to its on-disk/on-wire byte form.
    pub fn encode(&self) -> Bytes {
        // Records payload.
        let mut payload = BytesMut::new();
        for record in &self.records {
            let mut rec = BytesMut::new();
            match &record.key {
                None => put_uvarint(&mut rec, 0),
                Some(key) => {
                    put_uvarint(&mut rec, key.len() as u64 + 1);
                    rec.extend_from_slice(key);
                }
            }
            put_uvarint(&mut rec, record.value.len() as u64);
            rec.extend_from_slice(&record.value);
            put_uvarint(&mut rec, zigzag_encode(record.timestamp_delta));
            put_uvarint(&mut payload, rec.len() as u64);
            payload.extend_from_slice(&rec);
        }
        let payload = match self.compression {
            Compression::None => payload,
            Compression::Lz4 => {
                BytesMut::from(lz4_flex::compress_prepend_size(&payload).as_slice())
            }
        };

        let extension_len = self.producer.map_or(0, |_| PRODUCER_EXTENSION_LEN);
        let batch_length = (MIN_BATCH_LENGTH + extension_len + payload.len()) as i32;
        let mut out = BytesMut::with_capacity(BATCH_HEADER_LEN + batch_length as usize);
        out.put_i64(self.base_offset);
        out.put_i32(batch_length);
        out.put_i32(self.leader_epoch);
        out.put_u8(if self.producer.is_some() {
            MAGIC_V2
        } else {
            MAGIC_V1
        });
        let crc_pos = out.len();
        out.put_u32(0); // crc placeholder, backfilled below
        out.put_u16(self.compression.to_bits() & COMPRESSION_MASK);
        out.put_i32(self.last_offset_delta());
        out.put_i64(self.max_timestamp);
        if let Some(producer) = self.producer {
            out.put_i64(producer.producer_id);
            out.put_i16(producer.producer_epoch);
            out.put_i32(producer.base_sequence);
        }
        out.extend_from_slice(&payload);

        let crc = crc32c::crc32c(&out[crc_pos + 4..]);
        out[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
        out.freeze()
    }

    /// Decode one batch from the front of `buf`, consuming exactly
    /// `BATCH_HEADER_LEN + batch_length` bytes on success.
    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < BATCH_HEADER_LEN {
            return Err(ProtocolError::Truncated {
                needed: BATCH_HEADER_LEN,
                available: buf.remaining(),
            });
        }
        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32();
        if batch_length < MIN_BATCH_LENGTH as i32 {
            return Err(ProtocolError::Malformed("batch_length too small"));
        }
        if buf.remaining() < batch_length as usize {
            return Err(ProtocolError::Truncated {
                needed: batch_length as usize,
                available: buf.remaining(),
            });
        }
        let mut body = buf.copy_to_bytes(batch_length as usize);

        let leader_epoch = body.get_i32();
        let magic = body.get_u8();
        if !matches!(magic, MAGIC_V1 | MAGIC_V2) {
            return Err(ProtocolError::UnsupportedMagic(magic));
        }
        let stored_crc = body.get_u32();
        let computed_crc = crc32c::crc32c(&body);
        if computed_crc != stored_crc {
            return Err(ProtocolError::CrcMismatch {
                stored: stored_crc,
                computed: computed_crc,
            });
        }
        let attributes = body.get_u16();
        let compression = Compression::from_bits(attributes & COMPRESSION_MASK)?;
        let last_offset_delta = body.get_i32();
        let max_timestamp = body.get_i64();
        let producer = if magic == MAGIC_V2 {
            if body.remaining() < PRODUCER_EXTENSION_LEN {
                return Err(ProtocolError::Truncated {
                    needed: PRODUCER_EXTENSION_LEN,
                    available: body.remaining(),
                });
            }
            Some(ProducerMetadata {
                producer_id: body.get_i64(),
                producer_epoch: body.get_i16(),
                base_sequence: body.get_i32(),
            })
        } else {
            None
        };

        let payload: Bytes = match compression {
            Compression::None => body,
            Compression::Lz4 => Bytes::from(
                lz4_flex::decompress_size_prepended(&body)
                    .map_err(|e| ProtocolError::Lz4(e.to_string()))?,
            ),
        };

        let records = decode_records(&payload)?;
        if !records.is_empty() && last_offset_delta != records.len() as i32 - 1 {
            return Err(ProtocolError::Malformed("last_offset_delta mismatch"));
        }

        Ok(RecordBatch {
            base_offset,
            leader_epoch,
            max_timestamp,
            records,
            compression,
            producer,
        })
    }
}

fn decode_records(payload: &[u8]) -> Result<Vec<Record>, ProtocolError> {
    let mut records = Vec::new();
    let mut slice = payload;
    while !slice.is_empty() {
        let record_len = get_uvarint(&mut slice)? as usize;
        if slice.len() < record_len {
            return Err(ProtocolError::Truncated {
                needed: record_len,
                available: slice.len(),
            });
        }
        let (mut rec, rest) = slice.split_at(record_len);
        slice = rest;

        let key_len_plus_one = get_uvarint(&mut rec)?;
        let key = if key_len_plus_one == 0 {
            None
        } else {
            let len = (key_len_plus_one - 1) as usize;
            if rec.len() < len {
                return Err(ProtocolError::Truncated {
                    needed: len,
                    available: rec.len(),
                });
            }
            let (k, r) = rec.split_at(len);
            rec = r;
            Some(Bytes::copy_from_slice(k))
        };

        let value_len = get_uvarint(&mut rec)? as usize;
        if rec.len() < value_len {
            return Err(ProtocolError::Truncated {
                needed: value_len,
                available: rec.len(),
            });
        }
        let (v, r) = rec.split_at(value_len);
        rec = r;
        let value = Bytes::copy_from_slice(v);

        let timestamp_delta = zigzag_decode(get_uvarint(&mut rec)?);
        if !rec.is_empty() {
            return Err(ProtocolError::Malformed("trailing bytes in record"));
        }
        records.push(Record {
            key,
            value,
            timestamp_delta,
        });
    }
    Ok(records)
}

/// Parsed and validated header fields of one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    pub base_offset: i64,
    pub batch_length: i32,
    pub leader_epoch: i32,
    pub magic: u8,
    pub compression: Compression,
    pub last_offset_delta: i32,
    pub max_timestamp: i64,
    pub producer: Option<ProducerMetadata>,
    /// Stable fingerprint of attributes, producer extension and payload.
    /// It deliberately excludes broker-stamped base offset / leader epoch.
    pub content_crc32c: u32,
}

/// Validate a complete batch's framing and CRC without decoding its records
/// (no decompression, no allocation). Used by the storage engine's recovery
/// scan and read path, where only the header fields are needed.
///
/// `bytes` must contain exactly one batch (or more — trailing bytes are
/// ignored) starting at offset 0.
pub fn validate_batch_header(bytes: &[u8]) -> Result<BatchHeader, ProtocolError> {
    if bytes.len() < BATCH_HEADER_LEN {
        return Err(ProtocolError::Truncated {
            needed: BATCH_HEADER_LEN,
            available: bytes.len(),
        });
    }
    let base_offset = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let batch_length = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
    if batch_length < MIN_BATCH_LENGTH as i32 {
        return Err(ProtocolError::Malformed("batch_length too small"));
    }
    let total = BATCH_HEADER_LEN + batch_length as usize;
    if bytes.len() < total {
        return Err(ProtocolError::Truncated {
            needed: total,
            available: bytes.len(),
        });
    }
    let body = &bytes[BATCH_HEADER_LEN..total];

    let leader_epoch = i32::from_be_bytes(body[0..4].try_into().unwrap());
    let magic = body[4];
    if !matches!(magic, MAGIC_V1 | MAGIC_V2) {
        return Err(ProtocolError::UnsupportedMagic(magic));
    }
    if magic == MAGIC_V2 && body.len() < MIN_BATCH_LENGTH + PRODUCER_EXTENSION_LEN {
        return Err(ProtocolError::Truncated {
            needed: MIN_BATCH_LENGTH + PRODUCER_EXTENSION_LEN,
            available: body.len(),
        });
    }
    let stored_crc = u32::from_be_bytes(body[5..9].try_into().unwrap());
    let computed_crc = crc32c::crc32c(&body[9..]);
    if computed_crc != stored_crc {
        return Err(ProtocolError::CrcMismatch {
            stored: stored_crc,
            computed: computed_crc,
        });
    }
    let attributes = u16::from_be_bytes(body[9..11].try_into().unwrap());
    let compression = Compression::from_bits(attributes & COMPRESSION_MASK)?;
    let last_offset_delta = i32::from_be_bytes(body[11..15].try_into().unwrap());
    let max_timestamp = i64::from_be_bytes(body[15..23].try_into().unwrap());
    let producer = (magic == MAGIC_V2).then(|| ProducerMetadata {
        producer_id: i64::from_be_bytes(body[23..31].try_into().unwrap()),
        producer_epoch: i16::from_be_bytes(body[31..33].try_into().unwrap()),
        base_sequence: i32::from_be_bytes(body[33..37].try_into().unwrap()),
    });

    Ok(BatchHeader {
        base_offset,
        batch_length,
        leader_epoch,
        magic,
        compression,
        last_offset_delta,
        max_timestamp,
        producer,
        content_crc32c: stored_crc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_records(n: usize) -> Vec<Record> {
        (0..n)
            .map(|i| {
                if i % 3 == 0 {
                    Record::new(format!("value-{i}").into_bytes())
                } else {
                    Record::with_key(
                        format!("key-{i}").into_bytes(),
                        format!("value-{i}").into_bytes(),
                        i as i64,
                    )
                }
            })
            .collect()
    }

    fn sample_batch(n: usize) -> RecordBatch {
        RecordBatch::new(42, 7, 1_700_000_000_000, sample_records(n))
    }

    #[test]
    fn round_trip_uncompressed() {
        let batch = sample_batch(10);
        let bytes = batch.encode();
        let mut buf = bytes.clone();
        let decoded = RecordBatch::decode(&mut buf).unwrap();
        assert_eq!(decoded, batch);
        assert!(buf.is_empty(), "decode must consume exactly one batch");
        // re-encoding the decoded batch is byte-identical
        assert_eq!(decoded.encode(), bytes);
    }

    #[test]
    fn decode_consumes_one_batch_from_stream() {
        let b1 = sample_batch(3);
        let b2 = RecordBatch::new(45, 7, 1_700_000_000_100, sample_records(2));
        let mut stream = BytesMut::new();
        stream.extend_from_slice(&b1.encode());
        stream.extend_from_slice(&b2.encode());
        let mut buf = stream.freeze();
        assert_eq!(RecordBatch::decode(&mut buf).unwrap(), b1);
        assert_eq!(RecordBatch::decode(&mut buf).unwrap(), b2);
        assert!(buf.is_empty());
    }

    #[test]
    fn empty_batch_round_trip() {
        let batch = sample_batch(0);
        let mut buf = batch.encode();
        let decoded = RecordBatch::decode(&mut buf).unwrap();
        assert_eq!(decoded, batch);
        assert_eq!(decoded.records.len(), 0);
        assert_eq!(decoded.last_offset_delta(), 0);
    }

    #[test]
    fn lz4_round_trip() {
        let batch = sample_batch(100).with_compression(Compression::Lz4);
        let mut buf = batch.encode();
        let decoded = RecordBatch::decode(&mut buf).unwrap();
        assert_eq!(decoded, batch);
        assert_eq!(decoded.compression, Compression::Lz4);
    }

    #[test]
    fn magic_v2_round_trip_exposes_fixed_producer_header() {
        let batch = sample_batch(4)
            .with_compression(Compression::Lz4)
            .with_producer(91, 3, 17);
        let bytes = batch.encode();
        assert_eq!(bytes[16], MAGIC_V2);
        let header = validate_batch_header(&bytes).unwrap();
        assert_eq!(header.magic, MAGIC_V2);
        assert_eq!(
            header.producer,
            Some(ProducerMetadata {
                producer_id: 91,
                producer_epoch: 3,
                base_sequence: 17,
            })
        );
        assert_eq!(header.batch_length as usize, bytes.len() - BATCH_HEADER_LEN);
        let mut input = bytes.clone();
        assert_eq!(RecordBatch::decode(&mut input).unwrap(), batch);
        assert!(input.is_empty());

        // Broker-stamped fields do not change the stable content fingerprint.
        let mut stamped = batch.clone();
        stamped.base_offset = 500;
        stamped.leader_epoch = 12;
        assert_eq!(
            validate_batch_header(&stamped.encode())
                .unwrap()
                .content_crc32c,
            header.content_crc32c
        );
    }

    #[test]
    fn magic_v1_layout_remains_the_default() {
        let batch = sample_batch(2);
        let bytes = batch.encode();
        let header = validate_batch_header(&bytes).unwrap();
        assert_eq!(bytes[16], MAGIC_V1);
        assert_eq!(header.magic, MAGIC_V1);
        assert_eq!(header.producer, None);
        assert_eq!(
            header.batch_length as usize,
            MIN_BATCH_LENGTH + bytes.len() - BATCH_HEADER_LEN - MIN_BATCH_LENGTH
        );
    }

    #[test]
    fn magic_v2_without_fixed_extension_is_rejected() {
        let mut bytes = sample_batch(1).encode().to_vec();
        bytes[16] = MAGIC_V2;
        let mut input = Bytes::from(bytes.clone());
        assert!(RecordBatch::decode(&mut input).is_err());
        assert!(validate_batch_header(&bytes).is_err());
    }

    #[test]
    fn crc_corruption_detected() {
        let mut bytes = sample_batch(5).encode().to_vec();
        // Flip a byte in the records payload (well after the crc field).
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let mut buf = Bytes::from(bytes);
        assert!(matches!(
            RecordBatch::decode(&mut buf),
            Err(ProtocolError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn crc_corruption_detected_by_validate_header() {
        let mut bytes = sample_batch(5).encode().to_vec();
        bytes[25] ^= 0x01; // inside max_timestamp / payload region, after crc
        assert!(matches!(
            validate_batch_header(&bytes),
            Err(ProtocolError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn truncated_inputs_error_at_every_cut_point() {
        let bytes = sample_batch(5).encode();
        for cut in 0..bytes.len() {
            let mut buf = bytes.slice(..cut);
            let err = RecordBatch::decode(&mut buf).unwrap_err();
            // Every truncation must be a typed error, never a panic.
            match err {
                ProtocolError::Truncated { .. }
                | ProtocolError::Malformed(_)
                | ProtocolError::CrcMismatch { .. } => {}
                other => panic!("cut {cut}: unexpected error {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_magic_rejected() {
        let mut bytes = sample_batch(3).encode().to_vec();
        // magic sits at offset 16 (base_offset 8 + batch_length 4 + leader_epoch 4).
        // Changing magic invalidates the crc too, but magic is checked first.
        bytes[16] = 99;
        let mut buf = Bytes::from(bytes);
        assert!(matches!(
            RecordBatch::decode(&mut buf),
            Err(ProtocolError::UnsupportedMagic(99))
        ));
    }

    #[test]
    fn unsupported_compression_rejected() {
        // Hand-build a batch with compression bits = 5 and a valid crc.
        let batch = sample_batch(2);
        let mut bytes = batch.encode().to_vec();
        // attributes at offset 12 + 4 (leader_epoch) + 1 (magic) + 4 (crc) = 21..23
        bytes[22] = 5; // low byte of attributes
        let crc = crc32c::crc32c(&bytes[21..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let mut buf = Bytes::from(bytes);
        assert!(matches!(
            RecordBatch::decode(&mut buf),
            Err(ProtocolError::UnsupportedCompression(5))
        ));
    }

    #[test]
    fn ten_thousand_records_round_trip() {
        let batch = RecordBatch::new(0, 0, 1_700_000_000_000, sample_records(10_000));
        for compression in [Compression::None, Compression::Lz4] {
            let batch = batch.clone().with_compression(compression);
            let mut buf = batch.encode();
            let decoded = RecordBatch::decode(&mut buf).unwrap();
            assert_eq!(decoded, batch);
            let offsets: Vec<i64> = decoded.iter().map(|(o, _)| o).collect();
            assert_eq!(offsets, (0..10_000).collect::<Vec<_>>());
        }
    }

    #[test]
    fn iter_yields_absolute_offsets() {
        let batch = RecordBatch::new(100, 0, 0, sample_records(4));
        let pairs: Vec<(i64, Bytes)> = batch.iter().map(|(o, r)| (o, r.value.clone())).collect();
        assert_eq!(pairs.len(), 4);
        assert_eq!(pairs[0].0, 100);
        assert_eq!(pairs[3].0, 103);
        assert_eq!(batch.next_offset(), 104);
    }

    #[test]
    fn validate_batch_header_reads_fields() {
        let batch = sample_batch(7).with_compression(Compression::Lz4);
        let bytes = batch.encode();
        let header = validate_batch_header(&bytes).unwrap();
        assert_eq!(header.base_offset, 42);
        assert_eq!(header.leader_epoch, 7);
        assert_eq!(header.compression, Compression::Lz4);
        assert_eq!(header.last_offset_delta, 6);
        assert_eq!(header.max_timestamp, 1_700_000_000_000);
        assert_eq!(header.batch_length as usize, bytes.len() - BATCH_HEADER_LEN);
    }
}
