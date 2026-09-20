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
//! attributes:        u16   (bits 0..=2: compression; 3: headers;
//!                            4: transactional; 5: control batch;
//!                            6: some record has a null value)
//! last_offset_delta: i32
//! max_timestamp:     i64
//! producer_id:       i64   (magic v2 only)
//! producer_epoch:    i16   (magic v2 only)
//! base_sequence:     i32   (magic v2 only)
//! records:           [Record]  (possibly compressed, per attributes)
//! ```
//!
//! Record layout:
//!
//! ```text
//! record_length:    uvarint  (bytes following this field)
//! key_len_plus_one: uvarint  (0 => null key, else key length + 1)
//! key:              [u8]
//! value_len:        uvarint  (NULL_VALUE_BIT clear: the length itself;
//!                             set: 0 => null value, else length + 1)
//! value:            [u8]
//! timestamp_delta:  uvarint  (zigzag-encoded i64)
//! header_count:     uvarint  (only when the batch's HEADERS_BIT is set)
//!   key_len:            uvarint
//!   key:                [u8]     (UTF-8)
//!   value_len_plus_one: uvarint  (0 => null value, else value length + 1)
//!   value:              [u8]
//! ```
//!
//! The header section is governed by an attributes bit rather than a magic
//! bump because the two are not the same question: magic already means "does
//! this batch carry producer metadata", and overloading it would make
//! "headers, no idempotence" unrepresentable. The bit is set only when some
//! record in the batch actually carries a header, so a batch without headers
//! encodes to exactly the bytes it did before headers existed — existing logs
//! decode unchanged, and adding the feature costs nothing to anyone who does
//! not use it.

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
/// Attributes bit 3: the records in this batch carry a header section.
const HEADERS_BIT: u16 = 0x0008;
/// Attributes bit 4: these records belong to a transaction and must not be
/// shown to a `read_committed` consumer until that transaction commits.
///
/// A bit rather than a magic bump, for the same reason headers are: magic
/// already means "does this batch carry producer metadata", and a
/// transactional batch always carries it. Overloading magic would make
/// "idempotent, not transactional" unrepresentable.
pub const TRANSACTIONAL_BIT: u16 = 0x0010;
/// Attributes bit 5: this batch is a transaction marker, not data.
///
/// Control batches are written by the transaction coordinator to say that
/// everything a producer wrote to this partition under a given transaction
/// is now committed or aborted. They occupy an offset — which is why a
/// transactional topic's offsets are not contiguous with its records — and
/// are never delivered to any consumer.
pub const CONTROL_BIT: u16 = 0x0020;
/// Attributes bit 6: some record in this batch has a *null* value — a
/// tombstone, which tells log compaction to delete the key rather than to
/// keep this record as its latest value.
///
/// A bit, and a per-batch one, for the reason headers are: a null value has
/// to be distinguishable from an empty one, which means the value length
/// must be encoded as `length + 1` with zero reserved for null — and that
/// re-encodes every record ever written. Setting the bit only when a batch
/// actually contains a tombstone means a batch without one encodes to
/// exactly the bytes it did before tombstones existed, so every log already
/// on disk decodes unchanged and the feature costs nothing to anyone who
/// does not use it.
pub const NULL_VALUE_BIT: u16 = 0x0040;

/// Compression applied to the records payload inside a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    Zstd = 2,
    Snappy = 3,
    Gzip = 4,
}

impl Compression {
    fn to_bits(self) -> u16 {
        self as u16
    }

    fn from_bits(bits: u16) -> Result<Self, ProtocolError> {
        match bits {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            3 => Ok(Compression::Snappy),
            4 => Ok(Compression::Gzip),
            other => Err(ProtocolError::UnsupportedCompression(other)),
        }
    }

    /// Parse the `compression.type` spelling Kafka uses.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Compression::None),
            "lz4" => Some(Compression::Lz4),
            "zstd" => Some(Compression::Zstd),
            "snappy" => Some(Compression::Snappy),
            "gzip" => Some(Compression::Gzip),
            _ => None,
        }
    }

    /// The name `parse` accepts, for config echo and error messages.
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Lz4 => "lz4",
            Compression::Zstd => "zstd",
            Compression::Snappy => "snappy",
            Compression::Gzip => "gzip",
        }
    }
}

/// Compress `payload` with `codec`.
///
/// Every codec is length-prepended or self-describing, so the decoder never
/// has to be told the uncompressed size out of band.
fn compress(codec: Compression, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    use std::io::Write;
    match codec {
        Compression::None => Ok(payload.to_vec()),
        Compression::Lz4 => Ok(lz4_flex::compress_prepend_size(payload)),
        // Level 3 is zstd's default: the knee of the ratio/CPU curve, and
        // the level Kafka's own default maps to.
        Compression::Zstd => {
            zstd::stream::encode_all(payload, 3).map_err(|e| ProtocolError::Compress(e.to_string()))
        }
        Compression::Snappy => snap::raw::Encoder::new()
            .compress_vec(payload)
            .map_err(|e| ProtocolError::Compress(e.to_string())),
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder
                .write_all(payload)
                .map_err(|e| ProtocolError::Compress(e.to_string()))?;
            encoder
                .finish()
                .map_err(|e| ProtocolError::Compress(e.to_string()))
        }
    }
}

/// Decompress a records payload produced by [`compress`].
///
/// A corrupt or hostile payload must fail rather than allocate without
/// bound, so every codec that can be asked for an arbitrary output size is
/// capped at [`MAX_DECOMPRESSED_BYTES`].
fn decompress(codec: Compression, body: &[u8]) -> Result<Bytes, ProtocolError> {
    use std::io::Read;
    match codec {
        Compression::None => Ok(Bytes::copy_from_slice(body)),
        Compression::Lz4 => Ok(Bytes::from(
            lz4_flex::decompress_size_prepended(body)
                .map_err(|e| ProtocolError::Lz4(e.to_string()))?,
        )),
        Compression::Zstd => {
            let mut out = Vec::new();
            zstd::stream::Decoder::new(body)
                .map_err(|e| ProtocolError::Compress(e.to_string()))?
                .take(MAX_DECOMPRESSED_BYTES as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| ProtocolError::Compress(e.to_string()))?;
            check_decompressed_len(out)
        }
        Compression::Snappy => {
            // The decoder reads the length from the frame itself, so check it
            // before allocating rather than after.
            let len = snap::raw::decompress_len(body)
                .map_err(|e| ProtocolError::Compress(e.to_string()))?;
            if len > MAX_DECOMPRESSED_BYTES {
                return Err(ProtocolError::Malformed("decompressed payload too large"));
            }
            snap::raw::Decoder::new()
                .decompress_vec(body)
                .map(Bytes::from)
                .map_err(|e| ProtocolError::Compress(e.to_string()))
        }
        Compression::Gzip => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(body)
                .take(MAX_DECOMPRESSED_BYTES as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| ProtocolError::Compress(e.to_string()))?;
            check_decompressed_len(out)
        }
    }
}

fn check_decompressed_len(out: Vec<u8>) -> Result<Bytes, ProtocolError> {
    if out.len() > MAX_DECOMPRESSED_BYTES {
        return Err(ProtocolError::Malformed("decompressed payload too large"));
    }
    Ok(Bytes::from(out))
}

/// Ceiling on what one batch may decompress to. A compressed batch is a
/// decompression bomb otherwise: a few KiB on the wire can name gigabytes of
/// output, and the broker allocates it before it can reject anything.
pub const MAX_DECOMPRESSED_BYTES: usize = 256 * 1024 * 1024;

/// A key/value annotation on a record, carried beside the payload rather
/// than inside it.
///
/// Keys are UTF-8 and may repeat — this is Kafka's model, where headers are
/// an ordered list rather than a map, because tracing systems legitimately
/// attach several values under one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

impl RecordHeader {
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        RecordHeader {
            key: key.into(),
            value: Some(value.into()),
        }
    }
}

/// A single record inside a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    /// `None` is a tombstone: on a compacted topic it deletes the key, and
    /// it is delivered to consumers as a null value so they can see the
    /// deletion. Distinct from `Some(empty)`, which is an ordinary record
    /// that happens to carry no bytes.
    pub value: Option<Bytes>,
    /// Milliseconds relative to the batch's `max_timestamp` base.
    ///
    /// The base is `max_timestamp` rather than a first-record timestamp, so
    /// a delta is normally zero or negative. That is deliberate: every
    /// record written before per-record timestamps existed has a delta of
    /// zero, which under this base still means exactly what it meant then —
    /// the batch timestamp. Changing the base would silently re-date every
    /// record already on disk.
    pub timestamp_delta: i64,
    /// Ordered, possibly repeating annotations. Empty for most records, and
    /// costs nothing to encode when empty.
    pub headers: Vec<RecordHeader>,
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
            value: Some(value.into()),
            timestamp_delta: 0,
            headers: Vec::new(),
        }
    }

    pub fn with_key(key: impl Into<Bytes>, value: impl Into<Bytes>, timestamp_delta: i64) -> Self {
        Record {
            key: Some(key.into()),
            value: Some(value.into()),
            timestamp_delta,
            headers: Vec::new(),
        }
    }

    /// A deletion for `key`: a record with a null value.
    ///
    /// On a compacted topic this is what makes a key go away; on any other
    /// topic it is an ordinary record whose value happens to be null.
    pub fn tombstone(key: impl Into<Bytes>, timestamp_delta: i64) -> Self {
        Record {
            key: Some(key.into()),
            value: None,
            timestamp_delta,
            headers: Vec::new(),
        }
    }

    /// Whether this record deletes its key rather than setting it.
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// The value bytes, with a tombstone reading as empty.
    ///
    /// For size accounting and for the many call sites that only want the
    /// payload; anything that must *distinguish* a deletion from an empty
    /// value has to look at `value` itself.
    pub fn payload(&self) -> &[u8] {
        self.value.as_deref().unwrap_or(&[])
    }

    /// Bytes this record's value occupies, counting a tombstone as zero.
    pub fn value_len(&self) -> usize {
        self.value.as_ref().map_or(0, |value| value.len())
    }

    pub fn with_headers(mut self, headers: Vec<RecordHeader>) -> Self {
        self.headers = headers;
        self
    }

    /// This record's absolute timestamp, given its batch's base.
    pub fn timestamp(&self, max_timestamp: i64) -> i64 {
        max_timestamp.saturating_add(self.timestamp_delta)
    }

    /// The first value stored under `key`, if any.
    pub fn header(&self, key: &str) -> Option<&Bytes> {
        self.headers
            .iter()
            .find(|header| header.key == key)
            .and_then(|header| header.value.as_ref())
    }
}

fn uvarint_len(value: u64) -> usize {
    ((64 - value.leading_zeros()).max(1) as usize).div_ceil(7)
}

// Compute framing before writing so records go straight into one payload
// allocation. The nullable-value bit changes the length encoding for every
// record in a batch, including its non-null records.
fn encoded_record_len(record: &Record, has_headers: bool, has_null_values: bool) -> usize {
    let key = record
        .key
        .as_ref()
        .map_or(1, |key| uvarint_len(key.len() as u64 + 1) + key.len());
    let value = record.value.as_ref().map_or(1, |value| {
        uvarint_len(value.len() as u64 + u64::from(has_null_values)) + value.len()
    });
    let headers = if has_headers {
        uvarint_len(record.headers.len() as u64)
            + record
                .headers
                .iter()
                .map(|header| {
                    uvarint_len(header.key.len() as u64)
                        + header.key.len()
                        + header
                            .value
                            .as_ref()
                            .map_or(1, |value| uvarint_len(value.len() as u64 + 1) + value.len())
                })
                .sum::<usize>()
    } else {
        0
    };
    key + value + uvarint_len(zigzag_encode(record.timestamp_delta)) + headers
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
    /// These records belong to an open transaction.
    ///
    /// Defaulted rather than required, so every existing construction site
    /// keeps producing exactly the bytes it did before transactions existed.
    pub transactional: bool,
    /// This batch is a commit or abort marker rather than data.
    pub control: bool,
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
            transactional: false,
            control: false,
        }
    }

    /// Build a batch from records carrying *absolute* create timestamps.
    ///
    /// The batch stores one base timestamp and a delta per record, so the
    /// rebasing has to happen somewhere; doing it here means both producer
    /// paths cannot disagree about it. `max_timestamp` becomes the newest
    /// record's time, which is what makes it a truthful answer to "how
    /// recent is this batch" for retention and timestamp seeks.
    ///
    /// An empty batch keeps `fallback_timestamp`, since there is no record
    /// to take a time from.
    pub fn from_timestamped(
        base_offset: i64,
        leader_epoch: i32,
        records: Vec<(Record, i64)>,
        fallback_timestamp: i64,
    ) -> Self {
        let max_timestamp = records
            .iter()
            .map(|(_, timestamp)| *timestamp)
            .max()
            .unwrap_or(fallback_timestamp);
        let records = records
            .into_iter()
            .map(|(mut record, timestamp)| {
                // Negative or zero by construction, since the base is the
                // maximum. A clock that jumped backwards mid-batch is still
                // representable rather than saturating.
                record.timestamp_delta = timestamp.saturating_sub(max_timestamp);
                record
            })
            .collect();
        RecordBatch {
            base_offset,
            leader_epoch,
            max_timestamp,
            records,
            compression: Compression::None,
            producer: None,
            transactional: false,
            control: false,
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
        // Only pay for the header section if some record actually uses it.
        // A batch with no headers must encode byte-identically to the way it
        // did before headers existed.
        let has_headers = self.records.iter().any(|r| !r.headers.is_empty());
        // Same rule, same reason: only widen the value length encoding when
        // a tombstone in this batch actually needs it.
        let has_null_values = self.records.iter().any(|r| r.value.is_none());

        // Records payload.
        let payload_len = self
            .records
            .iter()
            .map(|record| {
                let len = encoded_record_len(record, has_headers, has_null_values);
                uvarint_len(len as u64) + len
            })
            .sum();
        let mut payload = BytesMut::with_capacity(payload_len);
        for record in &self.records {
            put_uvarint(
                &mut payload,
                encoded_record_len(record, has_headers, has_null_values) as u64,
            );
            match &record.key {
                None => put_uvarint(&mut payload, 0),
                Some(key) => {
                    put_uvarint(&mut payload, key.len() as u64 + 1);
                    payload.extend_from_slice(key);
                }
            }
            match (&record.value, has_null_values) {
                (Some(value), false) => {
                    put_uvarint(&mut payload, value.len() as u64);
                    payload.extend_from_slice(value);
                }
                (Some(value), true) => {
                    put_uvarint(&mut payload, value.len() as u64 + 1);
                    payload.extend_from_slice(value);
                }
                // Unreachable when the bit is clear: it is set from exactly
                // this condition.
                (None, _) => put_uvarint(&mut payload, 0),
            }
            put_uvarint(&mut payload, zigzag_encode(record.timestamp_delta));
            if has_headers {
                put_uvarint(&mut payload, record.headers.len() as u64);
                for header in &record.headers {
                    put_uvarint(&mut payload, header.key.len() as u64);
                    payload.extend_from_slice(header.key.as_bytes());
                    match &header.value {
                        None => put_uvarint(&mut payload, 0),
                        Some(value) => {
                            put_uvarint(&mut payload, value.len() as u64 + 1);
                            payload.extend_from_slice(value);
                        }
                    }
                }
            }
        }
        debug_assert_eq!(payload.len(), payload_len);
        // Compression cannot fail for any codec here (all are pure encoders
        // over an in-memory buffer), but an encoder that did fail must not
        // silently ship uncompressed bytes under a compressed attribute —
        // that would be unreadable. Fall back to `None` honestly instead.
        let (payload, compression) = match self.compression {
            Compression::None => (payload.freeze(), Compression::None),
            codec => match compress(codec, &payload) {
                Ok(compressed) => (Bytes::from(compressed), codec),
                Err(_) => (payload.freeze(), Compression::None),
            },
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
        let mut attributes = compression.to_bits() & COMPRESSION_MASK;
        if has_headers {
            attributes |= HEADERS_BIT;
        }
        if self.transactional {
            attributes |= TRANSACTIONAL_BIT;
        }
        if self.control {
            attributes |= CONTROL_BIT;
        }
        if has_null_values {
            attributes |= NULL_VALUE_BIT;
        }
        out.put_u16(attributes);
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
            // The uncompressed case already owns the right bytes; copying
            // them again would undo the zero-copy property of a read.
            Compression::None => body,
            codec => decompress(codec, &body)?,
        };

        let records = decode_records(
            &payload,
            attributes & HEADERS_BIT != 0,
            attributes & NULL_VALUE_BIT != 0,
        )?;
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
            transactional: attributes & TRANSACTIONAL_BIT != 0,
            control: attributes & CONTROL_BIT != 0,
        })
    }
}

fn decode_records(
    payload: &[u8],
    has_headers: bool,
    has_null_values: bool,
) -> Result<Vec<Record>, ProtocolError> {
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

        let raw_value_len = get_uvarint(&mut rec)?;
        let value = if has_null_values && raw_value_len == 0 {
            None
        } else {
            let value_len = if has_null_values {
                (raw_value_len - 1) as usize
            } else {
                raw_value_len as usize
            };
            if rec.len() < value_len {
                return Err(ProtocolError::Truncated {
                    needed: value_len,
                    available: rec.len(),
                });
            }
            let (v, r) = rec.split_at(value_len);
            rec = r;
            Some(Bytes::copy_from_slice(v))
        };

        let timestamp_delta = zigzag_decode(get_uvarint(&mut rec)?);

        let mut headers = Vec::new();
        if has_headers {
            let count = get_uvarint(&mut rec)?;
            // A count is a promise about bytes that follow; if it exceeds
            // what is left it is corrupt, and reserving on it would let a
            // 2-byte record ask for gigabytes.
            if count > rec.len() as u64 {
                return Err(ProtocolError::Malformed(
                    "record header count exceeds record",
                ));
            }
            headers.reserve(count as usize);
            for _ in 0..count {
                let key_len = get_uvarint(&mut rec)? as usize;
                if rec.len() < key_len {
                    return Err(ProtocolError::Truncated {
                        needed: key_len,
                        available: rec.len(),
                    });
                }
                let (raw_key, r) = rec.split_at(key_len);
                rec = r;
                let key = std::str::from_utf8(raw_key)
                    .map_err(|_| ProtocolError::Malformed("record header key is not UTF-8"))?
                    .to_string();

                let value_len_plus_one = get_uvarint(&mut rec)?;
                let value = if value_len_plus_one == 0 {
                    None
                } else {
                    let len = (value_len_plus_one - 1) as usize;
                    if rec.len() < len {
                        return Err(ProtocolError::Truncated {
                            needed: len,
                            available: rec.len(),
                        });
                    }
                    let (v, r) = rec.split_at(len);
                    rec = r;
                    Some(Bytes::copy_from_slice(v))
                };
                headers.push(RecordHeader { key, value });
            }
        }

        if !rec.is_empty() {
            return Err(ProtocolError::Malformed("trailing bytes in record"));
        }
        records.push(Record {
            key,
            value,
            timestamp_delta,
            headers,
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
    /// Records here belong to an open transaction.
    pub transactional: bool,
    /// A commit or abort marker rather than data.
    pub control: bool,
}

impl BatchHeader {
    /// Exclusive end offset of this batch: one past its last record.
    pub fn next_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta) + 1
    }

    /// The producer that wrote this batch, when it declared one.
    pub fn producer_id(&self) -> Option<i64> {
        self.producer.map(|producer| producer.producer_id)
    }
}

/// How much of a partition a consumer is willing to see.
///
/// The default is `ReadUncommitted`, as in Kafka: a topic nobody writes
/// transactionally to behaves identically either way, and making the
/// stricter mode the default would silently change what every existing
/// consumer reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Everything up to the high watermark, including records of
    /// transactions that have not been decided.
    #[default]
    ReadUncommitted = 0,
    /// Nothing past the last stable offset, no control markers, and nothing
    /// written by a transaction that aborted.
    ReadCommitted = 1,
}

impl IsolationLevel {
    pub fn from_wire(value: i32) -> Self {
        match value {
            1 => IsolationLevel::ReadCommitted,
            // Anything unrecognised reads as the permissive default rather
            // than as an error: an isolation level a broker does not know
            // is not a reason to refuse the fetch.
            _ => IsolationLevel::ReadUncommitted,
        }
    }

    pub fn to_wire(self) -> i32 {
        self as i32
    }

    pub fn is_committed(self) -> bool {
        matches!(self, IsolationLevel::ReadCommitted)
    }

    /// Parse the `isolation.level` spelling Kafka uses.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "read_uncommitted" => Some(IsolationLevel::ReadUncommitted),
            "read_committed" => Some(IsolationLevel::ReadCommitted),
            _ => None,
        }
    }
}

/// What a control batch says about the transaction it closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMarker {
    Abort = 0,
    Commit = 1,
}

impl ControlMarker {
    fn to_bytes(self) -> Vec<u8> {
        // Two bytes: a version, then the marker. Versioned because a
        // marker is written to the log forever, and the one thing worse
        // than an unreadable marker is one that reads as the wrong kind.
        vec![CONTROL_MARKER_VERSION, self as u8]
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [CONTROL_MARKER_VERSION, 0] => Some(ControlMarker::Abort),
            [CONTROL_MARKER_VERSION, 1] => Some(ControlMarker::Commit),
            _ => None,
        }
    }
}

const CONTROL_MARKER_VERSION: u8 = 1;

/// Build the control batch that closes a producer's transaction on one
/// partition.
///
/// It carries the producer's identity in the same magic-v2 extension a data
/// batch uses, because that is what tells a reader *whose* transaction just
/// ended — a marker that did not name a producer could not be matched to
/// the records it resolves.
pub fn control_batch(
    producer: ProducerMetadata,
    marker: ControlMarker,
    timestamp: i64,
) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        leader_epoch: 0,
        max_timestamp: timestamp,
        records: vec![Record::new(marker.to_bytes())],
        compression: Compression::None,
        producer: Some(producer),
        // A marker is itself part of the transaction it closes: bounded by
        // the same rules, and invisible to a `read_committed` consumer.
        transactional: true,
        control: true,
    }
}

/// Read the marker out of a decoded control batch, or `None` if this is not
/// a control batch or its payload is not one this version understands.
pub fn read_control_marker(batch: &RecordBatch) -> Option<ControlMarker> {
    if !batch.control {
        return None;
    }
    ControlMarker::from_bytes(batch.records.first()?.payload())
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
        transactional: attributes & TRANSACTIONAL_BIT != 0,
        control: attributes & CONTROL_BIT != 0,
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
    fn direct_record_encoding_preserves_wire_bytes_at_varint_boundaries() {
        // Reference the old framing algorithm, which measured a separately
        // serialized record. This deliberately does not use encoded_record_len.
        fn legacy_payload(records: &[Record]) -> BytesMut {
            let headers = records.iter().any(|r| !r.headers.is_empty());
            let nullable = records.iter().any(|r| r.value.is_none());
            let mut payload = BytesMut::new();
            for record in records {
                let mut rec = BytesMut::new();
                match &record.key {
                    None => put_uvarint(&mut rec, 0),
                    Some(key) => {
                        put_uvarint(&mut rec, key.len() as u64 + 1);
                        rec.extend_from_slice(key);
                    }
                }
                match &record.value {
                    None => put_uvarint(&mut rec, 0),
                    Some(value) => {
                        put_uvarint(&mut rec, value.len() as u64 + u64::from(nullable));
                        rec.extend_from_slice(value);
                    }
                }
                put_uvarint(&mut rec, zigzag_encode(record.timestamp_delta));
                if headers {
                    put_uvarint(&mut rec, record.headers.len() as u64);
                    for header in &record.headers {
                        put_uvarint(&mut rec, header.key.len() as u64);
                        rec.extend_from_slice(header.key.as_bytes());
                        match &header.value {
                            None => put_uvarint(&mut rec, 0),
                            Some(value) => {
                                put_uvarint(&mut rec, value.len() as u64 + 1);
                                rec.extend_from_slice(value);
                            }
                        }
                    }
                }
                put_uvarint(&mut payload, rec.len() as u64);
                payload.extend_from_slice(&rec);
            }
            payload
        }
        for nullable in [false, true] {
            for headers in [false, true] {
                let mut records = Vec::new();
                for len in [0, 1, 126, 127, 128, 16_382, 16_383, 16_384] {
                    for delta in [0, 63, 64, -64, -65, i64::MIN, i64::MAX] {
                        let mut record = Record::with_key(vec![b'k'; len], vec![b'v'; len], delta);
                        if headers {
                            record.headers = vec![
                                RecordHeader::new("trace-λ", vec![b'h'; len]),
                                RecordHeader {
                                    key: "null".into(),
                                    value: None,
                                },
                                RecordHeader::new("empty", Vec::new()),
                            ];
                        }
                        records.push(record);
                        records.push(Record::new(vec![b'x'; len]));
                    }
                }
                if nullable {
                    records.push(Record::tombstone(Vec::new(), -1));
                }
                let expected = legacy_payload(&records);
                for codec in [
                    Compression::None,
                    Compression::Lz4,
                    Compression::Gzip,
                    Compression::Snappy,
                    Compression::Zstd,
                ] {
                    for idempotent in [false, true] {
                        let mut batch =
                            RecordBatch::new(42, 7, 1234, records.clone()).with_compression(codec);
                        if idempotent {
                            batch = batch.with_producer(5, 2, 0);
                        }
                        let mut bytes = batch.encode();
                        let start = 35
                            + if idempotent {
                                PRODUCER_EXTENSION_LEN
                            } else {
                                0
                            };
                        assert_eq!(
                            &bytes[start..],
                            compress(codec, &expected).unwrap(),
                            "{codec:?}"
                        );
                        assert_eq!(RecordBatch::decode(&mut bytes).unwrap(), batch);
                        assert!(bytes.is_empty());
                    }
                }
            }
        }
    }

    /// Every codec must return exactly what it was given, and must do so
    /// through the framing — a codec that round-trips in isolation but
    /// disagrees with the attributes bits is still broken.
    #[test]
    fn every_codec_round_trips() {
        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Snappy,
            Compression::Gzip,
        ] {
            let batch = sample_batch(40).with_compression(codec);
            let mut bytes = batch.encode();
            let decoded = RecordBatch::decode(&mut bytes).unwrap();
            assert_eq!(decoded.records, batch.records, "{codec:?} lost record data");
            assert_eq!(decoded.compression, codec, "{codec:?} lost its attribute");
        }
    }

    /// Compression has to actually compress, or the attribute is a lie that
    /// costs CPU. Highly repetitive input is the easy case; if a codec
    /// cannot win here it is misconfigured.
    #[test]
    fn compressible_input_gets_smaller() {
        let records: Vec<Record> = (0..200).map(|_| Record::new(vec![b'a'; 512])).collect();
        let plain = RecordBatch::new(0, 0, 1, records.clone()).encode().len();
        for codec in [
            Compression::Lz4,
            Compression::Zstd,
            Compression::Snappy,
            Compression::Gzip,
        ] {
            let compressed = RecordBatch::new(0, 0, 1, records.clone())
                .with_compression(codec)
                .encode()
                .len();
            assert!(
                compressed < plain / 2,
                "{codec:?} produced {compressed} bytes from {plain}"
            );
        }
    }

    #[test]
    fn compression_names_round_trip() {
        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Snappy,
            Compression::Gzip,
        ] {
            assert_eq!(Compression::parse(codec.name()), Some(codec));
        }
        assert_eq!(Compression::parse("brotli"), None);
    }

    #[test]
    fn headers_round_trip() {
        let record = Record::new(b"payload".to_vec()).with_headers(vec![
            RecordHeader::new("trace-id", b"abc123".to_vec()),
            RecordHeader::new("content-type", b"application/json".to_vec()),
            // A null value is distinct from an empty one, and both are legal.
            RecordHeader {
                key: "tombstone-reason".into(),
                value: None,
            },
            RecordHeader::new("empty", Vec::new()),
        ]);
        let batch = RecordBatch::new(0, 0, 1_700_000_000_000, vec![record.clone()]);
        let mut bytes = batch.encode();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        assert_eq!(decoded.records[0], record);
        assert_eq!(
            decoded.records[0].header("trace-id").unwrap().as_ref(),
            b"abc123"
        );
        assert_eq!(decoded.records[0].headers[2].value, None);
        assert_eq!(
            decoded.records[0].headers[3].value.as_deref(),
            Some(&b""[..])
        );
    }

    /// The distinction compaction is built on: a null value deletes a key,
    /// an empty value sets it to nothing.
    #[test]
    fn a_tombstone_is_not_an_empty_value() {
        let batch = RecordBatch::new(
            0,
            0,
            1_700_000_000_000,
            vec![
                Record::with_key(b"k1".to_vec(), b"set".to_vec(), 0),
                Record::with_key(b"k2".to_vec(), Vec::new(), 0),
                Record::tombstone(b"k3".to_vec(), 0),
            ],
        );
        let mut bytes = batch.encode();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        assert_eq!(decoded.records, batch.records);
        assert_eq!(decoded.records[0].value.as_deref(), Some(&b"set"[..]));
        assert_eq!(decoded.records[1].value.as_deref(), Some(&b""[..]));
        assert_eq!(decoded.records[2].value, None);
        assert!(!decoded.records[1].is_tombstone());
        assert!(decoded.records[2].is_tombstone());
    }

    /// A batch with no tombstone must encode to exactly the bytes it did
    /// before tombstones existed, or every log already on disk changes
    /// meaning.
    #[test]
    fn a_batch_without_a_tombstone_does_not_set_the_bit() {
        let batch = RecordBatch::new(
            0,
            0,
            1_700_000_000_000,
            vec![Record::with_key(b"k".to_vec(), b"v".to_vec(), 0)],
        );
        let encoded = batch.encode();
        // attributes sit after base_offset, batch_length, leader_epoch,
        // magic and crc.
        let attributes = u16::from_be_bytes([encoded[21], encoded[22]]);
        assert_eq!(attributes & NULL_VALUE_BIT, 0);

        let with_tombstone = RecordBatch::new(0, 0, 1, vec![Record::tombstone(b"k".to_vec(), 0)]);
        let encoded = with_tombstone.encode();
        let attributes = u16::from_be_bytes([encoded[21], encoded[22]]);
        assert_eq!(attributes & NULL_VALUE_BIT, NULL_VALUE_BIT);
    }

    /// Tombstones must survive every codec, since the value length lives
    /// inside the compressed payload.
    #[test]
    fn tombstones_survive_compression() {
        for codec in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Snappy,
            Compression::Gzip,
        ] {
            let batch = RecordBatch::new(
                0,
                0,
                1,
                vec![
                    Record::tombstone(b"gone".to_vec(), 0),
                    Record::with_key(b"here".to_vec(), b"value".to_vec(), 0),
                ],
            )
            .with_compression(codec);
            let mut bytes = batch.encode();
            let decoded = RecordBatch::decode(&mut bytes).unwrap();
            assert_eq!(decoded.records[0].value, None, "codec {codec:?}");
            assert_eq!(
                decoded.records[1].value.as_deref(),
                Some(&b"value"[..]),
                "codec {codec:?}"
            );
        }
    }

    /// Kafka's header list is ordered and may repeat a key; a map would
    /// silently drop the duplicates that tracing systems rely on.
    #[test]
    fn headers_keep_order_and_duplicates() {
        let record = Record::new(b"v".to_vec()).with_headers(vec![
            RecordHeader::new("tag", b"first".to_vec()),
            RecordHeader::new("tag", b"second".to_vec()),
        ]);
        let mut bytes = RecordBatch::new(0, 0, 1, vec![record]).encode();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        let tags: Vec<_> = decoded.records[0]
            .headers
            .iter()
            .map(|h| h.value.clone().unwrap())
            .collect();
        assert_eq!(tags, vec![Bytes::from("first"), Bytes::from("second")]);
        assert_eq!(decoded.records[0].header("tag").unwrap().as_ref(), b"first");
    }

    #[test]
    fn transactional_and_control_bits_survive_a_round_trip() {
        let producer = ProducerMetadata {
            producer_id: 77,
            producer_epoch: 3,
            base_sequence: 0,
        };
        let mut batch = RecordBatch::new(10, 1, 5_000, vec![Record::new(b"payload".to_vec())]);
        batch.producer = Some(producer);
        batch.transactional = true;

        let encoded = batch.encode();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert!(decoded.transactional);
        assert!(!decoded.control);
        // The header path is what the broker reads on every fetch, and it
        // has to agree with the full decode or a read_committed fetch would
        // filter on one answer while the consumer saw another.
        let header = validate_batch_header(&encoded).unwrap();
        assert!(header.transactional);
        assert!(!header.control);
        assert_eq!(header.producer_id(), Some(77));
        assert_eq!(header.next_offset(), 11);

        let marker = control_batch(producer, ControlMarker::Commit, 6_000);
        let encoded = marker.encode();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert!(decoded.control && decoded.transactional);
        assert_eq!(read_control_marker(&decoded), Some(ControlMarker::Commit));
        let header = validate_batch_header(&encoded).unwrap();
        assert!(header.control);

        let aborted = control_batch(producer, ControlMarker::Abort, 6_000);
        let decoded = RecordBatch::decode(&mut aborted.encode()).unwrap();
        assert_eq!(read_control_marker(&decoded), Some(ControlMarker::Abort));

        // A data batch is not a marker, however it is inspected.
        assert_eq!(read_control_marker(&batch), None);
    }

    #[test]
    fn a_non_transactional_batch_encodes_exactly_as_it_always_did() {
        // The bits are additive: adding transactions must not change one
        // byte of a batch that uses none of it, or every log written before
        // today would decode differently.
        let batch = RecordBatch::new(4, 2, 900, vec![Record::new(b"plain".to_vec())]);
        let encoded = batch.encode();
        let attributes = u16::from_be_bytes([encoded[21], encoded[22]]);
        assert_eq!(attributes & (TRANSACTIONAL_BIT | CONTROL_BIT), 0);
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert!(!decoded.transactional && !decoded.control);
    }

    /// The whole point of the attributes bit: a batch nobody attached a
    /// header to must encode to the bytes it always did, so existing logs
    /// stay readable and the feature costs non-users nothing.
    #[test]
    fn a_batch_without_headers_is_byte_identical_to_the_old_format() {
        let batch = sample_batch(10);
        let encoded = batch.encode();
        // Attributes sit after base_offset(8) + batch_length(4) +
        // leader_epoch(4) + magic(1) + crc(4).
        let attributes = u16::from_be_bytes([encoded[21], encoded[22]]);
        assert_eq!(
            attributes & HEADERS_BIT,
            0,
            "the headers bit must stay clear when no record uses headers"
        );

        let with_headers = RecordBatch::new(
            42,
            7,
            1_700_000_000_000,
            vec![Record::new(b"v".to_vec())
                .with_headers(vec![RecordHeader::new("k", b"v".to_vec())])],
        );
        let encoded = with_headers.encode();
        let attributes = u16::from_be_bytes([encoded[21], encoded[22]]);
        assert_ne!(attributes & HEADERS_BIT, 0, "the bit must be set when used");
    }

    /// A record's timestamp is its own, not its batch's. Rebasing against
    /// the maximum is what lets the old on-disk delta of zero keep meaning
    /// exactly what it always meant.
    #[test]
    fn per_record_timestamps_survive_a_round_trip() {
        let base = 1_700_000_000_000i64;
        let batch = RecordBatch::from_timestamped(
            0,
            0,
            vec![
                (Record::new(b"a".to_vec()), base),
                (Record::new(b"b".to_vec()), base + 250),
                (Record::new(b"c".to_vec()), base + 100),
            ],
            0,
        );
        assert_eq!(batch.max_timestamp, base + 250, "the newest record wins");

        let mut bytes = batch.encode();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        let times: Vec<i64> = decoded
            .records
            .iter()
            .map(|r| r.timestamp(decoded.max_timestamp))
            .collect();
        assert_eq!(times, vec![base, base + 250, base + 100]);
    }

    /// A record written before per-record timestamps existed has a delta of
    /// zero and must still read back as the batch timestamp.
    #[test]
    fn a_zero_delta_still_means_the_batch_timestamp() {
        let base = 1_700_000_000_000i64;
        let record = Record::new(b"legacy".to_vec());
        assert_eq!(record.timestamp_delta, 0);
        assert_eq!(record.timestamp(base), base);
    }

    #[test]
    fn an_empty_batch_keeps_the_fallback_timestamp() {
        let batch = RecordBatch::from_timestamped(0, 0, Vec::new(), 99);
        assert_eq!(batch.max_timestamp, 99);
    }

    /// A corrupt header count must be refused rather than used to size an
    /// allocation.
    #[test]
    fn an_impossible_header_count_is_refused() {
        // header_count = 200 in a record with only a couple of bytes left.
        let mut payload = BytesMut::new();
        let mut rec = BytesMut::new();
        put_uvarint(&mut rec, 0); // null key
        put_uvarint(&mut rec, 1); // value length
        rec.extend_from_slice(b"v");
        put_uvarint(&mut rec, zigzag_encode(0));
        put_uvarint(&mut rec, 200); // header count, a lie
        put_uvarint(&mut payload, rec.len() as u64);
        payload.extend_from_slice(&rec);

        let error = decode_records(&payload, true, false).unwrap_err();
        assert!(
            matches!(error, ProtocolError::Malformed(_)),
            "expected a malformed error, got {error:?}"
        );
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
        let pairs: Vec<(i64, Bytes)> = batch
            .iter()
            .map(|(o, r)| (o, r.value.clone().unwrap_or_default()))
            .collect();
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
