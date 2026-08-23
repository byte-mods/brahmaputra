//! Brahmaputra wire protocol primitives.
//!
//! Pure, synchronous codec for the record batch format defined in
//! DESIGN.md §4.1 — the only unit on disk and on the wire. Batches are
//! produced, stored, replicated and consumed as unmodified bytes.
//!
//! Also contains the data-plane frame header codec ([`frame`]), the
//! request/response body helpers ([`codec`]) and the BitPacker-generated
//! message structs ([`gen`], from `schemas/protocol.buff`).
//!
//! No async, no tokio.

mod batch;
pub mod codec;
mod error;
pub mod frame;
pub mod gen;
pub mod producer;
pub mod replica;
mod varint;

pub use batch::{
    control_batch, read_control_marker, validate_batch_header, BatchHeader, Compression,
    ControlMarker, IsolationLevel, ProducerMetadata, Record, RecordBatch, RecordHeader,
    BATCH_HEADER_LEN, CONTROL_BIT, MAGIC_V1, MAGIC_V2, MAX_DECOMPRESSED_BYTES, MIN_BATCH_LENGTH,
    PRODUCER_EXTENSION_LEN, TRANSACTIONAL_BIT,
};
pub use error::ProtocolError;
pub use frame::{
    decode_payload, encode_frame, encode_frame_prefix, encode_payload, error_code, ApiKey,
    FrameHeader, API_VERSION,
};
