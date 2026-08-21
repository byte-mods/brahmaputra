use thiserror::Error;

/// Errors produced while decoding or validating a record batch.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// The CRC32C stored in the batch does not match the computed one.
    /// This is the typed corruption error callers (e.g. crash recovery in
    /// the storage engine) match on.
    #[error("crc32c mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },

    /// Unknown batch format version.
    #[error("unsupported magic byte: {0}")]
    UnsupportedMagic(u8),

    /// Unknown compression type in the attributes field.
    #[error("unsupported compression type: {0}")]
    UnsupportedCompression(u16),

    /// Input ended before a complete batch/record could be read.
    #[error("truncated input: needed {needed} bytes, only {available} available")]
    Truncated { needed: usize, available: usize },

    /// Structurally invalid batch (bad length field, inconsistent
    /// `last_offset_delta`, varint overflow, ...).
    #[error("malformed batch: {0}")]
    Malformed(&'static str),

    /// LZ4 (de)compression failure.
    #[error("lz4 error: {0}")]
    Lz4(String),

    /// zstd / snappy / gzip (de)compression failure.
    #[error("compression error: {0}")]
    Compress(String),

    /// Unknown `api_key` in a frame header.
    #[error("unknown api key: {0}")]
    UnknownApiKey(i16),

    /// BitPacker-generated message codec failure.
    #[error("message codec error: {0}")]
    Message(String),
}
