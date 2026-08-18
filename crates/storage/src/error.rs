use std::io;

use brahmaputra_protocol::ProtocolError;
use thiserror::Error;

/// Errors produced by the storage engine.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// Read offset is outside the retained range of the log.
    #[error("offset {offset} out of range: log covers [{start}, {end})")]
    OffsetOutOfRange { offset: i64, start: i64, end: i64 },

    /// Batches without records carry no offsets and cannot be appended.
    #[error("cannot append an empty batch")]
    EmptyBatch,

    #[error("replica batch is not contiguous: expected base offset {expected}, got {actual}")]
    NonContiguousReplicaBatch { expected: i64, actual: i64 },

    #[error("leader epoch {epoch} is not newer than the last checkpoint epoch {last_epoch}")]
    NonMonotonicLeaderEpoch { epoch: i32, last_epoch: i32 },

    #[error("leader epoch checkpoint offset regressed from {last_offset} to {offset}")]
    RegressingLeaderEpochOffset { last_offset: i64, offset: i64 },

    #[error("invalid high watermark {requested}: current={current}, log_end={log_end}")]
    InvalidHighWatermark {
        requested: i64,
        current: i64,
        log_end: i64,
    },

    #[error(
        "refusing to truncate committed data at {requested}; high watermark is {high_watermark}"
    )]
    TruncateBelowHighWatermark { requested: i64, high_watermark: i64 },

    #[error("invalid log config: {0}")]
    InvalidConfig(&'static str),
}
