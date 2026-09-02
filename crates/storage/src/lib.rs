//! Brahmaputra storage engine.
//!
//! Per-partition, segmented, append-only log on local disk, per
//! DESIGN.md §4.1–4.5. Pure synchronous code — no tokio; async lives at
//! the edges (broker crate, later milestone).
//!
//! Directory layout:
//!
//! ```text
//! <log_dir>/                            # one directory per topic-partition
//!   00000000000000000000.log            # record batches, append-only
//!   00000000000000000000.index          # sparse offset -> position index
//!   00000000000000000000.timeindex      # sparse timestamp -> offset index
//!   00000000000000481234.log
//!   ...
//!   hwm                                 # high-watermark checkpoint (i64 BE)
//!   logstart                            # log start offset after DeleteRecords
//!   txnindex                            # open and aborted transactions
//! ```

mod epoch;
mod error;
mod index;
mod log;
mod segment;
mod txn;

pub use epoch::{LeaderEpochCheckpoint, LeaderEpochEntry};
pub use error::StorageError;
pub use log::{Log, LogConfig, LogRegion};
pub use segment::read_exact_at;
pub use txn::{AbortedTransaction, TransactionIndex};
