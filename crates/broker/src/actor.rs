//! Partition actors (Blueprint 02 §3): one tokio task per (topic,
//! partition) owning its `storage::Log`. All access goes through bounded
//! `mpsc` commands with `oneshot` replies — the actor task is the only
//! writer (and reader) of the log, so append ordering IS the log order and
//! no locks are needed anywhere on the data path.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::pending;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_protocol::{validate_batch_header, BatchHeader, ProducerMetadata, RecordBatch};
use brahmaputra_storage::{LeaderEpochEntry, Log, LogConfig, StorageError};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tracing::{debug, trace, warn};

const RESET_MARKER: &str = "replica-reset";
const PRODUCER_DEDUP_WINDOW: usize = 5;

/// Successful idempotent append result. A duplicate returns the original
/// offsets without writing another batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerAppendOutcome {
    pub base_offset: i64,
    pub next_offset: i64,
    pub duplicate: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProducerAppendError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid idempotent producer metadata")]
    InvalidMetadata,
    #[error("producer epoch {requested} is fenced by epoch {current}")]
    FencedEpoch { requested: i16, current: i16 },
    #[error("producer sequence {requested} is out of order; expected {expected}")]
    OutOfOrderSequence { requested: i32, expected: i32 },
}

#[derive(Debug, Clone, Copy)]
struct RecentProducerBatch {
    base_sequence: i32,
    record_count: i32,
    content_crc32c: u32,
    base_offset: i64,
}

#[derive(Debug)]
struct ProducerPartitionState {
    epoch: i16,
    next_sequence: i32,
    recent: VecDeque<RecentProducerBatch>,
}

#[derive(Debug, Default)]
struct ProducerStateTable {
    producers: HashMap<i64, ProducerPartitionState>,
}

enum ProducerDecision {
    Append {
        metadata: ProducerMetadata,
        record_count: i32,
        content_crc32c: u32,
    },
    Duplicate(ProducerAppendOutcome),
}

impl ProducerStateTable {
    fn rebuild(log: &Log) -> Result<Self, StorageError> {
        let mut table = Self::default();
        let mut offset = log.log_start_offset();
        let end = log.log_end_offset();
        while offset < end {
            let batches = log.read(offset, 8 * 1024 * 1024)?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                let header = validate_batch_header(&batch)?;
                table.observe_replicated(&header);
                offset = header.base_offset + i64::from(header.last_offset_delta) + 1;
            }
        }
        Ok(table)
    }

    fn decide(&self, batch: &RecordBatch) -> Result<ProducerDecision, ProducerAppendError> {
        let metadata = batch.producer.ok_or(ProducerAppendError::InvalidMetadata)?;
        let record_count = i32::try_from(batch.records.len())
            .ok()
            .filter(|count| *count > 0)
            .ok_or(ProducerAppendError::InvalidMetadata)?;
        if metadata.producer_id < 0
            || metadata.producer_epoch < 0
            || metadata.base_sequence < 0
            || metadata.base_sequence.checked_add(record_count).is_none()
        {
            return Err(ProducerAppendError::InvalidMetadata);
        }
        let encoded = batch.encode();
        let content_crc32c = validate_batch_header(&encoded)
            .map_err(StorageError::from)?
            .content_crc32c;
        let Some(state) = self.producers.get(&metadata.producer_id) else {
            return if metadata.base_sequence == 0 {
                Ok(ProducerDecision::Append {
                    metadata,
                    record_count,
                    content_crc32c,
                })
            } else {
                Err(ProducerAppendError::OutOfOrderSequence {
                    requested: metadata.base_sequence,
                    expected: 0,
                })
            };
        };
        if metadata.producer_epoch < state.epoch {
            return Err(ProducerAppendError::FencedEpoch {
                requested: metadata.producer_epoch,
                current: state.epoch,
            });
        }
        if metadata.producer_epoch > state.epoch {
            return if metadata.base_sequence == 0 {
                Ok(ProducerDecision::Append {
                    metadata,
                    record_count,
                    content_crc32c,
                })
            } else {
                Err(ProducerAppendError::OutOfOrderSequence {
                    requested: metadata.base_sequence,
                    expected: 0,
                })
            };
        }
        if metadata.base_sequence == state.next_sequence {
            return Ok(ProducerDecision::Append {
                metadata,
                record_count,
                content_crc32c,
            });
        }
        if let Some(previous) = state.recent.iter().find(|previous| {
            previous.base_sequence == metadata.base_sequence
                && previous.record_count == record_count
        }) {
            if previous.content_crc32c == content_crc32c {
                return Ok(ProducerDecision::Duplicate(ProducerAppendOutcome {
                    base_offset: previous.base_offset,
                    next_offset: previous.base_offset + i64::from(previous.record_count),
                    duplicate: true,
                }));
            }
        }
        Err(ProducerAppendError::OutOfOrderSequence {
            requested: metadata.base_sequence,
            expected: state.next_sequence,
        })
    }

    fn record_append(
        &mut self,
        metadata: ProducerMetadata,
        record_count: i32,
        content_crc32c: u32,
        base_offset: i64,
    ) {
        let next_sequence = metadata.base_sequence + record_count;
        let state = self
            .producers
            .entry(metadata.producer_id)
            .or_insert_with(|| ProducerPartitionState {
                epoch: metadata.producer_epoch,
                next_sequence,
                recent: VecDeque::new(),
            });
        if state.epoch != metadata.producer_epoch {
            state.epoch = metadata.producer_epoch;
            state.recent.clear();
        }
        state.next_sequence = next_sequence;
        state.recent.push_back(RecentProducerBatch {
            base_sequence: metadata.base_sequence,
            record_count,
            content_crc32c,
            base_offset,
        });
        while state.recent.len() > PRODUCER_DEDUP_WINDOW {
            state.recent.pop_front();
        }
    }

    /// Followers and restart recovery trust the leader-written log order and
    /// materialize the latest bounded window without re-encoding payloads.
    fn observe_replicated(&mut self, header: &BatchHeader) {
        let Some(metadata) = header.producer else {
            return;
        };
        let Some(record_count) = header.last_offset_delta.checked_add(1) else {
            return;
        };
        if metadata.producer_id < 0
            || metadata.producer_epoch < 0
            || metadata.base_sequence < 0
            || record_count <= 0
            || metadata.base_sequence.checked_add(record_count).is_none()
        {
            return;
        }
        let replace = self
            .producers
            .get(&metadata.producer_id)
            .is_none_or(|state| metadata.producer_epoch > state.epoch);
        if replace {
            self.producers.insert(
                metadata.producer_id,
                ProducerPartitionState {
                    epoch: metadata.producer_epoch,
                    next_sequence: metadata.base_sequence + record_count,
                    recent: VecDeque::new(),
                },
            );
        }
        let Some(state) = self.producers.get(&metadata.producer_id) else {
            return;
        };
        if state.epoch != metadata.producer_epoch {
            return;
        }
        self.record_append(
            metadata,
            record_count,
            header.content_crc32c,
            header.base_offset,
        );
    }
}

/// Commands accepted by a partition actor.
pub enum Cmd {
    /// Append one batch (the actor stamps the base offset) and reply with
    /// the assigned base offset.
    Append {
        batch: RecordBatch,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
    /// Append a producer's encoded batch as-is, stamping only base offset
    /// and leader epoch in the header. Replies with `(base, next)` offsets.
    AppendProducerBatch {
        batch: Bytes,
        leader_epoch: i32,
        reply: oneshot::Sender<Result<(i64, i64), StorageError>>,
    },
    /// Validate producer epoch/sequence and either append exactly once or
    /// return the original offset of an exact recent duplicate.
    AppendIdempotent {
        batch: RecordBatch,
        reply: oneshot::Sender<Result<ProducerAppendOutcome, ProducerAppendError>>,
    },
    /// Where to start scanning for the first record at or after a
    /// timestamp. `None` means no record in the log qualifies.
    ScanStartForTimestamp {
        timestamp: i64,
        reply: oneshot::Sender<Option<i64>>,
    },
    /// On-disk size and offset bounds, for `DescribeLogDirs`.
    Usage {
        reply: oneshot::Sender<PartitionUsage>,
    },
    /// Who has written to this partition and what is still open, for
    /// `DescribeProducers`.
    Producers {
        reply: oneshot::Sender<PartitionProducers>,
    },
    /// Discard every record below an offset; reply with the resulting log
    /// start offset.
    DeleteRecordsBefore {
        offset: i64,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
    /// Read as [`Cmd::Read`] does, but showing only committed data:
    /// bounded by the last stable offset, with control batches and aborted
    /// records removed.
    ReadCommitted {
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<Result<ReadOutcome, StorageError>>,
    },
    /// Read raw batch bytes from `offset`, capped by the high watermark.
    Read {
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<Result<ReadOutcome, StorageError>>,
    },
    /// Describe a read as file ranges, for the zero-copy fetch path.
    ReadRegions {
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<Result<RegionOutcome, StorageError>>,
    },
    /// Read raw batch bytes through the log end, including data above HWM.
    ReadUncommitted {
        offset: i64,
        max_bytes: usize,
        reply: oneshot::Sender<Result<ReadOutcome, StorageError>>,
    },
    /// Append one already encoded leader batch without changing any byte.
    AppendReplicaBatch {
        batch: Bytes,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
    /// Advance and persist the follower high watermark.
    UpdateHighWatermark {
        high_watermark: i64,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Record the log-end boundary at which a leader epoch begins.
    RecordLeaderEpoch {
        leader_epoch: i32,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Look up the exclusive end offset of a known leader epoch.
    EndOffsetForLeaderEpoch {
        leader_epoch: i32,
        reply: oneshot::Sender<Option<i64>>,
    },
    /// Snapshot the persistent leader epoch checkpoints.
    LeaderEpochEntries {
        reply: oneshot::Sender<Vec<LeaderEpochEntry>>,
    },
    /// Truncate uncommitted data to a whole-batch prefix.
    TruncateTo {
        offset: i64,
        reply: oneshot::Sender<Result<i64, StorageError>>,
    },
    /// Rebase a replica whose complete local log is older than the leader's
    /// retained start. The resulting empty log has start=end=HWM=`offset`.
    ResetToOffset {
        offset: i64,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Swap this partition's log configuration without reopening it.
    ///
    /// Sent when a topic's configuration changes in the metadata, so that a
    /// live partition picks up a new `retention.ms` (or any other per-topic
    /// setting) rather than waiting for a broker restart.
    Reconfigure {
        config: brahmaputra_storage::LogConfig,
        reply: oneshot::Sender<()>,
    },
    /// Current (start, end, high watermark) offsets.
    Offsets {
        reply: oneshot::Sender<(i64, i64, i64)>,
    },
    /// Stop accepting work, persist the final watermark, and close every
    /// log file before acknowledging. Topic deletion uses this before it
    /// removes the partition directory.
    Shutdown { reply: oneshot::Sender<()> },
}

/// Result of a partition read.
#[derive(Debug)]
/// The same selection as [`ReadOutcome`], described as file ranges the
/// broker can hand straight to `sendfile` instead of buffers it has read.
pub struct RegionOutcome {
    pub regions: Vec<brahmaputra_storage::LogRegion>,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
}

/// What one partition occupies on this broker, for `DescribeLogDirs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionUsage {
    pub size_bytes: u64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub high_watermark: i64,
    pub segments: usize,
}

/// One producer's state on one partition, as `DescribeProducers` reports it.
#[derive(Debug, Clone, Copy)]
pub struct ProducerSnapshot {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// Sequence of the last batch accepted, or -1 if none is remembered.
    pub last_sequence: i32,
    /// First offset of this producer's open transaction here, or -1.
    pub current_txn_start_offset: i64,
}

/// What `DescribeProducers` needs from a partition: who has written, and
/// the two offsets whose gap is the symptom an operator is chasing.
#[derive(Debug, Default)]
pub struct PartitionProducers {
    pub producers: Vec<ProducerSnapshot>,
    pub last_stable_offset: i64,
    pub high_watermark: i64,
}

pub struct ReadOutcome {
    /// Raw, unmodified batch bytes, all strictly below the high watermark.
    pub batches: Vec<Bytes>,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
}

/// Cheap, cloneable handle to a running partition actor.
#[derive(Clone)]
pub struct PartitionHandle {
    tx: mpsc::Sender<Cmd>,
    /// Latest high watermark (== log end in M1); used for fetch long-polls.
    watermark: watch::Receiver<i64>,
    /// Latest log end offset, published on every append. A consumer's
    /// long poll waits on the watermark, because a consumer may not read
    /// past it; a *follower* must wait on this instead, because under
    /// `acks=all` the watermark cannot advance until that follower has
    /// fetched — waiting on the watermark would deadlock against itself.
    appends: watch::Receiver<i64>,
    /// How many times this replica has had its log changed by something
    /// other than a local leader append: a batch replicated from a leader,
    /// or a truncation on becoming a follower. Anything that caches state
    /// derived from the log — a coordinator shard — compares this against
    /// the value it loaded at, because a change means the state on disk
    /// moved without going through the cache.
    disruptions: Arc<AtomicU64>,
}

impl PartitionHandle {
    pub(crate) fn disruptions(&self) -> u64 {
        self.disruptions.load(Ordering::Acquire)
    }

    /// Whether two handles drive the same actor.
    pub(crate) fn same_actor(&self, other: &PartitionHandle) -> bool {
        self.tx.same_channel(&other.tx)
    }

    pub(crate) async fn shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown { reply }).await.is_ok() {
            let _ = rx.await;
        }
    }

    pub async fn append(&self, batch: RecordBatch) -> Result<i64, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Append { batch, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Append a producer batch without decoding it. Returns
    /// `(base_offset, next_offset)`.
    pub(crate) async fn append_producer_batch(
        &self,
        batch: Bytes,
        leader_epoch: i32,
    ) -> Result<(i64, i64), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::AppendProducerBatch {
                batch,
                leader_epoch,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    pub(crate) async fn append_idempotent(
        &self,
        batch: RecordBatch,
    ) -> Result<ProducerAppendOutcome, ProducerAppendError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::AppendIdempotent { batch, reply })
            .await
            .map_err(|_| ProducerAppendError::Storage(actor_gone()))?;
        rx.await
            .map_err(|_| ProducerAppendError::Storage(actor_gone()))?
    }

    /// Describe a read as file ranges instead of buffers, so a plaintext
    /// fetch can send them with `sendfile` and never read the payload into
    /// the process at all.
    pub async fn read_regions(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<RegionOutcome, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::ReadRegions {
                offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Read only committed data. `high_watermark` in the outcome carries
    /// the last stable offset, which is the ceiling that actually applied.
    pub async fn read_committed(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadOutcome, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::ReadCommitted {
                offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Read at `isolation`, so a caller that has a level in hand does not
    /// have to branch on it.
    pub async fn read_at(
        &self,
        offset: i64,
        max_bytes: usize,
        isolation: brahmaputra_protocol::IsolationLevel,
    ) -> Result<ReadOutcome, StorageError> {
        if isolation.is_committed() {
            self.read_committed(offset, max_bytes).await
        } else {
            self.read(offset, max_bytes).await
        }
    }

    pub async fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutcome, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Read {
                offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Read raw batches through the log end instead of stopping at the high
    /// watermark. This is exclusively for leader-to-follower replication;
    /// client Fetch must continue to use [`Self::read`].
    pub async fn read_uncommitted(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<ReadOutcome, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::ReadUncommitted {
                offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Append a leader-provided encoded batch byte-for-byte. The batch must
    /// begin exactly at the local log end; this does not advance HWM.
    pub async fn append_replica_batch(&self, batch: Bytes) -> Result<i64, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::AppendReplicaBatch { batch, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Monotonically advance a follower's persisted high watermark.
    pub async fn update_high_watermark(&self, high_watermark: i64) -> Result<(), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::UpdateHighWatermark {
                high_watermark,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Persist the current log end as the first offset for `leader_epoch`.
    pub async fn record_leader_epoch(&self, leader_epoch: i32) -> Result<(), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::RecordLeaderEpoch {
                leader_epoch,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Return the exclusive end offset of `leader_epoch`, if checkpointed.
    pub async fn end_offset_for_leader_epoch(
        &self,
        leader_epoch: i32,
    ) -> Result<Option<i64>, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::EndOffsetForLeaderEpoch {
                leader_epoch,
                reply,
            })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// Snapshot known epoch boundaries, ordered by increasing epoch.
    pub async fn leader_epoch_entries(&self) -> Result<Vec<LeaderEpochEntry>, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::LeaderEpochEntries { reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// Remove the uncommitted suffix at `offset`, rounding down to the
    /// previous whole-batch boundary. Storage refuses targets below HWM.
    pub async fn truncate_to(&self, offset: i64) -> Result<i64, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::TruncateTo { offset, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Discard a wholly obsolete replica and create an empty log beginning
    /// at a leader-retained batch boundary. HWM moves forward to that start
    /// and can never be lowered by this operation.
    pub async fn reset_to_offset(&self, offset: i64) -> Result<(), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::ResetToOffset { offset, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Swap this partition's log configuration in place, so a topic config
    /// change reaches a running partition instead of waiting for a restart.
    pub async fn reconfigure(
        &self,
        config: brahmaputra_storage::LogConfig,
    ) -> Result<(), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Reconfigure { config, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }
    /// (log_start_offset, log_end_offset, high_watermark).
    pub async fn offsets(&self) -> Result<(i64, i64, i64), StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Offsets { reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// What this partition occupies on disk and where its offsets sit.
    pub async fn usage(&self) -> Result<PartitionUsage, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Usage { reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// Who has written to this partition, and what is still open.
    pub async fn producers(&self) -> Result<PartitionProducers, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Producers { reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// Discard every record below `offset`, returning the new log start.
    pub async fn delete_records_before(&self, offset: i64) -> Result<i64, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::DeleteRecordsBefore { offset, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())?
    }

    /// Where a timestamp lookup should start reading. `None` means the log
    /// holds no record at or after `timestamp`.
    pub async fn scan_start_for_timestamp(
        &self,
        timestamp: i64,
    ) -> Result<Option<i64>, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::ScanStartForTimestamp { timestamp, reply })
            .await
            .map_err(|_| actor_gone())?;
        rx.await.map_err(|_| actor_gone())
    }

    /// Watch the high watermark; cluster-mode changes fire only when
    /// replication explicitly advances it.
    pub fn watermark_watch(&self) -> watch::Receiver<i64> {
        self.watermark.clone()
    }

    /// Watch the log end offset, which moves on every append regardless of
    /// commit state. This is what a follower fetch long-polls on.
    pub fn append_watch(&self) -> watch::Receiver<i64> {
        self.appends.clone()
    }
}

fn actor_gone() -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "partition actor is gone",
    ))
}

/// Spawn the actor task for an already-open `log`. Returns the handle and
/// the task's join handle (awaited during graceful shutdown).
pub fn spawn(log: Log, capacity: usize) -> (PartitionHandle, tokio::task::JoinHandle<()>) {
    spawn_mode(log, capacity, None, true, None)
}

/// Spawn a cluster replica actor. Local client appends remain uncommitted
/// until leader-side replication progress explicitly advances HWM.
pub(crate) fn spawn_cluster(
    log: Log,
    capacity: usize,
    dir: PathBuf,
    config: LogConfig,
) -> (PartitionHandle, tokio::task::JoinHandle<()>) {
    spawn_mode(
        log,
        capacity,
        None,
        false,
        Some(ResetContext { dir, config }),
    )
}

/// Spawn an actor and, when configured, periodically apply the log's
/// retention policy on the same single-owner task as reads and appends.
pub(crate) fn spawn_with_retention(
    log: Log,
    capacity: usize,
    retention_check_interval: Option<Duration>,
) -> (PartitionHandle, tokio::task::JoinHandle<()>) {
    spawn_mode(log, capacity, retention_check_interval, true, None)
}

pub(crate) fn spawn_cluster_with_retention(
    log: Log,
    capacity: usize,
    retention_check_interval: Option<Duration>,
    dir: PathBuf,
    config: LogConfig,
) -> (PartitionHandle, tokio::task::JoinHandle<()>) {
    spawn_mode(
        log,
        capacity,
        retention_check_interval,
        false,
        Some(ResetContext { dir, config }),
    )
}

#[derive(Clone)]
struct ResetContext {
    dir: PathBuf,
    config: LogConfig,
}

fn spawn_mode(
    log: Log,
    capacity: usize,
    retention_check_interval: Option<Duration>,
    auto_commit: bool,
    reset_context: Option<ResetContext>,
) -> (PartitionHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(capacity);
    let (wm_tx, wm_rx) = watch::channel(log.high_watermark());
    let (append_tx, append_rx) = watch::channel(log.log_end_offset());
    let retention_tick = retention_check_interval.map(|duration| {
        let duration = duration.max(Duration::from_millis(1));
        let mut interval = tokio::time::interval_at(Instant::now() + duration, duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval
    });
    let disruptions = Arc::new(AtomicU64::new(0));
    let task = tokio::spawn(run(
        log,
        rx,
        wm_tx,
        append_tx,
        retention_tick,
        auto_commit,
        reset_context,
        Arc::clone(&disruptions),
    ));
    (
        PartitionHandle {
            tx,
            watermark: wm_rx,
            appends: append_rx,
            disruptions,
        },
        task,
    )
}

#[allow(clippy::too_many_arguments)]
// Eight parameters because the actor is wired to exactly these signals
// — its log, its mailbox, the two watches it publishes, its timers and the
// disruption counter — and a struct would only move the list.
async fn run(
    log: Log,
    mut rx: mpsc::Receiver<Cmd>,
    watermark: watch::Sender<i64>,
    appends: watch::Sender<i64>,
    mut retention_tick: Option<Interval>,
    auto_commit: bool,
    reset_context: Option<ResetContext>,
    disruptions: Arc<AtomicU64>,
) {
    let mut log = Some(log);
    let mut producer_state = match ProducerStateTable::rebuild(log.as_ref().expect("partition log"))
    {
        Ok(state) => state,
        Err(error) => {
            warn!(%error, "could not reconstruct idempotent producer state");
            ProducerStateTable::default()
        }
    };
    debug!(
        end = log.as_ref().expect("partition log").log_end_offset(),
        "partition actor started"
    );
    // recv() keeps draining buffered commands after all senders drop, then
    // returns None — that is the graceful-shutdown path: every accepted
    // append is fully processed (and acked) before the actor exits.
    loop {
        let cmd = tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { break };
                cmd
            }
            _ = async {
                match retention_tick.as_mut() {
                    Some(interval) => {
                        interval.tick().await;
                    }
                    None => pending::<()>().await,
                }
            } => {
                let current = log.as_mut().expect("partition log");
                // A compacted topic keeps the latest record per key rather
                // than dropping whole aged segments; deleting by age would
                // throw away offsets a group still depends on.
                if current.is_compacted() {
                    match current.compact() {
                        Ok(outcome) if outcome.records_removed > 0 => {
                            debug!(
                                removed = outcome.records_removed,
                                tombstones = outcome.tombstones_removed,
                                bytes_before = outcome.bytes_before,
                                bytes_after = outcome.bytes_after,
                                "compaction removed superseded records"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => warn!(%error, "compaction pass failed"),
                    }
                }
                match current.apply_retention() {
                    Ok(deleted) if deleted > 0 => {
                        debug!(deleted, start = current.log_start_offset(), "retention applied");
                        match ProducerStateTable::rebuild(current) {
                            Ok(state) => producer_state = state,
                            Err(error) => warn!(%error, "producer state rebuild after retention failed"),
                        }
                    }
                    Ok(_) => {}
                    Err(error) => warn!(%error, "retention pass failed"),
                }
                // `flush.interval.ms` must fire on an idle partition too,
                // where no append is coming along to trigger it.
                if current.flush_due() {
                    if let Err(error) = current.flush() {
                        warn!(%error, "time-based flush failed");
                    }
                }
                // A partition that has gone quiet must still seal its
                // segment when `segment.ms` expires, or retention has
                // nothing to delete and the topic never expires anything.
                if current.roll_due() {
                    if let Err(error) = current.roll_now() {
                        warn!(%error, "time-based segment roll failed");
                    }
                }
                // Same reasoning for the watermark checkpoint: a partition
                // that has gone quiet should still persist the watermark it
                // reached rather than wait for the next append.
                if let Err(error) = current.checkpoint_high_watermark_if_due() {
                    warn!(%error, "high-watermark checkpoint failed");
                }
                continue;
            }
        };
        match cmd {
            Cmd::Shutdown { reply } => {
                if let Some(current) = log.as_mut() {
                    if let Err(error) = current.checkpoint_high_watermark() {
                        warn!(%error, "high-watermark checkpoint before partition shutdown failed");
                    }
                }
                drop(log.take());
                let _ = reply.send(());
                break;
            }
            Cmd::Append { batch, reply } => {
                let current = log.as_mut().expect("partition log");
                let result = current.append(batch).and_then(|base| {
                    if auto_commit {
                        // Standalone M1: ISR = {self}, so every complete
                        // local append is immediately committed.
                        let end = current.log_end_offset();
                        current.set_high_watermark(end)?;
                        let _ = watermark.send(end);
                    }
                    Ok(base)
                });
                trace!(?result, "append");
                let _ = reply.send(result);
            }
            Cmd::AppendProducerBatch {
                batch,
                leader_epoch,
                reply,
            } => {
                let current = log.as_mut().expect("partition log");
                let result = current
                    .append_producer_batch(&batch, leader_epoch)
                    .and_then(|offsets| {
                        if auto_commit {
                            let end = current.log_end_offset();
                            current.set_high_watermark(end)?;
                            let _ = watermark.send(end);
                        }
                        Ok(offsets)
                    });
                trace!(?result, "append producer batch");
                let _ = reply.send(result);
            }
            Cmd::AppendIdempotent { mut batch, reply } => {
                let current = log.as_mut().expect("partition log");
                let result = match producer_state.decide(&batch) {
                    Ok(ProducerDecision::Duplicate(outcome)) => Ok(outcome),
                    Ok(ProducerDecision::Append {
                        metadata,
                        record_count,
                        content_crc32c,
                    }) => {
                        // `Log::append` stamps only base_offset; the remaining
                        // content fingerprint therefore stays stable.
                        batch.producer = Some(metadata);
                        match current.append(batch) {
                            Ok(base_offset) => {
                                let next_offset = base_offset + i64::from(record_count);
                                // The append is already durable in the log;
                                // publish its dedup state before any separate
                                // HWM checkpoint can fail.
                                producer_state.record_append(
                                    metadata,
                                    record_count,
                                    content_crc32c,
                                    base_offset,
                                );
                                if auto_commit {
                                    if let Err(error) = current.set_high_watermark(next_offset) {
                                        Err(ProducerAppendError::Storage(error))
                                    } else {
                                        let _ = watermark.send(next_offset);
                                        Ok(ProducerAppendOutcome {
                                            base_offset,
                                            next_offset,
                                            duplicate: false,
                                        })
                                    }
                                } else {
                                    Ok(ProducerAppendOutcome {
                                        base_offset,
                                        next_offset,
                                        duplicate: false,
                                    })
                                }
                            }
                            Err(error) => Err(ProducerAppendError::Storage(error)),
                        }
                    }
                    Err(error) => Err(error),
                };
                trace!(?result, "idempotent append");
                let _ = reply.send(result);
            }
            Cmd::ReadRegions {
                offset,
                max_bytes,
                reply,
            } => {
                let current = log.as_ref().expect("partition log");
                let result = current
                    .read_regions(offset, max_bytes)
                    .map(|regions| RegionOutcome {
                        regions,
                        high_watermark: current.high_watermark(),
                        log_start_offset: current.log_start_offset(),
                        log_end_offset: current.log_end_offset(),
                    });
                let _ = reply.send(result);
            }
            Cmd::ReadCommitted {
                offset,
                max_bytes,
                reply,
            } => {
                let current = log.as_ref().expect("partition log");
                // Bounded by the last stable offset rather than the high
                // watermark, and already stripped of control batches and
                // aborted records — so the take_while below has nothing
                // left to trim.
                let stable = current.last_stable_offset();
                let result = current
                    .read_committed(offset, max_bytes)
                    .map(|batches| ReadOutcome {
                        batches,
                        high_watermark: stable,
                        log_start_offset: current.log_start_offset(),
                        log_end_offset: current.log_end_offset(),
                    });
                let _ = reply.send(result);
            }
            Cmd::Read {
                offset,
                max_bytes,
                reply,
            } => {
                let current = log.as_ref().expect("partition log");
                let result = current.read(offset, max_bytes).map(|batches| {
                    let hw = current.high_watermark();
                    // Never serve data at or beyond the high watermark:
                    // drop any batch whose last record is not yet covered.
                    let batches = batches
                        .into_iter()
                        .take_while(|b| {
                            validate_batch_header(b)
                                .map(|h| h.base_offset + (h.last_offset_delta as i64) < hw)
                                .unwrap_or(false)
                        })
                        .collect();
                    ReadOutcome {
                        batches,
                        high_watermark: hw,
                        log_start_offset: current.log_start_offset(),
                        log_end_offset: current.log_end_offset(),
                    }
                });
                let _ = reply.send(result);
            }
            Cmd::ReadUncommitted {
                offset,
                max_bytes,
                reply,
            } => {
                let current = log.as_ref().expect("partition log");
                let result = current.read(offset, max_bytes).map(|batches| ReadOutcome {
                    batches,
                    high_watermark: current.high_watermark(),
                    log_start_offset: current.log_start_offset(),
                    log_end_offset: current.log_end_offset(),
                });
                let _ = reply.send(result);
            }
            Cmd::AppendReplicaBatch { batch, reply } => {
                let current = log.as_mut().expect("partition log");
                let result = (|| -> Result<i64, StorageError> {
                    let header = validate_batch_header(&batch)?;
                    let latest_epoch = current
                        .leader_epoch_entries()
                        .last()
                        .map(|entry| entry.epoch);
                    if latest_epoch != Some(header.leader_epoch) {
                        current.record_leader_epoch(header.leader_epoch)?;
                    }
                    let base = current.append_replica_batch(batch)?;
                    producer_state.observe_replicated(&header);
                    Ok(base)
                })();
                if result.is_ok() {
                    disruptions.fetch_add(1, Ordering::AcqRel);
                }
                trace!(?result, "replica append");
                let _ = reply.send(result);
            }
            Cmd::UpdateHighWatermark {
                high_watermark,
                reply,
            } => {
                let current = log.as_mut().expect("partition log");
                let old = current.high_watermark();
                let result = current.set_high_watermark(high_watermark);
                if result.is_ok() && high_watermark > old {
                    let _ = watermark.send(high_watermark);
                }
                let _ = reply.send(result);
            }
            Cmd::RecordLeaderEpoch {
                leader_epoch,
                reply,
            } => {
                let current = log.as_mut().expect("partition log");
                // Metadata refreshes and every internal fetch may repeat the
                // current epoch after more data has arrived. The original
                // start offset is authoritative, so repeated observations
                // of that epoch are idempotent rather than attempts to move
                // its checkpoint forward.
                let already_current = current
                    .leader_epoch_entries()
                    .last()
                    .is_some_and(|entry| entry.epoch == leader_epoch);
                let result = if already_current {
                    Ok(())
                } else {
                    current.record_leader_epoch(leader_epoch)
                };
                let _ = reply.send(result);
            }
            Cmd::EndOffsetForLeaderEpoch {
                leader_epoch,
                reply,
            } => {
                let current = log.as_ref().expect("partition log");
                let _ = reply.send(current.end_offset_for_leader_epoch(leader_epoch));
            }
            Cmd::LeaderEpochEntries { reply } => {
                let current = log.as_ref().expect("partition log");
                let _ = reply.send(current.leader_epoch_entries().to_vec());
            }
            Cmd::TruncateTo { offset, reply } => {
                let current = log.as_mut().expect("partition log");
                let result = current.truncate_to(offset).and_then(|truncated| {
                    producer_state = ProducerStateTable::rebuild(current)?;
                    Ok(truncated)
                });
                disruptions.fetch_add(1, Ordering::AcqRel);
                trace!(?result, "replica truncate");
                let _ = reply.send(result);
            }
            Cmd::ResetToOffset { offset, reply } => {
                let result = match reset_context.as_ref() {
                    Some(context) => {
                        let current = log.as_ref().expect("partition log");
                        if offset < current.high_watermark() || offset < current.log_end_offset() {
                            Err(StorageError::InvalidHighWatermark {
                                requested: offset,
                                current: current.high_watermark(),
                                log_end: current.log_end_offset(),
                            })
                        } else if offset == current.log_end_offset() {
                            let current = log.as_mut().expect("partition log");
                            let old = current.high_watermark();
                            let result = current_reset_hwm(current, offset);
                            if result.is_ok() && offset > old {
                                let _ = watermark.send(offset);
                            }
                            result
                        } else {
                            let old = log.take().expect("partition log");
                            drop(old);
                            match reset_partition_log(context, offset) {
                                Ok(reopened) => {
                                    disruptions.fetch_add(1, Ordering::AcqRel);
                                    log = Some(reopened);
                                    producer_state = ProducerStateTable::default();
                                    let _ = watermark.send(offset);
                                    Ok(())
                                }
                                Err(error) => {
                                    // Best effort reopen keeps the actor usable
                                    // if reset failed before replacing all files.
                                    log = Log::open(&context.dir, context.config.clone()).ok();
                                    Err(error)
                                }
                            }
                        }
                    }
                    None => Err(StorageError::InvalidConfig(
                        "replica reset is unavailable for this actor",
                    )),
                };
                trace!(?result, offset, "replica log reset");
                let _ = reply.send(result);
            }
            Cmd::Reconfigure { config, reply } => {
                if let Some(current) = log.as_mut() {
                    current.set_config(config);
                }
                let _ = reply.send(());
            }
            Cmd::Offsets { reply } => {
                let current = log.as_ref().expect("partition log");
                let _ = reply.send((
                    current.log_start_offset(),
                    current.log_end_offset(),
                    current.high_watermark(),
                ));
            }
            Cmd::ScanStartForTimestamp { timestamp, reply } => {
                let current = log.as_ref().expect("partition log");
                let _ = reply.send(current.scan_start_for_timestamp(timestamp));
            }
            Cmd::Usage { reply } => {
                let current = log.as_ref().expect("partition log");
                let _ = reply.send(PartitionUsage {
                    size_bytes: current.size_bytes(),
                    log_start_offset: current.log_start_offset(),
                    log_end_offset: current.log_end_offset(),
                    high_watermark: current.high_watermark(),
                    segments: current.segment_count(),
                });
            }
            Cmd::Producers { reply } => {
                let current = log.as_ref().expect("partition log");
                let open: std::collections::HashMap<i64, i64> =
                    current.open_transactions().into_iter().collect();
                let mut producers: Vec<ProducerSnapshot> = producer_state
                    .producers
                    .iter()
                    .map(|(producer_id, state)| ProducerSnapshot {
                        producer_id: *producer_id,
                        producer_epoch: state.epoch,
                        last_sequence: state.next_sequence.saturating_sub(1),
                        current_txn_start_offset: open.get(producer_id).copied().unwrap_or(-1),
                    })
                    .collect();
                // A producer with an open transaction but no remembered
                // batch — one whose state was rebuilt after retention —
                // still has to appear: it is the one holding the LSO.
                for (producer_id, first_offset) in open {
                    if !producers
                        .iter()
                        .any(|snapshot| snapshot.producer_id == producer_id)
                    {
                        producers.push(ProducerSnapshot {
                            producer_id,
                            producer_epoch: -1,
                            last_sequence: -1,
                            current_txn_start_offset: first_offset,
                        });
                    }
                }
                producers.sort_by_key(|snapshot| snapshot.producer_id);
                let _ = reply.send(PartitionProducers {
                    producers,
                    last_stable_offset: current.last_stable_offset(),
                    high_watermark: current.high_watermark(),
                });
            }
            Cmd::DeleteRecordsBefore { offset, reply } => {
                let current = log.as_mut().expect("partition log");
                let _ = reply.send(current.delete_records_before(offset));
            }
        }
        // Publish the log end offset whenever it moves, from one place
        // rather than from each append arm: a follower long-polling this
        // leader is woken by it, and a notification that a future append
        // command forgot to send would look exactly like a stalled
        // replica. Truncation moves the end backwards and is published
        // too — a waiter that re-reads after one is still correct.
        if let Some(current) = log.as_ref() {
            let end = current.log_end_offset();
            if end != *appends.borrow() {
                let _ = appends.send(end);
            }
        }
        if log.is_none() {
            warn!("partition actor stopped after an unrecoverable reset failure");
            break;
        }
    }
    // A clean stop persists the watermark it reached. Only an unclean stop
    // should ever have to recover from a checkpoint that lags the log.
    if let Some(current) = log.as_mut() {
        if let Err(error) = current.checkpoint_high_watermark() {
            warn!(%error, "high-watermark checkpoint on shutdown failed");
        }
    }
    debug!(
        end = log
            .as_ref()
            .map_or(-1, brahmaputra_storage::Log::log_end_offset),
        "partition actor stopped"
    );
}

fn current_reset_hwm(log: &mut Log, offset: i64) -> Result<(), StorageError> {
    if offset > log.high_watermark() {
        log.set_high_watermark(offset)?;
    }
    Ok(())
}

fn reset_partition_log(context: &ResetContext, offset: i64) -> Result<Log, StorageError> {
    if offset < 0 {
        return Err(StorageError::OffsetOutOfRange {
            offset,
            start: 0,
            end: 0,
        });
    }
    fs::write(context.dir.join(RESET_MARKER), offset.to_be_bytes())?;
    complete_pending_replica_reset(&context.dir)?;
    Log::open(&context.dir, context.config.clone())
}

/// Finish an actor reset interrupted by a process crash. The marker is
/// written before any old segment is removed and deleted only after the
/// replacement HWM and base segment exist, making the operation restartable.
pub(crate) fn complete_pending_replica_reset(dir: &Path) -> Result<(), StorageError> {
    let marker = dir.join(RESET_MARKER);
    let bytes = match fs::read(&marker) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if bytes.len() != 8 {
        return Err(StorageError::InvalidConfig(
            "replica reset marker must contain one i64 offset",
        ));
    }
    let offset = i64::from_be_bytes(bytes.try_into().expect("eight bytes"));
    if offset < 0 {
        return Err(StorageError::OffsetOutOfRange {
            offset,
            start: 0,
            end: 0,
        });
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let log_artifact = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| matches!(extension, "log" | "index" | "timeindex"));
        let checkpoint = matches!(name.as_ref(), "hwm" | "leader-epochs" | "leader-epochs.tmp");
        if log_artifact || checkpoint {
            fs::remove_file(path)?;
        }
    }
    fs::write(dir.join("hwm"), offset.to_be_bytes())?;
    fs::File::create(dir.join(format!("{offset:020}.log")))?;
    fs::remove_file(marker)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brahmaputra_protocol::Record;
    use brahmaputra_storage::LogConfig;

    fn batch(first_value: usize, n: usize) -> RecordBatch {
        let records = (0..n)
            .map(|i| Record::new(format!("v{}", first_value + i).into_bytes()))
            .collect();
        RecordBatch::new(0, 0, 1_000, records)
    }

    fn producer_batch(epoch: i16, sequence: i32, values: &[&str]) -> RecordBatch {
        RecordBatch::new(
            0,
            0,
            1_000,
            values
                .iter()
                .map(|value| Record::new(value.as_bytes().to_vec()))
                .collect(),
        )
        .with_producer(44, epoch, sequence)
    }

    #[tokio::test]
    async fn idempotent_append_deduplicates_and_fences_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);

        let first = producer_batch(0, 0, &["a", "b"]);
        let outcome = handle.append_idempotent(first.clone()).await.unwrap();
        assert_eq!((outcome.base_offset, outcome.next_offset), (0, 2));
        assert!(!outcome.duplicate);
        let duplicate = handle.append_idempotent(first).await.unwrap();
        assert_eq!((duplicate.base_offset, duplicate.next_offset), (0, 2));
        assert!(duplicate.duplicate);
        assert_eq!(handle.offsets().await.unwrap(), (0, 2, 2));

        assert!(matches!(
            handle
                .append_idempotent(producer_batch(0, 0, &["different", "payload"]))
                .await,
            Err(ProducerAppendError::OutOfOrderSequence {
                requested: 0,
                expected: 2
            })
        ));
        assert!(matches!(
            handle
                .append_idempotent(producer_batch(0, 3, &["gap"]))
                .await,
            Err(ProducerAppendError::OutOfOrderSequence {
                requested: 3,
                expected: 2
            })
        ));
        assert_eq!(
            handle
                .append_idempotent(producer_batch(0, 2, &["c"]))
                .await
                .unwrap()
                .base_offset,
            2
        );
        assert_eq!(
            handle
                .append_idempotent(producer_batch(1, 0, &["new epoch"]))
                .await
                .unwrap()
                .base_offset,
            3
        );
        assert!(matches!(
            handle
                .append_idempotent(producer_batch(0, 3, &["stale epoch"]))
                .await,
            Err(ProducerAppendError::FencedEpoch {
                requested: 0,
                current: 1
            })
        ));

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn restart_reconstructs_original_duplicate_offset() {
        let dir = tempfile::tempdir().unwrap();
        let original = producer_batch(0, 0, &["once"]);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);
        assert_eq!(
            handle
                .append_idempotent(original.clone())
                .await
                .unwrap()
                .base_offset,
            0
        );
        drop(handle);
        task.await.unwrap();

        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);
        let duplicate = handle.append_idempotent(original).await.unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.base_offset, 0);
        assert_eq!(handle.offsets().await.unwrap(), (0, 1, 1));
        assert_eq!(
            handle
                .append_idempotent(producer_batch(0, 1, &["after restart"]))
                .await
                .unwrap()
                .base_offset,
            1
        );
        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn replicated_magic_v2_state_is_ready_for_leader_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig::default();
        let log = Log::open(dir.path(), config.clone()).unwrap();
        let (handle, task) = spawn_cluster(log, 16, dir.path().to_owned(), config);
        let mut batch = producer_batch(0, 0, &["replicated"]);
        batch.leader_epoch = 7;
        let raw = batch.encode();
        assert_eq!(handle.append_replica_batch(raw.clone()).await.unwrap(), 0);
        assert_eq!(
            handle
                .read_uncommitted(0, usize::MAX)
                .await
                .unwrap()
                .batches,
            vec![raw]
        );

        // The same actor can become leader through metadata without a
        // restart; its follower-side raw append already materialized state.
        let duplicate = handle.append_idempotent(batch).await.unwrap();
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.base_offset, 0);
        assert_eq!(handle.offsets().await.unwrap(), (0, 1, 0));

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn append_advances_watermark_and_read_caps_at_it() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);

        assert_eq!(handle.append(batch(0, 3)).await.unwrap(), 0);
        let (_, end, hw) = handle.offsets().await.unwrap();
        assert_eq!((end, hw), (3, 3), "M1: HW == LEO after append");

        let outcome = handle.read(0, usize::MAX).await.unwrap();
        assert_eq!(outcome.batches.len(), 1);
        assert_eq!(outcome.high_watermark, 3);

        assert_eq!(handle.append(batch(3, 2)).await.unwrap(), 3);
        let mut watch = handle.watermark_watch();
        assert_eq!(*watch.borrow_and_update(), 5);
        assert_eq!(handle.append(batch(5, 1)).await.unwrap(), 5);
        watch.changed().await.unwrap();
        assert_eq!(*watch.borrow(), 6);

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn actor_exit_is_clean_when_senders_drop() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);
        handle.append(batch(0, 1)).await.unwrap();
        drop(handle);
        task.await.unwrap();
        // Data survived the actor lifecycle.
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_end_offset(), 1);
        assert_eq!(log.high_watermark(), 1);
    }

    #[tokio::test]
    async fn replica_operations_preserve_bytes_and_commit_only_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn(log, 16);

        handle.record_leader_epoch(4).await.unwrap();
        let mut first_batch = batch(0, 2);
        first_batch.leader_epoch = 4;
        let first = first_batch.encode();
        let mut second_batch = batch(2, 3);
        second_batch.base_offset = 2;
        second_batch.leader_epoch = 4;
        let second = second_batch.encode();
        assert_eq!(handle.append_replica_batch(first.clone()).await.unwrap(), 0);
        assert_eq!(
            handle.append_replica_batch(second.clone()).await.unwrap(),
            2
        );

        let raw = handle.read_uncommitted(0, usize::MAX).await.unwrap();
        assert_eq!(raw.batches, vec![first.clone(), second.clone()]);
        assert_eq!((raw.high_watermark, raw.log_end_offset), (0, 5));
        assert!(handle.read(0, usize::MAX).await.unwrap().batches.is_empty());

        handle.update_high_watermark(2).await.unwrap();
        assert_eq!(
            handle.read(0, usize::MAX).await.unwrap().batches,
            vec![first]
        );
        assert!(handle.update_high_watermark(1).await.is_err());

        handle.record_leader_epoch(5).await.unwrap();
        assert_eq!(
            handle.end_offset_for_leader_epoch(4).await.unwrap(),
            Some(5)
        );
        assert_eq!(
            handle.end_offset_for_leader_epoch(5).await.unwrap(),
            Some(5)
        );

        // Offset 4 lies inside the second batch, so truncation lands at its
        // base offset while retaining all committed data below HWM.
        assert_eq!(handle.truncate_to(4).await.unwrap(), 2);
        assert_eq!(handle.offsets().await.unwrap(), (0, 2, 2));
        assert_eq!(
            handle
                .read_uncommitted(0, usize::MAX)
                .await
                .unwrap()
                .batches
                .len(),
            1
        );

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn replica_append_checkpoints_each_batch_epoch_at_its_exact_base() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = spawn_cluster(log, 16, dir.path().to_owned(), LogConfig::default());

        let first = RecordBatch::new(0, 8, 1_000, vec![Record::new("old")]).encode();
        let second = RecordBatch::new(1, 9, 2_000, vec![Record::new("new")]).encode();
        handle.append_replica_batch(first).await.unwrap();
        handle.append_replica_batch(second).await.unwrap();
        let entries = handle.leader_epoch_entries().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!((entries[0].epoch, entries[0].start_offset), (8, 0));
        assert_eq!((entries[1].epoch, entries[1].start_offset), (9, 1));
        assert_eq!(handle.offsets().await.unwrap(), (0, 2, 0));

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn replica_reset_rebases_empty_log_without_offset_fillers() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig::default();
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        log.append_replica_batch(
            RecordBatch::new(0, 1, 1_000, vec![Record::new("obsolete")]).encode(),
        )
        .unwrap();
        log.set_high_watermark(1).unwrap();
        let (handle, task) = spawn_cluster(log, 16, dir.path().to_owned(), config);

        handle.reset_to_offset(5).await.unwrap();
        assert_eq!(handle.offsets().await.unwrap(), (5, 5, 5));
        let retained = RecordBatch::new(5, 2, 2_000, vec![Record::new("retained")]).encode();
        handle.append_replica_batch(retained.clone()).await.unwrap();
        assert_eq!(handle.offsets().await.unwrap(), (5, 6, 5));
        assert_eq!(
            handle
                .read_uncommitted(5, usize::MAX)
                .await
                .unwrap()
                .batches,
            vec![retained]
        );

        drop(handle);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn periodic_retention_deletes_expired_sealed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(
            dir.path(),
            LogConfig {
                segment_bytes: 1,
                retention_ms: Some(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        let (handle, task) = spawn_with_retention(log, 16, Some(Duration::from_millis(10)));

        for i in 0..4 {
            handle.append(batch(i, 1)).await.unwrap();
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (start, end, _) = handle.offsets().await.unwrap();
                if start > 0 {
                    assert_eq!(end, 4);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("periodic retention did not run");
        assert!(matches!(
            handle.read(0, usize::MAX).await,
            Err(StorageError::OffsetOutOfRange { .. })
        ));

        drop(handle);
        task.await.unwrap();
    }
}
