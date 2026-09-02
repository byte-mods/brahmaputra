//! The transaction coordinator: the thing that decides, once, whether a
//! producer's writes across many partitions all count.
//!
//! # Why a coordinator exists at all
//!
//! Records are appended as they are produced, not held back — holding them
//! would mean buffering a transaction's whole output somewhere, and that
//! somewhere would be the durability problem the log was supposed to solve.
//! What makes the writes atomic is *marking* them afterwards: every
//! partition the transaction touched receives a control batch saying
//! committed or aborted, and a `read_committed` consumer refuses to look
//! past the first record of any transaction that has not been marked yet.
//!
//! That only works if something outlives the producer. A producer that
//! crashes between its last write and its commit has left records in doubt
//! on several partitions; if nobody but that producer could resolve them,
//! they would stay in doubt forever and block every committed reader behind
//! them. So the coordinator, not the client, owns the decision and the
//! markers, and it keeps enough durable state to finish a transaction whose
//! producer never came back.
//!
//! # Where the state lives
//!
//! In `__transaction_state`, a compacted internal topic, keyed by
//! `transactional.id` so the log compacts to one record per producer. The
//! coordinator for an id is the leader of
//! `hash(transactional_id) % partitions`, exactly as a consumer group's
//! coordinator is chosen out of `__consumer_offsets` — which means a client
//! finds it by routing to that partition and there is no separate discovery
//! API whose answer could disagree with metadata.
//!
//! # The sequence, and why it is in that order
//!
//! ```text
//! InitProducerId(transactional.id) -> fence any older instance, get an epoch
//! AddPartitionsToTxn(p1, p2)       -> BEFORE writing to p1 or p2
//! (produce transactional batches)
//! AddOffsetsToTxn(group)           -> optional: read-process-write
//! TxnOffsetCommit(offsets)         -> to the *group* coordinator
//! EndTxn(commit)                   -> PrepareCommit, markers, CompleteCommit
//! ```
//!
//! `AddPartitionsToTxn` comes first because a partition the coordinator
//! never heard of gets no marker, and its records stay in doubt forever.
//! `EndTxn` writes `PrepareCommit` to the state log *before* sending any
//! marker, so a coordinator that dies mid-commit is recovered by replaying
//! its log and finishing the markers — never by guessing which way a
//! half-marked transaction went.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    TransactionStateRecord, TxnMarker, TxnPartition, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
};
use brahmaputra_protocol::{ApiKey, Record, RecordBatch};
use dashmap::DashMap;
use std::sync::Mutex;
use tracing::{debug, warn};

use crate::error::BrokerError;
use crate::group::now_ms;
use crate::server::Broker;

/// Internal topic holding coordinator state, one record per
/// `transactional.id`.
pub(crate) const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";

/// Record-value kind tag, matching the convention `__consumer_offsets` uses.
const KIND_TRANSACTION_STATE: u8 = 1;

/// Bytes read per replay step when loading a shard.
const REPLAY_MAX_BYTES: usize = 1024 * 1024;

/// How long a marker write may take before the coordinator gives up on that
/// broker and leaves the transaction to be retried.
const MARKER_TIMEOUT: Duration = Duration::from_secs(10);

/// Which `__transaction_state` partition coordinates `transactional_id`.
///
/// The same hash the group coordinator uses, for the same reason: a client
/// computes it locally and routes to that partition's leader, so
/// coordinator discovery is metadata the client already has rather than an
/// API whose answer could disagree with it.
pub(crate) fn coordinator_partition(transactional_id: &str, partition_count: i32) -> i32 {
    (crc32c::crc32c(transactional_id.as_bytes()) % partition_count.max(1) as u32) as i32
}

/// Where a transaction is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionState {
    /// A producer identity exists but no transaction is in flight.
    Empty,
    /// Partitions are being written to.
    Ongoing,
    /// The decision is made and durable; markers may not all be written.
    PrepareCommit,
    PrepareAbort,
    /// Markers are written; the transaction is over.
    CompleteCommit,
    CompleteAbort,
}

impl TransactionState {
    fn as_str(self) -> &'static str {
        match self {
            TransactionState::Empty => "Empty",
            TransactionState::Ongoing => "Ongoing",
            TransactionState::PrepareCommit => "PrepareCommit",
            TransactionState::PrepareAbort => "PrepareAbort",
            TransactionState::CompleteCommit => "CompleteCommit",
            TransactionState::CompleteAbort => "CompleteAbort",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "Ongoing" => TransactionState::Ongoing,
            "PrepareCommit" => TransactionState::PrepareCommit,
            "PrepareAbort" => TransactionState::PrepareAbort,
            "CompleteCommit" => TransactionState::CompleteCommit,
            "CompleteAbort" => TransactionState::CompleteAbort,
            // Anything else — including a state a newer version wrote —
            // reads as Empty, which refuses requests rather than acting on
            // a transaction whose shape is not understood.
            _ => TransactionState::Empty,
        }
    }

    /// Whether a decision has been made but not yet finished.
    fn is_prepared(self) -> bool {
        matches!(
            self,
            TransactionState::PrepareCommit | TransactionState::PrepareAbort
        )
    }

    fn committed(self) -> bool {
        matches!(
            self,
            TransactionState::PrepareCommit | TransactionState::CompleteCommit
        )
    }
}

/// One producer's transaction, as the coordinator knows it.
#[derive(Debug, Clone)]
pub(crate) struct TransactionMetadata {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TransactionState,
    /// Partitions written to under the current transaction, sorted so the
    /// persisted record is stable and a replay produces the same set.
    pub partitions: BTreeSet<(String, i32)>,
    pub timeout_ms: i32,
    pub last_update_ms: i64,
}

impl TransactionMetadata {
    fn to_record(&self) -> TransactionStateRecord {
        TransactionStateRecord {
            transactional_id: self.transactional_id.clone(),
            producer_id: self.producer_id,
            producer_epoch: i32::from(self.producer_epoch),
            state: self.state.as_str().to_owned(),
            timeout_ms: self.timeout_ms,
            last_update_ms: self.last_update_ms,
            partitions: self
                .partitions
                .iter()
                .map(|(topic, partition)| TxnPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect(),
        }
    }

    fn from_record(record: TransactionStateRecord) -> Self {
        TransactionMetadata {
            transactional_id: record.transactional_id,
            producer_id: record.producer_id,
            // Narrowed from the wire's i32: BitPacker has no 16-bit type,
            // but the record batch header the epoch ends up in does.
            producer_epoch: record.producer_epoch as i16,
            state: TransactionState::parse(&record.state),
            partitions: record
                .partitions
                .into_iter()
                .map(|partition| (partition.topic, partition.partition))
                .collect(),
            timeout_ms: record.timeout_ms,
            last_update_ms: record.last_update_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadState {
    Unloaded,
    Loading,
    Loaded,
}

/// The coordinator state owned by one `__transaction_state` partition.
pub(crate) struct TransactionShard {
    partition: i32,
    handle: crate::actor::PartitionHandle,
    /// See [`crate::group::CoordinatorShard`]: refreshed on lookup.
    leader_epoch: AtomicI32,
    /// The handle's disruption count at creation; a change means the log
    /// moved without this shard, and the shard is rebuilt.
    loaded_at_disruption: u64,
    transactions: DashMap<String, TransactionMetadata>,
    load: Mutex<LoadState>,
}

impl TransactionShard {
    fn new(partition: i32, handle: crate::actor::PartitionHandle, leader_epoch: i32) -> Self {
        TransactionShard {
            partition,
            loaded_at_disruption: handle.disruptions(),
            handle,
            leader_epoch: AtomicI32::new(leader_epoch),
            transactions: DashMap::new(),
            load: Mutex::new(LoadState::Unloaded),
        }
    }

    /// Replay this partition's log before serving. A request arriving mid
    /// replay is told to retry rather than answered from half the state.
    async fn ensure_loaded(&self) -> Result<(), BrokerError> {
        {
            let mut load = self.load.lock().expect("transaction load state");
            match *load {
                LoadState::Loaded => return Ok(()),
                LoadState::Loading => {
                    return Err(BrokerError::CoordinatorLoadInProgress {
                        partition: self.partition,
                    })
                }
                LoadState::Unloaded => *load = LoadState::Loading,
            }
        }
        let result = self.replay().await;
        let mut load = self.load.lock().expect("transaction load state");
        *load = if result.is_ok() {
            LoadState::Loaded
        } else {
            LoadState::Unloaded
        };
        result
    }

    async fn replay(&self) -> Result<(), BrokerError> {
        let (log_start, _, high_watermark) = self.handle.offsets().await?;
        let mut position = log_start;
        while position < high_watermark {
            let outcome = self.handle.read(position, REPLAY_MAX_BYTES).await?;
            if outcome.batches.is_empty() {
                break;
            }
            let mut advanced = false;
            for raw in &outcome.batches {
                let mut buffer = raw.clone();
                let batch = RecordBatch::decode(&mut buffer)?;
                for record in &batch.records {
                    self.apply_record(record);
                }
                position = position.max(batch.next_offset());
                advanced = true;
            }
            if !advanced {
                break;
            }
        }
        debug!(
            partition = self.partition,
            transactions = self.transactions.len(),
            "transaction shard replayed"
        );
        Ok(())
    }

    fn apply_record(&self, record: &Record) {
        let Some(value) = record.value.as_ref() else {
            // A null value under a `transactional.id` retires it: the
            // producer has been idle past `transactional.id.expiration.ms`
            // and its state is gone, here and, once compaction runs, on
            // disk too.
            if let Some(key) = record.key.as_ref() {
                if let Ok(id) = std::str::from_utf8(key) {
                    self.transactions.remove(id);
                }
            }
            return;
        };
        let Some((&kind, payload)) = value.split_first() else {
            return;
        };
        if kind != KIND_TRANSACTION_STATE {
            warn!(kind, "skipping unknown transaction coordinator record");
            return;
        }
        match TransactionStateRecord::decode(payload) {
            Ok(decoded) => {
                let metadata = TransactionMetadata::from_record(decoded);
                self.transactions
                    .insert(metadata.transactional_id.clone(), metadata);
            }
            Err(error) => warn!(%error, "skipping undecodable transaction state record"),
        }
    }

    /// Persist one transaction's state, keyed by its id so the topic
    /// compacts to the latest state per producer.
    async fn persist(&self, metadata: &TransactionMetadata) -> Result<i64, BrokerError> {
        let mut value = Vec::with_capacity(64);
        value.push(KIND_TRANSACTION_STATE);
        value.extend_from_slice(
            &metadata
                .to_record()
                .encode()
                .map_err(|error| BrokerError::Meta(error.to_string()))?,
        );
        let batch = RecordBatch::new(
            0,
            self.leader_epoch.load(Ordering::Acquire),
            now_ms(),
            vec![Record::with_key(
                metadata.transactional_id.clone().into_bytes(),
                value,
                0,
            )],
        );
        Ok(self.handle.append(batch).await?)
    }

    /// Append a null value under `transactional_id`, deleting it.
    async fn persist_tombstone(&self, transactional_id: &str) -> Result<i64, BrokerError> {
        let batch = RecordBatch::new(
            0,
            self.leader_epoch.load(Ordering::Acquire),
            now_ms(),
            vec![Record::tombstone(
                transactional_id.to_owned().into_bytes(),
                0,
            )],
        );
        Ok(self.handle.append(batch).await?)
    }

    fn get(&self, transactional_id: &str) -> Option<TransactionMetadata> {
        self.transactions
            .get(transactional_id)
            .map(|entry| entry.clone())
    }

    fn put(&self, metadata: TransactionMetadata) {
        self.transactions
            .insert(metadata.transactional_id.clone(), metadata);
    }
}

/// Broker-side transaction coordinator: one lazily loaded shard per local
/// `__transaction_state` partition.
#[derive(Default)]
pub(crate) struct TransactionCoordinator {
    shards: DashMap<i32, Arc<TransactionShard>>,
}

impl TransactionCoordinator {
    fn shard(
        &self,
        partition: i32,
        handle: crate::actor::PartitionHandle,
        leader_epoch: i32,
    ) -> Arc<TransactionShard> {
        let mut entry = self.shards.entry(partition);
        if let dashmap::mapref::entry::Entry::Occupied(ref mut occupied) = entry {
            let existing = occupied.get();
            if existing.handle.same_actor(&handle)
                && existing.loaded_at_disruption == handle.disruptions()
            {
                existing.leader_epoch.store(leader_epoch, Ordering::Release);
                return Arc::clone(existing);
            }
            debug!(
                partition,
                "transaction shard is stale after a leadership change; rebuilding"
            );
        }
        let fresh = Arc::new(TransactionShard::new(partition, handle, leader_epoch));
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                occupied.insert(Arc::clone(&fresh));
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(Arc::clone(&fresh));
            }
        }
        fresh
    }
}

/// Resolve and load the shard coordinating `transactional_id`.
pub(crate) async fn shard_for(
    broker: &Broker,
    transactional_id: &str,
) -> Result<Arc<TransactionShard>, BrokerError> {
    let (partition, leader_epoch, handle) = if let Some(cache) = broker.metadata_cache() {
        let image = cache.snapshot();
        broker.validate_local_broker_epoch(&image)?;
        let not_coordinator = || BrokerError::NotCoordinator {
            group_id: transactional_id.to_owned(),
        };
        let Some(topic) = image.topics.get(TRANSACTION_STATE_TOPIC) else {
            return Err(not_coordinator());
        };
        let partition = coordinator_partition(transactional_id, topic.partitions.len() as i32);
        let Some(assignment) = topic.partitions.get(&partition) else {
            return Err(not_coordinator());
        };
        if assignment.leader != broker.config().broker_id {
            return Err(not_coordinator());
        }
        let handle = broker
            .partition(TRANSACTION_STATE_TOPIC, partition)
            .map_err(|error| match error {
                BrokerError::NotLeaderOrFollower { .. } => not_coordinator(),
                other => other,
            })?;
        (partition, assignment.leader_epoch, handle)
    } else {
        // Standalone: one broker coordinates everything, and the topic is
        // created on first use exactly as `__consumer_offsets` is.
        let partition_count = broker.state().ensure_topic(TRANSACTION_STATE_TOPIC)?;
        let partition = coordinator_partition(transactional_id, partition_count);
        let handle = broker.partition_auto_create(TRANSACTION_STATE_TOPIC, partition)?;
        (partition, 0, handle)
    };
    let shard = broker.transactions().shard(partition, handle, leader_epoch);
    shard.ensure_loaded().await?;
    Ok(shard)
}

/// Begin a transactional producer session, fencing any older one.
///
/// A `transactional.id` names a *role*, not a process: when a replacement
/// instance starts, the previous one must be prevented from finishing
/// whatever it was doing, or two producers would be writing the same
/// transaction. Bumping the epoch is what does that — every later request
/// carries an epoch, and one behind the coordinator's is refused.
///
/// An in-flight transaction belonging to the fenced producer is aborted on
/// the spot. Leaving it open would block committed readers on every
/// partition it touched, waiting on a producer that is never coming back.
pub(crate) async fn init_transactional_producer(
    broker: &Broker,
    transactional_id: &str,
    timeout_ms: i32,
) -> Result<(i64, i16), BrokerError> {
    let shard = shard_for(broker, transactional_id).await?;

    let existing = shard.get(transactional_id);
    let mut metadata = match existing {
        Some(mut metadata) => {
            // Fence the previous instance before doing anything else.
            metadata.producer_epoch = metadata.producer_epoch.wrapping_add(1);
            metadata
        }
        None => {
            let (producer_id, producer_epoch) = broker
                .producer_ids()
                .allocate()
                .map_err(|error| BrokerError::Meta(error.to_string()))?;
            TransactionMetadata {
                transactional_id: transactional_id.to_owned(),
                producer_id,
                producer_epoch,
                state: TransactionState::Empty,
                partitions: BTreeSet::new(),
                timeout_ms,
                last_update_ms: now_ms(),
            }
        }
    };

    // Whatever the fenced producer left behind has to be resolved now, or
    // its records block every committed reader on those partitions forever.
    //
    // Which way it resolves is not a guess. A transaction still `Ongoing`
    // never reached a decision, so it aborts. One already `Prepare*` did:
    // that record is durable, and finishing it is the whole reason it is
    // written before any marker is sent.
    let abandoned =
        matches!(metadata.state, TransactionState::Ongoing) || metadata.state.is_prepared();
    if abandoned && !metadata.partitions.is_empty() {
        let committed = metadata.state.committed();
        let partitions = metadata.partitions.clone();
        let fenced_epoch = metadata.producer_epoch.wrapping_sub(1);
        debug!(
            transactional_id,
            committed,
            partitions = partitions.len(),
            "resolving a transaction abandoned by a fenced producer"
        );
        write_markers(
            broker,
            metadata.producer_id,
            fenced_epoch,
            committed,
            &partitions,
        )
        .await;
    }

    metadata.state = TransactionState::Empty;
    metadata.partitions.clear();
    metadata.timeout_ms = timeout_ms;
    metadata.last_update_ms = now_ms();
    shard.persist(&metadata).await?;
    let identity = (metadata.producer_id, metadata.producer_epoch);
    shard.put(metadata);
    Ok(identity)
}

/// How often the coordinator looks for transactions that have outlived
/// their timeout.
///
/// Kafka checks every 10 s. This is faster because the cost is walking an
/// in-memory map of the ids this broker coordinates, and because the thing
/// being waited on — an abandoned transaction — is holding every
/// `read_committed` consumer of its partitions still while it waits.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How long a transaction may sit in `Prepare*` before its markers are
/// re-sent.
///
/// A marker write that failed leaves the decision durable and the
/// partitions unmarked, which is safe but not finished: a committed reader
/// stays held at the LSO until someone completes it. Before this existed
/// the only thing that would was the next `InitProducerId` for that id,
/// which for a producer that never came back is never.
const MARKER_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Smallest `transaction.timeout.ms` a producer may ask for.
///
/// Below this the coordinator would abort transactions out from under a
/// producer that is merely slow, which turns a latency spike into a
/// correctness-looking failure.
const MIN_TRANSACTION_TIMEOUT_MS: i32 = 1_000;

/// Clamp a client's requested transaction timeout to what this broker will
/// police.
pub(crate) fn clamp_transaction_timeout(broker: &Broker, requested: i32) -> i32 {
    let max = broker
        .config()
        .transaction_max_timeout
        .as_millis()
        .min(i32::MAX as u128) as i32;
    requested.clamp(
        MIN_TRANSACTION_TIMEOUT_MS,
        max.max(MIN_TRANSACTION_TIMEOUT_MS),
    )
}

/// Every `__transaction_state` partition this broker coordinates *and has
/// already opened*.
///
/// In cluster mode that is the partitions it leads; in standalone mode
/// every local one. Empty when the topic does not exist yet, which is the
/// normal state of a broker nothing transactional has ever talked to.
///
/// **Already opened** is the important half. This runs on a timer on every
/// broker, and `Broker::partition` *creates* a partition actor if one is
/// not open — so asking it here would let a background sweep spawn an
/// actor, with its own maintenance tick and checkpoint fsync, for every
/// one of the fifty `__transaction_state` partitions a broker leads and
/// nothing has ever used. A timer should observe state, not conjure it.
///
/// Nothing is lost by waiting. A broker opens every partition it hosts at
/// startup, so after the restart or failover that recovery actually cares
/// about, the shards are open and this sees them. Before that, a partition
/// nobody has touched holds no transaction that could need resolving.
fn local_coordinator_partitions(broker: &Broker) -> Vec<(i32, crate::actor::PartitionHandle, i32)> {
    let mut out = Vec::new();
    match broker.metadata_cache() {
        Some(cache) => {
            let image = cache.snapshot();
            let Some(topic) = image.topics.get(TRANSACTION_STATE_TOPIC) else {
                return out;
            };
            for (partition, assignment) in &topic.partitions {
                if assignment.leader != broker.config().broker_id {
                    continue;
                }
                if let Some(handle) = broker.hosted_partition(TRANSACTION_STATE_TOPIC, *partition) {
                    out.push((*partition, handle, assignment.leader_epoch));
                }
            }
        }
        None => {
            let Some(count) = broker.state().partitions(TRANSACTION_STATE_TOPIC) else {
                return out;
            };
            for partition in 0..count {
                if let Some(handle) = broker.hosted_partition(TRANSACTION_STATE_TOPIC, partition) {
                    out.push((partition, handle, 0));
                }
            }
        }
    }
    out
}

/// Abort transactions that have outlived their timeout, finish the ones
/// left half-marked, and forget producers nobody has used in a long time.
///
/// This is the half of the coordinator that does not need a client. Without
/// it, `transaction.timeout.ms` is a number the client sends and nothing
/// enforces: a producer that dies mid-transaction — scaled down, redeployed
/// under a different id, crashed for good — leaves records in doubt on
/// every partition it wrote to, and the last stable offset on those
/// partitions never advances past them. Every `read_committed` consumer
/// stops there, permanently.
pub(crate) async fn sweep_expired(broker: &Broker) {
    for (partition, handle, leader_epoch) in local_coordinator_partitions(broker) {
        let shard = broker.transactions().shard(partition, handle, leader_epoch);
        // Loading is what makes this work after a failover: the new
        // coordinator has to know what the old one left open before it can
        // finish it, and nothing else would make it read the log until a
        // client happened to ask.
        if let Err(error) = shard.ensure_loaded().await {
            debug!(partition, %error, "transaction shard not ready to sweep");
            continue;
        }
        let now = now_ms();
        let candidates: Vec<TransactionMetadata> = shard
            .transactions
            .iter()
            .filter(|entry| {
                let age = now.saturating_sub(entry.last_update_ms);
                match entry.state {
                    TransactionState::Ongoing => age > entry.timeout_ms.max(0) as i64,
                    state if state.is_prepared() => age > MARKER_RETRY_INTERVAL.as_millis() as i64,
                    _ => age > broker.config().transactional_id_expiration.as_millis() as i64,
                }
            })
            .map(|entry| entry.clone())
            .collect();

        for metadata in candidates {
            let result = match metadata.state {
                TransactionState::Ongoing => abort_timed_out(broker, &shard, metadata).await,
                state if state.is_prepared() => finish_prepared(broker, &shard, metadata).await,
                _ => expire_transactional_id(&shard, metadata).await,
            };
            if let Err(error) = result {
                warn!(%error, partition, "transaction expiry sweep failed");
            }
        }
    }
}

/// Abort a transaction whose producer stopped talking, fencing it first.
///
/// The epoch bump is not bookkeeping: without it a producer that is merely
/// slow would carry on writing records into a transaction the coordinator
/// has already marked aborted, and those records would be in doubt with no
/// marker ever coming for them. Fencing turns that into a
/// `FENCED_PRODUCER_EPOCH` the producer must re-initialise after, which is
/// exactly what Kafka does.
async fn abort_timed_out(
    broker: &Broker,
    shard: &Arc<TransactionShard>,
    mut metadata: TransactionMetadata,
) -> Result<(), BrokerError> {
    let abandoned_epoch = metadata.producer_epoch;
    metadata.producer_epoch = metadata.producer_epoch.wrapping_add(1);
    metadata.state = TransactionState::PrepareAbort;
    metadata.last_update_ms = now_ms();
    debug!(
        transactional_id = %metadata.transactional_id,
        partitions = metadata.partitions.len(),
        "aborting a transaction that outlived transaction.timeout.ms"
    );
    // Durable before any marker, for the same reason `EndTxn` is: a
    // coordinator that dies here comes back knowing the decision rather
    // than guessing it.
    shard.persist(&metadata).await?;
    shard.put(metadata.clone());

    // The records were written under the epoch before the bump, so that is
    // the epoch their markers carry.
    write_markers(
        broker,
        metadata.producer_id,
        abandoned_epoch,
        false,
        &metadata.partitions,
    )
    .await;

    metadata.state = TransactionState::CompleteAbort;
    metadata.partitions.clear();
    metadata.last_update_ms = now_ms();
    shard.persist(&metadata).await?;
    shard.put(metadata);
    Ok(())
}

/// Re-send the markers of a transaction whose decision is durable but whose
/// partitions never heard about it.
async fn finish_prepared(
    broker: &Broker,
    shard: &Arc<TransactionShard>,
    mut metadata: TransactionMetadata,
) -> Result<(), BrokerError> {
    let committed = metadata.state.committed();
    debug!(
        transactional_id = %metadata.transactional_id,
        committed,
        partitions = metadata.partitions.len(),
        "re-sending the markers of a transaction left half-finished"
    );
    // A duplicate marker is harmless: a partition that already closed this
    // transaction has nothing open to close, and says so by doing nothing.
    write_markers(
        broker,
        metadata.producer_id,
        metadata.producer_epoch,
        committed,
        &metadata.partitions,
    )
    .await;
    metadata.state = if committed {
        TransactionState::CompleteCommit
    } else {
        TransactionState::CompleteAbort
    };
    metadata.partitions.clear();
    metadata.last_update_ms = now_ms();
    shard.persist(&metadata).await?;
    shard.put(metadata);
    Ok(())
}

/// Forget a `transactional.id` nobody has used in a very long time.
///
/// Written as a tombstone rather than only dropped from memory: the state
/// log is compacted, so a null value is what makes the record stop
/// occupying disk. Without it the coordinator's log grows with every
/// transactional id that ever existed, and its replay after a failover
/// grows with it.
async fn expire_transactional_id(
    shard: &Arc<TransactionShard>,
    metadata: TransactionMetadata,
) -> Result<(), BrokerError> {
    debug!(
        transactional_id = %metadata.transactional_id,
        "expiring an idle transactional id"
    );
    shard.persist_tombstone(&metadata.transactional_id).await?;
    shard.transactions.remove(&metadata.transactional_id);
    Ok(())
}

/// Every transaction this broker coordinates, for `ListTransactions`.
///
/// Reads the loaded shards rather than the log: a coordinator's in-memory
/// state *is* the replay of that log, and asking it costs nothing where
/// re-reading the log would cost a scan per query.
pub(crate) async fn list_local_transactions(
    broker: &Broker,
) -> Vec<brahmaputra_protocol::gen::TransactionListing> {
    let mut out = Vec::new();
    for (partition, handle, leader_epoch) in local_coordinator_partitions(broker) {
        let shard = broker.transactions().shard(partition, handle, leader_epoch);
        if shard.ensure_loaded().await.is_err() {
            continue;
        }
        for entry in shard.transactions.iter() {
            out.push(brahmaputra_protocol::gen::TransactionListing {
                transactional_id: entry.transactional_id.clone(),
                producer_id: entry.producer_id,
                state: entry.state.as_str().to_owned(),
                last_update_ms: entry.last_update_ms,
                timeout_ms: entry.timeout_ms,
                partition_count: entry.partitions.len() as i32,
            });
        }
    }
    out.sort_by(|a, b| a.transactional_id.cmp(&b.transactional_id));
    out
}

/// One transaction in full, or `None` when this broker does not coordinate
/// that id.
pub(crate) async fn describe_local_transaction(
    broker: &Broker,
    transactional_id: &str,
) -> Result<Option<brahmaputra_protocol::gen::DescribeTransactionsResponse>, BrokerError> {
    let shard = shard_for(broker, transactional_id).await?;
    let Some(metadata) = shard.get(transactional_id) else {
        return Ok(None);
    };
    Ok(Some(
        brahmaputra_protocol::gen::DescribeTransactionsResponse {
            error_code: ec::NONE,
            transactional_id: metadata.transactional_id,
            producer_id: metadata.producer_id,
            producer_epoch: i32::from(metadata.producer_epoch),
            state: metadata.state.as_str().to_owned(),
            timeout_ms: metadata.timeout_ms,
            last_update_ms: metadata.last_update_ms,
            partitions: metadata
                .partitions
                .into_iter()
                .map(|(topic, partition)| TxnPartition { topic, partition })
                .collect(),
        },
    ))
}

/// Transaction-expiry sweeper. Spawned from [`Broker::run`]; stops on the
/// broker shutdown watch.
pub(crate) async fn run_expiry_sweeper(broker: Arc<Broker>) {
    let mut shutdown = broker.shutdown_receiver();
    let mut interval = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = async {
                while !*shutdown.borrow_and_update() {
                    if shutdown.changed().await.is_err() {
                        break;
                    }
                }
            } => break,
            _ = interval.tick() => sweep_expired(&broker).await,
        }
    }
}

/// Check that a request comes from the current producer instance.
fn check_epoch(
    metadata: &TransactionMetadata,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<(), i32> {
    if metadata.producer_id != producer_id {
        return Err(ec::INVALID_PRODUCER_ID_MAPPING);
    }
    if metadata.producer_epoch != producer_epoch {
        // Behind means fenced; ahead means a client inventing epochs.
        return Err(ec::FENCED_PRODUCER_EPOCH);
    }
    Ok(())
}

/// Record the partitions a transaction is about to write to.
pub(crate) async fn add_partitions(
    broker: &Broker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    partitions: Vec<(String, i32)>,
) -> Result<(), i32> {
    let shard = shard_for(broker, transactional_id)
        .await
        .map_err(|error| crate::handlers::code_of(&error))?;
    let Some(mut metadata) = shard.get(transactional_id) else {
        return Err(ec::INVALID_PRODUCER_ID_MAPPING);
    };
    check_epoch(&metadata, producer_id, producer_epoch)?;
    if metadata.state.is_prepared() {
        // The decision is already made; adding partitions now would mean
        // records nobody is going to mark.
        return Err(ec::CONCURRENT_TRANSACTIONS);
    }

    metadata.state = TransactionState::Ongoing;
    metadata.partitions.extend(partitions);
    metadata.last_update_ms = now_ms();
    shard
        .persist(&metadata)
        .await
        .map_err(|error| crate::handlers::code_of(&error))?;
    shard.put(metadata);
    Ok(())
}

/// Bring a consumer group's offsets topic into the transaction.
///
/// The offsets are committed to the group coordinator as transactional
/// records; this is what tells *this* coordinator to mark that partition
/// too, so the offsets become visible with the output records and never
/// without them.
pub(crate) async fn add_offsets(
    broker: &Broker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> Result<(), i32> {
    let offsets_partition =
        crate::group::coordinator_partition(group_id, offsets_topic_partition_count(broker).await);
    add_partitions(
        broker,
        transactional_id,
        producer_id,
        producer_epoch,
        vec![(crate::group::OFFSETS_TOPIC.to_owned(), offsets_partition)],
    )
    .await
}

async fn offsets_topic_partition_count(broker: &Broker) -> i32 {
    match broker.metadata_cache() {
        Some(cache) => cache
            .snapshot()
            .topics
            .get(crate::group::OFFSETS_TOPIC)
            .map_or(1, |topic| topic.partitions.len() as i32),
        None => broker
            .state()
            .partitions(crate::group::OFFSETS_TOPIC)
            .unwrap_or(1),
    }
}

/// Commit or abort, marking every partition the transaction touched.
///
/// The prepare record is written *before* any marker. That ordering is the
/// whole recovery story: a coordinator that dies after it is durable comes
/// back knowing which way the transaction went and finishes the markers,
/// and one that dies before it comes back with the transaction still open
/// and aborts it. Neither case has to guess.
pub(crate) async fn end_transaction(
    broker: &Broker,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<(), i32> {
    let shard = shard_for(broker, transactional_id)
        .await
        .map_err(|error| crate::handlers::code_of(&error))?;
    let Some(mut metadata) = shard.get(transactional_id) else {
        return Err(ec::INVALID_PRODUCER_ID_MAPPING);
    };
    check_epoch(&metadata, producer_id, producer_epoch)?;
    if metadata.state == TransactionState::Empty {
        return Err(ec::INVALID_TXN_STATE);
    }

    metadata.state = if committed {
        TransactionState::PrepareCommit
    } else {
        TransactionState::PrepareAbort
    };
    metadata.last_update_ms = now_ms();
    shard
        .persist(&metadata)
        .await
        .map_err(|error| crate::handlers::code_of(&error))?;
    shard.put(metadata.clone());

    write_markers(
        broker,
        metadata.producer_id,
        metadata.producer_epoch,
        committed,
        &metadata.partitions,
    )
    .await;

    metadata.state = if committed {
        TransactionState::CompleteCommit
    } else {
        TransactionState::CompleteAbort
    };
    metadata.partitions.clear();
    metadata.last_update_ms = now_ms();
    shard
        .persist(&metadata)
        .await
        .map_err(|error| crate::handlers::code_of(&error))?;
    shard.put(metadata);
    Ok(())
}

/// Send a marker to every partition of the transaction, grouped by the
/// broker that leads it.
///
/// Failures are logged rather than returned. A partition whose marker did
/// not land keeps its records in doubt, which is the safe direction, and
/// the transaction's state log still says `Prepare*` — so the next time
/// this producer initialises, the resolution above finishes the job.
async fn write_markers(
    broker: &Broker,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    partitions: &BTreeSet<(String, i32)>,
) {
    if partitions.is_empty() {
        return;
    }
    let mut local = Vec::new();
    let mut remote: std::collections::BTreeMap<i32, Vec<TxnPartition>> = Default::default();

    for (topic, partition) in partitions {
        match leader_of(broker, topic, *partition) {
            Some(leader) if leader == broker.config().broker_id => {
                local.push((topic.clone(), *partition));
            }
            Some(leader) => remote.entry(leader).or_default().push(TxnPartition {
                topic: topic.clone(),
                partition: *partition,
            }),
            // Standalone mode, or a partition whose leader is unknown right
            // now: try locally, which is correct in the first case and
            // fails loudly in the second.
            None => local.push((topic.clone(), *partition)),
        }
    }

    for (topic, partition) in local {
        if let Err(error) = append_marker(
            broker,
            &topic,
            partition,
            producer_id,
            producer_epoch,
            committed,
        )
        .await
        {
            warn!(%topic, partition, %error, "could not write a transaction marker locally");
        }
    }

    for (leader, partitions) in remote {
        if let Err(error) = send_markers(
            broker,
            leader,
            producer_id,
            producer_epoch,
            committed,
            partitions,
        )
        .await
        {
            warn!(leader, %error, "could not write transaction markers to a peer");
        }
    }
}

fn leader_of(broker: &Broker, topic: &str, partition: i32) -> Option<i32> {
    let cache = broker.metadata_cache()?;
    let image = cache.snapshot();
    let assignment = image.topics.get(topic)?.partitions.get(&partition)?;
    Some(assignment.leader)
}

/// Append one control batch to a partition this broker leads.
pub(crate) async fn append_marker(
    broker: &Broker,
    topic: &str,
    partition: i32,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> Result<(), BrokerError> {
    let handle = broker.partition(topic, partition)?;
    let mut marker = brahmaputra_protocol::control_batch(
        brahmaputra_protocol::ProducerMetadata {
            producer_id,
            producer_epoch,
            // A marker carries no sequence: it is not part of the
            // producer's idempotent record stream and must not be
            // deduplicated against it.
            base_sequence: -1,
        },
        if committed {
            brahmaputra_protocol::ControlMarker::Commit
        } else {
            brahmaputra_protocol::ControlMarker::Abort
        },
        now_ms(),
    );

    // The same serialisation the produce path uses, so advancing the high
    // watermark cannot observe a stale ISR — and the same reason it
    // matters here: a marker that is appended but not *committed* still
    // leaves the transaction's records in doubt for every committed reader.
    let assignment = match broker.metadata_cache() {
        Some(cache) => {
            let image = cache.snapshot();
            broker.validate_local_broker_epoch(&image)?;
            image
                .topics
                .get(topic)
                .and_then(|entry| entry.partitions.get(&partition))
                .cloned()
        }
        None => None,
    };
    // A marker is replicated like any other batch, and a follower checks
    // every batch's leader epoch against its checkpoint. Left at zero it
    // is rejected as non-monotonic after the partition's first leader
    // change, and the follower retries the same offset forever.
    if let Some(assignment) = assignment.as_ref() {
        marker.leader_epoch = assignment.leader_epoch;
    }
    let guard = if assignment.is_some() {
        Some(broker.partition_mutation_guard(topic, partition).await)
    } else {
        None
    };
    handle.append(marker).await?;
    if let Some(assignment) = assignment.as_ref() {
        broker
            .replication_tracker()
            .advance_leader_high_watermark(topic, assignment, &handle)
            .await?;
    }
    drop(guard);
    // The group coordinator parks transactional offset commits until this
    // marker; it is the same broker, because a marker is only ever written
    // by the partition's leader and the leader is the coordinator.
    if topic == crate::group::OFFSETS_TOPIC {
        broker
            .groups()
            .resolve_transaction(partition, producer_id, committed);
    }
    Ok(())
}

/// Ask a peer broker to write markers for partitions it leads.
async fn send_markers(
    broker: &Broker,
    leader: i32,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
    partitions: Vec<TxnPartition>,
) -> Result<(), BrokerError> {
    let address = broker
        .broker_address(leader)
        .ok_or_else(|| BrokerError::Meta(format!("broker {leader} has no known address")))?;
    let request = WriteTxnMarkersRequest {
        markers: vec![TxnMarker {
            producer_id,
            producer_epoch: i32::from(producer_epoch),
            committed,
            partitions,
        }],
    };
    let body = request
        .encode()
        .map_err(|error| BrokerError::Meta(error.to_string()))?;

    let client = brahmaputra_client::ReplicaClient::connect_with(
        broker.internal_transport(),
        address,
        format!("txn-coordinator-{}", broker.config().broker_id),
        4,
    )
    .await
    .map_err(|error| BrokerError::Meta(error.to_string()))?;

    let response = tokio::time::timeout(
        MARKER_TIMEOUT,
        client.connection().request(ApiKey::WriteTxnMarkers, &body),
    )
    .await
    .map_err(|_| BrokerError::Meta("timed out writing transaction markers".into()))?
    .map_err(|error| BrokerError::Meta(error.to_string()))?;
    let decoded = WriteTxnMarkersResponse::decode(&response)
        .map_err(|error| BrokerError::Meta(error.to_string()))?;
    for result in decoded.results {
        if result.error_code != ec::NONE {
            warn!(
                topic = %result.topic,
                partition = result.partition,
                error_code = result.error_code,
                "peer refused a transaction marker"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transactional_id_always_maps_to_the_same_partition() {
        for id in ["orders-etl", "billing", "a", ""] {
            let first = coordinator_partition(id, 50);
            assert_eq!(first, coordinator_partition(id, 50));
            assert!((0..50).contains(&first));
        }
    }

    #[test]
    fn transaction_states_round_trip_through_their_names() {
        for state in [
            TransactionState::Empty,
            TransactionState::Ongoing,
            TransactionState::PrepareCommit,
            TransactionState::PrepareAbort,
            TransactionState::CompleteCommit,
            TransactionState::CompleteAbort,
        ] {
            assert_eq!(TransactionState::parse(state.as_str()), state);
        }
        // A state written by a future version must not be acted on.
        assert_eq!(
            TransactionState::parse("SomethingNewer"),
            TransactionState::Empty
        );
    }

    #[test]
    fn only_a_prepared_transaction_is_half_finished() {
        assert!(TransactionState::PrepareCommit.is_prepared());
        assert!(TransactionState::PrepareAbort.is_prepared());
        assert!(!TransactionState::Ongoing.is_prepared());
        assert!(!TransactionState::CompleteCommit.is_prepared());
        assert!(TransactionState::PrepareCommit.committed());
        assert!(!TransactionState::PrepareAbort.committed());
    }

    #[test]
    fn a_stale_epoch_is_fenced_and_a_foreign_producer_id_is_refused() {
        let metadata = TransactionMetadata {
            transactional_id: "etl".into(),
            producer_id: 5,
            producer_epoch: 3,
            state: TransactionState::Ongoing,
            partitions: BTreeSet::new(),
            timeout_ms: 60_000,
            last_update_ms: 0,
        };
        assert!(check_epoch(&metadata, 5, 3).is_ok());
        assert_eq!(check_epoch(&metadata, 5, 2), Err(ec::FENCED_PRODUCER_EPOCH));
        assert_eq!(
            check_epoch(&metadata, 6, 3),
            Err(ec::INVALID_PRODUCER_ID_MAPPING)
        );
    }

    #[test]
    fn state_survives_the_record_it_is_persisted_as() {
        let metadata = TransactionMetadata {
            transactional_id: "etl".into(),
            producer_id: 91,
            producer_epoch: 7,
            state: TransactionState::PrepareCommit,
            partitions: BTreeSet::from([("orders".to_owned(), 2), ("audit".to_owned(), 0)]),
            timeout_ms: 60_000,
            last_update_ms: 1_700_000_000_000,
        };
        let restored = TransactionMetadata::from_record(metadata.to_record());
        assert_eq!(restored.producer_id, metadata.producer_id);
        assert_eq!(restored.producer_epoch, metadata.producer_epoch);
        assert_eq!(restored.state, metadata.state);
        assert_eq!(restored.partitions, metadata.partitions);
        assert_eq!(restored.last_update_ms, metadata.last_update_ms);
    }
}
