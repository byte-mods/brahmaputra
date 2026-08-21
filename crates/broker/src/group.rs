//! Group coordinator (Blueprint 05): consumer-group membership, rebalancing,
//! and committed offsets, all backed by the internal `__consumer_offsets` log.
//!
//! The coordinator for a group is the leader broker of partition
//! `crc32c(group_id) % partition_count` of `__consumer_offsets`. Each such
//! partition gets a [`CoordinatorShard`] holding the in-memory group state
//! machines and committed offsets, lazily rebuilt by replaying the local
//! partition log on first use. Every membership change and offset commit is
//! appended to the log first, so coordinator failover is ordinary partition
//! leadership failover.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AssignedPartition, DescribeGroupResponse, DescribedMember, GroupMemberInfo, GroupMemberRecord,
    GroupMetadataRecord, HeartbeatRequest, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    ListedGroup, OffsetCommitRecord, OffsetCommitRequest, OffsetFetchEntry, OffsetFetchRequest,
    OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse, TombstoneRecord,
};
use brahmaputra_protocol::{Record, RecordBatch};
use dashmap::DashMap;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};

use crate::actor::PartitionHandle;
use crate::error::BrokerError;
use crate::handlers::wait_for_high_watermark;
use crate::server::Broker;

/// Internal topic whose partition leaders act as group coordinators.
pub(crate) const OFFSETS_TOPIC: &str = "__consumer_offsets";

const DEFAULT_SESSION_TIMEOUT_MS: i64 = 3_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: i64 = 3_000;
/// Extra grace beyond the rebalance deadline before a blocked JoinGroup or
/// SyncGroup gives up waiting for the group watch.
const REBALANCE_WAIT_GRACE: Duration = Duration::from_millis(1_000);
/// How long an offset commit waits for the appended batch to be committed.
const COMMIT_WATERMARK_TIMEOUT: Duration = Duration::from_secs(5);
/// Session-expiry sweep cadence.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
/// Log-read chunk while replaying a coordinator partition.
const REPLAY_MAX_BYTES: usize = 1024 * 1024;

/// Record-value kind tags inside `__consumer_offsets` (Blueprint 05 §1).
const KIND_OFFSET_COMMIT: u8 = 1;
const KIND_GROUP_METADATA: u8 = 2;
const KIND_TOMBSTONE: u8 = 3;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Coordinator partition for a group: `crc32c(group_id) % partition_count`.
pub(crate) fn coordinator_partition(group_id: &str, partition_count: i32) -> i32 {
    (crc32c::crc32c(group_id.as_bytes()) % partition_count.max(1) as u32) as i32
}

/// The group state machine (Blueprint 05 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupState {
    Empty,
    PreparingRebalance,
    AwaitingSync,
    Stable,
    #[allow(dead_code)]
    Dead,
}

impl GroupState {
    /// Wire/CLI name, as reported by ListGroups and DescribeGroup.
    fn name(self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::AwaitingSync => "AwaitingSync",
            GroupState::Stable => "Stable",
            GroupState::Dead => "Dead",
        }
    }
}

#[derive(Debug, Clone)]
struct Member {
    subscription_topics: Vec<String>,
    last_heartbeat_ms: i64,
    session_timeout_ms: i64,
    assignment: Vec<AssignedPartition>,
    /// Stable identity across restarts (KIP-345), empty for a dynamic
    /// member. A restarting instance that presents the same one reclaims
    /// this member slot instead of being treated as a new arrival.
    group_instance_id: String,
}

struct Group {
    state: GroupState,
    generation: i32,
    leader: Option<String>,
    members: HashMap<String, Member>,
    /// Members known when the current rebalance began that have not rejoined.
    pending_rejoin: HashSet<String>,
    rebalance_deadline_ms: i64,
    rebalance_timeout_ms: i64,
    next_member_seq: u64,
    /// When the group last became memberless, for `offsets.retention.ms`.
    /// `None` while it has members.
    ///
    /// The clock runs from emptiness rather than from each commit because
    /// a live group is still using its offsets however old they are —
    /// expiring under it would silently rewind the consumer.
    empty_since_ms: Option<i64>,
    /// Bumped on every state change; JoinGroup/SyncGroup waiters subscribe.
    watch: watch::Sender<()>,
}

impl Group {
    fn new() -> Self {
        Group {
            state: GroupState::Empty,
            generation: 0,
            leader: None,
            members: HashMap::new(),
            pending_rejoin: HashSet::new(),
            rebalance_deadline_ms: 0,
            rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            next_member_seq: 0,
            empty_since_ms: None,
            watch: watch::channel(()).0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadState {
    Unloaded,
    Loading,
    Loaded,
}

/// All coordinator state owned by one `__consumer_offsets` partition.
pub(crate) struct CoordinatorShard {
    partition: i32,
    handle: PartitionHandle,
    leader_epoch: i32,
    groups: DashMap<String, Group>,
    /// (group, topic, partition) -> committed offset.
    offsets: DashMap<(String, String, i32), i64>,
    load: Mutex<LoadState>,
}

impl CoordinatorShard {
    fn new(partition: i32, handle: PartitionHandle, leader_epoch: i32) -> Self {
        CoordinatorShard {
            partition,
            handle,
            leader_epoch,
            groups: DashMap::new(),
            offsets: DashMap::new(),
            load: Mutex::new(LoadState::Unloaded),
        }
    }

    fn is_loaded(&self) -> bool {
        matches!(
            *self.load.lock().expect("coordinator load state"),
            LoadState::Loaded
        )
    }

    /// Replay the local partition log `[start, HWM)` on first use. Requests
    /// arriving while a replay is in flight get COORDINATOR_LOAD_IN_PROGRESS.
    async fn ensure_loaded(&self) -> Result<(), BrokerError> {
        {
            let mut load = self.load.lock().expect("coordinator load state");
            match *load {
                LoadState::Loaded => return Ok(()),
                LoadState::Loading => {
                    return Err(BrokerError::CoordinatorLoadInProgress {
                        partition: self.partition,
                    });
                }
                LoadState::Unloaded => *load = LoadState::Loading,
            }
        }
        let result = self.replay().await;
        let mut load = self.load.lock().expect("coordinator load state");
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
                let mut buf = raw.clone();
                let batch = RecordBatch::decode(&mut buf)?;
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
            groups = self.groups.len(),
            offsets = self.offsets.len(),
            "coordinator shard replayed"
        );
        Ok(())
    }

    fn apply_record(&self, record: &Record) {
        let Some((&kind, payload)) = record.value.split_first() else {
            return;
        };
        match kind {
            KIND_OFFSET_COMMIT => match OffsetCommitRecord::decode(payload) {
                Ok(commit) => {
                    self.offsets.insert(
                        (commit.group_id, commit.topic, commit.partition),
                        commit.offset,
                    );
                }
                Err(error) => warn!(%error, "skipping undecodable offset commit record"),
            },
            KIND_GROUP_METADATA => match GroupMetadataRecord::decode(payload) {
                Ok(metadata) => self.apply_group_metadata(metadata),
                Err(error) => warn!(%error, "skipping undecodable group metadata record"),
            },
            KIND_TOMBSTONE => match TombstoneRecord::decode(payload) {
                Ok(tombstone) => {
                    if tombstone.topic.is_empty() {
                        self.offsets
                            .retain(|(group, _, _), _| group != &tombstone.group_id);
                        self.groups.remove(&tombstone.group_id);
                    } else {
                        self.offsets.remove(&(
                            tombstone.group_id,
                            tombstone.topic,
                            tombstone.partition,
                        ));
                    }
                }
                Err(error) => warn!(%error, "skipping undecodable tombstone record"),
            },
            other => warn!(kind = other, "skipping unknown coordinator record"),
        }
    }

    fn apply_group_metadata(&self, metadata: GroupMetadataRecord) {
        let mut group = self
            .groups
            .entry(metadata.group_id.clone())
            .or_insert_with(Group::new);
        let now = now_ms();
        group.generation = metadata.generation;
        group.leader = if metadata.leader_member_id.is_empty() {
            None
        } else {
            Some(metadata.leader_member_id)
        };
        group.members = metadata
            .members
            .into_iter()
            .map(|member| {
                (
                    member.member_id,
                    Member {
                        subscription_topics: member.subscription_topics,
                        // The record carries no liveness data; members must
                        // heartbeat within the default session timeout after a
                        // coordinator failover or the sweeper evicts them.
                        last_heartbeat_ms: now,
                        session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
                        assignment: member.assignment,
                        group_instance_id: member.group_instance_id,
                    },
                )
            })
            .collect();
        group.pending_rejoin.clear();
        group.state = if group.members.is_empty() {
            GroupState::Empty
        } else {
            GroupState::Stable
        };
        group.watch.send_replace(());
    }

    /// Append one batch of keyed coordinator records; the actor stamps the
    /// real base offset, which is returned.
    async fn append_records(&self, records: Vec<(String, Vec<u8>)>) -> Result<i64, BrokerError> {
        let batch = RecordBatch::new(
            0,
            self.leader_epoch,
            now_ms(),
            records
                .into_iter()
                .map(|(key, value)| Record::with_key(key.into_bytes(), value, 0))
                .collect(),
        );
        Ok(self.handle.append(batch).await?)
    }
}

/// Broker-side consumer-group coordinator: one lazily loaded shard per local
/// `__consumer_offsets` partition.
#[derive(Default)]
pub(crate) struct GroupCoordinator {
    shards: DashMap<i32, Arc<CoordinatorShard>>,
}

impl GroupCoordinator {
    /// The shard for `partition`, creating it around the partition's log
    /// handle on first use.
    pub(crate) fn shard(
        &self,
        partition: i32,
        handle: PartitionHandle,
        leader_epoch: i32,
    ) -> Arc<CoordinatorShard> {
        self.shards
            .entry(partition)
            .or_insert_with(|| Arc::new(CoordinatorShard::new(partition, handle, leader_epoch)))
            .clone()
    }

    /// Ensure a shard has replayed its log before serving group traffic.
    pub(crate) async fn ensure_loaded(&self, shard: &CoordinatorShard) -> Result<(), BrokerError> {
        shard.ensure_loaded().await
    }

    /// Append keyed records and, in cluster mode, publish them by advancing
    /// the partition high watermark. Returns the batch base offset.
    async fn append_and_commit(
        &self,
        broker: &Broker,
        shard: &CoordinatorShard,
        records: Vec<(String, Vec<u8>)>,
    ) -> Result<i64, BrokerError> {
        let cluster_assignment = if let Some(cache) = broker.metadata_cache() {
            let image = cache.snapshot();
            broker.validate_local_broker_epoch(&image)?;
            let assignment = image
                .topics
                .get(OFFSETS_TOPIC)
                .and_then(|topic| topic.partitions.get(&shard.partition))
                .cloned()
                .ok_or_else(|| BrokerError::UnknownTopicOrPartition {
                    topic: OFFSETS_TOPIC.to_owned(),
                    partition: shard.partition,
                })?;
            if assignment.leader != broker.config().broker_id {
                return Err(BrokerError::NotCoordinator {
                    group_id: String::new(),
                });
            }
            Some(assignment)
        } else {
            None
        };
        // Serialize with Produce and controller ISR mutations, exactly like
        // the produce path, so the HWM advance cannot observe a stale ISR.
        let mutation_guard = if cluster_assignment.is_some() {
            Some(
                broker
                    .partition_mutation_guard(OFFSETS_TOPIC, shard.partition)
                    .await,
            )
        } else {
            None
        };
        let base = shard.append_records(records).await?;
        if let Some(assignment) = cluster_assignment.as_ref() {
            broker
                .replication_tracker()
                .advance_leader_high_watermark(OFFSETS_TOPIC, assignment, &shard.handle)
                .await?;
        }
        drop(mutation_guard);
        Ok(base)
    }

    /// Persist the group's current membership as a `GroupMetadataRecord`.
    async fn persist_group(
        &self,
        broker: &Broker,
        shard: &CoordinatorShard,
        group_id: &str,
    ) -> Result<(), BrokerError> {
        let record = {
            let Some(group) = shard.groups.get(group_id) else {
                return Ok(());
            };
            GroupMetadataRecord {
                group_id: group_id.to_owned(),
                generation: group.generation,
                leader_member_id: group.leader.clone().unwrap_or_default(),
                members: {
                    let mut members: Vec<GroupMemberRecord> = group
                        .members
                        .iter()
                        .map(|(member_id, member)| GroupMemberRecord {
                            member_id: member_id.clone(),
                            subscription_topics: member.subscription_topics.clone(),
                            assignment: member.assignment.clone(),
                            group_instance_id: member.group_instance_id.clone(),
                        })
                        .collect();
                    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
                    members
                },
            }
        };
        let mut value = vec![KIND_GROUP_METADATA];
        value.extend(record.encode()?);
        self.append_and_commit(broker, shard, vec![(group_id.to_owned(), value)])
            .await?;
        Ok(())
    }

    /// JoinGroup: upsert the member, (re)start the rebalance when the group
    /// is not already collecting members, then complete once every member
    /// known at rebalance start has rejoined or the rebalance deadline hits.
    pub(crate) async fn join(
        &self,
        broker: &Broker,
        shard: &Arc<CoordinatorShard>,
        request: JoinGroupRequest,
    ) -> Result<JoinGroupResponse, BrokerError> {
        let now = now_ms();
        let rebalance_timeout = if request.rebalance_timeout_ms > 0 {
            i64::from(request.rebalance_timeout_ms)
        } else {
            DEFAULT_REBALANCE_TIMEOUT_MS
        };
        let session_timeout = if request.session_timeout_ms > 0 {
            i64::from(request.session_timeout_ms)
        } else {
            DEFAULT_SESSION_TIMEOUT_MS
        };

        let (member_id, _generation) = {
            let mut entry = shard
                .groups
                .entry(request.group_id.clone())
                .or_insert_with(Group::new);
            let group = entry.value_mut();
            group.rebalance_timeout_ms = rebalance_timeout;
            // Static membership (KIP-345): an instance that presents a
            // `group_instance_id` the group already knows is the *same*
            // member coming back, whatever member id it now claims. This is
            // what makes a rolling restart cheap — the returning instance
            // reclaims its slot and its partitions, so the group does not
            // rebalance once when it goes and again when it returns.
            let reclaimed_member_id = (!request.group_instance_id.is_empty())
                .then(|| {
                    group
                        .members
                        .iter()
                        .find(|(_, member)| member.group_instance_id == request.group_instance_id)
                        .map(|(member_id, _)| member_id.clone())
                })
                .flatten();

            let known_member = reclaimed_member_id.is_some()
                || (!request.member_id.is_empty()
                    && group.members.contains_key(&request.member_id));
            // A rejoin from a known member while a rebalance is already
            // collecting (PreparingRebalance) or distributing assignments
            // (AwaitingSync) is part of the current cycle — restarting here
            // would leapfrog generations forever. A genuinely new member
            // during AwaitingSync does restart so it gets partitions in this
            // cycle rather than waiting for the next one.
            let restart = match group.state {
                GroupState::PreparingRebalance => false,
                GroupState::AwaitingSync => !known_member,
                // A returning static member changes nothing about the
                // group's shape: same identity, same subscription, same
                // partitions. Rebalancing here would throw away the very
                // saving static membership exists to provide.
                _ => reclaimed_member_id.is_none(),
            };
            if restart {
                debug!(group = %request.group_id, member = %request.member_id, state = ?group.state, gen = group.generation + 1, "join restarts rebalance");
                restart_rebalance(group, now);
            }
            let member_id = match reclaimed_member_id {
                Some(existing) => existing,
                None if request.member_id.is_empty() => {
                    let assigned = format!("member-{}", group.next_member_seq);
                    group.next_member_seq += 1;
                    assigned
                }
                None => request.member_id.clone(),
            };
            // A reclaiming static member keeps the partitions it already
            // held; wiping the assignment would strand them until the next
            // rebalance, which is exactly the pause being avoided.
            let previous_assignment = group
                .members
                .get(&member_id)
                .map(|member| member.assignment.clone())
                .unwrap_or_default();
            group.members.insert(
                member_id.clone(),
                Member {
                    subscription_topics: request.subscription_topics.clone(),
                    last_heartbeat_ms: now,
                    session_timeout_ms: session_timeout,
                    assignment: if restart {
                        Vec::new()
                    } else {
                        previous_assignment
                    },
                    group_instance_id: request.group_instance_id.clone(),
                },
            );
            group.pending_rejoin.remove(&member_id);
            debug!(group = %request.group_id, %member_id, state = ?group.state, gen = group.generation, pending = ?group.pending_rejoin, "join upserted member");
            if group.state == GroupState::PreparingRebalance && group.pending_rejoin.is_empty() {
                finalize_rebalance(group);
            }
            group.watch.send_replace(());
            (member_id, group.generation)
        };
        self.persist_group(broker, shard, &request.group_id).await?;

        // Await rebalance completion: the group leaves PreparingRebalance
        // when every awaited member rejoins or the deadline elapses.
        let mut watch = shard
            .groups
            .get(&request.group_id)
            .map(|group| group.watch.subscribe())
            .ok_or_else(|| BrokerError::UnknownMemberId {
                group_id: request.group_id.clone(),
                member_id: member_id.clone(),
            })?;
        let hard_cap = Duration::from_millis(rebalance_timeout as u64) + REBALANCE_WAIT_GRACE;
        let deadline = tokio::time::Instant::now() + hard_cap;
        loop {
            {
                let Some(group) = shard.groups.get(&request.group_id) else {
                    return Err(BrokerError::UnknownMemberId {
                        group_id: request.group_id.clone(),
                        member_id,
                    });
                };
                if !group.members.contains_key(&member_id) {
                    return Err(BrokerError::UnknownMemberId {
                        group_id: request.group_id.clone(),
                        member_id,
                    });
                }
                if matches!(group.state, GroupState::AwaitingSync | GroupState::Stable) {
                    return Ok(join_response(&group, &member_id));
                }
            }
            if now_ms() >= shard_rebalance_deadline(shard, &request.group_id) {
                // Deadline elapsed: finalize so waiters (including this one)
                // complete with the members that did rejoin.
                if let Some(mut group) = shard.groups.get_mut(&request.group_id) {
                    if group.state == GroupState::PreparingRebalance {
                        finalize_rebalance(&mut group);
                        group.watch.send_replace(());
                    }
                }
                continue;
            }
            let remaining = shard_rebalance_deadline(shard, &request.group_id) - now_ms();
            let wake = Duration::from_millis((remaining.max(1) as u64).min(50));
            match tokio::time::timeout_at(deadline, tokio::time::timeout(wake, watch.changed()))
                .await
            {
                Ok(Ok(Ok(()))) => {}
                // The group watch is never closed while the shard lives.
                Ok(Ok(Err(_))) => return Err(BrokerError::ActorUnavailable("group watch".into())),
                Ok(Err(_)) => {} // wake tick: re-check deadline/completion
                Err(_) => {
                    // Hard cap: never leave a client hanging on a wedged
                    // rebalance; force completion and answer.
                    if let Some(mut group) = shard.groups.get_mut(&request.group_id) {
                        if group.state == GroupState::PreparingRebalance {
                            finalize_rebalance(&mut group);
                            group.watch.send_replace(());
                        } else if group.members.contains_key(&member_id) {
                            return Ok(join_response(&group, &member_id));
                        }
                    }
                }
            }
        }
    }

    /// SyncGroup: the leader's assignment is stored and fanned out through
    /// the group watch; non-leaders await the Stable transition.
    pub(crate) async fn sync(
        &self,
        broker: &Broker,
        shard: &Arc<CoordinatorShard>,
        request: SyncGroupRequest,
    ) -> Result<SyncGroupResponse, BrokerError> {
        let (mut watch, rebalance_timeout, leader_synced) = {
            let Some(mut entry) = shard.groups.get_mut(&request.group_id) else {
                return Err(BrokerError::UnknownMemberId {
                    group_id: request.group_id.clone(),
                    member_id: request.member_id.clone(),
                });
            };
            let group = entry.value_mut();
            if !group.members.contains_key(&request.member_id) {
                return Err(BrokerError::UnknownMemberId {
                    group_id: request.group_id.clone(),
                    member_id: request.member_id.clone(),
                });
            }
            if request.generation != group.generation {
                return Err(BrokerError::IllegalGeneration {
                    group_id: request.group_id.clone(),
                    requested: request.generation,
                    current: group.generation,
                });
            }
            match group.state {
                GroupState::Stable => {
                    let assignment = group
                        .members
                        .get(&request.member_id)
                        .map(|member| member.assignment.clone())
                        .unwrap_or_default();
                    return Ok(SyncGroupResponse {
                        error_code: ec::NONE,
                        assignment,
                    });
                }
                GroupState::AwaitingSync => {}
                _ => {
                    return Err(BrokerError::RebalanceInProgress {
                        group_id: request.group_id.clone(),
                    });
                }
            }
            let watch = group.watch.subscribe();
            let is_leader_with_assignment = group.leader.as_deref()
                == Some(request.member_id.as_str())
                && !request.assignments.is_empty();
            if is_leader_with_assignment {
                debug!(group = %request.group_id, member = %request.member_id, gen = group.generation, assignments = ?request.assignments, "leader sync installs assignment");
                for member_assignment in &request.assignments {
                    if let Some(member) = group.members.get_mut(&member_assignment.member_id) {
                        member.assignment = member_assignment.partitions.clone();
                    }
                }
                group.state = GroupState::Stable;
                group.watch.send_replace(());
            }
            (watch, group.rebalance_timeout_ms, is_leader_with_assignment)
        };
        if leader_synced {
            self.persist_group(broker, shard, &request.group_id).await?;
        }
        if let Some(assignment) = shard
            .groups
            .get(&request.group_id)
            .filter(|group| group.state == GroupState::Stable)
            .map(|group| {
                group
                    .members
                    .get(&request.member_id)
                    .map(|member| member.assignment.clone())
                    .unwrap_or_default()
            })
        {
            return Ok(SyncGroupResponse {
                error_code: ec::NONE,
                assignment,
            });
        }

        // Non-leader member: wait for the leader's assignment to land.
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(rebalance_timeout.max(1) as u64);
        loop {
            {
                let Some(group) = shard.groups.get(&request.group_id) else {
                    return Err(BrokerError::UnknownMemberId {
                        group_id: request.group_id.clone(),
                        member_id: request.member_id.clone(),
                    });
                };
                if !group.members.contains_key(&request.member_id) {
                    return Err(BrokerError::UnknownMemberId {
                        group_id: request.group_id.clone(),
                        member_id: request.member_id.clone(),
                    });
                }
                if group.state == GroupState::Stable && group.generation == request.generation {
                    let assignment = group
                        .members
                        .get(&request.member_id)
                        .map(|member| member.assignment.clone())
                        .unwrap_or_default();
                    return Ok(SyncGroupResponse {
                        error_code: ec::NONE,
                        assignment,
                    });
                }
            }
            match tokio::time::timeout_at(deadline, watch.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(BrokerError::ActorUnavailable("group watch".into())),
                Err(_) => {
                    return Err(BrokerError::RebalanceInProgress {
                        group_id: request.group_id.clone(),
                    });
                }
            }
        }
    }

    /// Heartbeat: refresh the member's liveness, fencing stale generations
    /// and heartbeats sent outside the Stable state.
    pub(crate) fn heartbeat(
        &self,
        shard: &CoordinatorShard,
        request: HeartbeatRequest,
    ) -> Result<(), BrokerError> {
        let Some(mut group) = shard.groups.get_mut(&request.group_id) else {
            return Err(BrokerError::UnknownMemberId {
                group_id: request.group_id,
                member_id: request.member_id,
            });
        };
        if !group.members.contains_key(&request.member_id) {
            return Err(BrokerError::UnknownMemberId {
                group_id: request.group_id,
                member_id: request.member_id,
            });
        }
        if request.generation != group.generation {
            return Err(BrokerError::IllegalGeneration {
                group_id: request.group_id,
                requested: request.generation,
                current: group.generation,
            });
        }
        // Only members that still owe this generation a rejoin are fenced:
        // a member that already rejoined (absent from pending_rejoin) is
        // done — rejecting its heartbeats would flag a spurious rejoin that
        // restarts the rebalance the moment it completes. AwaitingSync is a
        // normal transient state and likewise refreshes liveness.
        if group.state == GroupState::PreparingRebalance
            && group.pending_rejoin.contains(&request.member_id)
        {
            return Err(BrokerError::RebalanceInProgress {
                group_id: request.group_id,
            });
        }
        if let Some(member) = group.members.get_mut(&request.member_id) {
            member.last_heartbeat_ms = now_ms();
        }
        Ok(())
    }

    /// LeaveGroup: remove one member on its own request and rebalance now.
    ///
    /// This is the same transition the session-timeout sweeper performs,
    /// taken immediately because the member told us rather than because we
    /// waited long enough to infer it. That difference is the whole point:
    /// a rolling restart otherwise stalls each group for a full
    /// `session.timeout.ms` per instance, which is downtime bought for no
    /// information — the member already knew it was leaving.
    ///
    /// Leaving is idempotent. A member that is already gone (evicted, or a
    /// retried request) is a success, not an error: the caller's intent —
    /// "I am not in this group" — already holds, and failing it would make
    /// a clean shutdown log spurious errors.
    pub(crate) async fn leave(
        &self,
        broker: &Broker,
        shard: &Arc<CoordinatorShard>,
        request: LeaveGroupRequest,
    ) -> Result<(), BrokerError> {
        let persist = {
            let Some(mut group) = shard.groups.get_mut(&request.group_id) else {
                return Ok(());
            };
            if group.members.remove(&request.member_id).is_none() {
                return Ok(());
            }
            group.pending_rejoin.remove(&request.member_id);
            debug!(group = %request.group_id, member = %request.member_id, "member left");
            if group.members.is_empty() {
                group.state = GroupState::Empty;
                group.leader = None;
                group.watch.send_replace(());
            } else {
                // The survivors need a new assignment covering the
                // partitions this member held, and the leader may itself
                // be the member that just left.
                restart_rebalance(&mut group, now_ms());
            }
            true
        };
        if persist {
            self.persist_group(broker, shard, &request.group_id).await?;
        }
        Ok(())
    }
    /// OffsetCommit: generation-fenced when part of a membership
    /// (`generation >= 0`); the batch must be covered by the partition high
    /// watermark before the in-memory offsets move.
    pub(crate) async fn commit(
        &self,
        broker: &Broker,
        shard: &Arc<CoordinatorShard>,
        request: OffsetCommitRequest,
    ) -> Result<(), BrokerError> {
        if request.offsets.is_empty() {
            return Ok(());
        }
        if request.generation >= 0 {
            let Some(group) = shard.groups.get(&request.group_id) else {
                return Err(BrokerError::UnknownMemberId {
                    group_id: request.group_id.clone(),
                    member_id: request.member_id.clone(),
                });
            };
            if !group.members.contains_key(&request.member_id) {
                return Err(BrokerError::UnknownMemberId {
                    group_id: request.group_id.clone(),
                    member_id: request.member_id.clone(),
                });
            }
            if request.generation != group.generation {
                return Err(BrokerError::IllegalGeneration {
                    group_id: request.group_id.clone(),
                    requested: request.generation,
                    current: group.generation,
                });
            }
        }

        let now = now_ms();
        let mut records = Vec::with_capacity(request.offsets.len());
        for entry in &request.offsets {
            let record = OffsetCommitRecord {
                group_id: request.group_id.clone(),
                topic: entry.topic.clone(),
                partition: entry.partition,
                offset: entry.offset,
                commit_timestamp_ms: now,
            };
            let mut value = vec![KIND_OFFSET_COMMIT];
            value.extend(record.encode()?);
            records.push((
                format!("{}/{}/{}", request.group_id, entry.topic, entry.partition),
                value,
            ));
        }
        let record_count = records.len() as i64;
        let mut watermark = shard.handle.watermark_watch();
        watermark.borrow_and_update();
        let base = self.append_and_commit(broker, shard, records).await?;
        if !wait_for_high_watermark(
            &mut watermark,
            base + record_count,
            COMMIT_WATERMARK_TIMEOUT,
        )
        .await
        {
            return Err(BrokerError::NotEnoughReplicas {
                required: 1,
                available: 0,
            });
        }
        for entry in &request.offsets {
            shard.offsets.insert(
                (
                    request.group_id.clone(),
                    entry.topic.clone(),
                    entry.partition,
                ),
                entry.offset,
            );
        }
        Ok(())
    }

    /// OffsetFetch: -1 for partitions with no commit; an empty partition
    /// list returns every offset known for the group.
    pub(crate) fn fetch_offsets(
        &self,
        shard: &CoordinatorShard,
        request: OffsetFetchRequest,
    ) -> OffsetFetchResponse {
        let offsets = if request.partitions.is_empty() {
            let mut entries: Vec<OffsetFetchEntry> = shard
                .offsets
                .iter()
                .filter(|entry| entry.key().0 == request.group_id)
                .map(|entry| {
                    let (_, topic, partition) = entry.key();
                    OffsetFetchEntry {
                        topic: topic.clone(),
                        partition: *partition,
                        offset: *entry.value(),
                    }
                })
                .collect();
            entries.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
            entries
        } else {
            request
                .partitions
                .iter()
                .map(|partition| OffsetFetchEntry {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                    offset: shard
                        .offsets
                        .get(&(
                            request.group_id.clone(),
                            partition.topic.clone(),
                            partition.partition,
                        ))
                        .map(|offset| *offset)
                        .unwrap_or(-1),
                })
                .collect()
        };
        OffsetFetchResponse {
            error_code: ec::NONE,
            offsets,
        }
    }

    /// ListGroups: every group on a coordinator partition this broker leads,
    /// sorted by (partition, group id). Shards are loaded on demand so a
    /// broker that has just taken over a partition reports the groups in its
    /// log, not an empty list.
    pub(crate) async fn list(
        &self,
        broker: &Broker,
        states: &[String],
    ) -> Result<Vec<ListedGroup>, BrokerError> {
        let mut listed = Vec::new();
        for (partition, handle, leader_epoch) in local_coordinator_partitions(broker)? {
            let shard = self.shard(partition, handle, leader_epoch);
            self.ensure_loaded(&shard).await?;
            for entry in shard.groups.iter() {
                let group = entry.value();
                if !states.is_empty() && !states.iter().any(|want| want == group.state.name()) {
                    continue;
                }
                listed.push(ListedGroup {
                    group_id: entry.key().clone(),
                    state: group.state.name().to_owned(),
                    generation: group.generation,
                    member_count: group.members.len() as i32,
                    coordinator_partition: partition,
                });
            }
        }
        listed.sort_by(|a, b| {
            (a.coordinator_partition, &a.group_id).cmp(&(b.coordinator_partition, &b.group_id))
        });
        Ok(listed)
    }

    /// DescribeGroup: membership plus every committed offset the coordinator
    /// holds for the group. `UnknownMemberId` with an empty member id means
    /// "no such group on this coordinator".
    pub(crate) fn describe(
        &self,
        shard: &CoordinatorShard,
        group_id: &str,
    ) -> Result<DescribeGroupResponse, BrokerError> {
        let Some(group) = shard.groups.get(group_id) else {
            return Err(BrokerError::UnknownMemberId {
                group_id: group_id.to_owned(),
                member_id: String::new(),
            });
        };
        let mut members: Vec<DescribedMember> = group
            .members
            .iter()
            .map(|(member_id, member)| DescribedMember {
                member_id: member_id.clone(),
                subscription_topics: member.subscription_topics.clone(),
                assignment: member.assignment.clone(),
            })
            .collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        let mut offsets: Vec<OffsetFetchEntry> = shard
            .offsets
            .iter()
            .filter(|entry| entry.key().0 == group_id)
            .map(|entry| {
                let (_, topic, partition) = entry.key();
                OffsetFetchEntry {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *entry.value(),
                }
            })
            .collect();
        offsets.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        Ok(DescribeGroupResponse {
            error_code: ec::NONE,
            group_id: group_id.to_owned(),
            state: group.state.name().to_owned(),
            generation: group.generation,
            leader_member_id: group.leader.clone().unwrap_or_default(),
            coordinator_partition: shard.partition,
            members,
            offsets,
        })
    }

    /// Evict members whose session expired. Eviction only happens while the
    /// group is Stable: during a rebalance (PreparingRebalance/AwaitingSync)
    /// liveness is bounded by the rebalance deadline instead — members that
    /// miss it are dropped at finalize, and a wedged AwaitingSync (leader
    /// died before distributing assignments) is restarted after a grace
    /// window. Expiring mid-rebalance members would ping-pong generations
    /// and evict consumers whose rejoin is merely in flight.
    pub(crate) async fn sweep_expired(&self, broker: &Broker) {
        let now = now_ms();
        let shards: Vec<Arc<CoordinatorShard>> = self
            .shards
            .iter()
            .filter(|entry| entry.value().is_loaded())
            .map(|entry| entry.value().clone())
            .collect();
        for shard in shards {
            let group_ids: Vec<String> = shard
                .groups
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            for group_id in group_ids {
                let persist = {
                    let Some(mut group) = shard.groups.get_mut(&group_id) else {
                        continue;
                    };
                    match group.state {
                        // Bounded by the rebalance deadline; nothing to do.
                        GroupState::PreparingRebalance => false,
                        // The leader never distributed assignments: restart so
                        // a new leader is picked from the members that rejoin.
                        GroupState::AwaitingSync
                            if now > group.rebalance_deadline_ms + group.rebalance_timeout_ms =>
                        {
                            debug!(group = %group_id, gen = group.generation, "awaiting-sync wedged; restarting rebalance");
                            restart_rebalance(&mut group, now);
                            true
                        }
                        GroupState::AwaitingSync | GroupState::Dead => false,
                        _ => {
                            let expired: Vec<String> = group
                                .members
                                .iter()
                                .filter(|(_, member)| {
                                    now - member.last_heartbeat_ms > member.session_timeout_ms
                                })
                                .map(|(member_id, _)| member_id.clone())
                                .collect();
                            if expired.is_empty() {
                                false
                            } else {
                                debug!(group = %group_id, ?expired, "group members expired");
                                for member_id in &expired {
                                    group.members.remove(member_id);
                                    group.pending_rejoin.remove(member_id);
                                }
                                if group.members.is_empty() {
                                    group.state = GroupState::Empty;
                                    group.leader = None;
                                    group.watch.send_replace(());
                                } else {
                                    restart_rebalance(&mut group, now);
                                }
                                true
                            }
                        }
                    }
                };
                if persist {
                    if let Err(error) = self.persist_group(broker, &shard, &group_id).await {
                        warn!(%error, group = %group_id, "failed to persist group after expiry");
                    }
                }

                // `offsets.retention.ms`. Tracked here rather than at each
                // transition into Empty because the sweeper already visits
                // every group, so one self-correcting place cannot drift
                // out of step with the several ways a group can empty.
                let expire = {
                    let Some(mut group) = shard.groups.get_mut(&group_id) else {
                        continue;
                    };
                    if group.members.is_empty() {
                        let since = *group.empty_since_ms.get_or_insert(now);
                        broker.config().offsets_retention.is_some_and(|retention| {
                            now.saturating_sub(since) >= retention.as_millis() as i64
                        })
                    } else {
                        // Still in use: the clock has not started.
                        group.empty_since_ms = None;
                        false
                    }
                };
                if expire {
                    if let Err(error) = self.expire_group_offsets(broker, &shard, &group_id).await {
                        warn!(%error, group = %group_id, "failed to expire group offsets");
                    }
                }
            }
        }
    }

    /// Append a whole-group tombstone, dropping its committed offsets.
    ///
    /// Written to the log rather than only to memory so the deletion
    /// survives a coordinator failover — and because compaction then
    /// reclaims the superseded commits, which is what stops a cluster's
    /// offsets topic growing forever with groups that no longer exist.
    async fn expire_group_offsets(
        &self,
        broker: &Broker,
        shard: &Arc<CoordinatorShard>,
        group_id: &str,
    ) -> Result<(), BrokerError> {
        let held: Vec<(String, String, i32)> = shard
            .offsets
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|(group, _, _)| group == group_id)
            .collect();
        if held.is_empty() {
            // Nothing to expire; drop the empty group so the sweep does not
            // reconsider it every tick forever.
            shard.groups.remove(group_id);
            return Ok(());
        }
        debug!(
            group = %group_id,
            offsets = held.len(),
            "expiring the offsets of a group that has been empty past offsets.retention.ms"
        );

        let tombstone = TombstoneRecord {
            group_id: group_id.to_owned(),
            // Empty topic means the whole group, matching what replay
            // already understands.
            topic: String::new(),
            partition: -1,
        };
        let mut value = vec![KIND_TOMBSTONE];
        value.extend_from_slice(
            &tombstone
                .encode()
                .map_err(|error| BrokerError::Meta(format!("encoding group tombstone: {error}")))?,
        );
        let mut watermark = shard.handle.watermark_watch();
        watermark.borrow_and_update();
        let base = self
            .append_and_commit(broker, shard, vec![(group_id.to_owned(), value)])
            .await?;
        // Only forget the offsets once the deletion is committed. Dropping
        // them from memory first would make a coordinator that then failed
        // over resurrect every one of them.
        if !wait_for_high_watermark(&mut watermark, base + 1, COMMIT_WATERMARK_TIMEOUT).await {
            return Err(BrokerError::NotEnoughReplicas {
                required: 1,
                available: 0,
            });
        }

        for key in held {
            shard.offsets.remove(&key);
        }
        shard.groups.remove(group_id);
        Ok(())
    }
}

/// Every `__consumer_offsets` partition this broker coordinates, as
/// `(partition, handle, leader_epoch)`: the partitions it leads in cluster
/// mode, all local partitions in standalone mode. Empty when the internal
/// topic does not exist yet — a broker that has never seen group traffic
/// coordinates nothing.
fn local_coordinator_partitions(
    broker: &Broker,
) -> Result<Vec<(i32, PartitionHandle, i32)>, BrokerError> {
    let mut owned = Vec::new();
    if let Some(cache) = broker.metadata_cache() {
        let image = cache.snapshot();
        broker.validate_local_broker_epoch(&image)?;
        let Some(topic) = image.topics.get(OFFSETS_TOPIC) else {
            return Ok(owned);
        };
        for (partition, assignment) in &topic.partitions {
            if assignment.leader != broker.config().broker_id {
                continue;
            }
            match broker.partition(OFFSETS_TOPIC, *partition) {
                Ok(handle) => owned.push((*partition, handle, assignment.leader_epoch)),
                // Leadership moved between the snapshot and the lookup; the
                // caller's view simply omits this partition.
                Err(BrokerError::NotLeaderOrFollower { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
    } else {
        let Some(partition_count) = broker.state().partitions(OFFSETS_TOPIC) else {
            return Ok(owned);
        };
        for partition in 0..partition_count {
            owned.push((partition, broker.partition(OFFSETS_TOPIC, partition)?, 0));
        }
    }
    Ok(owned)
}

/// (Re)start a rebalance: bump the generation and require every current
/// member to rejoin before the deadline.
fn restart_rebalance(group: &mut Group, now: i64) {
    group.state = GroupState::PreparingRebalance;
    group.generation += 1;
    group.pending_rejoin = group.members.keys().cloned().collect();
    group.leader = None;
    group.rebalance_deadline_ms = now + group.rebalance_timeout_ms;
    for member in group.members.values_mut() {
        member.assignment.clear();
    }
    group.watch.send_replace(());
}

/// Pick the group leader (deterministic: lowest member id) and move the
/// group to AwaitingSync once every awaited member has rejoined. Members
/// that missed the window are dropped first so they cannot be picked as
/// leader or receive assignments for a generation they never joined.
fn finalize_rebalance(group: &mut Group) {
    let pending = std::mem::take(&mut group.pending_rejoin);
    for member_id in &pending {
        group.members.remove(member_id);
    }
    group.leader = group.members.keys().min().cloned();
    group.state = GroupState::AwaitingSync;
}

fn shard_rebalance_deadline(shard: &CoordinatorShard, group_id: &str) -> i64 {
    shard
        .groups
        .get(group_id)
        .map(|group| group.rebalance_deadline_ms)
        .unwrap_or(0)
}

fn join_response(group: &Group, member_id: &str) -> JoinGroupResponse {
    let leader = group.leader.clone().unwrap_or_default();
    // Only the group leader receives the full member list; it computes the
    // partition assignment for everyone.
    let members = if leader == member_id {
        let mut members: Vec<GroupMemberInfo> = group
            .members
            .iter()
            .map(|(member_id, member)| GroupMemberInfo {
                member_id: member_id.clone(),
                subscription_topics: member.subscription_topics.clone(),
                // Carried so the leader can compute a sticky assignment;
                // empty for a member joining for the first time.
                assignment: member.assignment.clone(),
            })
            .collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        members
    } else {
        Vec::new()
    };
    JoinGroupResponse {
        error_code: ec::NONE,
        generation: group.generation,
        member_id: member_id.to_owned(),
        leader_member_id: leader,
        members,
    }
}

/// Session-expiry sweeper (Blueprint 05 §2): ticks every 100ms in standalone
/// and cluster mode, evicting silent members and restarting rebalances.
/// Spawned from [`Broker::run`]; stops on the broker shutdown watch.
pub(crate) async fn run_expiry_sweeper(broker: Arc<Broker>) {
    let mut shutdown = broker.shutdown_receiver();
    let mut interval = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
            _ = interval.tick() => broker.groups().sweep_expired(&broker).await,
        }
    }
}
