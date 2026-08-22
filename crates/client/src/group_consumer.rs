//! Group-coordinated consumer (M4, Blueprint consumer groups).
//!
//! A `GroupConsumer` joins a consumer group through the broker-side group
//! coordinator, receives a partition assignment from SyncGroup, heartbeats
//! in the background, and commits consumed positions to the internal
//! `__consumer_offsets` topic. The coordinator for a group is the leader of
//! `__consumer_offsets` partition `crc32c(group_id) % partitions`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AssignedPartition, GroupMemberInfo, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, MemberAssignment, OffsetCommitEntry,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchEntry, OffsetFetchRequest,
    OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse,
};
use brahmaputra_protocol::{ApiKey, ProtocolError, RecordHeader};
use bytes::Bytes;
use tokio::task::JoinHandle;

use crate::consumer::{Consumer, EARLIEST, LATEST};
use crate::error::ClientError;
use crate::router::BrokerRouter;
use crate::transport::{Transport, TransportConfig};

/// Internal topic whose partition leaders act as group coordinators.
const OFFSETS_TOPIC: &str = "__consumer_offsets";
/// Retries per coordinator request after a coordinator move/load.
const COORDINATOR_ATTEMPTS: usize = 4;
/// Join+sync rounds before giving up on a stabilizing group.
const JOIN_ATTEMPTS: usize = 4;
/// Idle delay between poll sweeps when no records arrive.
const POLL_IDLE_DELAY: Duration = Duration::from_millis(100);
/// `max.poll.records` default, matching Kafka's.
const DEFAULT_MAX_POLL_RECORDS: usize = 500;

type TopicPartition = (String, i32);
type Positions = Arc<Mutex<BTreeMap<TopicPartition, i64>>>;

/// What to do when a partition has no valid position to start from —
/// either the group never committed one, or the committed one has fallen
/// off the front of the log because retention deleted it.
///
/// These are the same situation from the consumer's point of view ("the
/// offset I want is not there"), so they take one policy, as Kafka's
/// `auto.offset.reset` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoOffsetReset {
    /// Start from the oldest record still retained. Reprocesses history;
    /// never silently skips records.
    #[default]
    Earliest,
    /// Start from the end. Skips whatever was missed; never reprocesses.
    Latest,
    /// Refuse to guess and surface [`ClientError::NoOffsetForPartition`].
    ///
    /// The honest choice when neither reprocessing nor skipping is safe —
    /// a consumer that must not double-count and must not miss data needs
    /// a human to decide, and this is what makes that decision reachable
    /// instead of silently made for it.
    None,
}
/// Partition assignment strategy used by the group leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignor {
    /// Contiguous partition ranges per topic across members sorted by id.
    Range,
    /// Partitions dealt one at a time across members sorted by id.
    RoundRobin,
    /// Keep members on the partitions they already hold, moving only what
    /// rebalancing actually requires. Prefer this when consumers carry
    /// per-partition state, because every partition that moves throws that
    /// state away.
    Sticky,
    /// Sticky, and incremental: a partition that has to move is withheld
    /// for one round so its current owner can revoke just that partition
    /// while continuing to consume the rest.
    ///
    /// Costs one extra rebalance round and removes the stop-the-world
    /// pause, which is the trade that matters once a group is large
    /// enough that a full revoke-and-rejoin is felt downstream.
    CooperativeSticky,
}

impl Assignor {
    /// Compute each member's assignment. `members` is `(member_id,
    /// subscribed_topics)`; `topic_partitions` maps each subscribed topic to
    /// its sorted partition ids; `previous` is what each member currently
    /// holds. Every member appears in the result, possibly with an empty
    /// assignment.
    ///
    /// `previous` is ignored by the stateless strategies, which is the
    /// point of passing it uniformly: the caller does not have to know
    /// which strategy needs history.
    fn assign(
        &self,
        members: &[(String, Vec<String>)],
        topic_partitions: &BTreeMap<String, Vec<i32>>,
        previous: &BTreeMap<String, Vec<TopicPartition>>,
    ) -> BTreeMap<String, Vec<TopicPartition>> {
        match self {
            Assignor::Range => range_assign(members, topic_partitions),
            Assignor::RoundRobin => roundrobin_assign(members, topic_partitions),
            Assignor::Sticky => sticky_assign(members, topic_partitions, previous),
            // Cooperative computes the same sticky target, then withholds
            // whatever is still held elsewhere so it can be revoked before
            // it is handed over.
            Assignor::CooperativeSticky => {
                let target = sticky_assign(members, topic_partitions, previous);
                withhold_moving_partitions(&target, previous)
            }
        }
    }
}

fn empty_assignment(members: &[(String, Vec<String>)]) -> BTreeMap<String, Vec<TopicPartition>> {
    members
        .iter()
        .map(|(member_id, _)| (member_id.clone(), Vec::new()))
        .collect()
}

/// Per topic, subscribed members (sorted by id) take contiguous partition
/// ranges; the first `partitions % members` members take one extra.
fn range_assign(
    members: &[(String, Vec<String>)],
    topic_partitions: &BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    let mut assignment = empty_assignment(members);
    for (topic, partitions) in topic_partitions {
        let mut subscribers: Vec<&str> = members
            .iter()
            .filter(|(_, topics)| topics.iter().any(|t| t == topic))
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        subscribers.sort_unstable();
        if subscribers.is_empty() {
            continue;
        }
        let base = partitions.len() / subscribers.len();
        let extra = partitions.len() % subscribers.len();
        let mut cursor = 0;
        for (index, member_id) in subscribers.iter().enumerate() {
            let count = base + usize::from(index < extra);
            for partition in &partitions[cursor..cursor + count] {
                assignment
                    .get_mut(*member_id)
                    .expect("member present")
                    .push((topic.clone(), *partition));
            }
            cursor += count;
        }
    }
    assignment
}

/// The union of all subscribed topic-partitions (sorted by topic, then
/// partition) is dealt around the circle of members sorted by id, skipping
/// members not subscribed to a partition's topic.
fn roundrobin_assign(
    members: &[(String, Vec<String>)],
    topic_partitions: &BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    let mut assignment = empty_assignment(members);
    let mut circle: Vec<&(String, Vec<String>)> = members.iter().collect();
    circle.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    if circle.is_empty() {
        return assignment;
    }
    let mut next = 0usize;
    for (topic, partitions) in topic_partitions {
        for partition in partitions {
            let start = next;
            loop {
                let member = circle[next % circle.len()];
                next += 1;
                if member.1.iter().any(|t| t == topic) {
                    assignment
                        .get_mut(&member.0)
                        .expect("member present")
                        .push((topic.clone(), *partition));
                    break;
                }
                if next - start >= circle.len() {
                    break; // no member subscribes to this topic
                }
            }
        }
    }
    assignment
}

/// Keep every member on the partitions it already holds, and move only what
/// balance actually requires.
///
/// `previous` is what each member holds now. The result differs from it by
/// the minimum needed to reach a balanced, valid assignment:
///
/// 1. A member keeps a partition only if it is still subscribed to that
///    topic and the partition still exists — a partition whose topic the
///    member dropped, or that was deleted, cannot be kept.
/// 2. A member holding more than its fair share gives up the excess.
/// 3. Everything unclaimed is dealt to whoever is under quota.
///
/// The reason to prefer this over range/roundrobin is not elegance: every
/// partition that moves costs the new owner a seek and the old owner a
/// discarded fetch buffer, and any consumer with per-partition local state
/// has to rebuild it. Recomputing from scratch reshuffles nearly everything
/// on a change that should have touched one member's share.
fn sticky_assign(
    members: &[(String, Vec<String>)],
    topic_partitions: &BTreeMap<String, Vec<i32>>,
    previous: &BTreeMap<String, Vec<TopicPartition>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    let mut assignment = empty_assignment(members);
    if members.is_empty() {
        return assignment;
    }

    let subscribes = |member_id: &str, topic: &str| -> bool {
        members
            .iter()
            .find(|(id, _)| id == member_id)
            .is_some_and(|(_, topics)| topics.iter().any(|t| t == topic))
    };

    // Every partition that needs an owner, and who currently has a valid
    // claim on it.
    let mut unassigned: Vec<TopicPartition> = Vec::new();
    let mut claimed: BTreeMap<TopicPartition, String> = BTreeMap::new();
    for (topic, partitions) in topic_partitions {
        for partition in partitions {
            let tp = (topic.clone(), *partition);
            let holder = previous
                .iter()
                .find(|(member_id, held)| held.contains(&tp) && subscribes(member_id, topic));
            match holder {
                Some((member_id, _)) => {
                    claimed.insert(tp, member_id.clone());
                }
                None => unassigned.push(tp),
            }
        }
    }

    // Fair share: the members subscribed to at least one live topic split
    // the partitions, and the remainder means some may hold one extra.
    let eligible: Vec<&String> = members
        .iter()
        .filter(|(_, topics)| topics.iter().any(|t| topic_partitions.contains_key(t)))
        .map(|(member_id, _)| member_id)
        .collect();
    if eligible.is_empty() {
        return assignment;
    }
    let total: usize = topic_partitions.values().map(Vec::len).sum();
    let base = total / eligible.len();
    let extra = total % eligible.len();
    // Sorted so the "who gets the extra one" decision is deterministic
    // across members computing it independently.
    let mut quota: BTreeMap<&String, usize> = BTreeMap::new();
    for (index, member_id) in eligible.iter().enumerate() {
        quota.insert(*member_id, base + usize::from(index < extra));
    }

    // Honour existing claims up to each member's quota; the overflow joins
    // the pool. Sorted for the same determinism reason.
    let mut kept: BTreeMap<String, Vec<TopicPartition>> = BTreeMap::new();
    for (tp, member_id) in claimed {
        let held = kept.entry(member_id.clone()).or_default();
        if held.len() < quota.get(&member_id).copied().unwrap_or(0) {
            held.push(tp);
        } else {
            unassigned.push(tp);
        }
    }

    for (member_id, held) in kept {
        if let Some(slot) = assignment.get_mut(&member_id) {
            *slot = held;
        }
    }

    // Deal the rest to whoever is still under quota and subscribed.
    unassigned.sort();
    for tp in unassigned {
        let taker = eligible.iter().find(|member_id| {
            subscribes(member_id, &tp.0)
                && assignment.get(**member_id).map_or(0, Vec::len)
                    < quota.get(**member_id).copied().unwrap_or(0)
        });
        // If quotas are exhausted (possible when subscriptions are uneven),
        // fall back to any subscribed member rather than dropping the
        // partition — an unassigned partition is a stalled partition.
        let taker = taker.or_else(|| {
            eligible
                .iter()
                .find(|member_id| subscribes(member_id, &tp.0))
        });
        if let Some(member_id) = taker {
            assignment
                .get_mut(*member_id)
                .expect("member present")
                .push(tp);
        }
    }

    for held in assignment.values_mut() {
        held.sort();
    }
    assignment
}

/// Withhold, from a computed assignment, every partition that another
/// member still holds — the cooperative half of incremental rebalancing
/// (KIP-429).
///
/// Eager rebalancing stops the world: every member revokes everything and
/// waits for a new assignment, so a group of a hundred consumers pauses
/// entirely because one joined. Cooperative rebalancing instead moves a
/// partition in two steps. In the first, a member that is losing a
/// partition simply is not given it, so it revokes only that one and keeps
/// consuming everything else. In the second — triggered because the
/// assignment changed — the partition is genuinely free and goes to its
/// new owner.
///
/// The cost is one extra rebalance round. The benefit is that a member
/// keeping a partition never stops consuming it, which is what makes a
/// rolling deploy something other than an outage.
fn withhold_moving_partitions(
    target: &BTreeMap<String, Vec<TopicPartition>>,
    previous: &BTreeMap<String, Vec<TopicPartition>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    // Who holds what right now, so "still owned by someone else" is a
    // lookup rather than a scan per partition.
    let mut current_owner: BTreeMap<&TopicPartition, &String> = BTreeMap::new();
    for (member_id, held) in previous {
        for slot in held {
            current_owner.insert(slot, member_id);
        }
    }

    target
        .iter()
        .map(|(member_id, wanted)| {
            let granted = wanted
                .iter()
                .filter(|slot| match current_owner.get(slot) {
                    // Held by someone else: withhold it this round so that
                    // member can revoke it cleanly first.
                    Some(owner) => *owner == member_id,
                    // Held by nobody — a new partition, or one already
                    // revoked in an earlier round. Safe to assign now.
                    None => true,
                })
                .cloned()
                .collect();
            (member_id.clone(), granted)
        })
        .collect()
}
/// One consumed record with its topic-partition and offset.
#[derive(Debug)]
pub struct ConsumedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    /// Absolute create time in unix milliseconds, already resolved against
    /// the batch base so a caller never has to know the batch existed.
    pub timestamp: i64,
    pub headers: Vec<RecordHeader>,
}

impl ConsumedRecord {
    /// The first value stored under `key`, if any.
    pub fn header(&self, key: &str) -> Option<&Bytes> {
        self.headers
            .iter()
            .find(|header| header.key == key)
            .and_then(|header| header.value.as_ref())
    }
}

#[derive(Debug, Default)]
struct Membership {
    member_id: String,
    generation: i32,
    joined: bool,
}

/// Cloneable handle that routes group requests to the group's coordinator.
#[derive(Clone)]
pub(crate) struct GroupCoordinator {
    pub(crate) router: BrokerRouter,
    pub(crate) group_id: String,
}

impl GroupCoordinator {
    /// Coordinator partition: `crc32c(group_id) % partitions(__consumer_offsets)`.
    pub(crate) async fn partition(&self) -> Result<i32, ClientError> {
        let partitions = self.router.partitions(OFFSETS_TOPIC).await?;
        let count = u32::try_from(partitions.len())
            .map_err(|_| ClientError::Configuration("too many offsets partitions".into()))?;
        Ok((crc32c::crc32c(self.group_id.as_bytes()) % count) as i32)
    }

    /// Send one group request to the coordinator, following NOT_COORDINATOR /
    /// NOT_LEADER_OR_FOLLOWER moves (after a metadata refresh) and waiting out
    /// COORDINATOR_LOAD_IN_PROGRESS. Any other non-zero code becomes a typed
    /// `ClientError` for the caller to interpret.
    pub(crate) async fn request<T>(
        &self,
        api_key: ApiKey,
        body: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, ClientError>,
        error_code: impl Fn(&T) -> i32,
    ) -> Result<T, ClientError> {
        for attempt in 0..COORDINATOR_ATTEMPTS {
            let partition = self.partition().await?;
            let response = self
                .router
                .request_partition(OFFSETS_TOPIC, partition, api_key, body)
                .await?;
            let parsed = decode(&response)?;
            match error_code(&parsed) {
                ec::NONE => return Ok(parsed),
                ec::NOT_COORDINATOR | ec::NOT_LEADER_OR_FOLLOWER
                    if attempt + 1 < COORDINATOR_ATTEMPTS =>
                {
                    let _ = self.router.refresh_topic(OFFSETS_TOPIC).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                ec::COORDINATOR_LOAD_IN_PROGRESS if attempt + 1 < COORDINATOR_ATTEMPTS => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                other => ClientError::from_error_code(other)?,
            }
        }
        unreachable!("the last attempt returns or fails")
    }

    async fn join(
        &self,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: &str,
        subscription_topics: &[String],
        group_instance_id: &str,
    ) -> Result<JoinGroupResponse, ClientError> {
        let request = JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id: member_id.to_owned(),
            subscription_topics: subscription_topics.to_vec(),
            group_instance_id: group_instance_id.to_owned(),
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::JoinGroup,
            &body,
            |bytes| JoinGroupResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn sync(
        &self,
        generation: i32,
        member_id: &str,
        assignments: Vec<MemberAssignment>,
    ) -> Result<SyncGroupResponse, ClientError> {
        let request = SyncGroupRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
            assignments,
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::SyncGroup,
            &body,
            |bytes| SyncGroupResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn heartbeat(
        &self,
        generation: i32,
        member_id: &str,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = HeartbeatRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::Heartbeat,
            &body,
            |bytes| HeartbeatResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    /// Tell the coordinator this member is going away.
    ///
    /// Best effort by nature: the caller is shutting down, so a failure
    /// here costs only the session timeout it was trying to avoid, and
    /// must never turn a clean close into an error.
    async fn leave(&self, member_id: &str) -> Result<LeaveGroupResponse, ClientError> {
        let request = LeaveGroupRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.to_owned(),
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::LeaveGroup,
            &body,
            |bytes| LeaveGroupResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }
    async fn commit(
        &self,
        generation: i32,
        member_id: &str,
        offsets: Vec<OffsetCommitEntry>,
    ) -> Result<OffsetCommitResponse, ClientError> {
        let request = OffsetCommitRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
            offsets,
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::OffsetCommit,
            &body,
            |bytes| OffsetCommitResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn fetch_offsets(
        &self,
        partitions: Vec<AssignedPartition>,
    ) -> Result<Vec<OffsetFetchEntry>, ClientError> {
        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            partitions,
        };
        let body = request.encode().map_err(msg_err)?;
        let response = self
            .request(
                ApiKey::OffsetFetch,
                &body,
                |bytes| OffsetFetchResponse::decode(bytes).map_err(msg_err),
                |response| response.error_code,
            )
            .await?;
        Ok(response.offsets)
    }
}

/// Commit current positions with the current membership. Membership errors
/// mean a rebalance is needed: flag it and let the next poll rejoin and
/// recommit instead of failing the caller.
async fn commit_snapshot(
    coordinator: &GroupCoordinator,
    membership: &Arc<Mutex<Membership>>,
    positions: &Positions,
    rejoin: &AtomicBool,
) -> Result<(), ClientError> {
    let (generation, member_id, joined) = {
        let membership = membership.lock().expect("membership");
        (
            membership.generation,
            membership.member_id.clone(),
            membership.joined,
        )
    };
    if !joined {
        return Ok(());
    }
    let offsets: Vec<OffsetCommitEntry> = {
        let positions = positions.lock().expect("positions");
        positions
            .iter()
            .map(|((topic, partition), offset)| OffsetCommitEntry {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            })
            .collect()
    };
    if offsets.is_empty() {
        return Ok(());
    }
    match coordinator.commit(generation, &member_id, offsets).await {
        Ok(_) => Ok(()),
        Err(ClientError::Server {
            code: ec::UNKNOWN_MEMBER_ID | ec::ILLEGAL_GENERATION | ec::REBALANCE_IN_PROGRESS,
            ..
        }) => {
            rejoin.store(true, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// A consumer-group consumer: joins a group, polls its assigned partitions,
/// heartbeats and commits offsets in the background.
pub struct GroupConsumer {
    coordinator: GroupCoordinator,
    consumer: Consumer,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    assignor: Assignor,
    auto_commit: Option<Duration>,
    /// What to do when a partition has no valid position; see [`AutoOffsetReset`].
    auto_offset_reset: AutoOffsetReset,
    /// Longest gap between `poll` calls before this member is presumed
    /// stuck (`max.poll.interval.ms`).
    /// Stable identity across restarts (`group.instance.id`), empty for a
    /// dynamic member.
    group_instance_id: String,
    max_poll_interval: Duration,
    /// When `poll` was last called, shared with the heartbeat task.
    ///
    /// Liveness has two independent questions — "is the process alive"
    /// (heartbeats) and "is the application still consuming" (this) — and
    /// conflating them is what lets a consumer wedged in a slow handler
    /// keep its partitions indefinitely while faithfully heartbeating.
    last_poll_ms: Arc<AtomicI64>,
    max_poll_records: usize,
    subscribed: Vec<String>,
    membership: Arc<Mutex<Membership>>,
    /// Next offset to *deliver* per partition — what gets committed. Only
    /// advances over records handed to the caller.
    positions: Positions,
    /// Next offset to *fetch* per partition. Runs ahead of `positions` by
    /// exactly the records sitting in `buffered`.
    fetch_positions: BTreeMap<TopicPartition, i64>,
    /// Records fetched but not yet returned by [`GroupConsumer::poll`].
    buffered: VecDeque<ConsumedRecord>,
    rejoin: Arc<AtomicBool>,
    assignment: Vec<TopicPartition>,
    assignment_version: Arc<AtomicU64>,
    heartbeat_task: Option<JoinHandle<()>>,
    auto_commit_task: Option<JoinHandle<()>>,
}

impl GroupConsumer {
    pub async fn connect(
        addr: SocketAddr,
        client_id: &str,
        group_id: &str,
    ) -> Result<GroupConsumer, ClientError> {
        GroupConsumer::connect_with(Transport::default(), addr, client_id, group_id).await
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        client_id: &str,
        group_id: &str,
    ) -> Result<GroupConsumer, ClientError> {
        let router =
            BrokerRouter::connect_with(transport, addr, Some(client_id.to_owned()), 5).await?;
        let consumer = Consumer::from_router(router.clone(), 8 * 1024 * 1024);
        let group = GroupConsumer {
            coordinator: GroupCoordinator {
                router,
                group_id: group_id.to_owned(),
            },
            consumer,
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 3_000,
            assignor: Assignor::Range,
            auto_commit: Some(Duration::from_secs(5)),
            auto_offset_reset: AutoOffsetReset::default(),
            // Kafka's default. Long enough that a slow batch handler is
            // not mistaken for a stuck one, short enough that a genuinely
            // wedged consumer releases its partitions the same day.
            group_instance_id: String::new(),
            max_poll_interval: Duration::from_millis(300_000),
            last_poll_ms: Arc::new(AtomicI64::new(now_ms())),
            max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            subscribed: Vec::new(),
            membership: Arc::new(Mutex::new(Membership::default())),
            positions: Arc::new(Mutex::new(BTreeMap::new())),
            fetch_positions: BTreeMap::new(),
            buffered: VecDeque::new(),
            rejoin: Arc::new(AtomicBool::new(false)),
            assignment: Vec::new(),
            assignment_version: Arc::new(AtomicU64::new(0)),
            heartbeat_task: None,
            auto_commit_task: None,
        };
        Ok(group)
    }

    /// Give this consumer a stable identity across restarts
    /// (`group.instance.id`, KIP-345).
    ///
    /// A static member that restarts reclaims its own member slot and its
    /// partitions rather than arriving as a stranger. That turns a rolling
    /// restart from two rebalances per instance — one when it leaves, one
    /// when it returns — into none, which is the difference between a
    /// deploy that pauses consumption and one that does not.
    ///
    /// The id must be unique within the group and stable for the life of
    /// the instance; a duplicate would have two processes claiming one
    /// member slot.
    pub fn with_group_instance_id(mut self, group_instance_id: impl Into<String>) -> Self {
        self.group_instance_id = group_instance_id.into();
        self
    }
    /// Longest gap between `poll` calls before this member gives up its
    /// partitions (`max.poll.interval.ms`).
    ///
    /// Separate from `session.timeout.ms` on purpose: heartbeats prove the
    /// process is alive, this proves the application is still consuming.
    /// A consumer stuck in a slow handler answers the first question
    /// perfectly while making no progress at all, and only this releases
    /// its partitions to a member that can.
    pub fn with_max_poll_interval_ms(mut self, max_poll_interval_ms: u64) -> Self {
        self.max_poll_interval = Duration::from_millis(max_poll_interval_ms.max(1));
        self
    }
    /// What to do when a partition has no valid position — never committed,
    /// or committed then aged off the log (`auto.offset.reset`).
    pub fn with_auto_offset_reset(mut self, policy: AutoOffsetReset) -> Self {
        self.auto_offset_reset = policy;
        self
    }

    /// Broker-side session timeout; heartbeats go out every timeout/3.
    pub fn with_session_timeout(mut self, session_timeout_ms: i32) -> Self {
        self.session_timeout_ms = session_timeout_ms;
        self
    }

    /// Maximum time the coordinator waits for members to rejoin.
    pub fn with_rebalance_timeout(mut self, rebalance_timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = rebalance_timeout_ms;
        self
    }

    /// Assignment strategy this member uses when it leads a rebalance.
    pub fn with_assignor(mut self, assignor: Assignor) -> Self {
        self.assignor = assignor;
        self
    }

    /// Background commit interval; `None` disables auto-commit (default 5s).
    pub fn with_auto_commit(mut self, interval: Option<Duration>) -> Self {
        self.auto_commit = interval;
        self
    }

    /// Maximum records one [`GroupConsumer::poll`] returns
    /// (`max.poll.records`, default 500). Records fetched beyond the cap
    /// stay buffered and are returned by later polls — the consumed
    /// position, and therefore every commit, only ever covers records the
    /// caller has actually received.
    pub fn with_max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.set_max_poll_records(max_poll_records);
        self
    }

    /// Adjust `max.poll.records` between polls — a bounded reader shrinks it
    /// to its remaining budget so the final poll cannot hand back (and
    /// therefore consume) more records than the caller will process.
    pub fn set_max_poll_records(&mut self, max_poll_records: usize) {
        self.max_poll_records = max_poll_records.max(1);
    }

    /// Cap on response batch bytes per fetch.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.consumer = Consumer::from_router(self.coordinator.router.clone(), max_bytes);
        self
    }

    /// Set the subscription; the next [`GroupConsumer::poll`] (re)joins the
    /// group with it.
    pub fn subscribe(&mut self, topics: &[&str]) {
        self.subscribed = topics.iter().map(|topic| (*topic).to_owned()).collect();
        {
            let mut membership = self.membership.lock().expect("membership");
            membership.member_id = String::new();
            membership.joined = false;
        }
        self.rejoin.store(true, Ordering::Relaxed);
    }

    /// Current partition assignment, sorted by (topic, partition).
    pub fn assignment(&self) -> &[(String, i32)] {
        &self.assignment
    }

    /// Member id assigned by the coordinator (empty before the first join).
    pub fn member_id(&self) -> String {
        self.membership
            .lock()
            .expect("membership")
            .member_id
            .clone()
    }

    /// Current group generation (0 before the first join).
    pub fn generation(&self) -> i32 {
        self.membership.lock().expect("membership").generation
    }

    /// Bumped every time [`GroupConsumer::assignment`] changes.
    pub fn assignment_version(&self) -> u64 {
        self.assignment_version.load(Ordering::Relaxed)
    }

    /// Fetch records from all assigned partitions, waiting up to
    /// `max_wait` when caught up. Rejoins the group first when a heartbeat
    /// or commit signalled a rebalance. Returns at most `max.poll.records`;
    /// anything fetched beyond that stays buffered for the next poll and is
    /// *not* counted as consumed.
    pub async fn poll(&mut self, max_wait: Duration) -> Result<Vec<ConsumedRecord>, ClientError> {
        if self.subscribed.is_empty() {
            return Err(ClientError::Configuration(
                "subscribe to at least one topic before polling".into(),
            ));
        }
        // Stamped on entry, not on return: the interval bounds how long the
        // *application* may go without asking for records, and a poll that
        // blocks for its full `max_wait` is the consumer working normally,
        // not stalling.
        self.last_poll_ms.store(now_ms(), Ordering::Relaxed);
        if !self.is_joined() || self.rejoin.load(Ordering::Relaxed) {
            tracing::debug!(member = %self.membership.lock().expect("membership").member_id, "poll triggers (re)join");
            self.join().await?;
        }

        let deadline = Instant::now() + max_wait;
        loop {
            if !self.buffered.is_empty() {
                return Ok(self.take_buffered());
            }
            // One request per broker for the whole assignment, instead of
            // one per partition: per-poll latency stops scaling with the
            // number of partitions this member owns.
            let requests: Vec<(String, i32, i64)> = self
                .assignment
                .iter()
                .map(|(topic, partition)| {
                    let position = self
                        .fetch_positions
                        .get(&(topic.clone(), *partition))
                        .copied()
                        .unwrap_or(0);
                    (topic.clone(), *partition, position)
                })
                .collect();
            let wait_ms = i32::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis()
                    .min(500),
            )
            .unwrap_or(i32::MAX);

            for fetched in self.consumer.fetch_many(&requests, wait_ms).await? {
                match fetched.error_code {
                    ec::NONE => {
                        for record in fetched.records {
                            self.fetch_positions.insert(
                                (fetched.topic.clone(), fetched.partition),
                                record.offset + 1,
                            );
                            self.buffered.push_back(ConsumedRecord {
                                topic: fetched.topic.clone(),
                                partition: fetched.partition,
                                offset: record.offset,
                                key: record.key,
                                value: record.value,
                                timestamp: record.timestamp,
                                headers: record.headers,
                            });
                        }
                    }
                    ec::OFFSET_OUT_OF_RANGE => {
                        // The committed offset fell off the log: restart
                        // where the policy says, dropping anything buffered
                        // for the partition.
                        let earliest = self.reset_offset(&fetched.topic, fetched.partition).await?;
                        self.buffered.retain(|record| {
                            record.topic != fetched.topic || record.partition != fetched.partition
                        });
                        self.fetch_positions
                            .insert((fetched.topic.clone(), fetched.partition), earliest);
                        self.positions
                            .lock()
                            .expect("positions")
                            .insert((fetched.topic, fetched.partition), earliest);
                    }
                    ec::NOT_LEADER_OR_FOLLOWER => {
                        // Leadership moved mid-poll; refresh routes and let
                        // the next sweep pick up the new leader.
                        let _ = self.consumer.refresh_topic(&fetched.topic).await;
                    }
                    code => ClientError::from_error_code(code)?,
                }
            }
            if !self.buffered.is_empty() {
                return Ok(self.take_buffered());
            }
            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }
            tokio::time::sleep(POLL_IDLE_DELAY).await;
        }
    }

    /// Hand out up to `max.poll.records` buffered records, advancing the
    /// consumed position over exactly those records.
    fn take_buffered(&mut self) -> Vec<ConsumedRecord> {
        let count = self.buffered.len().min(self.max_poll_records);
        let records: Vec<ConsumedRecord> = self.buffered.drain(..count).collect();
        let mut positions = self.positions.lock().expect("positions");
        for record in &records {
            positions.insert((record.topic.clone(), record.partition), record.offset + 1);
        }
        records
    }

    /// Commit current positions for the assigned partitions now.
    pub async fn commit_sync(&self) -> Result<(), ClientError> {
        commit_snapshot(
            &self.coordinator,
            &self.membership,
            &self.positions,
            &self.rejoin,
        )
        .await
    }

    /// Commit current positions, then leave the group so its partitions
    /// move immediately, then stop the background tasks.
    ///
    /// Leaving is what separates a clean shutdown from a crash. Without it
    /// the coordinator cannot tell the difference and must wait out
    /// `session.timeout.ms` before reassigning — so a rolling restart of N
    /// instances costs N session timeouts of stalled partitions for no
    /// reason. Dropping a `GroupConsumer` cannot do this (no async in
    /// `Drop`), so a caller that cares about handover latency closes
    /// explicitly, exactly as Kafka's own consumer requires.
    ///
    /// A failed leave is not an error: the group still converges via the
    /// timeout, and the commit that precedes it is the part that matters
    /// for correctness.
    pub async fn close(mut self) -> Result<(), ClientError> {
        let result = self.commit_sync().await;

        let member_id = self
            .membership
            .lock()
            .expect("membership")
            .member_id
            .clone();
        if !member_id.is_empty() {
            if let Err(error) = self.coordinator.leave(&member_id).await {
                tracing::debug!(%error, "leave-group failed; falling back to session timeout");
            }
        }
        // Stop heartbeating before returning, or the background task can
        // re-register the member we just removed.
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
        if let Some(task) = self.auto_commit_task.take() {
            task.abort();
        }
        result
    }

    /// Resolve the start offset for a partition with no usable position,
    /// per [`AutoOffsetReset`].
    ///
    /// Both callers — a first assignment with nothing committed, and a
    /// committed offset that retention has deleted — route through here so
    /// the policy cannot be honoured in one place and ignored in the other.
    async fn reset_offset(&self, topic: &str, partition: i32) -> Result<i64, ClientError> {
        match self.auto_offset_reset {
            AutoOffsetReset::Earliest => {
                self.consumer.list_offsets(topic, partition, EARLIEST).await
            }
            AutoOffsetReset::Latest => self.consumer.list_offsets(topic, partition, LATEST).await,
            AutoOffsetReset::None => Err(ClientError::NoOffsetForPartition {
                topic: topic.to_owned(),
                partition,
            }),
        }
    }
    fn is_joined(&self) -> bool {
        self.membership.lock().expect("membership").joined
    }

    /// Join + sync until the group hands this member an assignment.
    async fn join(&mut self) -> Result<(), ClientError> {
        for _ in 0..JOIN_ATTEMPTS {
            let member_id = self
                .membership
                .lock()
                .expect("membership")
                .member_id
                .clone();
            let join = match self
                .coordinator
                .join(
                    self.session_timeout_ms,
                    self.rebalance_timeout_ms,
                    &member_id,
                    &self.subscribed,
                    &self.group_instance_id,
                )
                .await
            {
                Ok(response) => response,
                Err(ClientError::Server { code, .. }) if code == ec::UNKNOWN_MEMBER_ID => {
                    // Stale member id: rejoin as a brand-new member.
                    self.membership.lock().expect("membership").member_id = String::new();
                    continue;
                }
                Err(error) => return Err(error),
            };
            {
                let mut membership = self.membership.lock().expect("membership");
                membership.member_id = join.member_id.clone();
                membership.generation = join.generation;
            }
            tracing::debug!(member = %join.member_id, gen = join.generation, leader = %join.leader_member_id, "joined group");

            let assignments = if join.member_id == join.leader_member_id {
                self.leader_assignments(&join.members).await?
            } else {
                Vec::new()
            };
            let sync = match self
                .coordinator
                .sync(join.generation, &join.member_id, assignments)
                .await
            {
                Ok(response) => response,
                Err(ClientError::Server { code, .. })
                    if code == ec::REBALANCE_IN_PROGRESS
                        || code == ec::UNKNOWN_MEMBER_ID
                        || code == ec::ILLEGAL_GENERATION =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };

            let assignment: Vec<TopicPartition> = sync
                .assignment
                .iter()
                .map(|assigned| (assigned.topic.clone(), assigned.partition))
                .collect();
            tracing::debug!(member = %join.member_id, gen = join.generation, ?assignment, "synced assignment");
            self.install_assignment(assignment).await?;
            self.membership.lock().expect("membership").joined = true;
            self.rejoin.store(false, Ordering::Relaxed);
            self.spawn_heartbeat();
            self.spawn_auto_commit();
            return Ok(());
        }
        Err(ClientError::Configuration(format!(
            "consumer group failed to stabilize after {JOIN_ATTEMPTS} join attempts"
        )))
    }

    /// Leader side of SyncGroup: assign the union of subscribed
    /// topic-partitions over the member list with the configured assignor.
    async fn leader_assignments(
        &self,
        members: &[GroupMemberInfo],
    ) -> Result<Vec<MemberAssignment>, ClientError> {
        let mut topic_partitions = BTreeMap::new();
        for topic in members
            .iter()
            .flat_map(|member| member.subscription_topics.iter())
        {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                topic_partitions.entry(topic.clone())
            {
                entry.insert(self.coordinator.router.partitions(topic).await?);
            }
        }
        let member_list: Vec<(String, Vec<String>)> = members
            .iter()
            .map(|member| (member.member_id.clone(), member.subscription_topics.clone()))
            .collect();
        // What each member holds going in, as the coordinator reported it.
        // A sticky assignor measures movement against this; the others
        // ignore it.
        let previous: BTreeMap<String, Vec<TopicPartition>> = members
            .iter()
            .map(|member| {
                (
                    member.member_id.clone(),
                    member
                        .assignment
                        .iter()
                        .map(|held| (held.topic.clone(), held.partition))
                        .collect(),
                )
            })
            .collect();
        Ok(self
            .assignor
            .assign(&member_list, &topic_partitions, &previous)
            .into_iter()
            .map(|(member_id, partitions)| MemberAssignment {
                member_id,
                partitions: partitions
                    .into_iter()
                    .map(|(topic, partition)| AssignedPartition { topic, partition })
                    .collect(),
            })
            .collect())
    }

    /// Adopt a new assignment: drop revoked positions, seed new partitions
    /// from the group's committed offset (or the partition's earliest).
    async fn install_assignment(
        &mut self,
        assignment: Vec<TopicPartition>,
    ) -> Result<(), ClientError> {
        let mut assignment = assignment;
        assignment.sort();

        let needed: Vec<TopicPartition> = {
            let positions = self.positions.lock().expect("positions");
            assignment
                .iter()
                .filter(|tp| !positions.contains_key(*tp))
                .cloned()
                .collect()
        };
        let committed: HashMap<TopicPartition, i64> = if needed.is_empty() {
            HashMap::new()
        } else {
            let partitions = needed
                .iter()
                .map(|(topic, partition)| AssignedPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect();
            self.coordinator
                .fetch_offsets(partitions)
                .await?
                .into_iter()
                .map(|entry| ((entry.topic, entry.partition), entry.offset))
                .collect()
        };
        let mut seeded = Vec::with_capacity(needed.len());
        for (topic, partition) in needed {
            let offset = match committed.get(&(topic.clone(), partition)) {
                Some(&offset) if offset >= 0 => offset,
                // Never committed, or committed a negative sentinel:
                // there is no position to resume from.
                _ => self.reset_offset(&topic, partition).await?,
            };
            seeded.push(((topic, partition), offset));
        }

        {
            let mut positions = self.positions.lock().expect("positions");
            positions.retain(|tp, _| assignment.binary_search(tp).is_ok());
            for (tp, offset) in seeded {
                positions.insert(tp, offset);
            }
            // Buffered records sit ahead of the consumed position and were
            // never delivered, so a new assignment simply drops them: fetch
            // resumes from the consumed position on partitions still owned.
            self.buffered.clear();
            self.fetch_positions = positions.clone();
        }
        // Cooperative rebalancing moves a partition in two rounds: this
        // one withheld it so the losing member could revoke it cleanly, and
        // a second is needed to hand it to its new owner. Nothing else will
        // trigger that round — the coordinator sees a completed rebalance —
        // so the member that just gave something up asks for it.
        //
        // Only on an actual loss. Rejoining because the assignment merely
        // *changed* would loop forever, since gaining a partition is also a
        // change.
        let revoked = self.assignor == Assignor::CooperativeSticky
            && self
                .assignment
                .iter()
                .any(|held| !assignment.contains(held));

        if self.assignment != assignment {
            self.assignment = assignment;
            self.assignment_version.fetch_add(1, Ordering::Relaxed);
        }
        if revoked {
            tracing::debug!("revoked partitions cooperatively; rejoining to place them");
            self.rejoin.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn spawn_heartbeat(&mut self) {
        if self.heartbeat_task.is_some() {
            return;
        }
        let coordinator = self.coordinator.clone();
        let membership = Arc::clone(&self.membership);
        let rejoin = Arc::clone(&self.rejoin);
        let last_poll_ms = Arc::clone(&self.last_poll_ms);
        let max_poll_interval = self.max_poll_interval;
        // This loop enforces two independent deadlines, so it has to wake
        // often enough for the shorter of them. Deriving the tick from the
        // session timeout alone means a consumer with a long session and a
        // short poll interval — a perfectly ordinary combination — would
        // not be checked for a stalled poll until long after it stalled.
        let heartbeat_every = u64::try_from(self.session_timeout_ms / 3)
            .unwrap_or(1)
            .max(1);
        let poll_check_every = (self.max_poll_interval.as_millis() as u64 / 3).max(1);
        let interval = Duration::from_millis(heartbeat_every.min(poll_check_every));
        self.heartbeat_task = Some(tokio::spawn(async move {
            // Once the poll interval is breached the member leaves, and it
            // must not keep leaving on every subsequent tick.
            let mut left_for_slow_poll = false;
            loop {
                tokio::time::sleep(interval).await;
                let (generation, member_id, joined) = {
                    let membership = membership.lock().expect("membership");
                    (
                        membership.generation,
                        membership.member_id.clone(),
                        membership.joined,
                    )
                };
                if !joined {
                    continue;
                }

                // `max.poll.interval.ms`: the application has stopped
                // consuming even though the process is alive. Continuing to
                // heartbeat would assert a liveness this member no longer
                // has, holding its partitions away from a consumer that
                // could actually make progress. Leaving explicitly hands
                // them over now instead of after a session timeout.
                let idle_ms = now_ms().saturating_sub(last_poll_ms.load(Ordering::Relaxed));
                if idle_ms >= max_poll_interval.as_millis() as i64 {
                    if !left_for_slow_poll {
                        tracing::warn!(
                            %member_id,
                            idle_ms,
                            "no poll within max.poll.interval.ms; leaving the group"
                        );
                        let _ = coordinator.leave(&member_id).await;
                        left_for_slow_poll = true;
                        // The next poll must rejoin rather than resume as a
                        // member the coordinator has already removed.
                        membership.lock().expect("membership").joined = false;
                        rejoin.store(true, Ordering::Relaxed);
                    }
                    continue;
                }
                left_for_slow_poll = false;

                match coordinator.heartbeat(generation, &member_id).await {
                    Ok(_) => {}
                    Err(ClientError::Server {
                        code:
                            ec::REBALANCE_IN_PROGRESS | ec::UNKNOWN_MEMBER_ID | ec::ILLEGAL_GENERATION,
                        ..
                    }) => {
                        tracing::debug!(%member_id, generation, "heartbeat demands rejoin");
                        rejoin.store(true, Ordering::Relaxed);
                    }
                    Err(_) => {} // transient failure: retry next tick
                }
            }
        }));
    }

    fn spawn_auto_commit(&mut self) {
        if self.auto_commit_task.is_some() {
            return;
        }
        let Some(interval) = self.auto_commit else {
            return;
        };
        let coordinator = self.coordinator.clone();
        let membership = Arc::clone(&self.membership);
        let positions = Arc::clone(&self.positions);
        let rejoin = Arc::clone(&self.rejoin);
        self.auto_commit_task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(error) =
                    commit_snapshot(&coordinator, &membership, &positions, &rejoin).await
                {
                    tracing::debug!(%error, "auto-commit failed");
                }
            }
        }));
    }
}

impl Drop for GroupConsumer {
    fn drop(&mut self) {
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
        if let Some(task) = self.auto_commit_task.take() {
            task.abort();
        }
    }
}

pub(crate) fn msg_err(e: std::io::Error) -> ClientError {
    ClientError::Protocol(ProtocolError::Message(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(list: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        list.iter()
            .map(|(id, topics)| {
                (
                    (*id).to_owned(),
                    topics.iter().map(|t| (*t).to_owned()).collect(),
                )
            })
            .collect()
    }

    fn topics(list: &[(&str, i32)]) -> BTreeMap<String, Vec<i32>> {
        list.iter()
            .map(|(name, count)| ((*name).to_owned(), (0..*count).collect()))
            .collect()
    }

    #[test]
    fn range_distributes_contiguous_ranges_per_topic() {
        let members = members(&[("a", &["t"]), ("b", &["t"])]);
        let topics = topics(&[("t", 5)]);
        let assignment = Assignor::Range.assign(&members, &topics, &BTreeMap::new());
        assert_eq!(
            assignment["a"],
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 1),
                ("t".to_owned(), 2)
            ]
        );
        assert_eq!(
            assignment["b"],
            vec![("t".to_owned(), 3), ("t".to_owned(), 4)]
        );
    }

    #[test]
    fn range_handles_uneven_and_multi_topic_subscriptions() {
        let members = members(&[("a", &["t1", "t2"]), ("b", &["t1"])]);
        let topics = topics(&[("t1", 3), ("t2", 2)]);
        let assignment = Assignor::Range.assign(&members, &topics, &BTreeMap::new());
        assert_eq!(
            assignment["a"],
            vec![
                ("t1".to_owned(), 0),
                ("t1".to_owned(), 1),
                ("t2".to_owned(), 0),
                ("t2".to_owned(), 1)
            ]
        );
        assert_eq!(assignment["b"], vec![("t1".to_owned(), 2)]);
    }

    #[test]
    fn roundrobin_deals_partitions_across_members() {
        let members = members(&[("a", &["t"]), ("b", &["t"])]);
        let topics = topics(&[("t", 5)]);
        let assignment = Assignor::RoundRobin.assign(&members, &topics, &BTreeMap::new());
        assert_eq!(
            assignment["a"],
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 2),
                ("t".to_owned(), 4)
            ]
        );
        assert_eq!(
            assignment["b"],
            vec![("t".to_owned(), 1), ("t".to_owned(), 3)]
        );
    }

    #[test]
    fn roundrobin_skips_members_not_subscribed_to_a_topic() {
        let members = members(&[("a", &["t1"]), ("b", &["t1", "t2"])]);
        let topics = topics(&[("t1", 2), ("t2", 2)]);
        let assignment = Assignor::RoundRobin.assign(&members, &topics, &BTreeMap::new());
        assert_eq!(assignment["a"], vec![("t1".to_owned(), 0)]);
        assert_eq!(
            assignment["b"],
            vec![
                ("t1".to_owned(), 1),
                ("t2".to_owned(), 0),
                ("t2".to_owned(), 1)
            ]
        );
    }

    #[test]
    fn single_member_gets_everything() {
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let members = members(&[("only", &["t"])]);
            let topics = topics(&[("t", 4)]);
            let assignment = assignor.assign(&members, &topics, &BTreeMap::new());
            assert_eq!(
                assignment["only"],
                (0..4).map(|p| ("t".to_owned(), p)).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn member_leaving_reassigns_its_partitions() {
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let three = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
            let topics = topics(&[("t", 3)]);
            let before = assignor.assign(&three, &topics, &BTreeMap::new());
            assert_eq!(before["b"], vec![("t".to_owned(), 1)]);

            let two = members(&[("a", &["t"]), ("c", &["t"])]);
            let after = assignor.assign(&two, &topics, &BTreeMap::new());
            let mut assigned: Vec<i32> = after
                .values()
                .flat_map(|tps| tps.iter().map(|(_, p)| *p))
                .collect();
            assigned.sort_unstable();
            assert_eq!(assigned, vec![0, 1, 2], "all partitions stay assigned");
        }
    }

    #[test]
    fn every_member_appears_even_with_no_partitions() {
        let members = members(&[("a", &["t"]), ("b", &["other"])]);
        let topics = topics(&[("t", 1)]);
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let assignment = assignor.assign(&members, &topics, &BTreeMap::new());
            assert_eq!(assignment["a"], vec![("t".to_owned(), 0)]);
            assert!(assignment["b"].is_empty());
        }
    }
}

/// Wall clock in unix milliseconds, for poll-liveness accounting.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod sticky_tests {
    use super::*;

    fn members(list: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        list.iter()
            .map(|(id, topics)| {
                (
                    (*id).to_string(),
                    topics.iter().map(|t| (*t).to_string()).collect(),
                )
            })
            .collect()
    }

    fn topics(list: &[(&str, i32)]) -> BTreeMap<String, Vec<i32>> {
        list.iter()
            .map(|(topic, count)| ((*topic).to_string(), (0..*count).collect()))
            .collect()
    }

    fn previous(list: &[(&str, &[(&str, i32)])]) -> BTreeMap<String, Vec<TopicPartition>> {
        list.iter()
            .map(|(id, held)| {
                (
                    (*id).to_string(),
                    held.iter()
                        .map(|(topic, partition)| ((*topic).to_string(), *partition))
                        .collect(),
                )
            })
            .collect()
    }

    /// How many partitions changed hands. This is the number the sticky
    /// assignor exists to keep small, so it is what these tests assert on.
    fn moved(
        before: &BTreeMap<String, Vec<TopicPartition>>,
        after: &BTreeMap<String, Vec<TopicPartition>>,
    ) -> usize {
        after
            .iter()
            .flat_map(|(member_id, held)| held.iter().map(move |tp| (member_id, tp)))
            .filter(|(member_id, tp)| before.get(*member_id).is_none_or(|held| !held.contains(tp)))
            .count()
    }

    fn all_assigned(
        assignment: &BTreeMap<String, Vec<TopicPartition>>,
        topic_partitions: &BTreeMap<String, Vec<i32>>,
    ) {
        let mut got: Vec<TopicPartition> = assignment
            .values()
            .flat_map(|v| v.iter().cloned())
            .collect();
        got.sort();
        let mut want: Vec<TopicPartition> = topic_partitions
            .iter()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |p| (topic.clone(), *p)))
            .collect();
        want.sort();
        assert_eq!(got, want, "every partition must have exactly one owner");
    }

    /// Nothing changed, so nothing should move. A sticky assignor that
    /// reshuffles a stable group is worse than useless.
    #[test]
    fn a_stable_group_moves_nothing() {
        let members = members(&[("a", &["t"]), ("b", &["t"])]);
        let topics = topics(&[("t", 4)]);
        let before = previous(&[("a", &[("t", 0), ("t", 1)]), ("b", &[("t", 2), ("t", 3)])]);
        let after = sticky_assign(&members, &topics, &before);
        assert_eq!(moved(&before, &after), 0);
        assert_eq!(after, before);
    }

    /// A member leaves: only its partitions move. The survivors keep
    /// everything they held — that is the whole difference from range,
    /// which recomputes and reshuffles.
    #[test]
    fn only_the_departed_partitions_move() {
        let topics = topics(&[("t", 6)]);
        let before = previous(&[
            ("a", &[("t", 0), ("t", 1)]),
            ("b", &[("t", 2), ("t", 3)]),
            ("c", &[("t", 4), ("t", 5)]),
        ]);
        let survivors = members(&[("a", &["t"]), ("c", &["t"])]);
        let after = sticky_assign(&survivors, &topics, &before);

        all_assigned(&after, &topics);
        assert_eq!(
            moved(&before, &after),
            2,
            "only the two orphaned partitions should change hands, got {after:?}"
        );
        for tp in &before["a"] {
            assert!(after["a"].contains(tp), "a kept {tp:?}");
        }
        for tp in &before["c"] {
            assert!(after["c"].contains(tp), "c kept {tp:?}");
        }
    }

    /// A member joins: it takes a fair share, and only that many move.
    #[test]
    fn a_joining_member_takes_only_its_fair_share() {
        let topics = topics(&[("t", 6)]);
        let before = previous(&[
            ("a", &[("t", 0), ("t", 1), ("t", 2)]),
            ("b", &[("t", 3), ("t", 4), ("t", 5)]),
        ]);
        let grown = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
        let after = sticky_assign(&grown, &topics, &before);

        all_assigned(&after, &topics);
        assert_eq!(after["a"].len(), 2);
        assert_eq!(after["b"].len(), 2);
        assert_eq!(after["c"].len(), 2);
        assert_eq!(
            moved(&before, &after),
            2,
            "exactly the two partitions the newcomer needs should move"
        );
    }

    /// Sticky must still be *correct*, not merely stable: a partition whose
    /// topic a member no longer subscribes to cannot be kept.
    #[test]
    fn a_partition_whose_topic_was_unsubscribed_is_reassigned() {
        let topics = topics(&[("t1", 2), ("t2", 2)]);
        let before = previous(&[
            ("a", &[("t1", 0), ("t1", 1)]),
            ("b", &[("t2", 0), ("t2", 1)]),
        ]);
        // `a` drops t1 and picks up t2; it cannot keep t1's partitions.
        let changed = members(&[("a", &["t2"]), ("b", &["t2"])]);
        let after = sticky_assign(&changed, &topics, &before);
        assert!(
            !after["a"].iter().any(|(topic, _)| topic == "t1"),
            "a must not keep a topic it no longer subscribes to"
        );
    }

    /// A partition that no longer exists must not be carried forward, and a
    /// newly created one must be handed out.
    #[test]
    fn deleted_and_created_partitions_are_handled() {
        let before = previous(&[("a", &[("t", 0), ("t", 1)]), ("b", &[("t", 2), ("t", 3)])]);
        let members = members(&[("a", &["t"]), ("b", &["t"])]);

        // As far as this assignment is concerned the topic has 2 partitions;
        // the vanished ones simply do not appear in the result.
        let shrunk = topics(&[("t", 2)]);
        let after = sticky_assign(&members, &shrunk, &before);
        all_assigned(&after, &shrunk);

        // Topic grew to 6; the new ones get owners.
        let grown = topics(&[("t", 6)]);
        let after = sticky_assign(&members, &grown, &before);
        all_assigned(&after, &grown);
        assert_eq!(after["a"].len(), 3);
        assert_eq!(after["b"].len(), 3);
    }

    /// With no history every partition is new, so sticky must still produce
    /// a complete, balanced assignment rather than an empty one.
    #[test]
    fn a_first_assignment_with_no_history_is_balanced() {
        let members = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
        let topics = topics(&[("t", 7)]);
        let after = sticky_assign(&members, &topics, &BTreeMap::new());
        all_assigned(&after, &topics);
        let sizes: Vec<usize> = after.values().map(Vec::len).collect();
        assert!(
            sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1,
            "sizes must differ by at most one, got {sizes:?}"
        );
    }

    /// Independent members computing the assignment must agree, or they
    /// would fight over partitions on every rebalance.
    #[test]
    fn assignment_is_deterministic() {
        let members = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
        let topics = topics(&[("t", 8)]);
        let before = previous(&[("a", &[("t", 0)]), ("b", &[("t", 1)])]);
        let first = sticky_assign(&members, &topics, &before);
        for _ in 0..5 {
            assert_eq!(sticky_assign(&members, &topics, &before), first);
        }
    }

    /// The comparison that justifies the strategy.
    ///
    /// A member *joining* is the discriminating case. When one leaves, its
    /// partitions have to move under any strategy, so both can hit the
    /// same floor. When one joins, only the newcomer's share needs to move
    /// — but range recomputes every boundary, so partitions shuffle between
    /// members that were never involved.
    #[test]
    fn sticky_moves_less_than_range_when_a_member_joins() {
        let topics = topics(&[("t", 12)]);
        let two = members(&[("a", &["t"]), ("b", &["t"])]);
        let initial = sticky_assign(&two, &topics, &BTreeMap::new());

        let three = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
        let sticky_after = sticky_assign(&three, &topics, &initial);
        let range_after = range_assign(&three, &topics);

        let sticky_moves = moved(&initial, &sticky_after);
        let range_moves = moved(&initial, &range_after);
        assert_eq!(
            sticky_moves, 4,
            "only the newcomer's four partitions need to move"
        );
        assert!(
            sticky_moves < range_moves,
            "sticky moved {sticky_moves}, range moved {range_moves}"
        );
        all_assigned(&sticky_after, &topics);
    }

    /// When a member leaves, its partitions must move under any strategy,
    /// so the meaningful claim is that sticky moves *exactly* that many and
    /// not one more.
    #[test]
    fn a_departure_moves_exactly_the_orphaned_partitions() {
        let topics = topics(&[("t", 9)]);
        let three = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
        let initial = sticky_assign(&three, &topics, &BTreeMap::new());
        let orphaned = initial["b"].len();

        let two = members(&[("a", &["t"]), ("c", &["t"])]);
        let after = sticky_assign(&two, &topics, &initial);
        assert_eq!(
            moved(&initial, &after),
            orphaned,
            "sticky must move the departed member's partitions and nothing else"
        );
        all_assigned(&after, &topics);
    }
}

#[cfg(test)]
mod cooperative_tests {
    use super::*;
    use std::collections::BTreeSet;

    fn members(list: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        list.iter()
            .map(|(id, topics)| {
                (
                    (*id).to_string(),
                    topics.iter().map(|t| (*t).to_string()).collect(),
                )
            })
            .collect()
    }

    fn topics(list: &[(&str, i32)]) -> BTreeMap<String, Vec<i32>> {
        list.iter()
            .map(|(topic, count)| ((*topic).to_string(), (0..*count).collect()))
            .collect()
    }

    fn previous(list: &[(&str, &[(&str, i32)])]) -> BTreeMap<String, Vec<TopicPartition>> {
        list.iter()
            .map(|(id, held)| {
                (
                    (*id).to_string(),
                    held.iter()
                        .map(|(topic, partition)| ((*topic).to_string(), *partition))
                        .collect(),
                )
            })
            .collect()
    }

    /// The property cooperative rebalancing exists for: a member never has
    /// a partition taken away from under it in the same round it is given
    /// to someone else. Anything moving is withheld first.
    #[test]
    fn a_partition_is_never_assigned_while_another_member_still_holds_it() {
        let topics = topics(&[("t", 6)]);
        let before = previous(&[
            ("a", &[("t", 0), ("t", 1), ("t", 2)]),
            ("b", &[("t", 3), ("t", 4), ("t", 5)]),
        ]);
        // `c` arrives and should eventually take two partitions.
        let grown = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);

        let target = sticky_assign(&grown, &topics, &before);
        let round_one = withhold_moving_partitions(&target, &before);

        // Nothing `c` receives may still belong to a or b.
        for slot in &round_one["c"] {
            for (member_id, held) in &before {
                assert!(
                    !held.contains(slot),
                    "{slot:?} was handed to c while {member_id} still holds it"
                );
            }
        }
        // And a and b keep everything they are not losing.
        for member_id in ["a", "b"] {
            for slot in &round_one[member_id] {
                assert!(
                    before[member_id].contains(slot),
                    "{member_id} was given {slot:?} it did not already hold"
                );
            }
        }
    }

    /// The second round is what actually places the withheld partitions.
    /// Without it a joining member would sit with nothing forever.
    #[test]
    fn the_second_round_places_what_the_first_withheld() {
        let topics = topics(&[("t", 6)]);
        let before = previous(&[
            ("a", &[("t", 0), ("t", 1), ("t", 2)]),
            ("b", &[("t", 3), ("t", 4), ("t", 5)]),
        ]);
        let grown = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);

        let target = sticky_assign(&grown, &topics, &before);
        let round_one = withhold_moving_partitions(&target, &before);
        // After round one, the revoked partitions belong to nobody.
        let round_two_target = sticky_assign(&grown, &topics, &round_one);
        let round_two = withhold_moving_partitions(&round_two_target, &round_one);

        let placed: BTreeSet<TopicPartition> = round_two
            .values()
            .flat_map(|held| held.iter().cloned())
            .collect();
        let expected: BTreeSet<TopicPartition> = (0..6).map(|p| ("t".to_string(), p)).collect();
        assert_eq!(
            placed, expected,
            "every partition must have an owner after the second round"
        );
        assert!(
            !round_two["c"].is_empty(),
            "the joining member must end up with partitions"
        );
    }

    /// A stable group must not be disturbed: with nothing to move, the
    /// cooperative pass is the identity.
    #[test]
    fn a_stable_group_is_untouched_by_the_cooperative_pass() {
        let topics = topics(&[("t", 4)]);
        let before = previous(&[("a", &[("t", 0), ("t", 1)]), ("b", &[("t", 2), ("t", 3)])]);
        let same = members(&[("a", &["t"]), ("b", &["t"])]);

        let target = sticky_assign(&same, &topics, &before);
        let withheld = withhold_moving_partitions(&target, &before);
        assert_eq!(withheld, target, "a settled group should not be reshuffled");
        assert_eq!(withheld, before);
    }

    /// A departing member frees its partitions outright — nobody holds
    /// them, so there is nothing to revoke and they can be placed at once.
    /// Cooperative must not add a needless round to a departure.
    #[test]
    fn a_departure_needs_no_extra_round() {
        let topics = topics(&[("t", 6)]);
        let before = previous(&[
            ("a", &[("t", 0), ("t", 1)]),
            ("b", &[("t", 2), ("t", 3)]),
            ("c", &[("t", 4), ("t", 5)]),
        ]);
        // `c` is gone; its partitions belong to nobody in `members`.
        let survivors = members(&[("a", &["t"]), ("b", &["t"])]);
        let mut without_c = before.clone();
        without_c.remove("c");

        let target = sticky_assign(&survivors, &topics, &without_c);
        let granted = withhold_moving_partitions(&target, &without_c);

        let placed: BTreeSet<TopicPartition> = granted
            .values()
            .flat_map(|held| held.iter().cloned())
            .collect();
        let expected: BTreeSet<TopicPartition> = (0..6).map(|p| ("t".to_string(), p)).collect();
        assert_eq!(
            placed, expected,
            "an orphaned partition has no current owner, so it can move immediately"
        );
    }
}
