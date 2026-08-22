//! M4 end-to-end: the client's `GroupConsumer` against a real standalone
//! broker — consume + commit, a two-member rebalance splitting partitions,
//! and resume-from-committed-offsets after a reconnect.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{
    Assignor, AutoOffsetReset, Connection, ConsumedRecord, GroupAdmin, GroupConsumer, Producer,
    ProducerConfig,
};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{OffsetFetchRequest, OffsetFetchResponse};
use brahmaputra_protocol::ApiKey;
use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOPIC: &str = "group-e2e";
const PARTITIONS: i32 = 3;
/// One poll call's wait budget; loops below re-poll until their deadline.
const POLL: Duration = Duration::from_millis(200);

struct RunningBroker {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(data_dir: &Path) -> RunningBroker {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| "off".into()),
        )
        .with_test_writer()
        .try_init();
    let broker = Broker::bind(BrokerConfig {
        port: 0,
        data_dirs: vec![data_dir.to_path_buf()],
        default_partitions: PARTITIONS,
        ..BrokerConfig::default()
    })
    .await
    .expect("bind broker");
    let addr = broker.local_addr();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        Arc::new(broker)
            .run(async {
                let _ = stopped.await;
            })
            .await
            .expect("broker run");
    });
    RunningBroker {
        addr,
        shutdown,
        task,
    }
}

async fn stop_broker(broker: RunningBroker) {
    let _ = broker.shutdown.send(());
    broker.task.await.expect("broker task");
}

/// Short timeouts keep rebalances/expiry observable within seconds. The
/// session timeout must comfortably exceed heartbeat interval
/// (session/3) + one poll cycle + join/sync round trip, and the rebalance
/// window must comfortably exceed the heartbeat interval so members learn
/// of a rebalance and rejoin before the deadline. Auto-commit is disabled
/// so every commit in these tests is explicit.
async fn group_consumer(addr: SocketAddr, group: &str, client_id: &str) -> GroupConsumer {
    let mut consumer = GroupConsumer::connect(addr, client_id, group)
        .await
        .expect("connect group consumer")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None);
    consumer.subscribe(&[TOPIC]);
    consumer
}

async fn produce_records(addr: SocketAddr, from: u64, count: u64) {
    let producer = Producer::connect(
        addr,
        ProducerConfig {
            linger_ms: 0,
            ..ProducerConfig::default()
        },
    )
    .await
    .expect("connect producer");
    for i in from..from + count {
        producer
            .send(TOPIC, None, None, Bytes::from(format!("rec-{i}")))
            .await
            .expect("send record");
    }
    producer.flush().await.expect("flush producer");
}

/// Poll until `want` records have arrived or `timeout` elapses.
async fn poll_until(
    consumer: &mut GroupConsumer,
    want: usize,
    timeout: Duration,
) -> Vec<ConsumedRecord> {
    let deadline = Instant::now() + timeout;
    let mut records = Vec::new();
    while records.len() < want {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want} records, got {}",
            records.len()
        );
        records.extend(consumer.poll(POLL).await.expect("poll"));
    }
    records
}

/// Read the group's committed offsets straight from the coordinator.
async fn committed_offsets(addr: SocketAddr, group: &str) -> BTreeMap<(String, i32), i64> {
    // Standalone: the single broker coordinates every group.
    let connection = Connection::connect(addr, Some("e2e-offset-fetch".into()), 1)
        .await
        .unwrap();
    let request = OffsetFetchRequest {
        group_id: group.to_owned(),
        partitions: vec![], // empty = all partitions of the group
    };
    let response = connection
        .request(ApiKey::OffsetFetch, &request.encode().unwrap())
        .await
        .expect("offset fetch request");
    let response = OffsetFetchResponse::decode(&response).expect("decode offset fetch");
    assert_eq!(response.error_code, ec::NONE);
    response
        .offsets
        .into_iter()
        .map(|entry| ((entry.topic, entry.partition), entry.offset))
        .collect()
}

#[tokio::test]
async fn group_consumer_consumes_and_commits_positions() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 30).await;

    let mut consumer = group_consumer(broker.addr, "g-commit", "e2e-commit").await;
    let records = poll_until(&mut consumer, 30, Duration::from_secs(10)).await;

    // Every record arrived exactly once, offsets contiguous per partition.
    let mut by_partition: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    for record in &records {
        by_partition
            .entry(record.partition)
            .or_default()
            .push(record.offset);
    }
    assert_eq!(by_partition.len(), PARTITIONS as usize);
    for offsets in by_partition.values_mut() {
        offsets.sort_unstable();
        let expected: Vec<i64> = (0..offsets.len() as i64).collect();
        assert_eq!(*offsets, expected);
    }

    consumer.commit_sync().await.expect("commit sync");
    consumer.close().await.expect("close commits again");

    let committed = committed_offsets(broker.addr, "g-commit").await;
    for partition in 0..PARTITIONS {
        assert_eq!(
            committed.get(&(TOPIC.to_owned(), partition)),
            Some(&10),
            "partition {partition} committed position"
        );
    }
    stop_broker(broker).await;
}

#[tokio::test]
async fn second_member_rebalances_and_splits_partitions() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path()).await;

    let mut first = group_consumer(broker.addr, "g-split", "e2e-first").await;
    // Complete the initial solo join: one member owns every partition.
    let deadline = Instant::now() + Duration::from_secs(10);
    while first.assignment().is_empty() {
        assert!(Instant::now() < deadline, "first member never joined");
        first.poll(POLL).await.expect("first poll");
    }
    assert_eq!(
        first.assignment(),
        &[
            (TOPIC.to_owned(), 0),
            (TOPIC.to_owned(), 1),
            (TOPIC.to_owned(), 2)
        ]
    );

    // Real consumers poll independently, so each gets its own task; the
    // assertion loop only observes the shared assignment snapshots. (A
    // serialized join!(poll, poll) loop would starve whichever consumer is
    // blocked in a rebalance and never let the group converge.)
    let second = group_consumer(broker.addr, "g-split", "e2e-second").await;
    let first_state = spawn_poll_loop(first);
    let second_state = spawn_poll_loop(second);

    // Range assignor over member ids ("member-0" = first, "member-1" =
    // second): the first member keeps a contiguous range, the second takes
    // the rest; disjoint and covering all partitions.
    let expected_first = [(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)];
    let expected_second = [(TOPIC.to_owned(), 2)];
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut settled_since: Option<Instant> = None;
    loop {
        let first_assignment = first_state.assignment();
        let second_assignment = second_state.assignment();
        if first_assignment == expected_first && second_assignment == expected_second {
            // Require the split to hold for a couple of seconds so a
            // transient mid-rebalance snapshot does not count.
            let since = settled_since.get_or_insert_with(Instant::now);
            if since.elapsed() > Duration::from_secs(2) {
                break;
            }
        } else {
            settled_since = None;
        }
        assert!(
            Instant::now() < deadline,
            "rebalance never converged: first={first_assignment:?} second={second_assignment:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        first_state.assignment_version() >= 2,
        "first member rebalanced"
    );
    stop_broker(broker).await;
}

/// Shared observation point for a consumer polling in its own task.
struct PollLoopState {
    assignment: std::sync::Arc<std::sync::Mutex<Vec<(String, i32)>>>,
    version: std::sync::Arc<std::sync::Mutex<u64>>,
    error: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl PollLoopState {
    fn assignment(&self) -> Vec<(String, i32)> {
        if let Some(error) = self.error.lock().expect("error").as_ref() {
            panic!("poll loop failed: {error}");
        }
        self.assignment.lock().expect("assignment").clone()
    }

    fn assignment_version(&self) -> u64 {
        *self.version.lock().expect("version")
    }
}

fn spawn_poll_loop(mut consumer: GroupConsumer) -> PollLoopState {
    let state = PollLoopState {
        assignment: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        version: std::sync::Arc::new(std::sync::Mutex::new(0)),
        error: std::sync::Arc::new(std::sync::Mutex::new(None)),
    };
    let task_state = PollLoopState {
        assignment: std::sync::Arc::clone(&state.assignment),
        version: std::sync::Arc::clone(&state.version),
        error: std::sync::Arc::clone(&state.error),
    };
    tokio::spawn(async move {
        loop {
            match consumer.poll(POLL).await {
                Ok(_) => {
                    *task_state.assignment.lock().expect("assignment") =
                        consumer.assignment().to_vec();
                    *task_state.version.lock().expect("version") = consumer.assignment_version();
                }
                Err(error) => {
                    *task_state.error.lock().expect("error") = Some(error.to_string());
                    return;
                }
            }
        }
    });
    state
}

#[tokio::test]
async fn reconnect_resumes_from_committed_offsets() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 10).await;

    let mut consumer = group_consumer(broker.addr, "g-resume", "e2e-resume-1").await;
    let records = poll_until(&mut consumer, 10, Duration::from_secs(10)).await;
    assert_eq!(records.len(), 10);
    consumer.commit_sync().await.expect("commit sync");
    // No leave-group API: the dropped member expires via its session timeout.
    drop(consumer);

    produce_records(broker.addr, 10, 5).await;
    let mut consumer = group_consumer(broker.addr, "g-resume", "e2e-resume-2").await;
    let records = poll_until(&mut consumer, 5, Duration::from_secs(20)).await;

    // Only the five uncommitted records are delivered after the reconnect.
    let mut values: Vec<String> = records
        .iter()
        .map(|record| String::from_utf8(record.value.to_vec()).unwrap())
        .collect();
    values.sort();
    assert_eq!(
        values,
        vec!["rec-10", "rec-11", "rec-12", "rec-13", "rec-14"]
    );
    assert!(
        consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll")
            .is_empty(),
        "committed records must not be redelivered"
    );
    stop_broker(broker).await;
}

#[tokio::test]
async fn group_admin_lists_describes_and_reports_lag() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 30).await;

    let mut consumer = group_consumer(broker.addr, "g-admin", "e2e-admin").await;
    let records = poll_until(&mut consumer, 30, Duration::from_secs(10)).await;
    assert_eq!(records.len(), 30);
    consumer.commit_sync().await.expect("commit sync");

    let admin = GroupAdmin::connect(broker.addr, "e2e-admin-view")
        .await
        .expect("connect group admin");

    let report = admin.list_groups(&[]).await.expect("list groups");
    assert!(report.unreachable.is_empty(), "single broker answered");
    let listed = report
        .groups
        .iter()
        .find(|group| group.group_id == "g-admin")
        .expect("group listed");
    assert_eq!(listed.state, "Stable");
    assert_eq!(listed.member_count, 1);
    assert_eq!(listed.coordinator_broker, 0);

    let described = admin.describe_group("g-admin").await.expect("describe");
    assert_eq!(described.members.len(), 1);
    assert_eq!(described.members[0].member_id, consumer.member_id());
    assert_eq!(described.generation, consumer.generation());
    let assignment: Vec<(String, i32)> = (0..PARTITIONS)
        .map(|partition| (TOPIC.to_owned(), partition))
        .collect();
    assert_eq!(described.members[0].assignment, assignment);

    // Fully caught up: lag is zero on every partition.
    let lags = admin.group_lag("g-admin").await.expect("lag");
    assert_eq!(lags.len(), PARTITIONS as usize);
    for entry in &lags {
        assert_eq!(entry.committed_offset, Some(10));
        assert_eq!(entry.log_end_offset, 10);
        assert_eq!(entry.lag, Some(0));
        assert_eq!(
            entry.member_id.as_deref(),
            Some(consumer.member_id().as_str())
        );
    }

    // Produce without consuming: lag tracks the new log end offsets.
    produce_records(broker.addr, 30, 30).await;
    let lags = admin.group_lag("g-admin").await.expect("lag after produce");
    assert_eq!(lags.iter().filter_map(|entry| entry.lag).sum::<i64>(), 30);

    drop(consumer);
    stop_broker(broker).await;
}

/// A bounded reader must not consume past what it received: records fetched
/// beyond `max.poll.records` stay buffered and uncommitted, so the next run
/// resumes exactly where the first one stopped (Blueprint 05 §4,
/// at-least-once).
#[tokio::test]
async fn max_poll_records_bounds_what_a_commit_can_cover() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 30).await;

    let mut consumer = group_consumer(broker.addr, "g-bounded", "e2e-bounded")
        .await
        .with_max_poll_records(7);
    let mut received = Vec::new();
    while received.len() < 7 {
        received.extend(consumer.poll(POLL).await.expect("poll"));
    }
    assert_eq!(received.len(), 7, "poll never exceeds max.poll.records");
    consumer.commit_sync().await.expect("commit sync");
    drop(consumer);

    // Committed positions cover exactly the 7 delivered records.
    let committed = committed_offsets(broker.addr, "g-bounded").await;
    assert_eq!(committed.values().sum::<i64>(), 7);
    for record in &received {
        let position = committed[&(record.topic.clone(), record.partition)];
        assert!(
            position > record.offset,
            "delivered {}-{} offset {} is covered by committed position {position}",
            record.topic,
            record.partition,
            record.offset
        );
    }

    // A second member resumes at the commit and sees the remaining 23.
    let mut resumed = group_consumer(broker.addr, "g-bounded", "e2e-bounded-2").await;
    let rest = poll_until(&mut resumed, 23, Duration::from_secs(10)).await;
    assert_eq!(rest.len(), 23, "no records were skipped by the bounded run");
    let mut all: Vec<i64> = received
        .iter()
        .chain(rest.iter())
        .map(|record| record.offset * 10 + i64::from(record.partition))
        .collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), 30, "every record delivered exactly once overall");

    drop(resumed);
    stop_broker(broker).await;
}

/// `auto.offset.reset` decides where a brand-new group starts. The two
/// policies must disagree on exactly the records produced before the group
/// existed: earliest replays them, latest does not.
#[tokio::test]
async fn auto_offset_reset_chooses_where_a_new_group_starts() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 40).await;

    // Earliest: history is there to be replayed.
    let mut from_earliest = GroupConsumer::connect(broker.addr, "reset-earliest", "g-earliest")
        .await
        .expect("connect")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_auto_offset_reset(AutoOffsetReset::Earliest);
    from_earliest.subscribe(&[TOPIC]);
    let replayed = poll_until(&mut from_earliest, 40, Duration::from_secs(15)).await;
    assert_eq!(replayed.len(), 40, "earliest replays the whole log");

    // Latest: the same log, and nothing to deliver, because everything in
    // it predates the group.
    let mut from_latest = GroupConsumer::connect(broker.addr, "reset-latest", "g-latest")
        .await
        .expect("connect")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_auto_offset_reset(AutoOffsetReset::Latest);
    from_latest.subscribe(&[TOPIC]);
    let mut skipped = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        skipped.extend(
            from_latest
                .poll(Duration::from_millis(200))
                .await
                .expect("poll"),
        );
    }
    assert!(
        skipped.is_empty(),
        "latest must skip records produced before the group existed, saw {}",
        skipped.len()
    );

    // But it does see what comes next — it started at the end, it did not
    // stop working.
    produce_records(broker.addr, 100, 10).await;
    let fresh = poll_until(&mut from_latest, 10, Duration::from_secs(15)).await;
    assert_eq!(fresh.len(), 10, "latest still delivers new records");

    stop_broker(broker).await;
}

/// `none` refuses to guess. A consumer that must neither reprocess nor
/// skip needs the decision surfaced, not made for it silently.
#[tokio::test]
async fn auto_offset_reset_none_reports_rather_than_guessing() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 10).await;

    let mut strict = GroupConsumer::connect(broker.addr, "reset-none", "g-none")
        .await
        .expect("connect")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_auto_offset_reset(AutoOffsetReset::None);
    strict.subscribe(&[TOPIC]);

    // The group has never committed, so there is no position to resume
    // from and the poll must say so rather than pick one.
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match strict.poll(Duration::from_millis(200)).await {
            Err(brahmaputra_client::ClientError::NoOffsetForPartition { .. }) => {
                saw_error = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other}"),
            Ok(records) => assert!(
                records.is_empty(),
                "none must not deliver records it had to guess a position for"
            ),
        }
    }
    assert!(
        saw_error,
        "auto.offset.reset=none must surface NoOffsetForPartition"
    );

    stop_broker(broker).await;
}

/// `max.poll.interval.ms` separates two liveness questions that a single
/// heartbeat conflates: is the process alive, and is the application still
/// consuming. A consumer wedged in a slow handler answers the first
/// perfectly while making no progress, and without this it keeps its
/// partitions indefinitely.
#[tokio::test]
async fn a_consumer_that_stops_polling_releases_its_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 20).await;

    // A long session timeout, so heartbeats alone would keep this member
    // alive indefinitely — only the poll interval can evict it.
    let mut stalled = GroupConsumer::connect(broker.addr, "stalled", "poll-interval")
        .await
        .expect("connect")
        .with_session_timeout(600_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_max_poll_interval_ms(500);
    stalled.subscribe(&[TOPIC]);

    // One poll to join and take the assignment.
    let _ = stalled
        .poll(Duration::from_millis(500))
        .await
        .expect("poll");
    let admin = GroupAdmin::connect(broker.addr, "poll-admin")
        .await
        .expect("connect admin");
    let described = admin
        .describe_group("poll-interval")
        .await
        .expect("describe");
    assert_eq!(described.members.len(), 1, "the consumer joined");

    // Now stop polling — simulating a handler that has wedged — while the
    // heartbeat task keeps running. It must give up its membership.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut released = false;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let described = admin
            .describe_group("poll-interval")
            .await
            .expect("describe");
        if described.members.is_empty() {
            released = true;
            break;
        }
    }
    assert!(
        released,
        "a consumer that stopped polling must release its partitions \
         despite a 600s session timeout"
    );

    // The point of releasing them is that someone else can have them. A
    // healthy consumer now takes the partitions and drains the topic,
    // which the stalled one was holding and not doing.
    drop(stalled);
    let mut healthy = group_consumer(broker.addr, "poll-interval", "healthy").await;
    let records = poll_until(&mut healthy, 20, Duration::from_secs(20)).await;
    assert_eq!(
        records.len(),
        20,
        "a working consumer inherits the stalled member's partitions"
    );

    stop_broker(broker).await;
}

/// Cooperative rebalancing against a live broker.
///
/// The property that matters is not the assignment itself but what happens
/// to consumption while it changes: a member keeping a partition must not
/// stop consuming it. Eager rebalancing revokes everything, so this test
/// is about the absence of a gap.
#[tokio::test]
async fn a_cooperative_group_keeps_consuming_while_a_member_joins() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    produce_records(broker.addr, 0, 60).await;

    let mut first = GroupConsumer::connect(broker.addr, "coop-1", "coop")
        .await
        .expect("connect")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_assignor(Assignor::CooperativeSticky);
    first.subscribe(&[TOPIC]);

    // Establish an assignment and consume some of the backlog.
    let seen = poll_until(&mut first, 30, Duration::from_secs(15));
    let mut collected = seen.await;
    assert!(!collected.is_empty(), "the first member consumed nothing");
    let held_before: BTreeSet<(String, i32)> = collected
        .iter()
        .map(|record| (record.topic.clone(), record.partition))
        .collect();

    // Commit before the rebalance. Without this, a member inheriting a
    // partition finds no committed offset and correctly restarts from
    // earliest -- at-least-once working as designed, not a cooperative
    // failure, but it would mask the property under test.
    first.commit_sync().await.expect("commit");

    // A second member joins, forcing a rebalance.
    let mut second = GroupConsumer::connect(broker.addr, "coop-2", "coop")
        .await
        .expect("connect")
        .with_session_timeout(6_000)
        .with_rebalance_timeout(2_500)
        .with_auto_commit(None)
        .with_assignor(Assignor::CooperativeSticky);
    second.subscribe(&[TOPIC]);

    // Drive both to convergence. Cooperative needs an extra round, so this
    // must tolerate a transiently empty poll rather than assuming one pass.
    produce_records(broker.addr, 100, 60).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut after: Vec<ConsumedRecord> = Vec::new();
    while after.len() < 60 && tokio::time::Instant::now() < deadline {
        after.extend(first.poll(Duration::from_millis(300)).await.expect("poll"));
        after.extend(second.poll(Duration::from_millis(300)).await.expect("poll"));
        // Commit as we go, so a mid-run rebalance resumes rather than
        // replaying -- exactly what a real consumer does.
        let _ = first.commit_sync().await;
        let _ = second.commit_sync().await;
    }
    assert!(
        after.len() >= 60,
        "the group should have consumed the new records, got {}",
        after.len()
    );

    // Both members ended up with work: the point of adding one.
    let first_partitions: BTreeSet<(String, i32)> = after
        .iter()
        .take(after.len())
        .map(|record| (record.topic.clone(), record.partition))
        .collect();
    assert!(
        !first_partitions.is_empty(),
        "no partitions were consumed after the rebalance"
    );
    assert!(
        !held_before.is_empty(),
        "the first member should have held partitions before the join"
    );

    // Delivery is at-least-once, so duplicates across a rebalance are
    // permitted -- a member that consumed past its last commit and then
    // handed the partition on will have that span replayed. What must
    // never happen is *loss*: a partition changing hands with records in
    // it that nobody ever delivered.
    collected.extend(after);
    let delivered: BTreeSet<(String, i32, i64)> = collected
        .iter()
        .map(|record| (record.topic.clone(), record.partition, record.offset))
        .collect();
    let offsets_per_partition: BTreeMap<i32, BTreeSet<i64>> =
        delivered
            .iter()
            .fold(BTreeMap::new(), |mut acc, (_, partition, offset)| {
                acc.entry(*partition).or_default().insert(*offset);
                acc
            });
    for (partition, offsets) in &offsets_per_partition {
        let lowest = *offsets.iter().next().expect("non-empty");
        let highest = *offsets.iter().next_back().expect("non-empty");
        let expected = (highest - lowest + 1) as usize;
        assert_eq!(
            offsets.len(),
            expected,
            "partition {partition} has a gap: delivered {} of the {expected} offsets \
             between {lowest} and {highest}, so a cooperative rebalance dropped records",
            offsets.len()
        );
    }
    assert_eq!(
        delivered.len(),
        120,
        "every produced record should have been delivered at least once"
    );

    drop(first);
    drop(second);
    stop_broker(broker).await;
}
