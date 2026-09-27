//! Fan-out: broker topics out to subscribed sockets.
//!
//! A gateway instance reads each subscribed topic **once**, however many
//! sockets subscribe to it. The topic's feed fetches every partition over
//! its own broker connection, serializes each record into its WebSocket
//! frame once, and hands one reference-counted copy to a broadcast channel
//! that every subscribing connection reads. A price tick that reaches a
//! million sockets costs the broker one fetch per instance and the gateway
//! one JSON encoding; each socket pays only its own write.
//!
//! **Snapshots.** Each feed keeps the latest record per key (a last-value
//! cache: the current price of every symbol). A subscriber may ask for it
//! and gets it, then the live stream, with nothing lost or repeated in
//! between: the snapshot is taken and the channel joined under the same
//! lock the feed holds while it publishes. When a feed starts it reads the
//! last `--snapshot-warmup-records` of each partition into the cache
//! without broadcasting them, so the first subscriber's snapshot is not
//! empty.
//!
//! **Slow subscribers.** The channel holds the last `--feed-buffer`
//! records. A socket that falls further behind than that is told how many
//! it skipped (`lagged`) and continues from the newest, which for a price
//! feed is the right answer: a stale quote is worth less than none. The
//! feed never waits for a subscriber, so one slow phone cannot delay
//! anyone else, and nothing queues per socket beyond its TCP buffers.
//!
//! **Lifetime.** A feed starts with its first subscriber and stops
//! `--feed-idle-secs` after its last one leaves, releasing its broker
//! connection.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use brahmaputra_client::{ClientError, Consumer, FetchedRecord, EARLIEST, LATEST};
use brahmaputra_protocol::error_code as ec;
use bytes::Bytes;
use serde_json::{Map, Value};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;

/// How long one fetch waits for new records. Also bounds how quickly an
/// abandoned feed notices it has no subscribers left.
const FETCH_WAIT_MS: i32 = 250;
/// Longest a subscription waits for a new feed to read its history, so the
/// first subscriber's snapshot is as complete as later ones'.
const WARMUP_WAIT: Duration = Duration::from_secs(5);
/// How often a feed looks for partitions added to its topic.
const PARTITION_REFRESH: Duration = Duration::from_secs(30);

/// One record, already encoded as the frame every subscriber receives.
#[derive(Debug)]
pub struct FeedRecord {
    pub key: Option<Bytes>,
    pub partition: i32,
    pub offset: i64,
    pub tombstone: bool,
    pub frame: Utf8Bytes,
}

pub type Receiver = broadcast::Receiver<Arc<FeedRecord>>;

pub struct HubConfig {
    pub broker: SocketAddr,
    pub client_id: String,
    pub feed_buffer: usize,
    pub snapshot_max_keys: usize,
    pub snapshot_warmup_records: i64,
    pub feed_idle: Duration,
}

pub struct Hub {
    config: HubConfig,
    metrics: Arc<Metrics>,
    feeds: Mutex<HashMap<String, Arc<Feed>>>,
}

struct Feed {
    topic: String,
    tx: broadcast::Sender<Arc<FeedRecord>>,
    /// Latest record per key. Held while publishing, so a subscriber that
    /// takes the snapshot and joins the channel under it sees each record
    /// exactly once: in the snapshot or on the channel, never both.
    cache: Mutex<HashMap<Bytes, Arc<FeedRecord>>>,
    /// True once the feed has read up to where it went live.
    warm: watch::Sender<bool>,
}

/// What a new subscription starts with.
pub struct Subscription {
    pub receiver: Receiver,
    /// The cached latest record of each requested key (or of every key),
    /// in partition and offset order. Empty unless a snapshot was asked for.
    pub snapshot: Vec<Arc<FeedRecord>>,
}

impl Hub {
    pub fn new(config: HubConfig, metrics: Arc<Metrics>) -> Arc<Hub> {
        Arc::new(Hub {
            config,
            metrics,
            feeds: Mutex::new(HashMap::new()),
        })
    }

    /// Join `topic`'s feed, starting it if this is its first subscriber.
    ///
    /// A feed that has just started gets a few seconds to find the end of
    /// its partitions and read its history first. That is what lets the
    /// confirmation promise something: every record written after the
    /// subscriber is told `subscribed` reaches it, and the snapshot is not
    /// empty merely because this subscriber was the one that started the
    /// feed.
    pub async fn subscribe(
        self: &Arc<Self>,
        topic: &str,
        keys: Option<&HashSet<Bytes>>,
        snapshot: bool,
    ) -> Subscription {
        let mut warm = self.feed(topic).warm.subscribe();
        let _ = tokio::time::timeout(WARMUP_WAIT, warm.wait_for(|w| *w)).await;
        // The feeds lock is held until the channel is joined, so a feed
        // retiring for want of subscribers cannot slip in between (and a
        // feed that retired while this one waited is simply restarted).
        let feeds = self.feeds.lock().expect("feeds");
        let feed = self.feed_locked(feeds, topic);
        let cache = feed.0.cache.lock().expect("feed cache");
        let receiver = feed.0.tx.subscribe();
        drop(feed.1);
        let mut records: Vec<Arc<FeedRecord>> = if !snapshot {
            Vec::new()
        } else if let Some(keys) = keys {
            keys.iter().filter_map(|k| cache.get(k).cloned()).collect()
        } else {
            cache.values().cloned().collect()
        };
        drop(cache);
        records.sort_by_key(|r| (r.partition, r.offset));
        Subscription {
            receiver,
            snapshot: records,
        }
    }

    fn feed(self: &Arc<Self>, topic: &str) -> Arc<Feed> {
        let feeds = self.feeds.lock().expect("feeds");
        self.feed_locked(feeds, topic).0
    }

    /// The topic's feed, started if missing, and the still-held lock.
    fn feed_locked<'a>(
        self: &Arc<Self>,
        mut feeds: std::sync::MutexGuard<'a, HashMap<String, Arc<Feed>>>,
        topic: &str,
    ) -> (
        Arc<Feed>,
        std::sync::MutexGuard<'a, HashMap<String, Arc<Feed>>>,
    ) {
        let feed = feeds
            .entry(topic.to_owned())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(self.config.feed_buffer.max(16));
                let feed = Arc::new(Feed {
                    topic: topic.to_owned(),
                    tx,
                    cache: Mutex::new(HashMap::new()),
                    warm: watch::channel(false).0,
                });
                self.metrics.feeds_active.fetch_add(1, Relaxed);
                tokio::spawn(run_feed(self.clone(), feed.clone()));
                feed
            })
            .clone();
        (feed, feeds)
    }

    /// Remove `feed` if nobody has joined it since it found itself idle.
    fn retire(&self, feed: &Arc<Feed>) -> bool {
        let mut feeds = self.feeds.lock().expect("feeds");
        if feed.tx.receiver_count() > 0 {
            return false;
        }
        if feeds.get(&feed.topic).is_some_and(|f| Arc::ptr_eq(f, feed)) {
            feeds.remove(&feed.topic);
        }
        true
    }

    pub fn feed_count(&self) -> usize {
        self.feeds.lock().expect("feeds").len()
    }
}

impl Feed {
    fn publish(&self, record: FeedRecord, live: bool, max_keys: usize, metrics: &Metrics) {
        let record = Arc::new(record);
        let mut cache = self.cache.lock().expect("feed cache");
        if let Some(key) = &record.key {
            if record.tombstone {
                cache.remove(key);
            } else if cache.len() < max_keys || cache.contains_key(key) {
                cache.insert(key.clone(), record.clone());
            } else {
                metrics.snapshot_keys_dropped.fetch_add(1, Relaxed);
            }
        }
        if live {
            // Err only means nobody is subscribed at this instant.
            let _ = self.tx.send(record);
            metrics.feed_records.fetch_add(1, Relaxed);
        }
    }
}

struct PartitionState {
    partition: i32,
    position: i64,
    /// Records below this offset existed before the feed started: they
    /// warm the snapshot but are not news to anyone.
    live_from: i64,
}

async fn run_feed(hub: Arc<Hub>, feed: Arc<Feed>) {
    let topic = feed.topic.clone();
    info!(%topic, "feed starting");
    let mut backoff = Duration::from_millis(100);
    let mut idle_since: Option<Instant> = None;
    let mut consumer: Option<Consumer> = None;
    let mut partitions: Vec<PartitionState> = Vec::new();
    let mut last_refresh = Instant::now();
    loop {
        if feed.tx.receiver_count() == 0 {
            let since = *idle_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= hub.config.feed_idle && hub.retire(&feed) {
                break;
            }
        } else {
            idle_since = None;
        }

        let result = async {
            if consumer.is_none() {
                let c =
                    Consumer::connect(hub.config.broker, &format!("{}-feed", hub.config.client_id))
                        .await?
                        .with_fetch_max_wait_ms(FETCH_WAIT_MS);
                consumer = Some(c);
            }
            let c = consumer.as_ref().expect("connected");
            if partitions.is_empty() || last_refresh.elapsed() >= PARTITION_REFRESH {
                last_refresh = Instant::now();
                let promised = *feed.warm.borrow();
                discover(
                    c,
                    &topic,
                    &mut partitions,
                    hub.config.snapshot_warmup_records,
                    promised,
                )
                .await?;
            }
            if partitions.is_empty() {
                // Nothing to read yet, so nothing to wait for.
                feed.warm.send_replace(true);
                tokio::time::sleep(Duration::from_millis(FETCH_WAIT_MS as u64)).await;
                return Ok(());
            }
            let requests: Vec<(String, i32, i64)> = partitions
                .iter()
                .map(|p| (topic.clone(), p.partition, p.position))
                .collect();
            let fetched = c.fetch_many_public(&requests, FETCH_WAIT_MS).await?;
            for (_, partition, records) in fetched {
                let Some(state) = partitions.iter_mut().find(|p| p.partition == partition) else {
                    continue;
                };
                for record in records {
                    if record.offset < state.position {
                        continue;
                    }
                    state.position = record.offset + 1;
                    let live = record.offset >= state.live_from;
                    let encoded = encode(&topic, partition, &record);
                    feed.publish(encoded, live, hub.config.snapshot_max_keys, &hub.metrics);
                }
            }
            if !*feed.warm.borrow() && partitions.iter().all(|p| p.position >= p.live_from) {
                feed.warm.send_replace(true);
            }
            Ok::<_, ClientError>(())
        }
        .await;

        match result {
            Ok(()) => backoff = Duration::from_millis(100),
            Err(ClientError::Server { code, .. }) if code == ec::OFFSET_OUT_OF_RANGE => {
                // Retention deleted what this feed was about to read. Skip
                // to what still exists.
                if let Some(c) = &consumer {
                    for p in &mut partitions {
                        if let Ok(earliest) = c.list_offsets(&topic, p.partition, EARLIEST).await {
                            p.position = p.position.max(earliest);
                        }
                    }
                }
            }
            Err(error) => {
                hub.metrics.feed_errors.fetch_add(1, Relaxed);
                warn!(%topic, %error, "feed fetch failed; retrying");
                if matches!(error, ClientError::Io(_) | ClientError::ConnectionClosed) {
                    consumer = None;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
    hub.metrics.feeds_active.fetch_sub(1, Relaxed);
    // Dropping the last reference to the feed drops its sender, which ends
    // any receiver that somehow outlived it.
    info!(%topic, "feed stopped: no subscribers");
}

/// Find the topic's partitions and where to start reading the new ones.
async fn discover(
    consumer: &Consumer,
    topic: &str,
    partitions: &mut Vec<PartitionState>,
    warmup: i64,
    promised: bool,
) -> Result<(), ClientError> {
    let metadata = consumer.metadata(&[topic.to_owned()]).await?;
    let Some(info) = metadata.topics.iter().find(|t| t.name == topic) else {
        return Ok(());
    };
    if info.error_code != ec::NONE {
        return ClientError::from_error_code(info.error_code);
    }
    // Partitions found when the feed starts are live from their end.
    // Partitions found after subscribers were told the feed is live (a
    // topic created after it was subscribed to, or grown since) are live
    // from their beginning: everything in them is news to someone.
    let from_end = partitions.is_empty() && !promised;
    for p in &info.partitions {
        if partitions.iter().any(|s| s.partition == p.partition) {
            continue;
        }
        let latest = consumer.list_offsets(topic, p.partition, LATEST).await?;
        let earliest = consumer.list_offsets(topic, p.partition, EARLIEST).await?;
        let live_from = if from_end { latest } else { earliest };
        let position = (live_from - warmup.max(0)).max(earliest);
        debug!(%topic, partition = p.partition, position, live_from, "feed partition");
        partitions.push(PartitionState {
            partition: p.partition,
            position,
            live_from,
        });
    }
    partitions.sort_by_key(|p| p.partition);
    Ok(())
}

/// The frame every subscriber of this record receives:
///
/// ```json
/// {"type":"record","topic":"prices","partition":3,"offset":1841,
///  "timestamp":1727460000000,"key":"AAPL","value":"{\"bid\":1}",
///  "headers":{"x-gw-user":"feed-1"}}
/// ```
///
/// Keys and values that are not UTF-8 travel as `key_b64` / `value_b64`;
/// `"value": null` is a tombstone.
pub fn encode(topic: &str, partition: i32, record: &FetchedRecord) -> FeedRecord {
    let mut frame = Map::with_capacity(8);
    frame.insert("type".into(), Value::from("record"));
    frame.insert("topic".into(), Value::from(topic));
    frame.insert("partition".into(), Value::from(partition));
    frame.insert("offset".into(), Value::from(record.offset));
    frame.insert("timestamp".into(), Value::from(record.timestamp));
    if let Some(key) = &record.key {
        put_bytes(&mut frame, "key", key);
    }
    match &record.value {
        Some(value) => put_bytes(&mut frame, "value", value),
        None => {
            frame.insert("value".into(), Value::Null);
        }
    }
    if !record.headers.is_empty() {
        let headers: Map<String, Value> = record
            .headers
            .iter()
            .map(|h| {
                let v = match &h.value {
                    Some(v) => Value::from(String::from_utf8_lossy(v).into_owned()),
                    None => Value::Null,
                };
                (h.key.clone(), v)
            })
            .collect();
        frame.insert("headers".into(), Value::Object(headers));
    }
    let text = serde_json::to_string(&Value::Object(frame)).expect("record frames serialize");
    FeedRecord {
        key: record.key.clone(),
        partition,
        offset: record.offset,
        tombstone: record.value.is_none(),
        frame: Utf8Bytes::from(text),
    }
}

fn put_bytes(frame: &mut Map<String, Value>, name: &str, bytes: &Bytes) {
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            frame.insert(name.into(), Value::from(text));
        }
        Err(_) => {
            frame.insert(format!("{name}_b64"), Value::from(STANDARD.encode(bytes)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brahmaputra_protocol::RecordHeader;

    fn fetched(offset: i64, key: &str, value: Option<&[u8]>) -> FetchedRecord {
        FetchedRecord {
            offset,
            key: Some(Bytes::from(key.to_owned())),
            value: value.map(Bytes::copy_from_slice),
            timestamp: 1_000,
            headers: vec![RecordHeader {
                key: "h".into(),
                value: Some(Bytes::from_static(b"v")),
            }],
        }
    }

    #[test]
    fn records_encode_text_binary_and_tombstones() {
        let r = encode("prices", 2, &fetched(7, "AAPL", Some(b"{\"bid\":1}")));
        let v: Value = serde_json::from_str(r.frame.as_str()).unwrap();
        assert_eq!(v["type"], "record");
        assert_eq!(v["partition"], 2);
        assert_eq!(v["offset"], 7);
        assert_eq!(v["key"], "AAPL");
        assert_eq!(v["value"], "{\"bid\":1}");
        assert_eq!(v["headers"]["h"], "v");
        assert!(!r.tombstone);

        let bin = encode("t", 0, &fetched(1, "k", Some(&[0xff, 0])));
        let v: Value = serde_json::from_str(bin.frame.as_str()).unwrap();
        assert_eq!(v["value_b64"], "/wA=");
        assert!(v.get("value").is_none());

        let tomb = encode("t", 0, &fetched(2, "k", None));
        assert!(tomb.tombstone);
        let v: Value = serde_json::from_str(tomb.frame.as_str()).unwrap();
        assert!(v["value"].is_null());
    }

    #[test]
    fn cache_keeps_latest_per_key_and_honours_tombstones_and_bounds() {
        let metrics = Metrics::default();
        let (tx, _) = broadcast::channel(16);
        let feed = Feed {
            topic: "t".into(),
            tx,
            cache: Mutex::new(HashMap::new()),
            warm: watch::channel(false).0,
        };
        feed.publish(
            encode("t", 0, &fetched(0, "a", Some(b"1"))),
            true,
            2,
            &metrics,
        );
        feed.publish(
            encode("t", 0, &fetched(1, "a", Some(b"2"))),
            true,
            2,
            &metrics,
        );
        feed.publish(
            encode("t", 0, &fetched(2, "b", Some(b"3"))),
            true,
            2,
            &metrics,
        );
        feed.publish(
            encode("t", 0, &fetched(3, "c", Some(b"4"))),
            true,
            2,
            &metrics,
        );
        {
            let cache = feed.cache.lock().unwrap();
            assert_eq!(cache.len(), 2, "bounded");
            assert_eq!(cache[&Bytes::from_static(b"a")].offset, 1);
        }
        assert_eq!(metrics.snapshot_keys_dropped.load(Relaxed), 1);
        feed.publish(encode("t", 0, &fetched(4, "a", None)), true, 2, &metrics);
        assert!(!feed
            .cache
            .lock()
            .unwrap()
            .contains_key(&Bytes::from_static(b"a")));
    }
}
