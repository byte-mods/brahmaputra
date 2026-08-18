//! In-process metrics: a registry, a bounded time-series store, and a
//! Prometheus text exporter (DESIGN.md §9.1).
//!
//! There is deliberately no external time-series database here. A broker
//! keeps its own recent history in a fixed-size ring per metric, which is
//! what the built-in dashboard charts. That bounds memory by construction
//! — a broker running for a month uses exactly as much metric memory as one
//! running for an hour — and it means the dashboard works on a laptop with
//! no Prometheus in sight. `GET /metrics` still exports the current values
//! in Prometheus text format for deployments that do have one.
//!
//! The registry is lock-free on the hot path: counters and gauges are
//! atomics behind a concurrent map, so instrumenting a request costs an
//! atomic add, not a mutex.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::Serialize;

/// Seconds between time-series samples.
pub const SAMPLE_INTERVAL_SECS: u64 = 5;
/// Samples retained per metric: six hours at the interval above.
pub const SERIES_CAPACITY: usize = (6 * 60 * 60 / SAMPLE_INTERVAL_SECS) as usize;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// A metric's identity: a name plus optional label pairs, e.g.
/// `partition_log_end_offset{topic="orders",partition="0"}`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricKey {
    pub name: &'static str,
    pub labels: Vec<(String, String)>,
}

impl MetricKey {
    pub fn new(name: &'static str) -> Self {
        MetricKey {
            name,
            labels: Vec::new(),
        }
    }

    pub fn with(name: &'static str, labels: &[(&str, &str)]) -> Self {
        MetricKey {
            name,
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    /// Prometheus exposition form.
    fn render(&self) -> String {
        if self.labels.is_empty() {
            return self.name.to_owned();
        }
        let labels = self
            .labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{}\"", escape(value)))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}{{{labels}}}", self.name)
    }
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// What a metric means, which decides how it is exported and charted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Monotonic total (requests served, bytes in). Charts show the rate.
    Counter,
    /// Point-in-time value (log end offset, ISR size, lag).
    Gauge,
}

/// One sample in a metric's history.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Sample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A bounded ring of samples for one metric.
#[derive(Debug, Default)]
struct Series {
    samples: Vec<Sample>,
    next: usize,
}

impl Series {
    fn push(&mut self, sample: Sample) {
        if self.samples.len() < SERIES_CAPACITY {
            self.samples.push(sample);
        } else {
            self.samples[self.next] = sample;
            self.next = (self.next + 1) % SERIES_CAPACITY;
        }
    }

    /// Samples in chronological order, oldest first.
    fn ordered(&self) -> Vec<Sample> {
        if self.samples.len() < SERIES_CAPACITY {
            return self.samples.clone();
        }
        let mut out = Vec::with_capacity(self.samples.len());
        out.extend_from_slice(&self.samples[self.next..]);
        out.extend_from_slice(&self.samples[..self.next]);
        out
    }
}

#[derive(Debug)]
enum Value {
    Counter(AtomicU64),
    Gauge(AtomicI64),
}

impl Value {
    fn read(&self) -> f64 {
        match self {
            Value::Counter(value) => value.load(Ordering::Relaxed) as f64,
            Value::Gauge(value) => value.load(Ordering::Relaxed) as f64,
        }
    }

    fn kind(&self) -> MetricKind {
        match self {
            Value::Counter(_) => MetricKind::Counter,
            Value::Gauge(_) => MetricKind::Gauge,
        }
    }
}

/// The broker's metric registry. Cheap to clone (it is an `Arc` inside).
#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    values: DashMap<MetricKey, Value>,
    help: DashMap<&'static str, &'static str>,
    series: DashMap<MetricKey, Series>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics::default()
    }

    /// Describe a metric once, for the Prometheus `# HELP` line.
    pub fn describe(&self, name: &'static str, help: &'static str) {
        self.inner.help.insert(name, help);
    }

    pub fn increment(&self, key: MetricKey, delta: u64) {
        match self.inner.values.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if let Value::Counter(counter) = entry.get() {
                    counter.fetch_add(delta, Ordering::Relaxed);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Value::Counter(AtomicU64::new(delta)));
            }
        }
    }

    pub fn count(&self, name: &'static str, delta: u64) {
        self.increment(MetricKey::new(name), delta);
    }

    pub fn set_gauge(&self, key: MetricKey, value: i64) {
        match self.inner.values.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if let Value::Gauge(gauge) = entry.get() {
                    gauge.store(value, Ordering::Relaxed);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Value::Gauge(AtomicI64::new(value)));
            }
        }
    }

    pub fn gauge(&self, name: &'static str, value: i64) {
        self.set_gauge(MetricKey::new(name), value);
    }

    /// Drop every gauge whose labels match a prefix — used when a topic or
    /// partition goes away so its series do not linger forever.
    pub fn remove_matching(&self, name: &'static str, labels: &[(&str, &str)]) {
        let matches = |key: &MetricKey| {
            key.name == name
                && labels.iter().all(|(label, value)| {
                    key.labels
                        .iter()
                        .any(|(k, v)| k == label && v == value)
                })
        };
        self.inner.values.retain(|key, _| !matches(key));
        self.inner.series.retain(|key, _| !matches(key));
    }

    /// Current value of every metric, sorted for stable output.
    pub fn snapshot(&self) -> BTreeMap<String, f64> {
        self.inner
            .values
            .iter()
            .map(|entry| (entry.key().render(), entry.value().read()))
            .collect()
    }

    /// Take one sample of every metric into its ring. Called on a timer.
    pub fn sample(&self) {
        let timestamp_ms = now_ms();
        for entry in self.inner.values.iter() {
            let sample = Sample {
                timestamp_ms,
                value: entry.value().read(),
            };
            self.inner
                .series
                .entry(entry.key().clone())
                .or_default()
                .push(sample);
        }
    }

    /// History for one metric, oldest sample first, optionally bounded to a
    /// time window.
    pub fn series(&self, name: &str, from_ms: Option<i64>, to_ms: Option<i64>) -> Vec<Sample> {
        self.inner
            .series
            .iter()
            .filter(|entry| entry.key().render() == name || entry.key().name == name)
            .flat_map(|entry| entry.value().ordered())
            .filter(|sample| {
                from_ms.is_none_or(|from| sample.timestamp_ms >= from)
                    && to_ms.is_none_or(|to| sample.timestamp_ms <= to)
            })
            .collect()
    }

    /// Every metric that currently has history, for the dashboard's picker.
    pub fn series_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .series
            .iter()
            .map(|entry| entry.key().render())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Prometheus text exposition of current values.
    pub fn prometheus(&self) -> String {
        // Group by metric name so HELP/TYPE are emitted once each, as the
        // exposition format requires.
        let mut by_name: BTreeMap<&'static str, (MetricKind, Vec<(String, f64)>)> = BTreeMap::new();
        for entry in self.inner.values.iter() {
            let key = entry.key();
            let rendered = key.render();
            by_name
                .entry(key.name)
                .or_insert_with(|| (entry.value().kind(), Vec::new()))
                .1
                .push((rendered, entry.value().read()));
        }

        let mut out = String::new();
        for (name, (kind, mut samples)) in by_name {
            if let Some(help) = self.inner.help.get(name) {
                out.push_str(&format!("# HELP {name} {}\n", *help));
            }
            out.push_str(&format!(
                "# TYPE {name} {}\n",
                match kind {
                    MetricKind::Counter => "counter",
                    MetricKind::Gauge => "gauge",
                }
            ));
            samples.sort_by(|a, b| a.0.cmp(&b.0));
            for (rendered, value) in samples {
                out.push_str(&format!("{rendered} {value}\n"));
            }
        }
        out
    }
}

/// Metric names, in one place so the broker and the dashboard cannot
/// disagree about what a series is called.
pub mod names {
    pub const PRODUCE_REQUESTS: &str = "brahmaputra_produce_requests_total";
    pub const PRODUCE_RECORDS: &str = "brahmaputra_produce_records_total";
    pub const PRODUCE_BYTES: &str = "brahmaputra_produce_bytes_total";
    pub const PRODUCE_ERRORS: &str = "brahmaputra_produce_errors_total";
    pub const FETCH_REQUESTS: &str = "brahmaputra_fetch_requests_total";
    pub const FETCH_BYTES: &str = "brahmaputra_fetch_bytes_total";
    pub const REQUESTS: &str = "brahmaputra_requests_total";
    pub const THROTTLED_REQUESTS: &str = "brahmaputra_throttled_requests_total";
    pub const THROTTLE_MS: &str = "brahmaputra_throttle_ms_total";
    pub const CONNECTIONS: &str = "brahmaputra_connections_open";
    pub const LOG_END_OFFSET: &str = "brahmaputra_partition_log_end_offset";
    pub const LOG_START_OFFSET: &str = "brahmaputra_partition_log_start_offset";
    pub const HIGH_WATERMARK: &str = "brahmaputra_partition_high_watermark";
    pub const ISR_SIZE: &str = "brahmaputra_partition_isr_size";
    pub const UNDER_REPLICATED: &str = "brahmaputra_under_replicated_partitions";
    pub const GROUP_MEMBERS: &str = "brahmaputra_group_members";
    pub const GROUP_LAG: &str = "brahmaputra_group_lag";
    pub const LEADER_PARTITIONS: &str = "brahmaputra_leader_partitions";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_and_gauges_replace() {
        let metrics = Metrics::new();
        metrics.count(names::PRODUCE_REQUESTS, 1);
        metrics.count(names::PRODUCE_REQUESTS, 4);
        metrics.gauge(names::CONNECTIONS, 7);
        metrics.gauge(names::CONNECTIONS, 3);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot[names::PRODUCE_REQUESTS], 5.0);
        assert_eq!(snapshot[names::CONNECTIONS], 3.0);
    }

    #[test]
    fn labelled_metrics_are_separate_series() {
        let metrics = Metrics::new();
        metrics.set_gauge(
            MetricKey::with(names::LOG_END_OFFSET, &[("topic", "a"), ("partition", "0")]),
            10,
        );
        metrics.set_gauge(
            MetricKey::with(names::LOG_END_OFFSET, &[("topic", "a"), ("partition", "1")]),
            20,
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot
            .keys()
            .any(|key| key.contains("partition=\"0\"") && key.contains("topic=\"a\"")));
    }

    #[test]
    fn the_series_ring_is_bounded_and_stays_chronological() {
        let metrics = Metrics::new();
        for value in 0..(SERIES_CAPACITY as i64 + 50) {
            metrics.gauge(names::CONNECTIONS, value);
            metrics.sample();
        }
        let series = metrics.series(names::CONNECTIONS, None, None);
        assert_eq!(
            series.len(),
            SERIES_CAPACITY,
            "history is capped at the ring size, however long the broker runs"
        );
        // Oldest first, and the newest sample is the last value written.
        assert_eq!(
            series.last().unwrap().value,
            (SERIES_CAPACITY as i64 + 49) as f64
        );
        assert!(series
            .windows(2)
            .all(|pair| pair[0].timestamp_ms <= pair[1].timestamp_ms));
    }

    #[test]
    fn prometheus_output_groups_by_name_with_one_type_line() {
        let metrics = Metrics::new();
        metrics.describe(names::PRODUCE_REQUESTS, "Produce requests served");
        metrics.count(names::PRODUCE_REQUESTS, 2);
        metrics.set_gauge(
            MetricKey::with(names::LOG_END_OFFSET, &[("topic", "t"), ("partition", "0")]),
            42,
        );

        let text = metrics.prometheus();
        assert_eq!(
            text.matches("# TYPE brahmaputra_produce_requests_total").count(),
            1
        );
        assert!(text.contains("# HELP brahmaputra_produce_requests_total Produce requests served"));
        assert!(text.contains("brahmaputra_produce_requests_total 2"));
        assert!(text.contains(
            "brahmaputra_partition_log_end_offset{topic=\"t\",partition=\"0\"} 42"
        ));
    }

    #[test]
    fn removing_a_partition_drops_its_series() {
        let metrics = Metrics::new();
        metrics.set_gauge(
            MetricKey::with(names::LOG_END_OFFSET, &[("topic", "gone"), ("partition", "0")]),
            1,
        );
        metrics.sample();
        assert_eq!(metrics.series_names().len(), 1);

        metrics.remove_matching(names::LOG_END_OFFSET, &[("topic", "gone")]);
        assert!(metrics.snapshot().is_empty());
        assert!(metrics.series_names().is_empty());
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Printing every series would be unreadable and would hold the map
        // locked; the count is what a config dump actually wants.
        f.debug_struct("Metrics")
            .field("metrics", &self.inner.values.len())
            .finish()
    }
}
