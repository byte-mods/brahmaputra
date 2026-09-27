//! Lock-free counters rendered in the Prometheus text format.
//!
//! Every hot-path update is one relaxed atomic add: with a million
//! sockets a mutex here would be the bottleneck the rest of the design
//! avoids.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Upper bounds, in milliseconds, of the produce-latency buckets.
const LATENCY_BUCKETS_MS: [u64; 13] = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

#[derive(Default)]
pub struct Metrics {
    pub connections_open: AtomicU64,
    pub connections_total: AtomicU64,
    pub handshakes_rejected_auth: AtomicU64,
    pub handshakes_rejected_forbidden: AtomicU64,
    pub handshakes_rejected_capacity: AtomicU64,
    pub handshakes_rejected_other: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_received: AtomicU64,
    pub messages_produced: AtomicU64,
    pub produce_errors: AtomicU64,
    pub rejected_bad_request: AtomicU64,
    pub rejected_topic: AtomicU64,
    pub rejected_rate_limited: AtomicU64,
    pub rejected_overloaded: AtomicU64,
    pub inflight: AtomicU64,
    pub closed_idle: AtomicU64,
    pub closed_slow_reader: AtomicU64,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_MS.len()],
    latency_count: AtomicU64,
    latency_sum_us: AtomicU64,
}

impl Metrics {
    pub fn observe_produce(&self, micros: u64) {
        let ms = micros / 1000;
        for (bucket, bound) in self.latency_buckets.iter().zip(LATENCY_BUCKETS_MS) {
            if ms < bound {
                bucket.fetch_add(1, Relaxed);
                break;
            }
        }
        self.latency_count.fetch_add(1, Relaxed);
        self.latency_sum_us.fetch_add(micros, Relaxed);
    }

    pub fn render(&self, ready: bool) -> String {
        let mut out = String::with_capacity(4096);
        let g = |out: &mut String, name: &str, help: &str, kind: &str, v: u64| {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}"
            );
        };
        g(
            &mut out,
            "ws_connections_open",
            "Open WebSocket connections.",
            "gauge",
            self.connections_open.load(Relaxed),
        );
        g(
            &mut out,
            "ws_connections_total",
            "Upgrades accepted.",
            "counter",
            self.connections_total.load(Relaxed),
        );
        let _ = writeln!(out, "# HELP ws_handshakes_rejected_total Upgrades refused, by reason.\n# TYPE ws_handshakes_rejected_total counter");
        for (reason, v) in [
            ("unauthenticated", &self.handshakes_rejected_auth),
            ("forbidden", &self.handshakes_rejected_forbidden),
            ("capacity", &self.handshakes_rejected_capacity),
            ("other", &self.handshakes_rejected_other),
        ] {
            let _ = writeln!(
                out,
                "ws_handshakes_rejected_total{{reason=\"{reason}\"}} {}",
                v.load(Relaxed)
            );
        }
        g(
            &mut out,
            "ws_messages_received_total",
            "Publishes received from clients.",
            "counter",
            self.messages_received.load(Relaxed),
        );
        g(
            &mut out,
            "ws_bytes_received_total",
            "WebSocket payload bytes received.",
            "counter",
            self.bytes_received.load(Relaxed),
        );
        g(
            &mut out,
            "ws_messages_produced_total",
            "Records the broker acknowledged.",
            "counter",
            self.messages_produced.load(Relaxed),
        );
        g(
            &mut out,
            "ws_produce_errors_total",
            "Records the broker or producer failed.",
            "counter",
            self.produce_errors.load(Relaxed),
        );
        let _ = writeln!(out, "# HELP ws_messages_rejected_total Publishes refused before reaching the broker.\n# TYPE ws_messages_rejected_total counter");
        for (reason, v) in [
            ("bad_request", &self.rejected_bad_request),
            ("topic_not_allowed", &self.rejected_topic),
            ("rate_limited", &self.rejected_rate_limited),
            ("overloaded", &self.rejected_overloaded),
        ] {
            let _ = writeln!(
                out,
                "ws_messages_rejected_total{{reason=\"{reason}\"}} {}",
                v.load(Relaxed)
            );
        }
        g(
            &mut out,
            "ws_inflight_messages",
            "Publishes awaiting a broker acknowledgement.",
            "gauge",
            self.inflight.load(Relaxed),
        );
        let _ = writeln!(out, "# HELP ws_connections_closed_total Connections the gateway closed, by reason.\n# TYPE ws_connections_closed_total counter");
        let _ = writeln!(
            out,
            "ws_connections_closed_total{{reason=\"idle\"}} {}",
            self.closed_idle.load(Relaxed)
        );
        let _ = writeln!(
            out,
            "ws_connections_closed_total{{reason=\"slow_reader\"}} {}",
            self.closed_slow_reader.load(Relaxed)
        );

        let _ = writeln!(out, "# HELP ws_produce_latency_seconds Publish to broker acknowledgement.\n# TYPE ws_produce_latency_seconds histogram");
        let mut cumulative = 0;
        for (bucket, bound) in self.latency_buckets.iter().zip(LATENCY_BUCKETS_MS) {
            cumulative += bucket.load(Relaxed);
            let _ = writeln!(
                out,
                "ws_produce_latency_seconds_bucket{{le=\"{}\"}} {cumulative}",
                bound as f64 / 1000.0
            );
        }
        let count = self.latency_count.load(Relaxed);
        let _ = writeln!(
            out,
            "ws_produce_latency_seconds_bucket{{le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(
            out,
            "ws_produce_latency_seconds_sum {}",
            self.latency_sum_us.load(Relaxed) as f64 / 1e6
        );
        let _ = writeln!(out, "ws_produce_latency_seconds_count {count}");

        g(
            &mut out,
            "ws_ready",
            "1 when accepting connections.",
            "gauge",
            ready as u64,
        );
        if let Some(rss) = resident_memory_bytes() {
            g(
                &mut out,
                "process_resident_memory_bytes",
                "Resident set size.",
                "gauge",
                rss,
            );
        }
        out
    }
}

/// Resident set size from /proc (Linux); what the per-connection memory
/// figures in the README are measured with.
pub fn resident_memory_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}
