//! Client byte-rate quotas (DESIGN.md §12, M5 hardening).
//!
//! A single misbehaving client should not be able to starve every other
//! client of a broker's disk and network. Kafka solves this by *delaying*
//! the offender's responses rather than rejecting its requests: the client
//! stays correct, it simply goes slower, and no data is lost. This does the
//! same.
//!
//! The accounting is a token bucket per (client id, direction), refilled
//! continuously at the configured byte rate. A request that overdraws the
//! bucket is served in full, and the *response* is held back for however
//! long the overdraft takes to repay. Because the delay is applied after
//! the work is done, a quota can never corrupt or drop a record — it only
//! ever costs latency.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Which direction a byte charge applies to. Produce and fetch have
/// separate budgets, as in Kafka: a client that reads a lot should not
/// lose its ability to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuotaKind {
    Produce,
    Fetch,
}

/// Per-client byte-rate limits. `None` for a direction means unlimited.
#[derive(Debug, Clone, Copy, Default)]
pub struct QuotaConfig {
    pub produce_bytes_per_sec: Option<u64>,
    pub fetch_bytes_per_sec: Option<u64>,
    /// Cap on how long a single response may be delayed, so a very large
    /// request against a very small quota cannot hang a client forever.
    pub max_throttle: Option<Duration>,
}

impl QuotaConfig {
    pub fn is_enabled(&self) -> bool {
        self.produce_bytes_per_sec.is_some() || self.fetch_bytes_per_sec.is_some()
    }

    fn rate(&self, kind: QuotaKind) -> Option<u64> {
        match kind {
            QuotaKind::Produce => self.produce_bytes_per_sec,
            QuotaKind::Fetch => self.fetch_bytes_per_sec,
        }
    }
}

/// The default ceiling on a single throttle, matching Kafka's
/// `quota.window.size.seconds` order of magnitude.
const DEFAULT_MAX_THROTTLE: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Bucket {
    /// Bytes of credit available now.
    available: f64,
    last_refill: Instant,
}

/// Byte-rate accounting for every client the broker has seen.
#[derive(Default)]
pub struct QuotaManager {
    config: QuotaConfig,
    buckets: Mutex<HashMap<(String, QuotaKind), Bucket>>,
}

impl QuotaManager {
    pub fn new(config: QuotaConfig) -> Self {
        QuotaManager {
            config,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Charge `bytes` to `client_id` and return how long the caller should
    /// wait before responding. `Duration::ZERO` means the client is inside
    /// its budget.
    ///
    /// A client that identifies itself as nothing shares one bucket: that
    /// is deliberate, since otherwise anonymity would be a way around the
    /// limit.
    pub fn throttle_for(&self, client_id: Option<&str>, kind: QuotaKind, bytes: u64) -> Duration {
        let Some(rate) = self.config.rate(kind) else {
            return Duration::ZERO;
        };
        if rate == 0 || bytes == 0 {
            return Duration::ZERO;
        }
        let key = (client_id.unwrap_or("").to_owned(), kind);
        let now = Instant::now();

        let mut buckets = self.buckets.lock().expect("quota buckets");
        let bucket = buckets.entry(key).or_insert_with(|| Bucket {
            // Start with one second of credit so a first request is not
            // throttled for having arrived first.
            available: rate as f64,
            last_refill: now,
        });

        let elapsed = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        bucket.last_refill = now;
        // Refill, capped at one second of burst: allowing unbounded credit
        // to accumulate would let an idle client spike arbitrarily hard.
        bucket.available = (bucket.available + elapsed * rate as f64).min(rate as f64);
        bucket.available -= bytes as f64;

        if bucket.available >= 0.0 {
            return Duration::ZERO;
        }
        let seconds_owed = -bucket.available / rate as f64;
        let throttle = Duration::from_secs_f64(seconds_owed);
        let ceiling = self.config.max_throttle.unwrap_or(DEFAULT_MAX_THROTTLE);
        throttle.min(ceiling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(rate: u64) -> QuotaManager {
        QuotaManager::new(QuotaConfig {
            produce_bytes_per_sec: Some(rate),
            fetch_bytes_per_sec: Some(rate),
            max_throttle: Some(Duration::from_secs(5)),
        })
    }

    #[test]
    fn no_quota_configured_never_throttles() {
        let manager = QuotaManager::default();
        assert!(!manager.is_enabled());
        assert_eq!(
            manager.throttle_for(Some("c"), QuotaKind::Produce, 10_000_000),
            Duration::ZERO
        );
    }

    #[test]
    fn traffic_inside_the_budget_is_not_delayed() {
        let manager = manager(1_000_000);
        assert_eq!(
            manager.throttle_for(Some("c"), QuotaKind::Produce, 500_000),
            Duration::ZERO
        );
    }

    #[test]
    fn overdrawing_the_budget_delays_in_proportion_to_the_overdraft() {
        let manager = manager(1_000);
        // Burn the initial second of credit, then overdraw by 2 000 bytes
        // against a 1 000 B/s rate: that is two seconds of repayment.
        assert_eq!(
            manager.throttle_for(Some("c"), QuotaKind::Produce, 1_000),
            Duration::ZERO
        );
        let throttle = manager.throttle_for(Some("c"), QuotaKind::Produce, 2_000);
        assert!(
            throttle >= Duration::from_millis(1_900) && throttle <= Duration::from_millis(2_100),
            "expected ~2s, got {throttle:?}"
        );
    }

    #[test]
    fn produce_and_fetch_budgets_are_independent() {
        let manager = manager(1_000);
        assert_eq!(
            manager.throttle_for(Some("c"), QuotaKind::Produce, 1_000),
            Duration::ZERO
        );
        // The produce bucket is empty, but fetch has its own full budget.
        assert_eq!(
            manager.throttle_for(Some("c"), QuotaKind::Fetch, 1_000),
            Duration::ZERO
        );
    }

    #[test]
    fn clients_are_accounted_separately() {
        let manager = manager(1_000);
        assert_eq!(
            manager.throttle_for(Some("noisy"), QuotaKind::Produce, 5_000),
            Duration::from_secs(4)
        );
        // A different client is untouched by the noisy one's overdraft.
        assert_eq!(
            manager.throttle_for(Some("quiet"), QuotaKind::Produce, 500),
            Duration::ZERO
        );
    }

    #[test]
    fn a_single_throttle_is_capped() {
        let manager = manager(1);
        let throttle = manager.throttle_for(Some("c"), QuotaKind::Produce, 1_000_000);
        assert_eq!(throttle, Duration::from_secs(5), "capped by max_throttle");
    }
}
