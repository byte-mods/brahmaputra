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

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Which direction a byte charge applies to. Produce and fetch have
/// separate budgets, as in Kafka: a client that reads a lot should not
/// lose its ability to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuotaKind {
    Produce,
    Fetch,
    /// Inter-broker replication: a follower catching up.
    ///
    /// Separate from Fetch because it is charged against the cluster
    /// rather than a client, and because the two want opposite defaults —
    /// client traffic should be limited only when an operator says so,
    /// while replication is exactly the traffic that needs a ceiling if a
    /// single broker restart is not to become a cluster-wide latency
    /// event.
    Replication,
}

/// Per-client byte-rate limits. `None` for a direction means unlimited.
#[derive(Debug, Clone, Copy, Default)]
pub struct QuotaConfig {
    pub produce_bytes_per_sec: Option<u64>,
    pub fetch_bytes_per_sec: Option<u64>,
    /// Ceiling on bytes served to *followers* catching up.
    ///
    /// Without one, a rejoining broker fetches as fast as the leader can
    /// read, competing with client traffic for the same disk and NIC. One
    /// restart then shows up as latency on every producer and consumer
    /// talking to that leader.
    pub replication_bytes_per_sec: Option<u64>,
    /// Cap on how long a single response may be delayed, so a very large
    /// request against a very small quota cannot hang a client forever.
    pub max_throttle: Option<Duration>,
}

impl QuotaConfig {
    pub fn is_enabled(&self) -> bool {
        self.produce_bytes_per_sec.is_some()
            || self.fetch_bytes_per_sec.is_some()
            || self.replication_bytes_per_sec.is_some()
    }

    fn rate(&self, kind: QuotaKind) -> Option<u64> {
        match kind {
            QuotaKind::Produce => self.produce_bytes_per_sec,
            QuotaKind::Fetch => self.fetch_bytes_per_sec,
            QuotaKind::Replication => self.replication_bytes_per_sec,
        }
    }
}

/// The default ceiling on a single throttle, matching Kafka's
/// `quota.window.size.seconds` order of magnitude.
const DEFAULT_MAX_THROTTLE: Duration = Duration::from_secs(30);
/// Hard ceiling on live accounting identities.
///
/// `client.id` is supplied by the peer, so an unbounded map lets one
/// connection grow broker memory forever merely by rotating IDs. Keys are
/// fixed-size hashes and the table is capped; eviction may grant a fresh
/// one-second burst to a churned identity, but can never lose or corrupt a
/// request.
const MAX_QUOTA_BUCKETS: usize = 4_096;

#[derive(Debug)]
struct Bucket {
    /// Bytes of credit available now.
    available: f64,
    last_refill: Instant,
}

/// What a bucket is accounted against.
///
/// Both halves of the identity, not just the client id: two tenants that
/// happen to ship the same `client.id` — the default one their library
/// picked, most likely — must not draw down each other's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BucketKey {
    identity: u64,
    kind: QuotaKind,
}

/// Byte-rate accounting for every client the broker has seen.
#[derive(Default)]
pub struct QuotaManager {
    config: QuotaConfig,
    buckets: Mutex<HashMap<BucketKey, Bucket>>,
    identity_hasher: RandomState,
}

impl QuotaManager {
    pub fn new(config: QuotaConfig) -> Self {
        QuotaManager {
            config,
            buckets: Mutex::new(HashMap::new()),
            identity_hasher: RandomState::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Charge `bytes` to one client and return how long the caller should
    /// wait before responding. `Duration::ZERO` means the client is inside
    /// its budget.
    ///
    /// `override_rate` is the ceiling a matching quota entity imposes; with
    /// `None` the broker-wide default applies. Accounting is per (user,
    /// client id) either way, so tightening one tenant's limit does not
    /// disturb anyone else's bucket.
    ///
    /// A client that identifies itself as nothing shares one bucket: that
    /// is deliberate, since otherwise anonymity would be a way around the
    /// limit.
    pub fn throttle_for(
        &self,
        user: Option<&str>,
        client_id: Option<&str>,
        kind: QuotaKind,
        bytes: u64,
        override_rate: Option<u64>,
    ) -> Duration {
        let Some(rate) = override_rate.or_else(|| self.config.rate(kind)) else {
            return Duration::ZERO;
        };
        if rate == 0 || bytes == 0 {
            return Duration::ZERO;
        }
        // Store no attacker-controlled strings in this process-lifetime
        // table. RandomState gives every broker a secret hash seed, so a
        // peer cannot cheaply manufacture collisions between tenants.
        let key = BucketKey {
            identity: self
                .identity_hasher
                .hash_one((user, client_id.unwrap_or(""))),
            kind,
        };
        let now = Instant::now();

        let mut buckets = self.buckets.lock().expect("quota buckets");
        if !buckets.contains_key(&key) && buckets.len() >= MAX_QUOTA_BUCKETS {
            // An arbitrary victim keeps insertion O(1) under hostile ID
            // churn. Exact LRU ordering would itself need unbounded or
            // attacker-amplified bookkeeping; all buckets are equivalent
            // for correctness and a re-created one merely regains its
            // normal initial burst.
            if let Some(victim) = buckets.keys().next().copied() {
                buckets.remove(&victim);
            }
        }
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
            replication_bytes_per_sec: Some(rate),
            fetch_bytes_per_sec: Some(rate),
            max_throttle: Some(Duration::from_secs(5)),
        })
    }

    #[test]
    fn no_quota_configured_never_throttles() {
        let manager = QuotaManager::default();
        assert!(!manager.is_enabled());
        assert_eq!(
            manager.throttle_for(None, Some("c"), QuotaKind::Produce, 10_000_000, None),
            Duration::ZERO
        );
    }

    #[test]
    fn traffic_inside_the_budget_is_not_delayed() {
        let manager = manager(1_000_000);
        assert_eq!(
            manager.throttle_for(None, Some("c"), QuotaKind::Produce, 500_000, None),
            Duration::ZERO
        );
    }

    #[test]
    fn overdrawing_the_budget_delays_in_proportion_to_the_overdraft() {
        let manager = manager(1_000);
        // Burn the initial second of credit, then overdraw by 2 000 bytes
        // against a 1 000 B/s rate: that is two seconds of repayment.
        assert_eq!(
            manager.throttle_for(None, Some("c"), QuotaKind::Produce, 1_000, None),
            Duration::ZERO
        );
        let throttle = manager.throttle_for(None, Some("c"), QuotaKind::Produce, 2_000, None);
        assert!(
            throttle >= Duration::from_millis(1_900) && throttle <= Duration::from_millis(2_100),
            "expected ~2s, got {throttle:?}"
        );
    }

    #[test]
    fn produce_and_fetch_budgets_are_independent() {
        let manager = manager(1_000);
        assert_eq!(
            manager.throttle_for(None, Some("c"), QuotaKind::Produce, 1_000, None),
            Duration::ZERO
        );
        // The produce bucket is empty, but fetch has its own full budget.
        assert_eq!(
            manager.throttle_for(None, Some("c"), QuotaKind::Fetch, 1_000, None),
            Duration::ZERO
        );
    }

    #[test]
    fn clients_are_accounted_separately() {
        let manager = manager(1_000);
        assert_eq!(
            manager.throttle_for(None, Some("noisy"), QuotaKind::Produce, 5_000, None),
            Duration::from_secs(4)
        );
        // A different client is untouched by the noisy one's overdraft.
        assert_eq!(
            manager.throttle_for(None, Some("quiet"), QuotaKind::Produce, 500, None),
            Duration::ZERO
        );
    }

    #[test]
    fn a_single_throttle_is_capped() {
        let manager = manager(1);
        let throttle = manager.throttle_for(None, Some("c"), QuotaKind::Produce, 1_000_000, None);
        assert_eq!(throttle, Duration::from_secs(5), "capped by max_throttle");
    }

    #[test]
    fn rotating_untrusted_client_ids_cannot_grow_the_bucket_table_forever() {
        let manager = manager(1_000);
        for id in 0..(MAX_QUOTA_BUCKETS + 512) {
            manager.throttle_for(
                Some("tenant"),
                Some(&format!("attacker-{id}")),
                QuotaKind::Produce,
                1,
                None,
            );
        }
        assert_eq!(manager.buckets.lock().unwrap().len(), MAX_QUOTA_BUCKETS);
    }
}

#[cfg(test)]
mod replication_quota_tests {
    use super::*;

    /// The three budgets are independent. A follower catching up must not
    /// be able to exhaust the budget a producer needs, and vice versa —
    /// that separation is the entire reason replication has its own
    /// ceiling rather than sharing the fetch one.
    #[test]
    fn replication_has_a_budget_of_its_own() {
        let manager = QuotaManager::new(QuotaConfig {
            produce_bytes_per_sec: Some(1_000),
            fetch_bytes_per_sec: Some(1_000),
            replication_bytes_per_sec: Some(1_000),
            max_throttle: Some(Duration::from_secs(30)),
        });

        // Spend the replication budget several times over.
        for _ in 0..5 {
            manager.throttle_for(None, Some("replica-2"), QuotaKind::Replication, 1_000, None);
        }
        assert!(
            !manager
                .throttle_for(None, Some("replica-2"), QuotaKind::Replication, 1_000, None)
                .is_zero(),
            "replication should be throttled once its budget is spent"
        );

        // A client's produce and fetch budgets are untouched by that.
        assert!(
            manager
                .throttle_for(None, Some("app"), QuotaKind::Produce, 500, None)
                .is_zero(),
            "a catching-up follower must not throttle a producer"
        );
        assert!(
            manager
                .throttle_for(None, Some("app"), QuotaKind::Fetch, 500, None)
                .is_zero(),
            "a catching-up follower must not throttle a consumer"
        );
    }

    /// Replication is unlimited unless an operator sets a ceiling, matching
    /// how the other two directions behave.
    #[test]
    fn replication_is_unlimited_by_default() {
        let manager = QuotaManager::new(QuotaConfig {
            produce_bytes_per_sec: Some(10),
            fetch_bytes_per_sec: Some(10),
            replication_bytes_per_sec: None,
            max_throttle: Some(Duration::from_secs(30)),
        });
        for _ in 0..100 {
            assert!(
                manager
                    .throttle_for(
                        None,
                        Some("replica-9"),
                        QuotaKind::Replication,
                        1_000_000,
                        None
                    )
                    .is_zero(),
                "no replication ceiling means no replication throttling"
            );
        }
    }

    /// Enabling only a replication ceiling must still switch quotas on, or
    /// the broker would skip the whole quota path and ignore it.
    #[test]
    fn a_replication_ceiling_alone_enables_quotas() {
        let config = QuotaConfig {
            produce_bytes_per_sec: None,
            fetch_bytes_per_sec: None,
            replication_bytes_per_sec: Some(1_000),
            max_throttle: None,
        };
        assert!(config.is_enabled());
    }
}
