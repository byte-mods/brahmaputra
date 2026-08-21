//! Broker-side topic metadata: topic → partition count.
//!
//! M1 simplification (Blueprint 02 §6): metadata is an in-memory map
//! persisted to `<data_dir>/meta.toml` so topics survive restart. From M2
//! this is materialized from the Raft metadata log instead.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use brahmaputra_storage::LogConfig;
use tracing::warn;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::error::BrokerError;

const META_FILE: &str = "meta.toml";
const META_TEMP_FILE: &str = "meta.toml.tmp";

#[derive(Debug, Default, Serialize, Deserialize)]
struct MetaFile {
    #[serde(default)]
    topics: BTreeMap<String, TopicMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicMeta {
    partitions: i32,
}

/// Whether `name` is a legal topic name (same alphabet as Kafka).
pub fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Directory holding the log of one partition: `<data_dir>/<topic>-<partition>`.
pub fn partition_dir(data_dir: &Path, topic: &str, partition: i32) -> PathBuf {
    data_dir.join(format!("{topic}-{partition}"))
}

/// Shared topic metadata map plus its `meta.toml` persistence.
pub struct BrokerState {
    /// topic -> partition count.
    topics: DashMap<String, i32>,
    meta_path: PathBuf,
    persist_lock: Mutex<()>,
    default_partitions: i32,
}

impl BrokerState {
    /// Load `meta.toml` from `data_dir` if present; start empty otherwise.
    pub fn load(data_dir: &Path, default_partitions: i32) -> Result<Self, BrokerError> {
        if default_partitions < 1 {
            return Err(BrokerError::Meta("default_partitions must be >= 1".into()));
        }
        fs::create_dir_all(data_dir)?;
        let meta_path = data_dir.join(META_FILE);
        let topics = DashMap::new();
        match fs::read_to_string(&meta_path) {
            Ok(text) => {
                let file: MetaFile =
                    toml::from_str(&text).map_err(|e| BrokerError::Meta(e.to_string()))?;
                for (name, meta) in file.topics {
                    topics.insert(name, meta.partitions);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(BrokerState {
            topics,
            meta_path,
            persist_lock: Mutex::new(()),
            default_partitions,
        })
    }

    /// Partition count for `topic`, if known.
    pub fn partitions(&self, topic: &str) -> Option<i32> {
        self.topics.get(topic).map(|r| *r)
    }

    /// Get the partition count for `topic`, auto-creating it with the
    /// broker default on first sight (M1 behavior) and persisting `meta.toml`.
    pub fn ensure_topic(&self, topic: &str) -> Result<i32, BrokerError> {
        if let Some(partitions) = self.partitions(topic) {
            return Ok(partitions);
        }
        if !valid_topic_name(topic) {
            return Err(BrokerError::InvalidTopic(topic.to_owned()));
        }
        // entry() holds the shard lock: concurrent creators cannot race.
        let entry = self
            .topics
            .entry(topic.to_owned())
            .or_insert(self.default_partitions);
        let partitions = *entry;
        drop(entry);
        self.persist()?;
        Ok(partitions)
    }

    /// All known topics and their partition counts, sorted by name.
    pub fn topics(&self) -> Vec<(String, i32)> {
        let mut out: Vec<(String, i32)> = self
            .topics
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();
        out.sort();
        out
    }

    fn persist(&self) -> Result<(), BrokerError> {
        // Serialize the snapshot and replacement so an older concurrent
        // snapshot cannot overwrite a newer one.
        let _guard = self
            .persist_lock
            .lock()
            .map_err(|_| BrokerError::Meta("metadata persistence lock poisoned".into()))?;
        let file = MetaFile {
            topics: self
                .topics
                .iter()
                .map(|r| {
                    (
                        r.key().clone(),
                        TopicMeta {
                            partitions: *r.value(),
                        },
                    )
                })
                .collect(),
        };
        let text = toml::to_string(&file).map_err(|e| BrokerError::Meta(e.to_string()))?;
        let temp_path = self.meta_path.with_file_name(META_TEMP_FILE);
        fs::write(&temp_path, text)?;
        fs::rename(temp_path, &self.meta_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn topics_survive_reload_via_meta_toml() {
        let dir = tempfile::tempdir().unwrap();
        {
            let state = BrokerState::load(dir.path(), 3).unwrap();
            assert_eq!(state.ensure_topic("orders").unwrap(), 3);
            assert_eq!(state.ensure_topic("orders").unwrap(), 3);
            assert_eq!(state.ensure_topic("payments").unwrap(), 3);
        }
        let state = BrokerState::load(dir.path(), 1).unwrap();
        assert_eq!(state.partitions("orders"), Some(3));
        assert_eq!(state.partitions("payments"), Some(3));
        assert_eq!(state.partitions("nope"), None);
    }

    #[test]
    fn invalid_topic_names_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let state = BrokerState::load(dir.path(), 1).unwrap();
        for bad in ["", "..", "a/b", "a\\b", "a b", "💶"] {
            assert!(matches!(
                state.ensure_topic(bad),
                Err(BrokerError::InvalidTopic(_))
            ));
        }
        for good in ["orders", "a.b", "A-1_x", ".hidden"] {
            assert!(state.ensure_topic(good).is_ok());
        }
    }

    #[test]
    fn concurrent_topic_creation_persists_every_topic() {
        const TOPIC_COUNT: usize = 32;

        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(BrokerState::load(dir.path(), 3).unwrap());
        let start = Arc::new(Barrier::new(TOPIC_COUNT));
        let mut threads = Vec::with_capacity(TOPIC_COUNT);

        for index in 0..TOPIC_COUNT {
            let state = Arc::clone(&state);
            let start = Arc::clone(&start);
            threads.push(thread::spawn(move || {
                start.wait();
                let topic = format!("concurrent-{index}");
                assert_eq!(state.ensure_topic(&topic).unwrap(), 3);
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let reloaded = BrokerState::load(dir.path(), 1).unwrap();
        assert_eq!(reloaded.topics().len(), TOPIC_COUNT);
        for index in 0..TOPIC_COUNT {
            assert_eq!(reloaded.partitions(&format!("concurrent-{index}")), Some(3));
        }
        assert!(!dir.path().join(META_TEMP_FILE).exists());
    }
}

/// Build the log configuration for one topic: broker defaults, overridden by
/// whatever that topic sets.
///
/// Topic configs used to be stored and ignored, which is worse than not
/// supporting them — an operator who sets `retention.ms` on a topic and sees
/// it echoed back reasonably believes it took effect. Anything unset or
/// unparseable falls back to the broker-wide value rather than to zero, so a
/// typo cannot silently delete a log.
pub(crate) fn log_config_for_topic(
    defaults: &LogConfig,
    topic: &str,
    configs: Option<&std::collections::BTreeMap<String, String>>,
) -> LogConfig {
    let mut config = defaults.clone();

    if topic == crate::group::OFFSETS_TOPIC {
        // Every commit rewrites the same key, so this topic must be
        // compacted or it grows without bound and coordinator failover
        // slows without limit.
        config.compact = true;
        // A committed consumer offset that disappears on restart is a
        // correctness break, not a lost optimisation, and commits arrive
        // far too slowly for an eager checkpoint to cost anything. User
        // topics keep the periodic one.
        config.hwm_checkpoint_interval_ms = 0;
        return config;
    }

    let Some(configs) = configs else {
        return config;
    };

    // `-1` is Kafka's "unlimited" for retention, and means the same here.
    if let Some(value) = configs.get("retention.ms") {
        match value.parse::<i64>() {
            Ok(-1) => config.retention_ms = None,
            Ok(ms) if ms >= 0 => config.retention_ms = Some(ms as u64),
            _ => warn!(topic, value, "ignoring unparseable retention.ms"),
        }
    }
    if let Some(value) = configs.get("retention.bytes") {
        match value.parse::<i64>() {
            Ok(-1) => config.retention_bytes = None,
            Ok(bytes) if bytes >= 0 => config.retention_bytes = Some(bytes as u64),
            _ => warn!(topic, value, "ignoring unparseable retention.bytes"),
        }
    }
    if let Some(value) = configs.get("segment.bytes") {
        match value.parse::<u64>() {
            // A segment has to be able to hold at least one batch header,
            // and a pathologically small value would roll on every append.
            Ok(bytes) if bytes >= 1024 => config.segment_bytes = bytes,
            _ => warn!(topic, value, "ignoring unusable segment.bytes"),
        }
    }
    if let Some(value) = configs.get("segment.ms") {
        match value.parse::<i64>() {
            Ok(-1) => config.segment_ms = None,
            Ok(ms) if ms > 0 => config.segment_ms = Some(ms as u64),
            _ => warn!(topic, value, "ignoring unparseable segment.ms"),
        }
    }
    if let Some(value) = configs.get("cleanup.policy") {
        match value.as_str() {
            "compact" => config.compact = true,
            "delete" => config.compact = false,
            _ => warn!(topic, value, "ignoring unknown cleanup.policy"),
        }
    }
    if let Some(value) = configs.get("flush.messages") {
        match value.parse::<u64>() {
            Ok(0) => config.flush_interval_messages = None,
            Ok(count) => config.flush_interval_messages = Some(count),
            _ => warn!(topic, value, "ignoring unparseable flush.messages"),
        }
    }
    if let Some(value) = configs.get("flush.ms") {
        match value.parse::<u64>() {
            Ok(0) => config.flush_interval_ms = None,
            Ok(ms) => config.flush_interval_ms = Some(ms),
            _ => warn!(topic, value, "ignoring unparseable flush.ms"),
        }
    }
    config
}

#[cfg(test)]
mod topic_config_tests {
    use super::*;

    fn configs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn defaults() -> LogConfig {
        LogConfig {
            segment_bytes: 64 * 1024 * 1024,
            retention_ms: Some(7 * 24 * 60 * 60 * 1000),
            ..LogConfig::default()
        }
    }

    #[test]
    fn a_topic_overrides_the_broker_default() {
        let resolved = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[
                ("retention.ms", "60000"),
                ("retention.bytes", "1048576"),
                ("segment.bytes", "32768"),
                ("segment.ms", "120000"),
            ])),
        );
        assert_eq!(resolved.retention_ms, Some(60_000));
        assert_eq!(resolved.retention_bytes, Some(1_048_576));
        assert_eq!(resolved.segment_bytes, 32_768);
        assert_eq!(resolved.segment_ms, Some(120_000));
    }

    #[test]
    fn an_unset_key_keeps_the_broker_default() {
        let resolved = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[("segment.bytes", "8192")])),
        );
        assert_eq!(resolved.segment_bytes, 8_192);
        assert_eq!(
            resolved.retention_ms,
            defaults().retention_ms,
            "an unset key must not clear the broker default"
        );
    }

    /// A typo must not be read as zero — that would delete a log rather
    /// than leave it alone.
    #[test]
    fn an_unparseable_value_is_ignored_not_treated_as_zero() {
        for bad in ["", "soon", "-5", "1e6"] {
            let resolved = log_config_for_topic(
                &defaults(),
                "orders",
                Some(&configs(&[("retention.ms", bad)])),
            );
            assert_eq!(
                resolved.retention_ms,
                defaults().retention_ms,
                "{bad:?} must fall back to the broker default"
            );
        }
    }

    /// Kafka spells "keep forever" as -1, and so does this.
    #[test]
    fn minus_one_means_unlimited() {
        let resolved = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[("retention.ms", "-1")])),
        );
        assert_eq!(resolved.retention_ms, None);
    }

    #[test]
    fn cleanup_policy_selects_compaction() {
        let compacted = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[("cleanup.policy", "compact")])),
        );
        assert!(compacted.compact, "a user topic can now be compacted");

        let deleted = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[("cleanup.policy", "delete")])),
        );
        assert!(!deleted.compact);
    }

    /// The offsets topic keeps its own rules whatever anyone configures:
    /// it must compact, and it must checkpoint eagerly.
    #[test]
    fn the_offsets_topic_is_not_overridable() {
        let resolved = log_config_for_topic(
            &defaults(),
            "__consumer_offsets",
            Some(&configs(&[
                ("cleanup.policy", "delete"),
                ("retention.ms", "1000"),
            ])),
        );
        assert!(resolved.compact);
        assert_eq!(resolved.hwm_checkpoint_interval_ms, 0);
    }

    /// A segment too small to hold a batch would roll on every append.
    #[test]
    fn an_absurd_segment_size_is_refused() {
        let resolved = log_config_for_topic(
            &defaults(),
            "orders",
            Some(&configs(&[("segment.bytes", "16")])),
        );
        assert_eq!(resolved.segment_bytes, defaults().segment_bytes);
    }
}
