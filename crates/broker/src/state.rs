//! Broker-side topic metadata: topic → partition count.
//!
//! M1 simplification (Blueprint 02 §6): metadata is an in-memory map
//! persisted to `<data_dir>/meta.toml` so topics survive restart. From M2
//! this is materialized from the Raft metadata log instead.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
