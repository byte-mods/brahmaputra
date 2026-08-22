//! Log directories: where each partition's data lives, and what happens
//! when one of those places stops working.
//!
//! # Why more than one
//!
//! A broker with twelve disks and one data directory has two options, both
//! bad. It can put a RAID array underneath — paying capacity and write
//! throughput to duplicate redundancy that replication already provides
//! across brokers — or it can use one disk and waste eleven.
//!
//! Giving the broker the disks directly ("just a bunch of disks") spends
//! neither. Each partition lives on exactly one disk, so twelve disks means
//! twelve independent queues rather than one array's.
//!
//! # Why it is really about blast radius
//!
//! Capacity is the smaller half. With one directory a disk failure has no
//! partial mode: the broker dies and *every* partition it led fails over at
//! once. With several, a failed directory takes only the partitions on it —
//! the broker keeps serving the rest, and the affected replicas fail over
//! individually.
//!
//! That last part needs no new machinery. A partition on a dead disk simply
//! stops being served here: its actor is closed, requests for it are
//! refused, and it stops fetching. The controller already treats a replica
//! that stops fetching as out of sync and elects around it. Failure
//! isolation is what this module adds; failover is what the cluster already
//! did.
//!
//! # Placement
//!
//! A new partition goes to the online directory holding the fewest, which
//! is what Kafka does and what keeps a disk added later from staying empty.
//! The choice is remembered by *where the directory actually is* rather
//! than in a side file: on startup every configured directory is scanned
//! for `<topic>-<partition>` entries, so the mapping is rebuilt from the
//! data itself and cannot disagree with it.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use dashmap::DashMap;
use tracing::{info, warn};

use crate::error::BrokerError;

/// A file written and removed to prove a directory still accepts writes.
const HEALTH_PROBE_FILE: &str = ".brahmaputra-health";

/// Where the partition-to-directory map is recorded, in the first
/// directory.
///
/// The directories themselves are the authority on placement — a scan
/// cannot disagree with the data the way a side file can. This file exists
/// for exactly one thing the scan cannot do: name the partitions on a disk
/// that is *already broken at startup*.
///
/// Without it, a broker restarting with a dead disk cannot see what was on
/// it, places those partitions fresh on a healthy disk, and presents them
/// empty — indistinguishable from a partition that lost every record. With
/// it, they are known to be on a directory that is offline, and are
/// reported as unavailable instead. In a replicated cluster the difference
/// is whether the replica re-fetches from the leader or claims to be
/// authoritative and empty.
const PLACEMENT_FILE: &str = "placement.toml";

/// One configured directory.
struct LogDir {
    path: PathBuf,
    /// Cleared the first time this directory fails an operation, and never
    /// set again while the broker runs.
    ///
    /// A disk that returns errors and then appears to recover is the worst
    /// case for a log: the recovery is usually the filesystem being
    /// remounted, potentially having lost the tail of every file on it.
    /// Kafka takes the same position — an offline log dir stays offline
    /// until the broker is restarted and its contents are re-checked from
    /// the beginning.
    online: AtomicBool,
    offline_reason: Mutex<Option<String>>,
}

/// The broker's log directories and the partition-to-directory mapping.
pub struct LogDirs {
    dirs: Vec<LogDir>,
    /// `(topic, partition)` -> index into `dirs`.
    assignment: DashMap<(String, i32), usize>,
}

impl LogDirs {
    /// Open every configured directory, creating it if needed, and rebuild
    /// the partition mapping by scanning what is already there.
    ///
    /// A directory that cannot be created or read starts offline rather
    /// than failing the broker: refusing to start because one disk of
    /// twelve is bad would give up exactly the isolation this exists for.
    /// All of them being unusable *is* fatal — there would be nowhere to
    /// put anything.
    pub fn open(paths: &[PathBuf]) -> Result<Self, BrokerError> {
        if paths.is_empty() {
            return Err(BrokerError::Meta("at least one data dir is required".into()));
        }

        let mut dirs = Vec::with_capacity(paths.len());
        for path in paths {
            let (online, reason) = match fs::create_dir_all(path) {
                Ok(()) => (true, None),
                Err(error) => {
                    warn!(dir = %path.display(), %error, "log directory is unusable at startup");
                    (false, Some(error.to_string()))
                }
            };
            dirs.push(LogDir {
                path: path.clone(),
                online: AtomicBool::new(online),
                offline_reason: Mutex::new(reason),
            });
        }
        if dirs.iter().all(|dir| !dir.online.load(Ordering::Relaxed)) {
            return Err(BrokerError::Meta(
                "every configured data dir is unusable".into(),
            ));
        }

        let assignment = DashMap::new();
        for (index, dir) in dirs.iter().enumerate() {
            if !dir.online.load(Ordering::Relaxed) {
                continue;
            }
            for (topic, partition) in scan_partitions(&dir.path)? {
                // A partition present in two directories is a torn move or
                // a hand-copied directory. Keeping the first and warning is
                // the containable answer: serving one of them is correct,
                // and serving both would be two logs claiming one identity.
                if let Some(existing) = assignment.insert((topic.clone(), partition), index) {
                    if existing != index {
                        warn!(
                            topic = %topic,
                            partition,
                            first = %dirs[existing].path.display(),
                            second = %dir.path.display(),
                            "partition exists in two log dirs; using the first"
                        );
                        assignment.insert((topic, partition), existing);
                    }
                }
            }
        }

        let log_dirs = LogDirs { dirs, assignment };
        log_dirs.adopt_recorded_placement();
        log_dirs.record_placement();
        Ok(log_dirs)
    }

    /// Learn about partitions on directories that are offline right now.
    ///
    /// The scan above covers every *online* directory and is authoritative
    /// for them. This covers the one gap it cannot: a disk that was already
    /// broken when the broker started, whose partitions would otherwise be
    /// invisible and get placed fresh somewhere else.
    fn adopt_recorded_placement(&self) {
        let recorded = match read_placement(&self.dirs[0].path) {
            Ok(recorded) => recorded,
            Err(error) => {
                warn!(%error, "could not read the recorded partition placement");
                return;
            }
        };
        let mut adopted = 0usize;
        for (topic, partition, dir) in recorded {
            let key = (topic, partition);
            if self.assignment.contains_key(&key) {
                // The scan found it; the disk wins over the record.
                continue;
            }
            let Some(index) = self.dirs.iter().position(|entry| entry.path == dir) else {
                // A directory the operator no longer configures. Its data
                // is not reachable and is not this broker's any more.
                continue;
            };
            if self.dirs[index].online.load(Ordering::Relaxed) {
                // Online and the scan did not find it: the partition was
                // genuinely removed. Do not resurrect a stale record.
                continue;
            }
            self.assignment.insert(key, index);
            adopted += 1;
        }
        if adopted > 0 {
            warn!(
                partitions = adopted,
                "partitions are on a log dir that is offline; they are unavailable on this broker \
                 rather than being re-created empty elsewhere"
            );
        }
    }

    /// Record the current placement, so a later start can see what was on a
    /// disk that has since failed.
    ///
    /// Best effort: a failure to write it costs the startup case above and
    /// nothing else, so it must never stop the broker.
    fn record_placement(&self) {
        let entries: Vec<(String, i32, PathBuf)> = self
            .assignment
            .iter()
            .map(|entry| {
                let (topic, partition) = entry.key().clone();
                (topic, partition, self.dirs[*entry.value()].path.clone())
            })
            .collect();
        if let Err(error) = write_placement(&self.dirs[0].path, &entries) {
            warn!(%error, "could not record the partition placement");
        }
    }

    /// Every configured directory, in the order the operator gave them.
    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.dirs.iter().map(|dir| dir.path.as_path())
    }

    /// The first directory, which is where broker-wide state that is not
    /// per-partition prefers to live.
    pub fn primary(&self) -> &Path {
        &self.dirs[0].path
    }

    pub fn is_online(&self, path: &Path) -> bool {
        self.dirs
            .iter()
            .find(|dir| dir.path == path)
            .is_some_and(|dir| dir.online.load(Ordering::Relaxed))
    }

    /// The directory for a partition, placing it if it has none yet.
    ///
    /// Returns [`BrokerError::LogDirOffline`] when the partition already
    /// lives on a failed directory — the caller must not silently move it
    /// somewhere else, because its data is on the disk that died and
    /// re-creating it empty would look like a partition that lost every
    /// record rather than one that is unavailable.
    pub fn resolve(&self, topic: &str, partition: i32) -> Result<PathBuf, BrokerError> {
        let key = (topic.to_owned(), partition);
        if let Some(index) = self.assignment.get(&key).map(|entry| *entry) {
            let dir = &self.dirs[index];
            if !dir.online.load(Ordering::Relaxed) {
                return Err(BrokerError::LogDirOffline {
                    topic: topic.to_owned(),
                    partition,
                    dir: dir.path.display().to_string(),
                });
            }
            return Ok(partition_path(&dir.path, topic, partition));
        }

        let index = self.least_loaded()?;
        // `entry` rather than `insert`: two requests for the same new
        // partition can arrive at once, and they must agree on where it
        // went or one of them creates a second copy.
        let index = *self.assignment.entry(key).or_insert(index);
        // Placement changes only when a partition is created, so keeping
        // the record current costs one small write per partition ever
        // rather than one per request.
        self.record_placement();
        Ok(partition_path(&self.dirs[index].path, topic, partition))
    }

    /// The directory a partition is already on, without placing it.
    pub fn existing(&self, topic: &str, partition: i32) -> Option<PathBuf> {
        let index = self
            .assignment
            .get(&(topic.to_owned(), partition))
            .map(|entry| *entry)?;
        Some(partition_path(&self.dirs[index].path, topic, partition))
    }

    /// Forget a partition this broker no longer holds, so a later
    /// re-assignment is placed afresh rather than pinned to the disk it
    /// used to be on.
    pub fn forget(&self, topic: &str, partition: i32) {
        if self
            .assignment
            .remove(&(topic.to_owned(), partition))
            .is_some()
        {
            self.record_placement();
        }
    }

    /// The online directory holding the fewest partitions.
    ///
    /// Fewest rather than most-free-space: a count is exact and free space
    /// is a moving target, and the thing that actually goes wrong without
    /// balancing is a disk added later staying empty forever.
    fn least_loaded(&self) -> Result<usize, BrokerError> {
        let mut counts = vec![0usize; self.dirs.len()];
        for entry in self.assignment.iter() {
            counts[*entry.value()] += 1;
        }
        (0..self.dirs.len())
            .filter(|index| self.dirs[*index].online.load(Ordering::Relaxed))
            .min_by_key(|index| (counts[*index], *index))
            .ok_or_else(|| BrokerError::Meta("no online data dir is available".into()))
    }

    /// Partitions currently placed on `path`.
    pub fn partitions_in(&self, path: &Path) -> Vec<(String, i32)> {
        let Some(index) = self.dirs.iter().position(|dir| dir.path == path) else {
            return Vec::new();
        };
        let mut out: Vec<_> = self
            .assignment
            .iter()
            .filter(|entry| *entry.value() == index)
            .map(|entry| entry.key().clone())
            .collect();
        out.sort();
        out
    }

    /// Take a directory offline and return the partitions that were on it.
    ///
    /// Idempotent: the second failure on the same disk reports nothing,
    /// because the first one already closed everything.
    pub fn mark_offline(&self, path: &Path, reason: impl Into<String>) -> Vec<(String, i32)> {
        let Some(index) = self.dirs.iter().position(|dir| dir.path == path) else {
            return Vec::new();
        };
        if !self.dirs[index].online.swap(false, Ordering::SeqCst) {
            return Vec::new();
        }
        let reason = reason.into();
        *self.dirs[index]
            .offline_reason
            .lock()
            .expect("offline reason") = Some(reason.clone());
        let affected = self.partitions_in(path);
        warn!(
            dir = %path.display(),
            %reason,
            partitions = affected.len(),
            "log directory taken offline; its partitions are unavailable on this broker"
        );
        affected
    }

    /// Which directory a path belongs to, for attributing an IO failure to
    /// the disk it happened on.
    pub fn owning_dir(&self, path: &Path) -> Option<PathBuf> {
        self.dirs
            .iter()
            .find(|dir| path.starts_with(&dir.path))
            .map(|dir| dir.path.clone())
    }

    /// Check that every online directory still accepts a write.
    ///
    /// Returns the partitions belonging to directories that just failed.
    /// A probe rather than waiting for a request, because a disk that dies
    /// while a partition happens to be idle would otherwise stay "online"
    /// until something tried to use it — which on a quiet topic can be
    /// hours after the disk is gone.
    pub fn probe(&self) -> Vec<(String, i32)> {
        let mut affected = Vec::new();
        for dir in &self.dirs {
            if !dir.online.load(Ordering::Relaxed) {
                continue;
            }
            if let Err(error) = probe_once(&dir.path) {
                affected.extend(self.mark_offline(&dir.path, error.to_string()));
            }
        }
        affected
    }
}

/// Directory holding the log of one partition: `<dir>/<topic>-<partition>`.
pub fn partition_path(dir: &Path, topic: &str, partition: i32) -> PathBuf {
    dir.join(format!("{topic}-{partition}"))
}

/// How often each online directory is asked to prove it still works.
///
/// Frequent enough that a dead disk is noticed in seconds rather than
/// whenever some client next happens to touch a partition on it, and rare
/// enough that the probe itself is not a workload: one small write and
/// fsync per directory per interval.
pub const HEALTH_PROBE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Watch every log directory, taking failed ones offline.
///
/// A partition on a failed directory has its actor closed, so the disk is
/// not touched again and every request for it is refused — which is
/// exactly what makes the rest of the broker keep working. In a cluster
/// the replica then stops fetching, the controller drops it from the ISR
/// on the usual lag timeout, and leadership moves. No new failover path
/// was needed for any of that.
pub async fn run_health_watcher(broker: std::sync::Arc<crate::server::Broker>) {
    let mut shutdown = broker.shutdown_receiver();
    let mut interval = tokio::time::interval(HEALTH_PROBE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
            _ = interval.tick() => {
                let lost = broker.log_dirs().probe();
                if !lost.is_empty() {
                    broker.close_partitions(&lost).await;
                }
            }
        }
    }
}

/// On-disk form of the placement record.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct PlacementFile {
    #[serde(default)]
    partitions: Vec<PlacementEntry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PlacementEntry {
    topic: String,
    partition: i32,
    dir: PathBuf,
}

fn read_placement(primary: &Path) -> Result<Vec<(String, i32, PathBuf)>, BrokerError> {
    let text = match fs::read_to_string(primary.join(PLACEMENT_FILE)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let file: PlacementFile =
        toml::from_str(&text).map_err(|error| BrokerError::Meta(error.to_string()))?;
    Ok(file
        .partitions
        .into_iter()
        .map(|entry| (entry.topic, entry.partition, entry.dir))
        .collect())
}

fn write_placement(primary: &Path, entries: &[(String, i32, PathBuf)]) -> Result<(), BrokerError> {
    let mut sorted: Vec<_> = entries.to_vec();
    sorted.sort();
    let file = PlacementFile {
        partitions: sorted
            .into_iter()
            .map(|(topic, partition, dir)| PlacementEntry {
                topic,
                partition,
                dir,
            })
            .collect(),
    };
    let text = toml::to_string(&file).map_err(|error| BrokerError::Meta(error.to_string()))?;
    // Written beside and renamed over, so a crash leaves the old record or
    // the new one and never half of either.
    let temporary = primary.join(format!("{PLACEMENT_FILE}.tmp"));
    fs::write(&temporary, text)?;
    fs::rename(temporary, primary.join(PLACEMENT_FILE))?;
    Ok(())
}

/// Write, sync and remove a probe file.
fn probe_once(dir: &Path) -> io::Result<()> {
    use std::io::Write;
    let path = dir.join(HEALTH_PROBE_FILE);
    let mut file = fs::File::create(&path)?;
    file.write_all(b"ok")?;
    // Without the sync this only proves the page cache is working, which a
    // failed disk will happily keep doing for a while.
    file.sync_all()?;
    drop(file);
    fs::remove_file(&path)
}

/// Every `<topic>-<partition>` directory directly inside `dir`.
fn scan_partitions(dir: &Path) -> Result<Vec<(String, i32)>, BrokerError> {
    let mut found = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(found),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // Split at the *last* hyphen: a topic name may contain them, a
        // partition number may not.
        let Some((topic, partition)) = name.rsplit_once('-') else {
            continue;
        };
        let Ok(partition) = partition.parse::<i32>() else {
            continue;
        };
        if topic.is_empty() || partition < 0 {
            continue;
        }
        found.push((topic.to_owned(), partition));
    }
    found.sort();
    Ok(found)
}

/// Per-directory usage, for `DescribeLogDirs`.
pub struct LogDirUsage {
    pub path: PathBuf,
    pub online: bool,
    pub offline_reason: Option<String>,
    pub partitions: Vec<(String, i32)>,
}

impl LogDirs {
    /// A description of every directory, online or not.
    ///
    /// A failed directory is reported *with its partitions* rather than
    /// omitted: "this disk is gone and here is what was on it" is the
    /// answer an operator needs, and an empty list would read as a disk
    /// that was never used.
    pub fn describe(&self) -> Vec<LogDirUsage> {
        self.dirs
            .iter()
            .map(|dir| LogDirUsage {
                path: dir.path.clone(),
                online: dir.online.load(Ordering::Relaxed),
                offline_reason: dir.offline_reason.lock().expect("offline reason").clone(),
                partitions: self.partitions_in(&dir.path),
            })
            .collect()
    }

    /// Log a one-line summary of placement at startup.
    pub fn log_layout(&self) {
        let mut per_dir: BTreeMap<&Path, usize> = BTreeMap::new();
        for dir in &self.dirs {
            per_dir.insert(dir.path.as_path(), 0);
        }
        for entry in self.assignment.iter() {
            let path = self.dirs[*entry.value()].path.as_path();
            *per_dir.entry(path).or_insert(0) += 1;
        }
        for (path, count) in per_dir {
            info!(
                dir = %path.display(),
                partitions = count,
                online = self.is_online(path),
                "log directory"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dirs(count: usize) -> (tempfile::TempDir, Vec<PathBuf>) {
        let root = tempfile::tempdir().expect("temp root");
        let paths = (0..count)
            .map(|index| root.path().join(format!("disk{index}")))
            .collect();
        (root, paths)
    }

    #[test]
    fn partitions_spread_across_directories() {
        let (_root, paths) = temp_dirs(3);
        let dirs = LogDirs::open(&paths).unwrap();

        for partition in 0..9 {
            dirs.resolve("orders", partition).unwrap();
        }
        let counts: Vec<usize> = paths
            .iter()
            .map(|path| dirs.partitions_in(path).len())
            .collect();
        assert_eq!(
            counts,
            vec![3, 3, 3],
            "nine partitions over three disks should be three each"
        );
    }

    #[test]
    fn a_partition_keeps_the_directory_it_was_placed_on() {
        let (_root, paths) = temp_dirs(3);
        let dirs = LogDirs::open(&paths).unwrap();
        let first = dirs.resolve("orders", 0).unwrap();
        // Placing other partitions must not move it.
        for partition in 1..6 {
            dirs.resolve("orders", partition).unwrap();
        }
        assert_eq!(dirs.resolve("orders", 0).unwrap(), first);
        assert_eq!(dirs.existing("orders", 0), Some(first));
    }

    #[test]
    fn placement_is_rebuilt_from_the_directories_themselves() {
        let (_root, paths) = temp_dirs(3);
        let placed: Vec<PathBuf> = {
            let dirs = LogDirs::open(&paths).unwrap();
            (0..6)
                .map(|partition| {
                    let path = dirs.resolve("orders", partition).unwrap();
                    fs::create_dir_all(&path).unwrap();
                    path
                })
                .collect()
        };

        // A restart reads no side file; it looks at what is on the disks.
        let reopened = LogDirs::open(&paths).unwrap();
        for (partition, expected) in placed.iter().enumerate() {
            assert_eq!(
                &reopened.resolve("orders", partition as i32).unwrap(),
                expected,
                "partition {partition} moved across a restart"
            );
        }
    }

    #[test]
    fn a_topic_name_containing_a_hyphen_still_parses() {
        let (_root, paths) = temp_dirs(1);
        {
            let dirs = LogDirs::open(&paths).unwrap();
            fs::create_dir_all(dirs.resolve("orders-eu-west", 12).unwrap()).unwrap();
        }
        let reopened = LogDirs::open(&paths).unwrap();
        assert_eq!(
            reopened.existing("orders-eu-west", 12),
            Some(partition_path(&paths[0], "orders-eu-west", 12)),
            "the split must take the last hyphen, not the first"
        );
    }

    #[test]
    fn a_failed_directory_takes_only_its_own_partitions() {
        let (_root, paths) = temp_dirs(3);
        let dirs = LogDirs::open(&paths).unwrap();
        for partition in 0..9 {
            dirs.resolve("orders", partition).unwrap();
        }

        let lost = dirs.mark_offline(&paths[1], "simulated disk failure");
        assert_eq!(lost.len(), 3, "only the partitions on that disk are lost");
        assert!(dirs.describe().iter().any(|dir| !dir.online));
        assert!(!dirs.is_online(&paths[1]));
        assert!(dirs.is_online(&paths[0]) && dirs.is_online(&paths[2]));

        // The survivors are still resolvable — that is the whole point.
        for (topic, partition) in dirs.partitions_in(&paths[0]) {
            assert!(dirs.resolve(&topic, partition).is_ok());
        }
        // The casualties are refused rather than silently re-placed onto a
        // healthy disk, which would present an empty partition as if it had
        // lost every record.
        for (topic, partition) in &lost {
            assert!(matches!(
                dirs.resolve(topic, *partition),
                Err(BrokerError::LogDirOffline { .. })
            ));
        }
        // Marking it again reports nothing new.
        assert!(dirs.mark_offline(&paths[1], "again").is_empty());
    }

    #[test]
    fn new_partitions_avoid_a_failed_directory() {
        let (_root, paths) = temp_dirs(2);
        let dirs = LogDirs::open(&paths).unwrap();
        dirs.mark_offline(&paths[0], "simulated");
        for partition in 0..4 {
            let path = dirs.resolve("orders", partition).unwrap();
            assert!(path.starts_with(&paths[1]));
        }
    }

    #[test]
    fn every_directory_failing_leaves_nowhere_to_place() {
        let (_root, paths) = temp_dirs(2);
        let dirs = LogDirs::open(&paths).unwrap();
        dirs.mark_offline(&paths[0], "simulated");
        dirs.mark_offline(&paths[1], "simulated");
        assert!(dirs.resolve("orders", 0).is_err());
    }

    #[test]
    fn forgetting_a_partition_lets_it_be_placed_again() {
        let (_root, paths) = temp_dirs(2);
        let dirs = LogDirs::open(&paths).unwrap();
        dirs.resolve("orders", 0).unwrap();
        assert!(dirs.existing("orders", 0).is_some());
        dirs.forget("orders", 0);
        assert_eq!(dirs.existing("orders", 0), None);
    }

    #[test]
    fn a_healthy_directory_passes_its_probe() {
        let (_root, paths) = temp_dirs(2);
        let dirs = LogDirs::open(&paths).unwrap();
        dirs.resolve("orders", 0).unwrap();
        assert!(dirs.probe().is_empty());
        assert!(dirs.describe().iter().all(|dir| dir.online));
    }

    #[test]
    fn describing_a_failed_directory_still_names_what_was_on_it() {
        let (_root, paths) = temp_dirs(2);
        let dirs = LogDirs::open(&paths).unwrap();
        for partition in 0..4 {
            dirs.resolve("orders", partition).unwrap();
        }
        dirs.mark_offline(&paths[0], "simulated disk failure");

        let described = dirs.describe();
        assert_eq!(described.len(), 2);
        let failed = described
            .iter()
            .find(|dir| dir.path == paths[0])
            .expect("the failed dir is still described");
        assert!(!failed.online);
        assert_eq!(
            failed.offline_reason.as_deref(),
            Some("simulated disk failure")
        );
        assert!(
            !failed.partitions.is_empty(),
            "an operator needs to know what was on the disk that died"
        );
    }

    #[test]
    fn a_disk_broken_at_startup_makes_its_partitions_unavailable_not_empty() {
        let (_root, paths) = temp_dirs(2);
        // Lay out six partitions across two disks and let the placement be
        // recorded.
        {
            let dirs = LogDirs::open(&paths).unwrap();
            for partition in 0..6 {
                fs::create_dir_all(dirs.resolve("orders", partition).unwrap()).unwrap();
            }
        }

        // Now the second disk is gone: the path exists as a *file*, so the
        // directory cannot be created or read — what a failed mount looks
        // like from the process's side.
        fs::remove_dir_all(&paths[1]).unwrap();
        fs::write(&paths[1], b"not a directory").unwrap();

        let reopened = LogDirs::open(&paths).unwrap();
        let described = reopened.describe();
        let failed = described
            .iter()
            .find(|dir| dir.path == paths[1])
            .expect("the broken disk is still described");
        assert!(!failed.online);
        assert!(
            !failed.partitions.is_empty(),
            "the recorded placement is what makes the partitions on a dead \
             disk visible at all; without it they would be invisible"
        );

        // The decisive assertion. Every partition either resolves to the
        // surviving disk or is refused — none is silently re-placed and
        // presented empty, which would be indistinguishable from having
        // lost every record.
        let mut refused = 0;
        for partition in 0..6 {
            match reopened.resolve("orders", partition) {
                Ok(path) => assert!(
                    path.starts_with(&paths[0]),
                    "a surviving partition must still be on the disk it was on"
                ),
                Err(BrokerError::LogDirOffline { .. }) => refused += 1,
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
        assert!(
            refused > 0,
            "the partitions on the dead disk must be refused, not re-created"
        );
    }

    #[test]
    fn a_partition_removed_while_its_disk_was_healthy_is_not_resurrected() {
        let (_root, paths) = temp_dirs(2);
        let placed = {
            let dirs = LogDirs::open(&paths).unwrap();
            let placed = dirs.resolve("orders", 0).unwrap();
            fs::create_dir_all(&placed).unwrap();
            placed
        };
        // The partition is deleted — drained after a reassignment, say —
        // while its disk is perfectly fine. A stale record must not bring
        // it back.
        fs::remove_dir_all(&placed).unwrap();

        let reopened = LogDirs::open(&paths).unwrap();
        assert_eq!(
            reopened.existing("orders", 0),
            None,
            "an online disk's contents are authoritative over the record"
        );
    }

    #[test]
    fn a_single_directory_broker_behaves_exactly_as_before() {
        let (_root, paths) = temp_dirs(1);
        let dirs = LogDirs::open(&paths).unwrap();
        assert_eq!(dirs.paths().count(), 1);
        assert_eq!(dirs.primary(), paths[0].as_path());
        assert_eq!(
            dirs.resolve("orders", 3).unwrap(),
            partition_path(&paths[0], "orders", 3)
        );
    }
}
