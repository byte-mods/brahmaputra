//! Persistent leader-epoch checkpoints used to find an exact common log
//! prefix after leadership changes (KIP-101 style).

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::StorageError;

const CHECKPOINT_FILE: &str = "leader-epochs";
const ENTRY_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderEpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
}

/// Append-only `(leader_epoch, start_offset)` checkpoint file.
///
/// Entries are fixed-width big-endian values. A torn final entry is ignored
/// and removed when opening, while non-monotonic complete entries are
/// rejected as corruption.
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<LeaderEpochEntry>,
}

impl LeaderEpochCheckpoint {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = dir.as_ref().join(CHECKPOINT_FILE);
        let mut bytes = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_end(&mut bytes)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let valid_len = bytes.len() - bytes.len() % ENTRY_LEN;
        let mut entries: Vec<LeaderEpochEntry> = Vec::with_capacity(valid_len / ENTRY_LEN);
        for chunk in bytes[..valid_len].as_chunks::<ENTRY_LEN>().0 {
            let entry = LeaderEpochEntry {
                epoch: i32::from_be_bytes(chunk[..4].try_into().expect("four bytes")),
                start_offset: i64::from_be_bytes(chunk[4..].try_into().expect("eight bytes")),
            };
            if let Some(last) = entries.last() {
                if entry.epoch <= last.epoch {
                    return Err(StorageError::NonMonotonicLeaderEpoch {
                        epoch: entry.epoch,
                        last_epoch: last.epoch,
                    });
                }
                if entry.start_offset < last.start_offset {
                    return Err(StorageError::RegressingLeaderEpochOffset {
                        last_offset: last.start_offset,
                        offset: entry.start_offset,
                    });
                }
            }
            entries.push(entry);
        }

        let checkpoint = Self { path, entries };
        if valid_len != bytes.len() {
            checkpoint.rewrite()?;
        }
        Ok(checkpoint)
    }

    pub fn entries(&self) -> &[LeaderEpochEntry] {
        &self.entries
    }

    pub fn record(&mut self, epoch: i32, start_offset: i64) -> Result<(), StorageError> {
        if let Some(last) = self.entries.last().copied() {
            if epoch == last.epoch && start_offset == last.start_offset {
                return Ok(());
            }
            if epoch <= last.epoch {
                return Err(StorageError::NonMonotonicLeaderEpoch {
                    epoch,
                    last_epoch: last.epoch,
                });
            }
            if start_offset < last.start_offset {
                return Err(StorageError::RegressingLeaderEpochOffset {
                    last_offset: last.start_offset,
                    offset: start_offset,
                });
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&epoch.to_be_bytes())?;
        file.write_all(&start_offset.to_be_bytes())?;
        file.flush()?;
        self.entries.push(LeaderEpochEntry {
            epoch,
            start_offset,
        });
        Ok(())
    }

    /// Return the exclusive end offset of `epoch` according to the next
    /// checkpoint, or `log_end_offset` for the newest known epoch.
    pub fn end_offset(&self, epoch: i32, log_end_offset: i64) -> Option<i64> {
        let index = self.entries.partition_point(|entry| entry.epoch <= epoch);
        if index == 0 {
            return None;
        }
        Some(
            self.entries
                .get(index)
                .map_or(log_end_offset, |entry| entry.start_offset)
                .min(log_end_offset),
        )
    }

    /// Drop epochs whose first offset no longer exists after truncation.
    pub fn truncate_to(&mut self, log_end_offset: i64) -> Result<(), StorageError> {
        let keep = self
            .entries
            .partition_point(|entry| entry.start_offset < log_end_offset);
        self.entries.truncate(keep);
        self.rewrite()
    }

    fn rewrite(&self) -> Result<(), StorageError> {
        let temporary = self.path.with_extension("tmp");
        {
            let mut file = File::create(&temporary)?;
            for entry in &self.entries {
                file.write_all(&entry.epoch.to_be_bytes())?;
                file.write_all(&entry.start_offset.to_be_bytes())?;
            }
            file.flush()?;
        }
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        fs::rename(temporary, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_round_trip_lookup_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let mut checkpoint = LeaderEpochCheckpoint::open(dir.path()).unwrap();
        checkpoint.record(4, 0).unwrap();
        checkpoint.record(5, 10).unwrap();
        checkpoint.record(9, 25).unwrap();
        assert_eq!(checkpoint.end_offset(3, 40), None);
        assert_eq!(checkpoint.end_offset(4, 40), Some(10));
        assert_eq!(checkpoint.end_offset(7, 40), Some(25));
        assert_eq!(checkpoint.end_offset(9, 40), Some(40));

        checkpoint.truncate_to(25).unwrap();
        assert_eq!(checkpoint.entries().len(), 2);
        drop(checkpoint);
        let checkpoint = LeaderEpochCheckpoint::open(dir.path()).unwrap();
        assert_eq!(
            checkpoint.entries(),
            &[
                LeaderEpochEntry {
                    epoch: 4,
                    start_offset: 0,
                },
                LeaderEpochEntry {
                    epoch: 5,
                    start_offset: 10,
                },
            ]
        );
    }

    #[test]
    fn torn_tail_is_removed_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut checkpoint = LeaderEpochCheckpoint::open(dir.path()).unwrap();
            checkpoint.record(1, 0).unwrap();
        }
        let path = dir.path().join(CHECKPOINT_FILE);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        let checkpoint = LeaderEpochCheckpoint::open(dir.path()).unwrap();
        assert_eq!(checkpoint.entries().len(), 1);
        assert_eq!(fs::metadata(path).unwrap().len(), ENTRY_LEN as u64);
    }
}
