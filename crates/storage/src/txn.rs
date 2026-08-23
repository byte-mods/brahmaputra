//! Per-partition transaction state: which transactions are still open, and
//! which of the closed ones aborted.
//!
//! Two questions have to be answerable about any partition, and neither can
//! be answered from the log alone without reading all of it:
//!
//! * **Where does the last stable offset sit?** A `read_committed` consumer
//!   may not see past the first record of the oldest *open* transaction,
//!   because whether those records will exist is not yet decided. That
//!   offset is the LSO, and it is what bounds such a fetch instead of the
//!   high watermark.
//! * **Which records must be skipped?** Records of an aborted transaction
//!   stay in the log — the log is append-only, and rewriting it to remove
//!   them would break every offset after them — so a reader is told to skip
//!   them instead.
//!
//! Both are maintained as the log is appended to, and journalled to disk so
//! a restart does not have to rebuild them by scanning. The journal is
//! append-only and one short record per transaction event, so it grows with
//! the number of transactions rather than with the data they carry, and it
//! is rewritten without the entries that have fallen below the log start
//! offset whenever retention or an explicit delete moves that start.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use crate::error::StorageError;

const TXN_INDEX_FILE: &str = "txnindex";
/// `kind` + `producer_id` + `first_offset` + `last_offset`.
const ENTRY_LEN: usize = 1 + 8 + 8 + 8;

const KIND_BEGIN: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_ABORT: u8 = 3;

/// A transaction that aborted, and the offsets its records occupy.
///
/// `last_offset` is the offset of the abort marker itself, which is the
/// point past which this producer's records are no longer in doubt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
    pub last_offset: i64,
}

/// Open and aborted transactions for one partition.
#[derive(Debug, Default)]
pub struct TransactionIndex {
    path: PathBuf,
    /// Producers with a transaction in flight, and the offset each one's
    /// first record landed at. A `BTreeMap` because the LSO is the minimum
    /// of its values and that is asked on every committed read.
    ongoing: BTreeMap<i64, i64>,
    aborted: Vec<AbortedTransaction>,
    /// `None` until the index is backed by a file — the in-memory form used
    /// by tests never writes.
    file: Option<File>,
}

impl TransactionIndex {
    /// Open the index in `dir`, replaying whatever is already there.
    ///
    /// A torn trailing entry is dropped rather than treated as corruption:
    /// the journal is appended to after the records it describes are
    /// durable, so a partial entry means a crash between the two, and the
    /// conservative reading — that transaction is still open — is exactly
    /// what a partial write should produce.
    pub fn open(dir: &Path) -> Result<Self, StorageError> {
        let path = dir.join(TXN_INDEX_FILE);
        let mut index = TransactionIndex {
            path: path.clone(),
            ongoing: BTreeMap::new(),
            aborted: Vec::new(),
            file: None,
        };

        match fs::read(&path) {
            Ok(bytes) => {
                for entry in bytes.chunks_exact(ENTRY_LEN) {
                    index.replay(entry);
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        index.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&path)?,
        );
        Ok(index)
    }

    fn replay(&mut self, entry: &[u8]) {
        let kind = entry[0];
        let producer_id = i64::from_be_bytes(entry[1..9].try_into().expect("fixed width"));
        let first_offset = i64::from_be_bytes(entry[9..17].try_into().expect("fixed width"));
        let last_offset = i64::from_be_bytes(entry[17..25].try_into().expect("fixed width"));
        match kind {
            KIND_BEGIN => {
                self.ongoing.insert(producer_id, first_offset);
            }
            KIND_COMMIT => {
                self.ongoing.remove(&producer_id);
            }
            KIND_ABORT => {
                self.ongoing.remove(&producer_id);
                self.aborted.push(AbortedTransaction {
                    producer_id,
                    first_offset,
                    last_offset,
                });
            }
            // An unknown kind is from a future version. Skipping it rather
            // than failing keeps a downgrade readable, and the effect —
            // treating the transaction as still open — errs towards showing
            // a `read_committed` consumer less, never more.
            _ => {}
        }
    }

    fn append(
        &mut self,
        kind: u8,
        producer_id: i64,
        first: i64,
        last: i64,
    ) -> Result<(), StorageError> {
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        let mut entry = [0u8; ENTRY_LEN];
        entry[0] = kind;
        entry[1..9].copy_from_slice(&producer_id.to_be_bytes());
        entry[9..17].copy_from_slice(&first.to_be_bytes());
        entry[17..25].copy_from_slice(&last.to_be_bytes());
        file.write_all(&entry)?;
        // Deliberately not fsynced per entry. Losing the tail of this
        // journal costs correctness in the safe direction — a transaction
        // whose `begin` is lost has its records held back from committed
        // readers, and one whose `commit` is lost stays held back too — and
        // the alternative is an fsync on the produce path of every
        // transactional batch.
        Ok(())
    }

    /// Note that `producer_id` has started writing at `first_offset`.
    ///
    /// Idempotent: the first record of a transaction is what opens it, and
    /// later batches of the same transaction must not move its start.
    pub fn begin(&mut self, producer_id: i64, first_offset: i64) -> Result<(), StorageError> {
        if self.ongoing.contains_key(&producer_id) {
            return Ok(());
        }
        self.ongoing.insert(producer_id, first_offset);
        self.append(KIND_BEGIN, producer_id, first_offset, -1)
    }

    /// Close `producer_id`'s transaction. `marker_offset` is where its
    /// control batch landed.
    ///
    /// A commit simply forgets the transaction: its records are now
    /// ordinary readable records. An abort is remembered, because every
    /// later committed read has to keep skipping those records.
    pub fn end(
        &mut self,
        producer_id: i64,
        marker_offset: i64,
        committed: bool,
    ) -> Result<(), StorageError> {
        let Some(first_offset) = self.ongoing.remove(&producer_id) else {
            // A marker with no open transaction: a duplicate delivery, or a
            // marker replayed after its `begin` was lost. Nothing to close.
            return Ok(());
        };
        if committed {
            self.append(KIND_COMMIT, producer_id, first_offset, marker_offset)
        } else {
            self.aborted.push(AbortedTransaction {
                producer_id,
                first_offset,
                last_offset: marker_offset,
            });
            self.append(KIND_ABORT, producer_id, first_offset, marker_offset)
        }
    }

    /// The last stable offset, given the partition's high watermark.
    ///
    /// With nothing in flight this *is* the high watermark: every committed
    /// record is stable. With a transaction open it is that transaction's
    /// first offset, because nothing at or after it is decided yet — and
    /// the oldest open transaction wins, since a later one committing does
    /// not make an earlier one's records visible.
    pub fn last_stable_offset(&self, high_watermark: i64) -> i64 {
        self.ongoing
            .values()
            .copied()
            .min()
            .map_or(high_watermark, |first| first.min(high_watermark))
    }

    /// Aborted transactions overlapping `[from, to)`, oldest first.
    ///
    /// A reader needs the ones whose records could appear in the range it
    /// is about to read, which is any transaction that started before the
    /// range ends and ended at or after it began.
    pub fn aborted_in_range(&self, from: i64, to: i64) -> Vec<AbortedTransaction> {
        let mut overlapping: Vec<_> = self
            .aborted
            .iter()
            .copied()
            .filter(|txn| txn.first_offset < to && txn.last_offset >= from)
            .collect();
        overlapping.sort_by_key(|txn| txn.first_offset);
        overlapping
    }

    /// Whether any transaction is in flight.
    pub fn has_ongoing(&self) -> bool {
        !self.ongoing.is_empty()
    }

    /// Producers with a transaction open, for diagnostics.
    pub fn ongoing_producers(&self) -> Vec<i64> {
        self.ongoing.keys().copied().collect()
    }

    /// Each open transaction as `(producer_id, first_offset)`.
    pub fn open_transactions(&self) -> Vec<(i64, i64)> {
        self.ongoing
            .iter()
            .map(|(producer_id, first)| (*producer_id, *first))
            .collect()
    }

    /// Drop aborted transactions that ended below `log_start_offset` and
    /// rewrite the journal without them.
    ///
    /// Their records are gone, so nothing will ever need to skip them
    /// again; without this the journal would be the one file in a partition
    /// that retention could never shrink.
    pub fn prune_below(&mut self, log_start_offset: i64) -> Result<(), StorageError> {
        let before = self.aborted.len();
        self.aborted
            .retain(|txn| txn.last_offset >= log_start_offset);
        if self.aborted.len() == before {
            return Ok(());
        }
        self.rewrite()
    }

    /// Rewrite the journal from the current in-memory state.
    fn rewrite(&mut self) -> Result<(), StorageError> {
        if self.file.is_none() {
            return Ok(());
        }
        let mut bytes = Vec::with_capacity((self.ongoing.len() + self.aborted.len()) * ENTRY_LEN);
        let mut encode = |kind: u8, producer_id: i64, first: i64, last: i64| {
            bytes.push(kind);
            bytes.extend_from_slice(&producer_id.to_be_bytes());
            bytes.extend_from_slice(&first.to_be_bytes());
            bytes.extend_from_slice(&last.to_be_bytes());
        };
        for (producer_id, first_offset) in &self.ongoing {
            encode(KIND_BEGIN, *producer_id, *first_offset, -1);
        }
        for txn in &self.aborted {
            encode(
                KIND_ABORT,
                txn.producer_id,
                txn.first_offset,
                txn.last_offset,
            );
        }

        // Write beside the live file and rename over it, so a crash leaves
        // either the old journal or the new one and never a half-written
        // mixture of the two.
        let temporary = self.path.with_extension("rewrite");
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &self.path)?;
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&self.path)?,
        );
        Ok(())
    }

    /// Force the journal to disk.
    pub fn sync(&self) -> Result<(), StorageError> {
        if let Some(file) = self.file.as_ref() {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Forget everything. Used when a follower truncates a divergent tail:
    /// the transactions it was tracking belonged to records that no longer
    /// exist.
    pub fn reset(&mut self) -> Result<(), StorageError> {
        self.ongoing.clear();
        self.aborted.clear();
        self.rewrite()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_stable_offset_is_held_by_the_oldest_open_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TransactionIndex::open(dir.path()).unwrap();

        // Nothing open: every committed record is stable.
        assert_eq!(index.last_stable_offset(100), 100);

        index.begin(7, 40).unwrap();
        index.begin(9, 60).unwrap();
        assert_eq!(index.last_stable_offset(100), 40);

        // The younger transaction committing does not release the older
        // one's records: offsets 40..60 are still undecided.
        index.end(9, 61, true).unwrap();
        assert_eq!(index.last_stable_offset(100), 40);

        index.end(7, 62, true).unwrap();
        assert_eq!(index.last_stable_offset(100), 100);
        assert!(!index.has_ongoing());
    }

    #[test]
    fn a_transactions_first_batch_fixes_its_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TransactionIndex::open(dir.path()).unwrap();
        index.begin(1, 10).unwrap();
        // Later batches of the same transaction must not move the LSO
        // forward — the records at 10 are still in doubt.
        index.begin(1, 20).unwrap();
        assert_eq!(index.last_stable_offset(50), 10);
    }

    #[test]
    fn aborted_transactions_are_remembered_and_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut index = TransactionIndex::open(dir.path()).unwrap();
            index.begin(3, 5).unwrap();
            index.end(3, 12, false).unwrap();
            index.begin(4, 20).unwrap();
            index.end(4, 25, true).unwrap();
            index.begin(5, 30).unwrap();
            index.sync().unwrap();
        }

        let index = TransactionIndex::open(dir.path()).unwrap();
        // The abort is remembered, the commit is forgotten, and the open
        // transaction is still open.
        assert_eq!(
            index.aborted_in_range(0, 100),
            vec![AbortedTransaction {
                producer_id: 3,
                first_offset: 5,
                last_offset: 12,
            }]
        );
        assert_eq!(index.ongoing_producers(), vec![5]);
        assert_eq!(index.last_stable_offset(100), 30);
    }

    #[test]
    fn only_aborted_transactions_overlapping_the_read_are_returned() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TransactionIndex::open(dir.path()).unwrap();
        for (producer_id, first, last) in [(1, 0, 5), (2, 10, 15), (3, 40, 45)] {
            index.begin(producer_id, first).unwrap();
            index.end(producer_id, last, false).unwrap();
        }
        let overlapping = index.aborted_in_range(10, 20);
        assert_eq!(
            overlapping
                .iter()
                .map(|t| t.producer_id)
                .collect::<Vec<_>>(),
            vec![2],
            "a read of [10, 20) needs only the transaction whose records are in it"
        );
        assert_eq!(index.aborted_in_range(0, 100).len(), 3);
    }

    #[test]
    fn pruning_forgets_transactions_whose_records_are_gone() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TransactionIndex::open(dir.path()).unwrap();
        for (producer_id, first, last) in [(1, 0, 5), (2, 10, 15), (3, 40, 45)] {
            index.begin(producer_id, first).unwrap();
            index.end(producer_id, last, false).unwrap();
        }
        index.begin(9, 90).unwrap();

        index.prune_below(20).unwrap();
        assert_eq!(
            index
                .aborted_in_range(0, 1_000)
                .iter()
                .map(|t| t.producer_id)
                .collect::<Vec<_>>(),
            vec![3]
        );

        // The rewrite must keep the open transaction: it is what the LSO
        // depends on, and losing it would expose undecided records.
        let reopened = TransactionIndex::open(dir.path()).unwrap();
        assert_eq!(reopened.ongoing_producers(), vec![9]);
        assert_eq!(reopened.aborted_in_range(0, 1_000).len(), 1);
    }
}
