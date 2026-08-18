//! Sparse index files (DESIGN.md §4.2).
//!
//! Offset index: fixed 8-byte big-endian entries
//! `(relative_offset: u32, position: u32)` mapping a batch's offset
//! (relative to the segment base) to its byte position in the `.log` file.
//!
//! Time index: fixed 12-byte big-endian entries
//! `(timestamp: i64, relative_offset: u32)`.
//!
//! Both are append-only, mirrored in memory, and binary-searched on read
//! (one entry per `index_interval_bytes` of log, so a lookup lands within
//! a few KB of the target batch).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) const OFFSET_ENTRY_LEN: usize = 8;
pub(crate) const TIME_ENTRY_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OffsetEntry {
    pub relative_offset: u32,
    pub position: u32,
}

impl OffsetEntry {
    fn encode(&self) -> [u8; OFFSET_ENTRY_LEN] {
        let mut out = [0u8; OFFSET_ENTRY_LEN];
        out[0..4].copy_from_slice(&self.relative_offset.to_be_bytes());
        out[4..8].copy_from_slice(&self.position.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Self {
        OffsetEntry {
            relative_offset: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            position: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimeEntry {
    pub timestamp: i64,
    pub relative_offset: u32,
}

impl TimeEntry {
    fn encode(&self) -> [u8; TIME_ENTRY_LEN] {
        let mut out = [0u8; TIME_ENTRY_LEN];
        out[0..8].copy_from_slice(&self.timestamp.to_be_bytes());
        out[8..12].copy_from_slice(&self.relative_offset.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Self {
        TimeEntry {
            timestamp: i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            relative_offset: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        }
    }
}

/// An append-only sparse index file with its entries mirrored in memory.
pub(crate) struct SparseIndex<E: IndexEntry> {
    entries: Vec<E>,
    file: File,
}

pub(crate) trait IndexEntry: Copy {
    const LEN: usize;
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: &[u8]) -> Self;
}

impl IndexEntry for OffsetEntry {
    const LEN: usize = OFFSET_ENTRY_LEN;
    fn encode(&self) -> Vec<u8> {
        OffsetEntry::encode(self).to_vec()
    }
    fn decode(bytes: &[u8]) -> Self {
        OffsetEntry::decode(bytes)
    }
}

impl IndexEntry for TimeEntry {
    const LEN: usize = TIME_ENTRY_LEN;
    fn encode(&self) -> Vec<u8> {
        TimeEntry::encode(self).to_vec()
    }
    fn decode(bytes: &[u8]) -> Self {
        TimeEntry::decode(bytes)
    }
}

impl<E: IndexEntry> SparseIndex<E> {
    /// Open (or create) an index file, loading all complete entries.
    /// A torn trailing partial entry is truncated away.
    pub fn open(path: &Path) -> io::Result<Self> {
        // No `truncate`: an existing index file must be preserved.
        #[allow(clippy::suspicious_open_options)]
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        let entries: Vec<E> = raw.chunks_exact(E::LEN).map(E::decode).collect();
        let valid_len = entries.len() * E::LEN;
        if valid_len < raw.len() {
            file.set_len(valid_len as u64)?;
        }
        Ok(SparseIndex { entries, file })
    }

    pub fn entries(&self) -> &[E] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn append(&mut self, entry: E) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&entry.encode())?;
        self.entries.push(entry);
        Ok(())
    }

    /// Force this index file to stable storage.
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Replace the whole file with the given entries (used by recovery,
    /// which rebuilds the active segment's index from validated batches).
    pub fn rewrite(&mut self, entries: Vec<E>) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        for entry in &entries {
            self.file.write_all(&entry.encode())?;
        }
        self.entries = entries;
        Ok(())
    }
}

impl SparseIndex<OffsetEntry> {
    /// Byte position of the last entry whose relative offset is `<= target`.
    /// Returns 0 (start of segment) when no entry qualifies.
    pub fn lookup(&self, relative_offset: u32) -> u32 {
        let idx = self
            .entries
            .partition_point(|e| e.relative_offset <= relative_offset);
        if idx == 0 {
            0
        } else {
            self.entries[idx - 1].position
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_index_lookup_binary_search() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut index: SparseIndex<OffsetEntry> = SparseIndex::open(&path).unwrap();
        for i in 0..10 {
            index
                .append(OffsetEntry {
                    relative_offset: i * 10,
                    position: i * 4096,
                })
                .unwrap();
        }
        assert_eq!(index.lookup(0), 0);
        assert_eq!(index.lookup(9), 0);
        assert_eq!(index.lookup(10), 4096);
        assert_eq!(index.lookup(55), 5 * 4096);
        assert_eq!(index.lookup(10_000), 9 * 4096);
    }

    #[test]
    fn offset_index_survives_reopen_and_drops_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("i");
        {
            let mut index: SparseIndex<OffsetEntry> = SparseIndex::open(&path).unwrap();
            index
                .append(OffsetEntry {
                    relative_offset: 3,
                    position: 128,
                })
                .unwrap();
        }
        // Simulate a torn write: 3 stray bytes at the end.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[1, 2, 3]).unwrap();
        }
        let index: SparseIndex<OffsetEntry> = SparseIndex::open(&path).unwrap();
        assert_eq!(index.entries().len(), 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            OFFSET_ENTRY_LEN as u64
        );
    }
}
