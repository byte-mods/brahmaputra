//! A single log segment: one `.log` file plus its `.index` / `.timeindex`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use brahmaputra_protocol::{validate_batch_header, BATCH_HEADER_LEN, MIN_BATCH_LENGTH};

use crate::error::StorageError;
use crate::index::{OffsetEntry, SparseIndex, TimeEntry};

/// Result of scanning a segment's batches during recovery.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecoveryInfo {
    /// Offset one past the last valid record in the segment.
    pub next_offset: i64,
}

pub(crate) struct Segment {
    pub base_offset: i64,
    /// Current length of the `.log` file in bytes.
    pub size: u64,
    /// Largest batch `max_timestamp` seen; `None` while the segment is empty.
    pub max_timestamp: Option<i64>,
    dir: PathBuf,
    log_file: File,
    pub index: SparseIndex<OffsetEntry>,
    pub timeindex: SparseIndex<TimeEntry>,
    index_interval_bytes: u64,
    /// Log bytes appended since the last index entry.
    bytes_since_index: u64,
}

fn log_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.log"))
}

fn index_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.index"))
}

fn timeindex_path(dir: &Path, base_offset: i64) -> PathBuf {
    dir.join(format!("{base_offset:020}.timeindex"))
}

impl Segment {
    pub fn open(dir: &Path, base_offset: i64, index_interval_bytes: u64) -> io::Result<Self> {
        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path(dir, base_offset))?;
        let size = log_file.metadata()?.len();
        let index = SparseIndex::open(&index_path(dir, base_offset))?;
        let timeindex = SparseIndex::open(&timeindex_path(dir, base_offset))?;
        Ok(Segment {
            base_offset,
            size,
            max_timestamp: None,
            dir: dir.to_path_buf(),
            log_file,
            index,
            timeindex,
            index_interval_bytes,
            bytes_since_index: 0,
        })
    }

    pub fn read_at(&self, position: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut file = &self.log_file;
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(buf)
    }

    /// Append one already-encoded batch whose first record sits at
    /// `base_offset` (absolute). Maintains the sparse indexes.
    /// Force this segment's data and indices to stable storage.
    ///
    /// Order matters: the log file goes first, so a crash between the two
    /// syncs leaves an index that describes *less* than the log holds
    /// (recovery rebuilds the tail) rather than an index pointing past the
    /// data it claims to describe.
    pub fn sync(&self) -> io::Result<()> {
        self.log_file.sync_data()?;
        self.index.sync()?;
        self.timeindex.sync()
    }

    pub fn append_batch(
        &mut self,
        base_offset: i64,
        bytes: &[u8],
        max_timestamp: i64,
    ) -> io::Result<()> {
        debug_assert!(base_offset >= self.base_offset);
        let relative_offset = (base_offset - self.base_offset) as u32;
        if self.index.is_empty() || self.bytes_since_index >= self.index_interval_bytes {
            self.index.append(OffsetEntry {
                relative_offset,
                position: self.size as u32,
            })?;
            self.timeindex.append(TimeEntry {
                timestamp: max_timestamp,
                relative_offset,
            })?;
            self.bytes_since_index = 0;
        }
        // File is opened in append mode: writes always land at EOF.
        self.log_file.write_all(bytes)?;
        self.size += bytes.len() as u64;
        self.bytes_since_index += bytes.len() as u64;
        self.max_timestamp = Some(
            self.max_timestamp
                .map_or(max_timestamp, |t| t.max(max_timestamp)),
        );
        Ok(())
    }

    /// Truncate at the first batch that is not wholly below `offset` and
    /// rebuild both indexes. Returns the resulting exclusive log end. If
    /// `offset` falls inside a batch, the entire batch is removed so a log
    /// is always a prefix of valid record batches.
    pub fn truncate_to_offset(&mut self, offset: i64) -> Result<i64, StorageError> {
        let file_len = self.size;
        let mut position = 0u64;
        let mut valid_end = 0u64;
        let mut next_offset = self.base_offset;
        while let Some((header, batch_len)) = self.read_valid_batch(position, file_len)? {
            let batch_end = header.base_offset + header.last_offset_delta as i64 + 1;
            if batch_end > offset {
                break;
            }
            position += batch_len;
            valid_end = position;
            next_offset = batch_end;
        }

        if valid_end < file_len {
            let truncation = OpenOptions::new()
                .write(true)
                .open(log_path(&self.dir, self.base_offset))?;
            truncation.set_len(valid_end)?;
            self.size = valid_end;
        }
        let recovered = self.recover()?;
        debug_assert_eq!(recovered.next_offset, next_offset);
        Ok(next_offset)
    }

    /// Scan every batch from position 0, validating framing and CRC, then
    /// truncate any torn/corrupt tail and rebuild both sparse indexes from
    /// the validated batches. Used on the active segment at open (DESIGN.md
    /// §4.4 crash recovery).
    pub fn recover(&mut self) -> Result<RecoveryInfo, StorageError> {
        let file_len = self.size;
        let mut position = 0u64;
        let mut valid_end = 0u64;
        let mut next_offset = self.base_offset;
        let mut max_timestamp: Option<i64> = None;
        let mut entries = Vec::new();
        let mut time_entries = Vec::new();
        let mut since_index = 0u64;

        while let Some((header, batch_len)) = self.read_valid_batch(position, file_len)? {
            if entries.is_empty() || since_index >= self.index_interval_bytes {
                let relative_offset = (header.base_offset - self.base_offset) as u32;
                entries.push(OffsetEntry {
                    relative_offset,
                    position: position as u32,
                });
                time_entries.push(TimeEntry {
                    timestamp: header.max_timestamp,
                    relative_offset,
                });
                since_index = 0;
            }
            next_offset = header.base_offset + header.last_offset_delta as i64 + 1;
            max_timestamp = Some(
                max_timestamp.map_or(header.max_timestamp, |t: i64| t.max(header.max_timestamp)),
            );
            position += batch_len;
            valid_end = position;
            since_index += batch_len;
        }

        if valid_end < file_len {
            // Windows denies set_len on handles opened with append(true)
            // (std strips FILE_WRITE_DATA in append mode), so truncate via a
            // dedicated write handle.
            let trunc = OpenOptions::new()
                .write(true)
                .open(log_path(&self.dir, self.base_offset))?;
            trunc.set_len(valid_end)?;
            self.size = valid_end;
        }
        self.bytes_since_index = valid_end - entries.last().map_or(0, |e| e.position as u64);
        self.max_timestamp = max_timestamp;
        self.index.rewrite(entries)?;
        self.timeindex.rewrite(time_entries)?;
        Ok(RecoveryInfo { next_offset })
    }

    /// Compute the segment's max timestamp by scanning from the last index
    /// entry to EOF (bounded by roughly `index_interval_bytes` plus one
    /// batch). Used for sealed segments at open, where a full rescan would
    /// be wasteful.
    pub fn scan_max_timestamp(&self) -> Result<Option<i64>, StorageError> {
        let file_len = self.size;
        let mut position = self.index.entries().last().map_or(0, |e| e.position as u64);
        let mut max_timestamp: Option<i64> = None;
        while let Some((header, batch_len)) = self.read_valid_batch(position, file_len)? {
            max_timestamp = Some(
                max_timestamp.map_or(header.max_timestamp, |t: i64| t.max(header.max_timestamp)),
            );
            position += batch_len;
        }
        Ok(max_timestamp)
    }

    /// Read and CRC-validate the batch starting at `position`. Returns
    /// `Ok(None)` when the tail is incomplete, corrupt, or malformed —
    /// callers treat that as the end of valid data.
    fn read_valid_batch(
        &self,
        position: u64,
        file_len: u64,
    ) -> Result<Option<(brahmaputra_protocol::BatchHeader, u64)>, StorageError> {
        if position + BATCH_HEADER_LEN as u64 > file_len {
            return Ok(None);
        }
        let mut hdr = [0u8; BATCH_HEADER_LEN];
        self.read_at(position, &mut hdr)?;
        let batch_length = i32::from_be_bytes(hdr[8..12].try_into().unwrap());
        if batch_length < MIN_BATCH_LENGTH as i32 {
            return Ok(None);
        }
        let total = BATCH_HEADER_LEN as u64 + batch_length as u64;
        if position + total > file_len {
            return Ok(None);
        }
        let mut batch = vec![0u8; total as usize];
        self.read_at(position, &mut batch)?;
        match validate_batch_header(&batch) {
            Ok(header) => Ok(Some((header, total))),
            Err(_) => Ok(None),
        }
    }

    /// Delete the segment's `.log`, `.index` and `.timeindex` files.
    pub fn delete(self) -> io::Result<()> {
        let paths = [
            log_path(&self.dir, self.base_offset),
            index_path(&self.dir, self.base_offset),
            timeindex_path(&self.dir, self.base_offset),
        ];
        // Close file handles before removing (required on Windows).
        drop(self);
        for path in &paths {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
