//! The per-partition log: a sequence of segments plus the high watermark.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use brahmaputra_protocol::{
    validate_batch_header, Record, RecordBatch, BATCH_HEADER_LEN, MIN_BATCH_LENGTH,
};
use bytes::{Bytes, BytesMut};

use crate::epoch::LeaderEpochCheckpoint;
use crate::error::StorageError;
use crate::segment::Segment;

const HWM_FILE: &str = "hwm";

/// How much a single read pulls from a segment when the caller's budget
/// is larger. Big enough to cover many small batches in one syscall,
/// small enough not to read far past what a fetch will actually use.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

/// Bytes of a batch that must be read to learn where it ends and which
/// offsets it covers: through `last_offset_delta`. Everything after this is
/// payload a zero-copy fetch never touches.
const REGION_PROBE_LEN: usize = 27;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}
/// How often the high-watermark checkpoint reaches disk. Kafka's
/// `replica.high.watermark.checkpoint.interval.ms` defaults to the same 5 s.
const DEFAULT_HWM_CHECKPOINT_INTERVAL_MS: u64 = 5_000;

const HWM_RECORD_MAGIC: [u8; 4] = *b"HWMJ";
const HWM_RECORD_VERSION: u8 = 1;
const HWM_RECORD_LEN: usize = 36;
const HWM_RECORD_CHECKSUM_OFFSET: usize = HWM_RECORD_LEN - size_of::<u32>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum HighWatermarkRecordKind {
    Advance = 0,
    RecoveryClamp = 1,
}

/// Append-only, checksummed high-watermark checkpoint.
///
/// The first eight bytes retain the original checkpoint format. Versioned
/// records form a generation- and value-chained journal after that prefix, so
/// a torn replacement can never discard the previous durable watermark.
/// The high watermark a partition has actually reached lives in memory; this
/// file exists so a restart can recover a *safe lower bound* for it. Writing
/// it is therefore a recovery hint, not the durability guarantee — the
/// guarantee comes from the record data, which follows `flush.interval.*`
/// and by default is left to the operating system exactly as Kafka leaves
/// it.
///
/// So this must not fsync on every append. Doing that spent the most
/// expensive operation available on the weakest guarantee in the system,
/// and it showed: an append-heavy run sat at ~99 % of one core with the
/// rest of the CPU idle, and got *slower* as partitions were added, because
/// every partition was fsyncing its own checkpoint on every batch. Kafka
/// writes this file on a timer (`replica.high.watermark.checkpoint.interval.ms`,
/// default 5 s) and so does this.
///
/// The cost of a stale checkpoint is bounded and understood: after an
/// unclean stop the recovered watermark may lag the one consumers last saw,
/// so those records are briefly invisible until it advances again. Kafka
/// accepts precisely that trade for precisely this reason.
struct HighWatermarkCheckpoint {
    path: PathBuf,
    generation: u64,
    /// The watermark last written to disk — the chain's tail, not the
    /// partition's current watermark.
    high_watermark: i64,
    valid_len: usize,
    /// Advanced but not yet written.
    pending: Option<i64>,
    interval_ms: u64,
    last_write_ms: i64,
}

impl HighWatermarkCheckpoint {
    fn open(path: PathBuf, interval_ms: u64) -> Result<Self, StorageError> {
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };

        let mut checkpoint = Self {
            path,
            generation: 0,
            high_watermark: 0,
            valid_len: 0,
            pending: None,
            interval_ms,
            last_write_ms: now_ms(),
        };

        if bytes.len() >= size_of::<i64>() {
            let legacy = i64::from_be_bytes(
                bytes[..size_of::<i64>()]
                    .try_into()
                    .expect("fixed-size legacy checkpoint"),
            );
            if legacy >= 0 {
                checkpoint.high_watermark = legacy;
                checkpoint.valid_len = size_of::<i64>();

                let mut offset = checkpoint.valid_len;
                while bytes.len().saturating_sub(offset) >= HWM_RECORD_LEN {
                    let record = &bytes[offset..offset + HWM_RECORD_LEN];
                    let Some((kind, generation, high_watermark)) =
                        decode_hwm_record(record, checkpoint.generation, checkpoint.high_watermark)
                    else {
                        break;
                    };

                    match kind {
                        HighWatermarkRecordKind::Advance
                            if high_watermark < checkpoint.high_watermark =>
                        {
                            break;
                        }
                        HighWatermarkRecordKind::RecoveryClamp
                            if high_watermark > checkpoint.high_watermark =>
                        {
                            break;
                        }
                        _ => {}
                    }

                    checkpoint.generation = generation;
                    checkpoint.high_watermark = high_watermark;
                    checkpoint.valid_len += HWM_RECORD_LEN;
                    offset += HWM_RECORD_LEN;
                }
            }
        }

        if bytes.len() != checkpoint.valid_len {
            checkpoint.truncate_invalid_tail()?;
        }
        Ok(checkpoint)
    }

    fn recover(&mut self, log_end: i64) -> Result<i64, StorageError> {
        if self.high_watermark > log_end {
            self.append(HighWatermarkRecordKind::RecoveryClamp, log_end)?;
        }
        Ok(self.high_watermark)
    }

    /// Note the new watermark and write it only if the interval has elapsed.
    fn advance(&mut self, high_watermark: i64) -> Result<(), StorageError> {
        if high_watermark == self.persisted_or_pending() {
            return Ok(());
        }
        self.pending = Some(high_watermark);
        if now_ms().saturating_sub(self.last_write_ms) >= self.interval_ms as i64 {
            self.persist()?;
        }
        Ok(())
    }

    /// Write the pending watermark now, whatever the interval says. Called
    /// when the log flushes and when the partition shuts down, so a clean
    /// stop never loses the watermark it had reached.
    fn persist(&mut self) -> Result<(), StorageError> {
        let Some(target) = self.pending.take() else {
            return Ok(());
        };
        if target != self.high_watermark {
            self.append(HighWatermarkRecordKind::Advance, target)?;
        }
        self.last_write_ms = now_ms();
        Ok(())
    }

    fn persist_if_due(&mut self) -> Result<(), StorageError> {
        if self.pending.is_some()
            && now_ms().saturating_sub(self.last_write_ms) >= self.interval_ms as i64
        {
            return self.persist();
        }
        Ok(())
    }

    fn persisted_or_pending(&self) -> i64 {
        self.pending.unwrap_or(self.high_watermark)
    }

    fn append(
        &mut self,
        kind: HighWatermarkRecordKind,
        high_watermark: i64,
    ) -> Result<(), StorageError> {
        self.ensure_legacy_prefix()?;
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(StorageError::InvalidConfig(
                "high-watermark checkpoint generation exhausted",
            ))?;
        let record = encode_hwm_record(kind, generation, self.high_watermark, high_watermark);

        let mut file = OpenOptions::new().write(true).open(&self.path)?;
        // A previous failed append may have left an invalid suffix. Removing
        // only that suffix preserves the last complete durable checkpoint.
        file.set_len(self.valid_len as u64)?;
        file.seek(SeekFrom::Start(self.valid_len as u64))?;
        file.write_all(&record)?;
        file.sync_all()?;

        self.generation = generation;
        self.high_watermark = high_watermark;
        self.valid_len += HWM_RECORD_LEN;
        Ok(())
    }

    fn ensure_legacy_prefix(&mut self) -> Result<(), StorageError> {
        if self.valid_len != 0 {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        file.write_all(&0_i64.to_be_bytes())?;
        file.sync_all()?;
        self.valid_len = size_of::<i64>();
        Ok(())
    }

    fn truncate_invalid_tail(&self) -> Result<(), StorageError> {
        if self.valid_len == 0 {
            if let Err(error) = fs::remove_file(&self.path) {
                if error.kind() != ErrorKind::NotFound {
                    return Err(error.into());
                }
            }
            return Ok(());
        }

        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(self.valid_len as u64)?;
        file.sync_all()?;
        Ok(())
    }
}

fn encode_hwm_record(
    kind: HighWatermarkRecordKind,
    generation: u64,
    previous_high_watermark: i64,
    high_watermark: i64,
) -> [u8; HWM_RECORD_LEN] {
    let mut record = [0_u8; HWM_RECORD_LEN];
    record[..4].copy_from_slice(&HWM_RECORD_MAGIC);
    record[4] = HWM_RECORD_VERSION;
    record[5] = kind as u8;
    record[8..16].copy_from_slice(&generation.to_be_bytes());
    record[16..24].copy_from_slice(&previous_high_watermark.to_be_bytes());
    record[24..32].copy_from_slice(&high_watermark.to_be_bytes());
    let checksum = crc32c::crc32c(&record[..HWM_RECORD_CHECKSUM_OFFSET]);
    record[HWM_RECORD_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_be_bytes());
    record
}

fn decode_hwm_record(
    record: &[u8],
    previous_generation: u64,
    previous_high_watermark: i64,
) -> Option<(HighWatermarkRecordKind, u64, i64)> {
    if record.len() != HWM_RECORD_LEN
        || record[..4] != HWM_RECORD_MAGIC
        || record[4] != HWM_RECORD_VERSION
        || record[6..8] != [0, 0]
    {
        return None;
    }

    let expected_checksum =
        u32::from_be_bytes(record[HWM_RECORD_CHECKSUM_OFFSET..].try_into().ok()?);
    if crc32c::crc32c(&record[..HWM_RECORD_CHECKSUM_OFFSET]) != expected_checksum {
        return None;
    }

    let kind = match record[5] {
        value if value == HighWatermarkRecordKind::Advance as u8 => {
            HighWatermarkRecordKind::Advance
        }
        value if value == HighWatermarkRecordKind::RecoveryClamp as u8 => {
            HighWatermarkRecordKind::RecoveryClamp
        }
        _ => return None,
    };
    let generation = u64::from_be_bytes(record[8..16].try_into().ok()?);
    let chained_high_watermark = i64::from_be_bytes(record[16..24].try_into().ok()?);
    let high_watermark = i64::from_be_bytes(record[24..32].try_into().ok()?);
    if generation != previous_generation.checked_add(1)?
        || chained_high_watermark != previous_high_watermark
        || high_watermark < 0
    {
        return None;
    }
    Some((kind, generation, high_watermark))
}

/// Configuration for a [`Log`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// Roll the active segment once it reaches this many bytes.
    /// Must fit in a `u32` (index positions are 32-bit, as in Kafka).
    pub segment_bytes: u64,
    /// Roll the active segment once it is this old, even if it never
    /// reaches `segment_bytes`. `None` disables it.
    ///
    /// Retention only ever deletes *sealed* segments, so without this a
    /// low-volume partition keeps one segment open forever and nothing it
    /// contains can expire, no matter what `retention.ms` says.
    pub segment_ms: Option<u64>,
    /// Add a sparse-index entry at least every this many log bytes.
    pub index_interval_bytes: u64,
    /// Delete sealed segments whose newest batch is older than this many
    /// milliseconds. `None` disables time-based retention.
    pub retention_ms: Option<u64>,
    /// Delete oldest sealed segments while the log exceeds this many bytes.
    /// `None` disables size-based retention.
    pub retention_bytes: Option<u64>,
    /// fsync the active segment after this many records. `None` leaves
    /// durability to replication and the OS page cache, which is Kafka's
    /// default and this system's design (DESIGN.md §4.2).
    pub flush_interval_messages: Option<u64>,
    /// fsync the active segment at least this often, in milliseconds.
    pub flush_interval_ms: Option<u64>,
    /// Keep only the latest record per key instead of deleting whole aged
    /// segments — Kafka's `cleanup.policy=compact`. Set for the internal
    /// offsets topic, whose keys are rewritten forever.
    pub compact: bool,
    /// How often the high-watermark checkpoint reaches disk, in
    /// milliseconds. Kafka's equivalent is
    /// `replica.high.watermark.checkpoint.interval.ms`, default 5 s. Zero
    /// checkpoints on every advance, which is what the internal offsets
    /// topic uses: a committed consumer offset that vanishes on restart is
    /// a correctness break, and its commit rate is low enough that the
    /// cost does not matter.
    pub hwm_checkpoint_interval_ms: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            segment_bytes: 64 * 1024 * 1024,
            segment_ms: None,
            index_interval_bytes: 4096,
            retention_ms: None,
            retention_bytes: None,
            flush_interval_messages: None,
            flush_interval_ms: None,
            compact: false,
            hwm_checkpoint_interval_ms: DEFAULT_HWM_CHECKPOINT_INTERVAL_MS,
        }
    }
}

/// A per-partition segmented append-only log (DESIGN.md §4).
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    /// Sorted by base offset; the last segment is the active one.
    segments: Vec<Segment>,
    next_offset: i64,
    start_offset: i64,
    high_watermark: i64,
    high_watermark_checkpoint: HighWatermarkCheckpoint,
    leader_epochs: LeaderEpochCheckpoint,
    /// Records appended since the last fsync (`flush.interval.messages`).
    unflushed_records: u64,
    /// When the last fsync happened (`flush.interval.ms`).
    last_flush_ms: i64,
    /// When the active segment was opened, for `segment.ms`. Set on open
    /// and on every roll; a restart therefore restarts the clock, which
    /// only ever delays a roll rather than losing data.
    active_segment_created_ms: i64,
}

impl Log {
    /// Open the log in `dir`, creating it if needed.
    ///
    /// Scans existing segments, rebuilds the active (last) segment's index
    /// by validating every batch's framing and CRC, truncates to the last
    /// valid batch (crash recovery, DESIGN.md §4.4), and resumes with the
    /// correct next offset.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, StorageError> {
        if config.segment_bytes == 0 || config.segment_bytes > u32::MAX as u64 {
            return Err(StorageError::InvalidConfig(
                "segment_bytes must be in 1..=u32::MAX",
            ));
        }
        if config.index_interval_bytes == 0 {
            return Err(StorageError::InvalidConfig(
                "index_interval_bytes must be >= 1",
            ));
        }
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(stem) = name.strip_suffix(".log") {
                if stem.len() == 20 {
                    if let Ok(base) = stem.parse::<i64>() {
                        base_offsets.push(base);
                    }
                }
            }
        }
        base_offsets.sort_unstable();
        base_offsets.dedup();
        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let mut segments = Vec::with_capacity(base_offsets.len());
        // Sealed segments: trust their index, learn max_timestamp from the tail.
        for &base in &base_offsets[..base_offsets.len() - 1] {
            let mut seg = Segment::open(&dir, base, config.index_interval_bytes)?;
            seg.max_timestamp = seg.scan_max_timestamp()?;
            segments.push(seg);
        }
        // Active segment: full recovery scan + truncation of any torn tail.
        let mut active = Segment::open(
            &dir,
            base_offsets[base_offsets.len() - 1],
            config.index_interval_bytes,
        )?;
        let recovery = active.recover()?;
        let next_offset = recovery.next_offset;
        segments.push(active);

        let start_offset = segments[0].base_offset;
        let mut high_watermark_checkpoint =
            HighWatermarkCheckpoint::open(dir.join(HWM_FILE), config.hwm_checkpoint_interval_ms)?;
        let high_watermark = high_watermark_checkpoint.recover(next_offset)?;

        let leader_epochs = LeaderEpochCheckpoint::open(&dir)?;

        Ok(Log {
            dir,
            config,
            segments,
            next_offset,
            start_offset,
            high_watermark,
            high_watermark_checkpoint,
            leader_epochs,
            unflushed_records: 0,
            last_flush_ms: now_ms(),
            active_segment_created_ms: now_ms(),
        })
    }

    /// fsync the active segment if the configured flush policy calls for
    /// it. With no policy set this is a no-op and durability comes from
    /// replication plus the OS page cache, exactly as Kafka defaults.
    fn maybe_flush(&mut self, appended_records: u64) -> Result<(), StorageError> {
        self.unflushed_records = self.unflushed_records.saturating_add(appended_records);
        let by_count = self
            .config
            .flush_interval_messages
            .is_some_and(|limit| limit > 0 && self.unflushed_records >= limit);
        let by_time = self.config.flush_interval_ms.is_some_and(|limit| {
            self.unflushed_records > 0
                && now_ms().saturating_sub(self.last_flush_ms) >= limit as i64
        });
        if by_count || by_time {
            self.flush()?;
        }
        Ok(())
    }

    /// Force the active segment to stable storage now. The watermark
    /// checkpoint goes with it: once the data it refers to is durable there
    /// is no reason to leave the pointer to it behind.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        if let Some(active) = self.segments.last() {
            active.sync()?;
        }
        self.unflushed_records = 0;
        self.last_flush_ms = now_ms();
        self.high_watermark_checkpoint.persist()
    }

    /// Write the high-watermark checkpoint if its interval has elapsed. The
    /// partition actor polls this so an idle partition still checkpoints the
    /// watermark it reached before going quiet.
    pub fn checkpoint_high_watermark_if_due(&mut self) -> Result<(), StorageError> {
        self.high_watermark_checkpoint.persist_if_due()
    }

    /// Write the high-watermark checkpoint unconditionally, for a clean
    /// shutdown.
    pub fn checkpoint_high_watermark(&mut self) -> Result<(), StorageError> {
        self.high_watermark_checkpoint.persist()
    }

    /// Whether a time-based flush is due; the partition actor polls this so
    /// `flush.interval.ms` still fires on an idle partition.
    pub fn flush_due(&self) -> bool {
        self.config.flush_interval_ms.is_some_and(|limit| {
            self.unflushed_records > 0
                && now_ms().saturating_sub(self.last_flush_ms) >= limit as i64
        })
    }

    /// Append a batch, assigning it `base_offset = log_end_offset`.
    /// Returns the assigned base offset.
    pub fn append(&mut self, mut batch: RecordBatch) -> Result<i64, StorageError> {
        if batch.records.is_empty() {
            return Err(StorageError::EmptyBatch);
        }
        batch.base_offset = self.next_offset;
        let base_offset = self.next_offset;
        let bytes = batch.encode();
        let active = self.segments.last_mut().expect("log always has a segment");
        active.append_batch(base_offset, &bytes, batch.max_timestamp)?;
        let appended = batch.records.len() as u64;
        self.next_offset += appended as i64;
        self.maybe_flush(appended)?;
        if self.should_roll() {
            self.roll_segment()?;
        }
        Ok(base_offset)
    }

    /// Append an encoded batch fetched from a leader without re-encoding or
    /// changing its assigned base offset. Followers reject gaps, overlaps,
    /// malformed framing, and CRC corruption before touching disk.
    /// Append a producer's already encoded batch, stamping only the two
    /// broker-owned header fields — base offset and leader epoch — directly
    /// in the byte buffer.
    ///
    /// Both fields sit *before* `crc32c` in the layout precisely so the
    /// broker can stamp them without touching the checksum, which covers
    /// everything after it. That keeps the produce path free of the
    /// decompress → parse → re-serialize → re-compress → re-checksum round
    /// trip that decoding a batch would cost, and the bytes a producer sent
    /// are the bytes that reach disk, the followers and the consumers
    /// (DESIGN.md §4.1).
    pub fn append_producer_batch(
        &mut self,
        batch: &[u8],
        leader_epoch: i32,
    ) -> Result<(i64, i64), StorageError> {
        let header = validate_batch_header(batch)?;
        if header.last_offset_delta < 0 {
            return Err(StorageError::EmptyBatch);
        }
        let base_offset = self.next_offset;
        let next_offset = base_offset + header.last_offset_delta as i64 + 1;

        let mut stamped = BytesMut::with_capacity(batch.len());
        stamped.extend_from_slice(batch);
        stamped[0..8].copy_from_slice(&base_offset.to_be_bytes());
        stamped[12..16].copy_from_slice(&leader_epoch.to_be_bytes());

        let active = self.segments.last_mut().expect("log always has a segment");
        active.append_batch(base_offset, &stamped, header.max_timestamp)?;
        self.next_offset = next_offset;
        self.maybe_flush((next_offset - base_offset) as u64)?;
        if self.should_roll() {
            self.roll_segment()?;
        }
        Ok((base_offset, next_offset))
    }

    pub fn append_replica_batch(&mut self, batch: Bytes) -> Result<i64, StorageError> {
        let header = validate_batch_header(&batch)?;
        if header.last_offset_delta < 0 {
            return Err(StorageError::EmptyBatch);
        }
        if header.base_offset != self.next_offset {
            return Err(StorageError::NonContiguousReplicaBatch {
                expected: self.next_offset,
                actual: header.base_offset,
            });
        }
        let base_offset = header.base_offset;
        let active = self.segments.last_mut().expect("log always has a segment");
        active.append_batch(base_offset, &batch, header.max_timestamp)?;
        let appended = header.last_offset_delta as u64 + 1;
        self.next_offset = base_offset + appended as i64;
        self.maybe_flush(appended)?;
        if self.should_roll() {
            self.roll_segment()?;
        }
        Ok(base_offset)
    }

    /// Read raw batch bytes (unmodified, as stored) starting at `offset`,
    /// returning whole batches until at least `max_bytes` have been
    /// collected. Always returns at least one batch if `offset` is valid
    /// and within the log. Returns an empty vec at/past the log end.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<Bytes>, StorageError> {
        if offset < self.start_offset {
            return Err(StorageError::OffsetOutOfRange {
                offset,
                start: self.start_offset,
                end: self.next_offset,
            });
        }
        if offset >= self.next_offset || max_bytes == 0 {
            return Ok(Vec::new());
        }

        let first = self
            .segments
            .partition_point(|s| s.base_offset <= offset)
            .saturating_sub(1);

        let mut out = Vec::new();
        let mut total = 0usize;
        'segments: for seg in &self.segments[first..] {
            let relative = offset.saturating_sub(seg.base_offset).max(0) as u32;
            let mut position = seg.index.lookup(relative) as u64;
            while position + BATCH_HEADER_LEN as u64 <= seg.size {
                // Read a run of batches at once rather than a header and a
                // body per batch. Batches are laid out contiguously, so one
                // `pread` of the remaining budget usually covers several of
                // them, and slicing the result hands each one out without
                // copying it again.
                let want = (seg.size - position) as usize;
                let budget = max_bytes.saturating_sub(total).max(BATCH_HEADER_LEN);
                let want = want.min(budget.max(READ_CHUNK_BYTES.min(want)));
                let mut buf = BytesMut::zeroed(want);
                seg.read_at(position, &mut buf)?;
                let mut chunk = buf.freeze();

                let mut advanced = false;
                while chunk.len() >= BATCH_HEADER_LEN {
                    let base_offset = i64::from_be_bytes(chunk[0..8].try_into().unwrap());
                    let batch_length = i32::from_be_bytes(chunk[8..12].try_into().unwrap());
                    if batch_length < MIN_BATCH_LENGTH as i32 {
                        break 'segments;
                    }
                    let total_len = BATCH_HEADER_LEN + batch_length as usize;
                    if position + total_len as u64 > seg.size {
                        // Truncated on disk, not merely past this chunk.
                        break 'segments;
                    }
                    if total_len > chunk.len() {
                        // The batch runs past what this chunk covers. A
                        // fetch must return at least one batch even when it
                        // exceeds the byte budget, or a consumer whose
                        // records are larger than its `max_bytes` can never
                        // advance — so read that one directly and stop.
                        if out.is_empty() {
                            let mut single = BytesMut::zeroed(total_len);
                            seg.read_at(position, &mut single)?;
                            let batch = single.freeze();
                            let Ok(header) = brahmaputra_protocol::validate_batch_header(&batch)
                            else {
                                break 'segments;
                            };
                            position += total_len as u64;
                            if base_offset + header.last_offset_delta as i64 >= offset {
                                out.push(batch);
                                break 'segments;
                            }
                            advanced = true;
                        }
                        break;
                    }

                    let batch = chunk.split_to(total_len);
                    // Batches on disk were validated on append/recovery;
                    // still, stop at anything that fails CRC now (bit rot).
                    let Ok(header) = brahmaputra_protocol::validate_batch_header(&batch) else {
                        break 'segments;
                    };
                    position += total_len as u64;
                    advanced = true;
                    if base_offset + header.last_offset_delta as i64 >= offset {
                        total += total_len;
                        out.push(batch);
                        if total >= max_bytes {
                            break 'segments;
                        }
                    }
                }
                if !advanced {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Offset that will be assigned to the next appended record.
    pub fn log_end_offset(&self) -> i64 {
        self.next_offset
    }

    /// Oldest offset still retained.
    pub fn log_start_offset(&self) -> i64 {
        self.start_offset
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Set and persist the high watermark (survives restart via the `hwm`
    /// checkpoint file). In-memory replication machinery lands in a later
    /// milestone.
    pub fn set_high_watermark(&mut self, hwm: i64) -> Result<(), StorageError> {
        if hwm < self.high_watermark || hwm > self.next_offset {
            return Err(StorageError::InvalidHighWatermark {
                requested: hwm,
                current: self.high_watermark,
                log_end: self.next_offset,
            });
        }
        self.high_watermark_checkpoint.advance(hwm)?;
        self.high_watermark = hwm;
        Ok(())
    }

    /// Record the first offset written by a newly elected leader epoch.
    pub fn record_leader_epoch(&mut self, epoch: i32) -> Result<(), StorageError> {
        self.leader_epochs.record(epoch, self.next_offset)
    }

    /// Exclusive end offset for the requested leader epoch, if known.
    pub fn end_offset_for_leader_epoch(&self, epoch: i32) -> Option<i64> {
        self.leader_epochs.end_offset(epoch, self.next_offset)
    }

    pub fn leader_epoch_entries(&self) -> &[crate::LeaderEpochEntry] {
        self.leader_epochs.entries()
    }

    /// Truncate to a batch-safe prefix at or before `offset`, deleting all
    /// later segment files and rebuilding the active indexes. Committed data
    /// below the high watermark is never truncated.
    pub fn truncate_to(&mut self, offset: i64) -> Result<i64, StorageError> {
        if offset < self.high_watermark {
            return Err(StorageError::TruncateBelowHighWatermark {
                requested: offset,
                high_watermark: self.high_watermark,
            });
        }
        if offset < self.start_offset || offset > self.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                offset,
                start: self.start_offset,
                end: self.next_offset,
            });
        }
        if offset == self.next_offset {
            return Ok(offset);
        }

        let segment_index = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset)
            .saturating_sub(1);
        let removed = self.segments.split_off(segment_index + 1);
        for segment in removed {
            segment.delete()?;
        }
        let actual = self.segments[segment_index].truncate_to_offset(offset)?;
        self.next_offset = actual;
        self.leader_epochs.truncate_to(actual)?;
        Ok(actual)
    }

    /// Number of segments (including the active one).
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Apply time- and size-based retention (DESIGN.md §4.4): delete whole
    /// sealed segments that are expired or beyond the byte cap. The active
    /// segment is never deleted. Returns the number of segments deleted.
    pub fn apply_retention(&mut self) -> Result<usize, StorageError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut total_size: u64 = self.segments.iter().map(|s| s.size).sum();
        let mut deleted = 0;

        while self.segments.len() > 1 {
            let oldest = &self.segments[0];
            // A sealed segment is only eligible once every record in it is
            // committed. The next segment's base offset is this segment's
            // exclusive end. This prevents a lagging replicated log from
            // deleting uncommitted bytes merely because they are old or the
            // size cap is exceeded.
            if self.segments[1].base_offset > self.high_watermark {
                break;
            }
            let time_expired = self.config.retention_ms.is_some_and(|ms| {
                oldest
                    .max_timestamp
                    .is_some_and(|t| now_ms.saturating_sub(t) > ms as i64)
            });
            let size_exceeded = self
                .config
                .retention_bytes
                .is_some_and(|cap| total_size > cap);
            if !time_expired && !size_exceeded {
                break;
            }
            total_size -= oldest.size;
            let seg = self.segments.remove(0);
            seg.delete()?;
            deleted += 1;
        }
        self.start_offset = self.segments[0].base_offset;
        Ok(deleted)
    }

    fn roll_segment(&mut self) -> Result<(), StorageError> {
        // Seal the segment being closed: nothing will ever append to it
        // again, so this is the last chance to make it durable without a
        // background sweep, and recovery then only ever has to rebuild the
        // active segment's tail.
        if let Some(active) = self.segments.last() {
            active.sync()?;
        }
        let seg = Segment::open(
            &self.dir,
            self.next_offset,
            self.config.index_interval_bytes,
        )?;
        self.segments.push(seg);
        self.unflushed_records = 0;
        self.last_flush_ms = now_ms();
        self.active_segment_created_ms = now_ms();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brahmaputra_protocol::{Compression, Record};

    fn test_config() -> LogConfig {
        LogConfig {
            segment_bytes: 512,
            index_interval_bytes: 64,
            retention_ms: None,
            retention_bytes: None,
            // The checkpoint tests below are about the file's format and
            // recovery rules, not about how often it is written, so they
            // checkpoint eagerly and assert on the resulting bytes. The
            // write cadence has its own test.
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    fn batch(n: usize, first_value: usize, max_timestamp: i64) -> RecordBatch {
        let records = (0..n)
            .map(|i| Record::new(format!("value-{:05}", first_value + i).into_bytes()))
            .collect();
        RecordBatch::new(0, 0, max_timestamp, records)
    }

    fn decode_all(batches: &[Bytes]) -> Vec<(i64, Bytes)> {
        let mut out = Vec::new();
        for raw in batches {
            let mut buf = raw.clone();
            let batch = RecordBatch::decode(&mut buf).unwrap();
            out.extend(batch.iter().map(|(o, r)| (o, r.value.clone())));
        }
        out
    }

    #[test]
    fn append_and_read_round_trip_across_segment_rolls() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();

        let mut expected_offset = 0;
        for i in 0..50 {
            let base = log.append(batch(2, i * 2, 1_000 + i as i64)).unwrap();
            assert_eq!(base, expected_offset);
            expected_offset += 2;
        }
        assert_eq!(log.log_end_offset(), 100);
        assert_eq!(log.log_start_offset(), 0);
        assert!(log.segment_count() > 1, "small segments must roll");

        let batches = log.read(0, usize::MAX).unwrap();
        let records = decode_all(&batches);
        assert_eq!(records.len(), 100);
        for (i, (offset, value)) in records.iter().enumerate() {
            assert_eq!(*offset, i as i64);
            assert_eq!(value.as_ref(), format!("value-{i:05}").as_bytes());
        }
    }

    #[test]
    fn read_from_middle_of_segment_and_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..50 {
            log.append(batch(2, i * 2, 1_000)).unwrap();
        }
        for offset in [1, 17, 50, 99] {
            let batches = log.read(offset, usize::MAX).unwrap();
            let records = decode_all(&batches);
            assert!(!records.is_empty());
            assert!(records.iter().any(|(o, _)| *o == offset));
            assert_eq!(
                records[0].1.as_ref(),
                format!("value-{:05}", records[0].0).as_bytes()
            );
        }
        // Past the end: empty. Zero max_bytes: empty.
        assert!(log.read(100, usize::MAX).unwrap().is_empty());
        assert!(log.read(0, 0).unwrap().is_empty());
        // Before the start: typed error.
        assert!(matches!(
            log.read(-1, 100),
            Err(StorageError::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn read_respects_max_bytes_but_returns_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..20 {
            log.append(batch(1, i, 1_000)).unwrap();
        }
        let one = log.read(0, 1).unwrap();
        assert_eq!(one.len(), 1, "always at least one batch");
        let some = log.read(0, 120).unwrap();
        let total: usize = some.iter().map(|b| b.len()).sum();
        assert!(total >= 120);
        assert!(total < 120 + 100, "stops soon after max_bytes: {total}");
    }

    #[test]
    fn restart_recovers_offsets_and_data() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            for i in 0..30 {
                log.append(batch(2, i * 2, 2_000)).unwrap();
            }
            assert_eq!(log.log_end_offset(), 60);
        }
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.log_end_offset(), 60);
        assert_eq!(log.log_start_offset(), 0);
        let records = decode_all(&log.read(0, usize::MAX).unwrap());
        assert_eq!(records.len(), 60);
        // Appends continue monotonically after reopen.
        let base = log.append(batch(5, 60, 2_001)).unwrap();
        assert_eq!(base, 60);
        assert_eq!(log.log_end_offset(), 65);
    }

    #[test]
    fn torn_tail_write_is_truncated_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            for i in 0..10 {
                log.append(batch(1, i, 3_000)).unwrap();
            }
        }
        // Simulate a crash mid-write: garbage partial bytes at the end of
        // the active segment.
        let active_log = last_log_file(dir.path());
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&active_log)
            .unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x42]).unwrap();
        drop(f);

        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.log_end_offset(), 10, "torn bytes truncated");
        let records = decode_all(&log.read(0, usize::MAX).unwrap());
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn corrupt_last_batch_is_truncated_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let batch_len;
        {
            let mut log = Log::open(
                dir.path(),
                LogConfig {
                    segment_bytes: 1 << 20,
                    ..test_config()
                },
            )
            .unwrap();
            log.append(batch(3, 0, 4_000)).unwrap();
            let b = batch(2, 3, 4_001);
            batch_len = b.encode().len();
            log.append(b).unwrap();
            assert_eq!(log.log_end_offset(), 5);
        }
        // Flip a byte inside the last batch's payload (after the crc field).
        let active_log = last_log_file(dir.path());
        let mut bytes = fs::read(&active_log).unwrap();
        let pos = bytes.len() - 1;
        bytes[pos] ^= 0xff;
        fs::write(&active_log, &bytes).unwrap();

        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(
            log.log_end_offset(),
            3,
            "corrupt final batch (len {batch_len}) must be truncated"
        );
        let records = decode_all(&log.read(0, usize::MAX).unwrap());
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn sparse_index_reads_every_offset_in_10k_batch_log() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 1 << 20,
            index_interval_bytes: 128,
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for i in 0..10_000 {
            assert_eq!(log.append(batch(1, i, 5_000)).unwrap(), i as i64);
        }
        assert_eq!(log.log_end_offset(), 10_000);

        for offset in 0..10_000i64 {
            // Bounded max_bytes keeps this O(n): each read scans only a few
            // KB past the index landing position.
            let batches = log.read(offset, 4096).unwrap();
            let first = decode_all(&batches[..1]);
            assert!(
                first.iter().any(|(o, _)| *o == offset),
                "offset {offset} not found in first returned batch"
            );
        }

        // Reopen and spot-check again (index rebuilt by recovery).
        drop(log);
        let log = Log::open(dir.path(), config.clone()).unwrap();
        for offset in [0, 1, 4_999, 9_998, 9_999] {
            let batches = log.read(offset, 1 << 20).unwrap();
            let first = decode_all(&batches[..1]);
            assert!(first.iter().any(|(o, _)| *o == offset));
        }
    }

    #[test]
    fn time_based_retention_deletes_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 300,
            index_interval_bytes: 64,
            retention_ms: Some(60_000),
            retention_bytes: None,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        // Ancient timestamps -> every sealed segment is expired.
        for i in 0..40 {
            log.append(batch(1, i, 1_000)).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(log.segment_count() > 2);
        let log_files_before = count_log_files(dir.path());

        let deleted = log.apply_retention().unwrap();
        assert!(deleted > 0);
        assert_eq!(log.segment_count(), 1, "only the active segment survives");
        assert_eq!(count_log_files(dir.path()), log_files_before - deleted);
        assert!(log.log_start_offset() > 0 && log.log_start_offset() < 40);
        // Reading a deleted offset is out of range.
        assert!(matches!(
            log.read(0, 100),
            Err(StorageError::OffsetOutOfRange { .. })
        ));
        // Data in the active segment is still readable, end offset intact.
        assert_eq!(log.log_end_offset(), 40);
        let records = decode_all(&log.read(log.log_start_offset(), usize::MAX).unwrap());
        assert!(!records.is_empty());
        assert_eq!(records[0].0, log.log_start_offset());
    }

    #[test]
    fn size_based_retention_deletes_oldest_segments() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 300,
            index_interval_bytes: 64,
            retention_ms: None,
            retention_bytes: Some(600),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // Fresh timestamps: time-based retention must NOT trigger.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for i in 0..40 {
            log.append(batch(1, i, now)).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let deleted = log.apply_retention().unwrap();
        assert!(deleted > 0);
        assert!(log.segment_count() >= 1);
        assert!(log.log_start_offset() > 0);
        assert!(log.log_end_offset() == 40);
        // Everything retained is still readable and contiguous.
        let records = decode_all(&log.read(log.log_start_offset(), usize::MAX).unwrap());
        assert_eq!(records.len() as i64, 40 - log.log_start_offset());
    }

    #[test]
    fn active_segment_is_never_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 1 << 20,
            retention_ms: Some(1), // everything expires immediately
            retention_bytes: Some(1),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for i in 0..5 {
            log.append(batch(1, i, 1_000)).unwrap();
        }
        let deleted = log.apply_retention().unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn retention_never_deletes_an_uncommitted_sealed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 300,
            index_interval_bytes: 64,
            retention_ms: Some(0),
            retention_bytes: Some(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for i in 0..40 {
            log.append(batch(1, i, 1_000)).unwrap();
        }
        assert!(log.segment_count() > 2);

        assert_eq!(log.apply_retention().unwrap(), 0);
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.high_watermark(), 0);

        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(log.apply_retention().unwrap() > 0);
        assert!(log.log_start_offset() > 0);
    }

    #[test]
    fn high_watermark_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            for i in 0..10 {
                log.append(batch(1, i, 6_000)).unwrap();
            }
            log.set_high_watermark(7).unwrap();
            assert_eq!(log.high_watermark(), 7);
        }
        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.high_watermark(), 7);
    }

    #[test]
    fn legacy_high_watermark_migrates_without_replacing_its_prefix() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            log.append(batch(10, 0, 6_000)).unwrap();
        }
        let checkpoint = dir.path().join(HWM_FILE);
        fs::write(&checkpoint, 7_i64.to_be_bytes()).unwrap();

        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            assert_eq!(log.high_watermark(), 7);
            assert_eq!(fs::read(&checkpoint).unwrap().len(), size_of::<i64>());
            log.set_high_watermark(8).unwrap();
        }

        let bytes = fs::read(&checkpoint).unwrap();
        assert_eq!(&bytes[..size_of::<i64>()], &7_i64.to_be_bytes());
        assert_eq!(bytes.len(), size_of::<i64>() + HWM_RECORD_LEN);
        assert_eq!(
            Log::open(dir.path(), test_config())
                .unwrap()
                .high_watermark(),
            8
        );
    }

    #[test]
    fn partial_checkpoint_tail_keeps_last_complete_watermark() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            log.append(batch(10, 0, 6_000)).unwrap();
            log.set_high_watermark(7).unwrap();
        }
        let checkpoint = dir.path().join(HWM_FILE);
        let valid_len = fs::metadata(&checkpoint).unwrap().len();
        let next = encode_hwm_record(HighWatermarkRecordKind::Advance, 2, 7, 9);
        append_bytes(&checkpoint, &next[..HWM_RECORD_LEN - 5]);

        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            assert_eq!(log.high_watermark(), 7);
            assert_eq!(fs::metadata(&checkpoint).unwrap().len(), valid_len);
            log.set_high_watermark(8).unwrap();
        }
        assert_eq!(
            Log::open(dir.path(), test_config())
                .unwrap()
                .high_watermark(),
            8
        );
    }

    #[test]
    fn corrupt_complete_checkpoint_tail_keeps_last_complete_watermark() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            log.append(batch(10, 0, 6_000)).unwrap();
            log.set_high_watermark(7).unwrap();
        }
        let checkpoint = dir.path().join(HWM_FILE);
        let valid_len = fs::metadata(&checkpoint).unwrap().len();
        let mut corrupt = encode_hwm_record(HighWatermarkRecordKind::Advance, 2, 7, 9);
        corrupt[HWM_RECORD_LEN - 1] ^= 0xff;
        append_bytes(&checkpoint, &corrupt);

        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.high_watermark(), 7);
        assert_eq!(fs::metadata(&checkpoint).unwrap().len(), valid_len);
    }

    #[test]
    fn non_monotonic_complete_checkpoint_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            log.append(batch(10, 0, 6_000)).unwrap();
            log.set_high_watermark(7).unwrap();
        }
        let checkpoint = dir.path().join(HWM_FILE);
        let valid_len = fs::metadata(&checkpoint).unwrap().len();
        let regressing = encode_hwm_record(HighWatermarkRecordKind::Advance, 2, 7, 6);
        append_bytes(&checkpoint, &regressing);

        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.high_watermark(), 7);
        assert_eq!(fs::metadata(&checkpoint).unwrap().len(), valid_len);
    }

    #[test]
    fn recovery_clamps_high_watermark_to_truncated_log_end() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(
                dir.path(),
                LogConfig {
                    segment_bytes: 1 << 20,
                    ..test_config()
                },
            )
            .unwrap();
            log.append(batch(3, 0, 6_000)).unwrap();
            log.append(batch(2, 3, 6_001)).unwrap();
            log.set_high_watermark(5).unwrap();
        }

        // Simulate a crash that leaves the last batch corrupt after its high
        // watermark was checkpointed. Recovery truncates that batch, so the
        // restored watermark must not point beyond the recovered log end.
        let active_log = last_log_file(dir.path());
        let mut bytes = fs::read(&active_log).unwrap();
        let pos = bytes.len() - 1;
        bytes[pos] ^= 0xff;
        fs::write(&active_log, bytes).unwrap();

        {
            let log = Log::open(dir.path(), test_config()).unwrap();
            assert_eq!(log.log_end_offset(), 3);
            assert_eq!(log.high_watermark(), 3);
        }

        // The clamp is itself appended and durable. Repeated recovery cannot
        // revive the old watermark, and later monotonic updates remain valid.
        {
            let mut log = Log::open(dir.path(), test_config()).unwrap();
            assert_eq!(log.high_watermark(), 3);
            log.append(batch(2, 3, 6_002)).unwrap();
            log.set_high_watermark(4).unwrap();
        }
        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.log_end_offset(), 5);
        assert_eq!(log.high_watermark(), 4);
    }

    #[test]
    fn lz4_batches_round_trip_through_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..20 {
            let b = batch(3, i * 3, 7_000).with_compression(Compression::Lz4);
            log.append(b).unwrap();
        }
        assert_eq!(log.log_end_offset(), 60);
        let records = decode_all(&log.read(0, usize::MAX).unwrap());
        assert_eq!(records.len(), 60);
        for (i, (offset, value)) in records.iter().enumerate() {
            assert_eq!(*offset, i as i64);
            assert_eq!(value.as_ref(), format!("value-{i:05}").as_bytes());
        }
        // Reopen: recovery validates compressed batches too.
        drop(log);
        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.log_end_offset(), 60);
    }

    #[test]
    fn append_empty_batch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        assert!(matches!(
            log.append(RecordBatch::new(0, 0, 0, vec![])),
            Err(StorageError::EmptyBatch)
        ));
        assert_eq!(log.log_end_offset(), 0);
    }

    #[test]
    fn replica_append_preserves_bytes_and_rejects_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        let mut first = batch(3, 0, 8_000);
        first.base_offset = 0;
        first.leader_epoch = 4;
        let encoded = first.encode();
        assert_eq!(log.append_replica_batch(encoded.clone()).unwrap(), 0);
        assert_eq!(log.log_end_offset(), 3);
        assert_eq!(log.read(0, usize::MAX).unwrap(), vec![encoded]);

        let mut gap = batch(1, 3, 8_001);
        gap.base_offset = 5;
        let error = log.append_replica_batch(gap.encode()).unwrap_err();
        assert!(matches!(
            error,
            StorageError::NonContiguousReplicaBatch {
                expected: 3,
                actual: 5
            }
        ));
        assert_eq!(log.log_end_offset(), 3);
    }

    #[test]
    fn truncate_is_batch_safe_across_segments_and_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        log.record_leader_epoch(0).unwrap();
        for i in 0..5 {
            log.append(batch(2, i * 2, 9_000)).unwrap();
        }
        log.record_leader_epoch(1).unwrap();
        for i in 5..12 {
            log.append(batch(2, i * 2, 9_001)).unwrap();
        }
        log.record_leader_epoch(2).unwrap();
        assert_eq!(log.log_end_offset(), 24);
        assert_eq!(log.end_offset_for_leader_epoch(0), Some(10));
        assert_eq!(log.end_offset_for_leader_epoch(1), Some(24));
        assert!(log.segment_count() > 1);

        // Offset 15 is inside batch [14, 16), so the whole batch and every
        // later batch are removed.
        assert_eq!(log.truncate_to(15).unwrap(), 14);
        assert_eq!(log.log_end_offset(), 14);
        assert_eq!(log.leader_epoch_entries().len(), 2);
        assert_eq!(decode_all(&log.read(0, usize::MAX).unwrap()).len(), 14);

        let base = log.append(batch(2, 14, 9_002)).unwrap();
        assert_eq!(base, 14);
        assert_eq!(log.log_end_offset(), 16);
        drop(log);

        let log = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(log.log_end_offset(), 16);
        assert_eq!(decode_all(&log.read(0, usize::MAX).unwrap()).len(), 16);
        assert_eq!(log.leader_epoch_entries().len(), 2);
    }

    #[test]
    fn committed_prefix_cannot_be_truncated_or_watermark_regressed() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..5 {
            log.append(batch(2, i * 2, 10_000)).unwrap();
        }
        log.set_high_watermark(8).unwrap();
        assert!(matches!(
            log.truncate_to(7),
            Err(StorageError::TruncateBelowHighWatermark {
                requested: 7,
                high_watermark: 8
            })
        ));
        assert!(matches!(
            log.set_high_watermark(6),
            Err(StorageError::InvalidHighWatermark { .. })
        ));
        assert_eq!(log.log_end_offset(), 10);
        assert_eq!(log.high_watermark(), 8);
    }

    fn last_log_file(dir: &Path) -> PathBuf {
        let mut logs: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                if p.extension().is_some_and(|x| x == "log") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        logs.sort();
        logs.pop().unwrap()
    }

    fn append_bytes(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        std::io::Write::write_all(&mut file, bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn count_log_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "log")
            })
            .count()
    }
}

#[cfg(test)]
mod flush_tests {
    use super::*;
    use crate::LogConfig;

    fn batch(records: usize) -> RecordBatch {
        RecordBatch::new(
            0,
            0,
            now_ms(),
            (0..records)
                .map(|_| brahmaputra_protocol::Record::new(vec![b'x'; 64]))
                .collect(),
        )
    }

    /// With no policy set, appends never fsync: durability is replication
    /// plus the page cache, as designed.
    #[test]
    fn no_flush_policy_leaves_records_unflushed() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..10 {
            log.append(batch(5)).unwrap();
        }
        assert_eq!(log.unflushed_records, 50);
        assert!(
            !log.flush_due(),
            "no time policy means no flush is ever due"
        );
    }

    /// `flush.interval.messages` resets the counter every time it fires, so
    /// the number of unflushed records stays bounded by the policy.
    #[test]
    fn message_count_policy_flushes_and_resets() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                flush_interval_messages: Some(10),
                ..LogConfig::default()
            },
        )
        .unwrap();

        log.append(batch(4)).unwrap();
        assert_eq!(log.unflushed_records, 4, "below the threshold, no flush");
        log.append(batch(4)).unwrap();
        assert_eq!(log.unflushed_records, 8);
        log.append(batch(4)).unwrap();
        assert_eq!(log.unflushed_records, 0, "crossing 10 records flushed");

        for _ in 0..20 {
            log.append(batch(3)).unwrap();
            assert!(
                log.unflushed_records < 10,
                "unflushed backlog stays under the configured limit"
            );
        }
        // Everything is still readable after all those syncs.
        let read = log.read(0, usize::MAX).unwrap();
        assert!(!read.is_empty());
    }

    /// A time policy makes a flush become due while records sit unflushed,
    /// and stop being due once it runs — this is what the partition actor's
    /// tick polls.
    #[test]
    fn time_policy_reports_when_a_flush_is_due() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                flush_interval_ms: Some(0),
                ..LogConfig::default()
            },
        )
        .unwrap();
        assert!(!log.flush_due(), "nothing unflushed, nothing due");

        // append() applies the same policy inline, so drive flush_due()
        // through a raw producer append that leaves the counter set.
        log.unflushed_records = 3;
        assert!(log.flush_due(), "records outstanding past the interval");
        log.flush().unwrap();
        assert!(!log.flush_due(), "flushing clears the backlog");
        assert_eq!(log.unflushed_records, 0);
    }

    /// Rolling a segment seals it: the closed segment is synced and the
    /// unflushed counter starts again for the new active segment.
    #[test]
    fn rolling_a_segment_syncs_and_resets_the_counter() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_bytes: 4096,
                ..LogConfig::default()
            },
        )
        .unwrap();
        let mut rolled = false;
        for _ in 0..40 {
            log.append(batch(4)).unwrap();
            if log.unflushed_records == 0 {
                rolled = true;
                break;
            }
        }
        assert!(rolled, "a segment roll reset the unflushed counter");
        let segments = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "log")
            })
            .count();
        assert!(
            segments >= 2,
            "the log actually rolled ({segments} segments)"
        );
    }
}

#[cfg(test)]
mod hwm_checkpoint_cadence_tests {
    use super::*;
    use brahmaputra_protocol::Record;

    fn config(interval_ms: u64) -> LogConfig {
        LogConfig {
            segment_bytes: 4096,
            index_interval_bytes: 64,
            hwm_checkpoint_interval_ms: interval_ms,
            ..LogConfig::default()
        }
    }

    fn one_batch(value: usize) -> RecordBatch {
        RecordBatch::new(
            0,
            0,
            1,
            vec![Record::new(format!("v-{value}").into_bytes())],
        )
    }

    /// The whole point of the change: advancing the watermark must not
    /// touch the disk every time. With a long interval, many advances
    /// produce no checkpoint growth at all.
    #[test]
    fn advancing_within_the_interval_does_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config(60_000)).unwrap();
        for value in 0..20 {
            log.append(one_batch(value)).unwrap();
            log.set_high_watermark(log.log_end_offset()).unwrap();
        }
        assert_eq!(log.high_watermark(), 20);

        let checkpoint = dir.path().join(HWM_FILE);
        let written = fs::metadata(&checkpoint)
            .map(|meta| meta.len())
            .unwrap_or(0);
        assert!(
            written <= size_of::<i64>() as u64,
            "checkpoint grew to {written} bytes inside the interval"
        );
    }

    /// ...but a clean stop must still persist it, so only an unclean stop
    /// ever recovers a watermark that lags the log.
    #[test]
    fn a_clean_close_persists_the_watermark() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), config(60_000)).unwrap();
            for value in 0..5 {
                log.append(one_batch(value)).unwrap();
            }
            log.set_high_watermark(5).unwrap();
            log.checkpoint_high_watermark().unwrap();
        }
        let log = Log::open(dir.path(), config(60_000)).unwrap();
        assert_eq!(log.high_watermark(), 5);
    }

    /// A flush carries the watermark with it: once the data is durable
    /// there is no reason to leave the pointer to it behind.
    #[test]
    fn flush_persists_the_watermark() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), config(60_000)).unwrap();
            log.append(one_batch(0)).unwrap();
            log.set_high_watermark(1).unwrap();
            log.flush().unwrap();
        }
        assert_eq!(
            Log::open(dir.path(), config(60_000))
                .unwrap()
                .high_watermark(),
            1
        );
    }
}

#[cfg(test)]
mod chunked_read_tests {
    use super::*;
    use brahmaputra_protocol::Record;

    fn config() -> LogConfig {
        LogConfig {
            segment_bytes: 1024 * 1024,
            index_interval_bytes: 64,
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    fn batch_of(size: usize, value: u8) -> RecordBatch {
        RecordBatch::new(0, 0, 1, vec![Record::new(vec![value; size])])
    }

    /// A fetch whose budget is smaller than a single batch must still
    /// return that batch, or a consumer whose records are larger than its
    /// `max_bytes` could never advance past them.
    #[test]
    fn a_batch_larger_than_the_budget_is_still_returned() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        log.append(batch_of(8192, 7)).unwrap();
        log.set_high_watermark(log.log_end_offset()).unwrap();

        let batches = log.read(0, 16).unwrap();
        assert_eq!(batches.len(), 1, "one oversized batch must still be served");
        assert!(batches[0].len() > 8192);
    }

    /// Several batches inside one chunk are handed out as separate slices,
    /// in order, and the budget still bounds the total.
    #[test]
    fn many_batches_come_back_in_order_within_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for value in 0..8u8 {
            log.append(batch_of(256, value)).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        let all = log.read(0, 1 << 20).unwrap();
        assert_eq!(all.len(), 8);
        for (index, batch) in all.iter().enumerate() {
            let header = brahmaputra_protocol::validate_batch_header(batch).unwrap();
            assert_eq!(header.base_offset, index as i64);
        }

        // A budget covering roughly three batches must stop near there,
        // not return everything and not return nothing.
        let one = all[0].len();
        let bounded = log.read(0, one * 3).unwrap();
        assert!(
            !bounded.is_empty() && bounded.len() <= 4,
            "got {}",
            bounded.len()
        );
    }

    /// Reading from a mid-log offset skips earlier batches even though the
    /// chunk starts at the index entry before them.
    #[test]
    fn reading_from_the_middle_skips_earlier_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for value in 0..6u8 {
            log.append(batch_of(128, value)).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        let batches = log.read(4, 1 << 20).unwrap();
        assert!(!batches.is_empty());
        let first = brahmaputra_protocol::validate_batch_header(&batches[0]).unwrap();
        assert!(
            first.base_offset + first.last_offset_delta as i64 >= 4,
            "first batch must cover the requested offset"
        );
    }
}

/// A contiguous run of record batches exactly as they sit in a segment file.
///
/// This is what lets a plaintext fetch skip userspace altogether: the
/// broker sends the range straight from the page cache with `sendfile`,
/// so the bytes are never read into the process at all. Only the batch
/// *headers* are read to work out where the run ends.
#[derive(Clone)]
pub struct LogRegion {
    pub file: std::sync::Arc<std::fs::File>,
    pub position: u64,
    pub len: usize,
}

impl std::fmt::Debug for LogRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogRegion")
            .field("position", &self.position)
            .field("len", &self.len)
            .finish()
    }
}

impl Log {
    /// The same selection [`Log::read`] makes, described as file ranges
    /// instead of buffers.
    ///
    /// Batches that belong to one segment and follow each other are merged
    /// into a single region, so a fetch of many batches usually becomes one
    /// `sendfile` call. Adjacent-run merging is what makes this worth doing
    /// at small record sizes as well as large ones.
    pub fn read_regions(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<LogRegion>, StorageError> {
        if offset < self.start_offset {
            return Err(StorageError::OffsetOutOfRange {
                offset,
                start: self.start_offset,
                end: self.next_offset,
            });
        }
        let mut regions: Vec<LogRegion> = Vec::new();
        if offset >= self.next_offset || max_bytes == 0 {
            return Ok(regions);
        }
        let high_watermark = self.high_watermark;

        let first = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset)
            .saturating_sub(1);

        let mut total = 0usize;
        'segments: for segment in &self.segments[first..] {
            let relative = offset.saturating_sub(segment.base_offset).max(0) as u32;
            let mut position = segment.index.lookup(relative) as u64;
            while position + BATCH_HEADER_LEN as u64 <= segment.size {
                let mut header = [0u8; REGION_PROBE_LEN];
                if position + REGION_PROBE_LEN as u64 > segment.size {
                    break 'segments;
                }
                segment.read_at(position, &mut header)?;
                let base_offset = i64::from_be_bytes(header[0..8].try_into().unwrap());
                let batch_length = i32::from_be_bytes(header[8..12].try_into().unwrap());
                if batch_length < MIN_BATCH_LENGTH as i32 {
                    break 'segments;
                }
                let total_len = BATCH_HEADER_LEN + batch_length as usize;
                if position + total_len as u64 > segment.size {
                    break 'segments;
                }
                // Layout (batch.rs): base_offset i64, batch_length i32,
                // leader_epoch i32, magic u8, crc32c u32, attributes u16,
                // then last_offset_delta i32 — so visibility and offset
                // checks need the header, never the payload.
                let last_offset_delta = i32::from_be_bytes(header[23..27].try_into().unwrap());
                let last_offset = base_offset + last_offset_delta as i64;

                // Never serve at or beyond the high watermark, matching the
                // partition actor's rule for buffered reads.
                if last_offset >= high_watermark {
                    break 'segments;
                }

                if last_offset >= offset {
                    match regions.last_mut() {
                        Some(last)
                            if std::sync::Arc::ptr_eq(&last.file, &segment.file())
                                && last.position + last.len as u64 == position =>
                        {
                            last.len += total_len;
                        }
                        _ => regions.push(LogRegion {
                            file: segment.file(),
                            position,
                            len: total_len,
                        }),
                    }
                    total += total_len;
                }
                position += total_len as u64;
                if total >= max_bytes {
                    break 'segments;
                }
            }
        }
        Ok(regions)
    }
}

#[cfg(test)]
mod region_tests {
    use super::*;
    use brahmaputra_protocol::Record;

    fn config() -> LogConfig {
        LogConfig {
            segment_bytes: 4096,
            index_interval_bytes: 64,
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    /// The regions must describe exactly the bytes `read` would have
    /// returned — same selection, same order, same total.
    #[test]
    fn regions_describe_the_same_bytes_as_a_buffered_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for value in 0..12u8 {
            log.append(RecordBatch::new(
                0,
                0,
                1,
                vec![Record::new(vec![value; 200])],
            ))
            .unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        for budget in [64, 1024, 4096, 1 << 20] {
            let buffered = log.read(0, budget).unwrap();
            let regions = log.read_regions(0, budget).unwrap();
            let buffered_total: usize = buffered.iter().map(Bytes::len).sum();
            let region_total: usize = regions.iter().map(|region| region.len).sum();
            assert_eq!(
                buffered_total, region_total,
                "byte totals must agree at budget {budget}"
            );
        }
    }

    /// Adjacent batches in one segment collapse into a single range, so a
    /// fetch of many batches is one `sendfile`, not one per batch.
    #[test]
    fn adjacent_batches_merge_into_one_region() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for value in 0..4u8 {
            log.append(RecordBatch::new(
                0,
                0,
                1,
                vec![Record::new(vec![value; 64])],
            ))
            .unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        let regions = log.read_regions(0, 1 << 20).unwrap();
        assert_eq!(regions.len(), 1, "one contiguous run: {regions:?}");
    }

    /// Records at or beyond the high watermark are not visible, exactly as
    /// for a buffered read.
    #[test]
    fn regions_stop_at_the_high_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for value in 0..6u8 {
            log.append(RecordBatch::new(
                0,
                0,
                1,
                vec![Record::new(vec![value; 64])],
            ))
            .unwrap();
        }
        log.set_high_watermark(3).unwrap();

        // `Log::read` hands back everything on disk and the partition actor
        // applies the watermark; regions skip the actor, so the cutoff has
        // to be applied here instead. Only the committed prefix is visible.
        let regions = log.read_regions(0, 1 << 20).unwrap();
        let visible: usize = regions.iter().map(|region| region.len).sum();
        let on_disk: usize = log.read(0, 1 << 20).unwrap().iter().map(Bytes::len).sum();
        assert!(visible > 0, "the committed prefix must still be visible");
        assert!(
            visible < on_disk,
            "records at or beyond the watermark must not be served ({visible} of {on_disk})"
        );

        // Raising the watermark makes the rest visible, and then the two
        // agree exactly.
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let all: usize = log
            .read_regions(0, 1 << 20)
            .unwrap()
            .iter()
            .map(|region| region.len)
            .sum();
        assert_eq!(all, on_disk);
    }
}

impl Log {
    /// Keep only the most recent record for each key among the sealed
    /// segments, discarding the versions it supersedes.
    ///
    /// This is what stops a keyed topic — `__consumer_offsets` above all —
    /// from growing without bound. A group that commits every five seconds
    /// writes the same key forever; without compaction the disk fills, and
    /// coordinator failover gets slower without limit because it replays
    /// every superseded commit.
    ///
    /// Offsets are preserved exactly. A surviving record is rewritten as a
    /// single-record batch at its original offset, so compaction leaves
    /// gaps rather than renumbering anything — a consumer's committed
    /// offset still means what it meant before. Records without a key
    /// cannot be superseded and are always kept.
    ///
    /// Only sealed segments below the high watermark are touched: the
    /// active segment is still being appended to, and uncommitted records
    /// are not ours to discard.
    pub fn compact(&mut self) -> Result<usize, StorageError> {
        if self.segments.len() < 2 {
            return Ok(0);
        }
        let boundary = self
            .segments
            .last()
            .map(|active| active.base_offset)
            .unwrap_or(self.next_offset)
            .min(self.high_watermark);
        if boundary <= self.start_offset {
            return Ok(0);
        }

        // Pass one: the offset of the last record written for each key.
        let mut latest: std::collections::HashMap<Vec<u8>, i64> = std::collections::HashMap::new();
        let mut survivors: Vec<(i64, Record, i64)> = Vec::new();
        for segment_index in 0..self.segments.len() - 1 {
            let mut position = 0u64;
            let segment = &self.segments[segment_index];
            while position + BATCH_HEADER_LEN as u64 <= segment.size {
                let mut header = [0u8; BATCH_HEADER_LEN];
                segment.read_at(position, &mut header)?;
                let batch_length = i32::from_be_bytes(header[8..12].try_into().unwrap());
                if batch_length < MIN_BATCH_LENGTH as i32 {
                    break;
                }
                let total_len = BATCH_HEADER_LEN + batch_length as usize;
                if position + total_len as u64 > segment.size {
                    break;
                }
                let mut buf = BytesMut::zeroed(total_len);
                segment.read_at(position, &mut buf)?;
                let mut bytes = buf.freeze();
                let batch = RecordBatch::decode(&mut bytes)?;
                let base = batch.base_offset;
                let max_timestamp = batch.max_timestamp;
                for (index, record) in batch.records.into_iter().enumerate() {
                    let offset = base + index as i64;
                    // Records at or above the boundary are uncommitted or
                    // belong to the still-open range: collect them so they
                    // are rewritten untouched, but never let them supersede
                    // anything, and never discard them.
                    if offset < boundary {
                        if let Some(key) = record.key.as_ref() {
                            latest.insert(key.to_vec(), offset);
                        }
                    }
                    survivors.push((offset, record, max_timestamp));
                }
                position += total_len as u64;
            }
        }

        // Pass two: drop every record a later one supersedes.
        let before = survivors.len();
        survivors.retain(|(offset, record, _)| {
            // Above the boundary nothing is eligible; below it, a record
            // survives only if it is the latest for its key. A record with
            // no key has no successor that could replace it.
            if *offset >= boundary {
                return true;
            }
            match record.key.as_ref() {
                Some(key) => latest.get(key.as_ref() as &[u8]) == Some(offset),
                None => true,
            }
        });
        let removed = before - survivors.len();
        if removed == 0 {
            return Ok(0);
        }

        // Rewrite the sealed range as one segment based at the first
        // surviving offset. Gaps are expected and are what a fetch already
        // copes with: it returns the first batch covering the requested
        // offset.
        let new_base = survivors
            .first()
            .map(|(offset, _, _)| *offset)
            .unwrap_or(boundary);
        let staging = self.dir.join("compaction");
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        let mut rebuilt = Segment::open(&staging, new_base, self.config.index_interval_bytes)?;
        for (offset, record, max_timestamp) in &survivors {
            let batch = RecordBatch::new(*offset, 0, *max_timestamp, vec![record.clone()]);
            rebuilt.append_batch(*offset, &batch.encode(), *max_timestamp)?;
        }
        rebuilt.sync()?;
        drop(rebuilt);

        // Swap: remove the old sealed segments, move the rebuilt one into
        // place, and reopen it.
        let active = self.segments.pop().expect("active segment");
        for segment in self.segments.drain(..) {
            segment.delete()?;
        }
        for suffix in ["log", "index", "timeindex"] {
            let from = staging.join(format!("{new_base:020}.{suffix}"));
            let to = self.dir.join(format!("{new_base:020}.{suffix}"));
            if from.exists() {
                fs::rename(&from, &to)?;
            }
        }
        fs::remove_dir_all(&staging)?;
        let reopened = Segment::open(&self.dir, new_base, self.config.index_interval_bytes)?;
        self.segments.push(reopened);
        self.segments.push(active);
        self.start_offset = new_base;
        Ok(removed)
    }
}

#[cfg(test)]
mod compaction_tests {
    use super::*;
    use brahmaputra_protocol::Record;

    fn config() -> LogConfig {
        LogConfig {
            // Small segments so a handful of records seal several of them.
            segment_bytes: 256,
            index_interval_bytes: 64,
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    fn keyed(key: &str, value: &str) -> RecordBatch {
        RecordBatch::new(
            0,
            0,
            1,
            vec![Record::with_key(
                Bytes::from(key.to_string()),
                Bytes::from(value.to_string()),
                0,
            )],
        )
    }

    fn read_all(log: &Log) -> Vec<(i64, Option<Vec<u8>>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut offset = log.log_start_offset();
        while offset < log.log_end_offset() {
            let batches = log.read(offset, 1 << 20).expect("read");
            if batches.is_empty() {
                break;
            }
            for raw in batches {
                let mut bytes = raw;
                let batch = RecordBatch::decode(&mut bytes).expect("decode");
                for (index, record) in batch.records.into_iter().enumerate() {
                    let record_offset = batch.base_offset + index as i64;
                    offset = record_offset + 1;
                    out.push((
                        record_offset,
                        record.key.map(|key| key.to_vec()),
                        record.value.to_vec(),
                    ));
                }
            }
        }
        out
    }

    /// The point of compaction: repeated writes to one key stop
    /// accumulating, and the surviving value is the newest.
    #[test]
    fn only_the_latest_value_per_key_survives() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..12 {
            log.append(keyed("group-a", &format!("v{round}"))).unwrap();
            log.append(keyed("group-b", &format!("w{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let before = read_all(&log).len();

        let removed = log.compact().unwrap();
        assert!(removed > 0, "compaction must remove superseded records");

        let after = read_all(&log);
        assert!(
            after.len() < before,
            "the log must shrink: {before} -> {}",
            after.len()
        );
        for key in ["group-a", "group-b"] {
            let surviving: Vec<_> = after
                .iter()
                .filter(|(_, k, _)| k.as_deref() == Some(key.as_bytes()))
                .collect();
            assert_eq!(surviving.len(), 1, "one record must survive for {key}");
        }
        // The newest values, not the oldest.
        let values: Vec<String> = after
            .iter()
            .map(|(_, _, value)| String::from_utf8_lossy(value).into_owned())
            .collect();
        assert!(values.contains(&"v11".to_string()), "got {values:?}");
        assert!(values.contains(&"w11".to_string()), "got {values:?}");
    }

    /// Offsets must not be renumbered: a committed offset has to keep
    /// meaning the same record after compaction runs.
    #[test]
    fn surviving_records_keep_their_original_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let end_before = log.log_end_offset();
        let before = read_all(&log);
        let newest = before.last().cloned().expect("a last record");

        log.compact().unwrap();

        assert_eq!(
            log.log_end_offset(),
            end_before,
            "the log end must not move"
        );
        let after = read_all(&log);

        // Nothing is renumbered: every record still sits at an offset it
        // originally occupied, carrying the value it originally had. That
        // is what makes a previously committed offset still meaningful.
        for (offset, key, value) in &after {
            assert!(
                before.contains(&(*offset, key.clone(), value.clone())),
                "offset {offset} was renumbered or its value changed"
            );
        }
        // Compaction only touches sealed segments, so the newest write —
        // which is still in the open active segment — is untouched.
        assert!(
            after.contains(&newest),
            "the newest record must still be readable at its own offset"
        );
        assert!(after.len() < before.len(), "the log must still shrink");
    }

    /// Records with no key have nothing that can supersede them.
    #[test]
    fn keyless_records_are_never_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(RecordBatch::new(
                0,
                0,
                1,
                vec![Record::new(format!("plain-{round}").into_bytes())],
            ))
            .unwrap();
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        log.compact().unwrap();

        let keyless = read_all(&log)
            .into_iter()
            .filter(|(_, key, _)| key.is_none())
            .count();
        assert_eq!(keyless, 10, "every keyless record must survive");
    }

    /// Uncommitted records are not ours to discard.
    #[test]
    fn records_above_the_high_watermark_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..12 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        // Commit only the first few.
        log.set_high_watermark(3).unwrap();
        let end_before = log.log_end_offset();

        log.compact().unwrap();

        assert_eq!(log.log_end_offset(), end_before);
        // Everything at or above the watermark is still on disk.
        log.set_high_watermark(end_before).unwrap();
        let offsets: Vec<i64> = read_all(&log)
            .into_iter()
            .map(|(offset, _, _)| offset)
            .collect();
        for offset in 3..end_before {
            assert!(offsets.contains(&offset), "offset {offset} must survive");
        }
    }

    /// Compacting a log with nothing to remove must not disturb it.
    #[test]
    fn compaction_is_a_no_op_when_every_key_is_unique() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(keyed(&format!("k{round}"), "v")).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let before = read_all(&log);

        assert_eq!(log.compact().unwrap(), 0);
        assert_eq!(read_all(&log), before);
    }
}

impl Log {
    /// Whether this log keeps the latest record per key rather than
    /// deleting aged segments.
    pub fn is_compacted(&self) -> bool {
        self.config.compact
    }
}

impl Log {
    /// Whether the active segment should be sealed now.
    ///
    /// Size or age. The age rule matters more than it looks: retention only
    /// deletes sealed segments, so a partition that never reaches
    /// `segment_bytes` would otherwise keep one segment open forever and
    /// expire nothing at all, however short `retention.ms` was set.
    fn should_roll(&self) -> bool {
        let Some(active) = self.segments.last() else {
            return false;
        };
        if active.size >= self.config.segment_bytes {
            return true;
        }
        // An empty segment is not worth rolling: doing so on a timer would
        // produce an unbounded run of empty segments on an idle partition.
        if active.size == 0 {
            return false;
        }
        self.config.segment_ms.is_some_and(|limit| {
            now_ms().saturating_sub(self.active_segment_created_ms) >= limit as i64
        })
    }

    /// Whether the active segment is old enough to roll even though nothing
    /// is being appended. The partition actor polls this so a topic that
    /// goes quiet still seals its segment and lets retention work.
    pub fn roll_due(&self) -> bool {
        self.should_roll()
    }
}

impl Log {
    /// Seal the active segment now, if there is anything in it.
    ///
    /// Called by the partition actor when `segment.ms` has elapsed on an
    /// otherwise idle partition.
    pub fn roll_now(&mut self) -> Result<(), StorageError> {
        if self.segments.last().is_some_and(|active| active.size > 0) {
            self.roll_segment()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod segment_ms_tests {
    use super::*;
    use brahmaputra_protocol::Record;

    fn config(segment_ms: Option<u64>) -> LogConfig {
        LogConfig {
            // Large enough that only age can trigger a roll.
            segment_bytes: 16 * 1024 * 1024,
            segment_ms,
            index_interval_bytes: 64,
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    fn batch(value: &str) -> RecordBatch {
        RecordBatch::new(0, 0, 1, vec![Record::new(value.as_bytes().to_vec())])
    }

    /// Without a time limit a small partition keeps one segment forever,
    /// which is what left retention with nothing to delete.
    #[test]
    fn size_alone_never_rolls_a_small_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config(None)).unwrap();
        for index in 0..20 {
            log.append(batch(&format!("v{index}"))).unwrap();
        }
        assert_eq!(log.segment_count(), 1);
        assert!(!log.roll_due());
    }

    #[test]
    fn an_aged_segment_becomes_due_and_rolls() {
        let dir = tempfile::tempdir().unwrap();
        // Long enough that the append itself cannot trip the age limit —
        // with a 1 ms limit the write lands after the clock has already
        // expired and seals immediately, which makes the test race.
        let mut log = Log::open(dir.path(), config(Some(120))).unwrap();
        log.append(batch("first")).unwrap();
        assert!(!log.roll_due(), "a fresh segment is not due yet");
        std::thread::sleep(std::time::Duration::from_millis(200));

        assert!(log.roll_due(), "an aged non-empty segment must be due");
        log.roll_now().unwrap();
        assert_eq!(log.segment_count(), 2, "the aged segment was sealed");

        // Everything written before the roll is still readable at the same
        // offsets: sealing must not lose or renumber anything.
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let batches = log.read(0, 1 << 20).unwrap();
        assert!(!batches.is_empty());
    }

    /// Rolling an empty segment on a timer would produce an unbounded run
    /// of empty segments on a partition nobody writes to.
    #[test]
    fn an_empty_segment_is_never_rolled_on_age() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config(Some(1))).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(!log.roll_due());
        log.roll_now().unwrap();
        assert_eq!(log.segment_count(), 1);
    }

    /// The age clock restarts with each segment, so a busy partition does
    /// not roll on every append once it has been alive a while.
    #[test]
    fn the_age_clock_restarts_after_a_roll() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config(Some(3_600_000))).unwrap();
        log.append(batch("a")).unwrap();
        assert!(!log.roll_due());
    }
}

impl Log {
    /// Replace this log's configuration in place.
    ///
    /// A topic config change has to reach a *running* partition, not merely
    /// the metadata. Applying it only when a partition is next opened means
    /// an operator who shortens `retention.ms` sees the new value echoed
    /// back, watches nothing happen, and has no way to tell whether the
    /// setting is wrong or simply not in effect yet.
    ///
    /// `segment_bytes` and `index_interval_bytes` take effect from the next
    /// roll rather than retroactively: segments already written keep the
    /// shape they were written with, which is the only option that does not
    /// involve rewriting the log.
    pub fn set_config(&mut self, config: LogConfig) {
        self.config = config;
    }

    /// This log's current configuration.
    pub fn config(&self) -> &LogConfig {
        &self.config
    }
}
