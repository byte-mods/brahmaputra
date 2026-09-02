//! The per-partition log: a sequence of segments plus the high watermark.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use brahmaputra_protocol::{
    read_control_marker, validate_batch_header, BatchHeader, Compression, ControlMarker, Record,
    RecordBatch, BATCH_HEADER_LEN, MIN_BATCH_LENGTH,
};
use bytes::{Bytes, BytesMut};

use crate::epoch::LeaderEpochCheckpoint;
use crate::error::StorageError;
use crate::segment::Segment;
use crate::txn::{AbortedTransaction, TransactionIndex};

const HWM_FILE: &str = "hwm";
/// Where the log start offset is remembered across restarts.
///
/// Retention alone would not need this: it deletes whole segments, so the
/// oldest surviving segment's base offset says where the log starts. An
/// explicit `DeleteRecords` can land *inside* a segment, and without a
/// record of that the records it hid would reappear on the next restart —
/// which is precisely what an operator who deleted them did not ask for.
const LOG_START_FILE: &str = "logstart";

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

/// The persisted log start offset, or `None` when nothing has ever moved it
/// off the oldest segment's base.
///
/// A file that is missing, short, or holds a negative offset is treated as
/// absent rather than as an error: the segments on disk are the authority
/// on where the log starts, and this only ever refines that.
fn read_log_start(dir: &Path) -> Result<Option<i64>, StorageError> {
    let bytes = match fs::read(dir.join(LOG_START_FILE)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if bytes.len() < size_of::<i64>() {
        return Ok(None);
    }
    let offset = i64::from_be_bytes(
        bytes[..size_of::<i64>()]
            .try_into()
            .expect("fixed-size log start checkpoint"),
    );
    Ok((offset >= 0).then_some(offset))
}

/// Record where the log now starts, durably.
///
/// Written and fsynced inline rather than on a timer: this runs only when
/// an operator deletes records or retention drops a segment, and a start
/// offset that survives the operation but not the next crash would hand
/// back records someone asked to be rid of.
fn write_log_start(dir: &Path, offset: i64) -> Result<(), StorageError> {
    let path = dir.join(LOG_START_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;
    file.write_all(&offset.to_be_bytes())?;
    file.sync_all()?;
    Ok(())
}
/// How often the high-watermark checkpoint reaches disk. Kafka's
/// `replica.high.watermark.checkpoint.interval.ms` defaults to the same 5 s.
const DEFAULT_HWM_CHECKPOINT_INTERVAL_MS: u64 = 5_000;
/// Kafka's `delete.retention.ms` default: a day for a consumer to see a
/// deletion before the tombstone that carries it is removed.
const DEFAULT_DELETE_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
/// Kafka's `min.cleanable.dirty.ratio` default.
const DEFAULT_MIN_CLEANABLE_DIRTY_RATIO: f64 = 0.5;

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

    /// Fold the journal back into its 8-byte prefix once it has grown
    /// past this. Every record is 36 bytes and nothing ever removed one,
    /// so an active partition grew this file by hundreds of kilobytes a
    /// day and re-read all of it on every open.
    const COMPACT_ABOVE_BYTES: usize = 64 * 1024;

    /// Rewrite the file as a legacy prefix carrying the current high
    /// watermark, atomically: written beside, synced, renamed over.
    fn compact(&mut self) -> Result<(), StorageError> {
        let temp = self.path.with_extension("hwm.tmp");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp)?;
            file.write_all(&self.high_watermark.to_be_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&temp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            sync_dir(dir)?;
        }
        // The chain restarts: the prefix is generation zero, and the next
        // record is generation one on top of the prefix's watermark.
        self.generation = 0;
        self.valid_len = size_of::<i64>();
        Ok(())
    }

    fn append(
        &mut self,
        kind: HighWatermarkRecordKind,
        high_watermark: i64,
    ) -> Result<(), StorageError> {
        self.ensure_legacy_prefix()?;
        if self.valid_len >= Self::COMPACT_ABOVE_BYTES {
            self.compact()?;
        }
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
// Not `Eq`: `min_cleanable_dirty_ratio` is a ratio, and Kafka spells it as
// one too.
#[derive(Debug, Clone, PartialEq)]
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
    /// How long a tombstone is kept after compaction could first have
    /// removed it — Kafka's `delete.retention.ms`, default 24 hours.
    ///
    /// This is the grace period a consumer gets to observe a deletion. Too
    /// short and a slow consumer never learns the key is gone; too long and
    /// a delete-heavy compacted topic never shrinks.
    pub delete_retention_ms: u64,
    /// Fraction of the cleanable log that must be dirty before a pass runs
    /// — Kafka's `min.cleanable.dirty.ratio`, default 0.5.
    ///
    /// Compaction rewrites everything it cleans, so running it to remove a
    /// handful of records makes it the dominant write load on a partition
    /// that is barely changing.
    pub min_cleanable_dirty_ratio: f64,
    /// How long a record is protected from being compacted away, in
    /// milliseconds — Kafka's `min.compaction.lag.ms`, default 0.
    pub min_compaction_lag_ms: u64,
    /// How long a dirty record may wait before a pass runs regardless of
    /// the dirty ratio — Kafka's `max.compaction.lag.ms`. `None` never
    /// forces one.
    pub max_compaction_lag_ms: Option<u64>,
    /// Stamp every appended batch with the broker's clock rather than
    /// trusting the producer's — Kafka's
    /// `message.timestamp.type=LogAppendTime`.
    ///
    /// Retention, `ListOffsets` by timestamp and the time index all read
    /// these timestamps, so a single client with a wrong clock can
    /// otherwise make a whole partition look ancient or far in the future.
    pub log_append_time: bool,
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
            delete_retention_ms: DEFAULT_DELETE_RETENTION_MS,
            min_cleanable_dirty_ratio: DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            min_compaction_lag_ms: 0,
            max_compaction_lag_ms: None,
            log_append_time: false,
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
    /// Which transactions are open here, and which of the closed ones
    /// aborted. Empty and free for a partition nobody writes
    /// transactionally to.
    transactions: TransactionIndex,
    /// Where the deduplicated prefix ends: everything below this was
    /// compacted by an earlier pass, so the next pass only has to build a
    /// key map for what arrived since.
    first_dirty_offset: i64,
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
        // Before the directory is scanned: a compaction pass interrupted by
        // a crash leaves files that are not segments yet, and finishing or
        // discarding it is what makes the scan below see one coherent set.
        recover_compaction(&dir)?;

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

        // A deleted segment cannot be brought back, and a stale checkpoint
        // must never resurrect records, so the file can only ever move the
        // start *forward* of where the segments already put it — and never
        // past the end of the log.
        let start_offset = read_log_start(&dir)?
            .unwrap_or(segments[0].base_offset)
            .clamp(segments[0].base_offset, next_offset);
        let mut high_watermark_checkpoint =
            HighWatermarkCheckpoint::open(dir.join(HWM_FILE), config.hwm_checkpoint_interval_ms)?;
        let high_watermark = high_watermark_checkpoint.recover(next_offset)?;

        let leader_epochs = LeaderEpochCheckpoint::open(&dir)?;
        let transactions = TransactionIndex::open(&dir)?;
        // Clamped for the same reason the log start is: a checkpoint that
        // disagrees with the segments must not be believed over them. Too
        // low only costs a wider key map; too high would let a superseded
        // record survive forever.
        let first_dirty_offset = read_cleaner_checkpoint(&dir)?
            .unwrap_or(start_offset)
            .clamp(start_offset, next_offset);

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
            transactions,
            first_dirty_offset,
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
        if self.config.log_append_time {
            batch.max_timestamp = now_ms();
        }
        let base_offset = self.next_offset;
        let bytes = batch.encode();
        let active = self.segments.last_mut().expect("log always has a segment");
        active.append_batch(base_offset, &bytes, batch.max_timestamp)?;
        let appended = batch.records.len() as u64;
        self.next_offset += appended as i64;
        // Transaction state is derived from the encoded batch on every
        // append path — this one, the producer path and the replica path —
        // so a batch cannot be transactional on one of them and invisible
        // to the LSO on another.
        if batch.transactional {
            let header = validate_batch_header(&bytes)?;
            self.track_transaction(&header, &bytes, base_offset, self.next_offset - 1)?;
        }
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
        // `message.timestamp.type=LogAppendTime`: the broker's clock, not
        // the producer's, decides when this batch happened.
        //
        // Overwritten in place and the CRC recomputed over the covered
        // region — the records are never decompressed, because the batch
        // timestamp lives in the header. Kafka additionally flattens every
        // record to the same instant; here the per-record deltas survive,
        // so records inside one batch keep their relative spacing. The
        // property that matters is the same either way: retention and
        // timestamp seeks stop depending on a client's clock.
        let max_timestamp = if self.config.log_append_time {
            let now = now_ms();
            stamped[27..35].copy_from_slice(&now.to_be_bytes());
            let crc = crc32c::crc32c(&stamped[21..]);
            stamped[17..21].copy_from_slice(&crc.to_be_bytes());
            now
        } else {
            header.max_timestamp
        };

        let active = self.segments.last_mut().expect("log always has a segment");
        active.append_batch(base_offset, &stamped, max_timestamp)?;
        self.next_offset = next_offset;
        self.track_transaction(&header, &stamped, base_offset, next_offset - 1)?;
        self.maybe_flush((next_offset - base_offset) as u64)?;
        if self.should_roll() {
            self.roll_segment()?;
        }
        Ok((base_offset, next_offset))
    }

    /// Update transaction state from a batch that has just been appended.
    ///
    /// Driven off the batch itself rather than off a side channel so that
    /// leader and follower reach the same state from the same bytes: a
    /// follower replays exactly these batches, and if the two derived their
    /// LSO differently, a failover would change what a `read_committed`
    /// consumer can see.
    fn track_transaction(
        &mut self,
        header: &BatchHeader,
        bytes: &[u8],
        base_offset: i64,
        last_offset: i64,
    ) -> Result<(), StorageError> {
        if !header.transactional {
            return Ok(());
        }
        let Some(producer_id) = header.producer_id() else {
            // Transactional without a producer identity is malformed, but
            // it has already been appended and its CRC checked; refusing to
            // track it is the containable response.
            return Ok(());
        };
        if !header.control {
            return self.transactions.begin(producer_id, base_offset);
        }

        // Only a control batch is decoded, and only to read two bytes. It
        // holds one uncompressed record by construction, so this is not the
        // decompression the fetch path so carefully avoids — and it happens
        // once per transaction per partition, not once per batch.
        let mut buffer = Bytes::copy_from_slice(bytes);
        let Ok(batch) = RecordBatch::decode(&mut buffer) else {
            return Ok(());
        };
        match read_control_marker(&batch) {
            Some(ControlMarker::Commit) => self.transactions.end(producer_id, last_offset, true),
            Some(ControlMarker::Abort) => self.transactions.end(producer_id, last_offset, false),
            // A marker this version cannot read: leave the transaction open
            // rather than guessing which way it went. Holding records back
            // is recoverable; releasing aborted ones is not.
            None => Ok(()),
        }
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
        // A follower derives its transaction state from the same bytes the
        // leader did, so the two agree on the LSO without any extra
        // replication traffic — and a promoted follower answers a
        // `read_committed` fetch identically.
        self.track_transaction(&header, &batch, base_offset, self.next_offset - 1)?;
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
                // Read up to the remaining budget, one chunk at a time. The
                // chunk is a cap, not a floor: a 64 KiB fetch must not
                // zero-fill and read a megabyte to satisfy it. A batch that
                // runs past the chunk is read on its own below, so a batch
                // larger than the budget still gets served.
                let want = want.min(budget.min(READ_CHUNK_BYTES));
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
                        // The batch runs past what this chunk covers. Read
                        // it on its own: a fetch stops *after* the batch
                        // that crosses its budget, and must return at least
                        // one batch even when that batch alone exceeds the
                        // budget, or a consumer whose records are larger
                        // than its `max_bytes` could never advance.
                        let mut single = BytesMut::zeroed(total_len);
                        seg.read_at(position, &mut single)?;
                        let batch = single.freeze();
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

    /// The highest offset a `read_committed` consumer may be shown.
    ///
    /// Equal to the high watermark whenever nothing is in flight. With an
    /// open transaction it stops at that transaction's first record,
    /// because whether those records will exist has not been decided —
    /// showing them and retracting them later is exactly what
    /// `read_committed` exists to prevent.
    pub fn last_stable_offset(&self) -> i64 {
        self.transactions.last_stable_offset(self.high_watermark)
    }

    /// Aborted transactions whose records fall in `[from, to)`.
    pub fn aborted_transactions(&self, from: i64, to: i64) -> Vec<AbortedTransaction> {
        self.transactions.aborted_in_range(from, to)
    }

    /// Whether any transaction is open on this partition.
    pub fn has_ongoing_transactions(&self) -> bool {
        self.transactions.has_ongoing()
    }

    /// Where each open transaction started, by producer id.
    ///
    /// The answer to "why will the last stable offset not advance": the
    /// minimum of these *is* the LSO whenever one is open.
    pub fn open_transactions(&self) -> Vec<(i64, i64)> {
        self.transactions.open_transactions()
    }

    /// Read batches from `offset` as [`Log::read`] does, but showing only
    /// what a `read_committed` consumer may see: nothing at or past the
    /// last stable offset, no control batches, and nothing written by a
    /// transaction that aborted.
    ///
    /// Filtering happens per *batch*, never per record, which is what keeps
    /// it cheap: a batch belongs entirely to one producer and one
    /// transaction, so dropping it needs no decompression and no re-encode.
    /// The offsets of the batches that survive are unchanged — a consumer
    /// sees gaps where the skipped records were, exactly as it does after
    /// compaction.
    pub fn read_committed(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<Bytes>, StorageError> {
        let stable = self.last_stable_offset();
        if offset >= stable {
            // Nothing is readable yet even though the log may hold more:
            // the records past here are undecided.
            if offset < self.start_offset {
                return Err(StorageError::OffsetOutOfRange {
                    offset,
                    start: self.start_offset,
                    end: self.next_offset,
                });
            }
            return Ok(Vec::new());
        }

        let raw = self.read(offset, max_bytes)?;
        if raw.is_empty() {
            return Ok(raw);
        }
        let aborted = self.transactions.aborted_in_range(offset, self.next_offset);
        let mut kept = Vec::with_capacity(raw.len());
        for batch in raw {
            let Ok(header) = validate_batch_header(&batch) else {
                // The read path already validates; a batch that fails here
                // is not one to hand a consumer.
                break;
            };
            // Never past the stable point, whatever the byte budget said.
            if header.base_offset >= stable {
                break;
            }
            if header.control {
                continue;
            }
            let discarded = header.transactional
                && header.producer_id().is_some_and(|producer_id| {
                    aborted.iter().any(|txn| {
                        txn.producer_id == producer_id
                            && header.base_offset >= txn.first_offset
                            && header.base_offset <= txn.last_offset
                    })
                });
            if discarded {
                continue;
            }
            kept.push(batch);
        }
        Ok(kept)
    }

    /// Offset to begin scanning from when answering "first record at or
    /// after timestamp `target`", or `None` when no record in the log can
    /// qualify.
    ///
    /// A `ListOffsets` by timestamp used to walk the log from its start,
    /// reading and CRC-checking every batch until one was new enough — so
    /// the cost of asking "where was I an hour ago?" was the cost of
    /// reading everything older than an hour. Consulting the time index
    /// first skips whole segments by their newest record and lands within
    /// one index interval inside the segment that can match, which is what
    /// the `.timeindex` files were being written for all along.
    ///
    /// This narrows *where to look*, not what the answer is: the caller
    /// still scans forward from here, so the offset returned is identical
    /// to the one a full scan would have produced.
    pub fn scan_start_for_timestamp(&self, target: i64) -> Option<i64> {
        for segment in &self.segments {
            // A segment whose newest record predates the target cannot hold
            // the answer, whatever its individual batches look like.
            if segment.max_timestamp.is_some_and(|newest| newest < target) {
                continue;
            }
            let relative = segment.timeindex.scan_start(target).unwrap_or(0);
            let start = segment.base_offset.saturating_add(i64::from(relative));
            // Retention may have deleted records this segment's index still
            // describes, so never point a reader below the log start.
            return Some(start.max(self.start_offset));
        }
        None
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
        // Transactions the discarded tail opened or closed no longer
        // happened. Rebuilding what survives means replaying the remaining
        // log, so this drops the lot: the follower that truncates is about
        // to refetch from the leader, and every batch it receives is
        // tracked again on the way in.
        self.transactions.reset()?;
        Ok(actual)
    }

    /// Number of segments (including the active one).
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Bytes of log data this partition occupies, summed across segments.
    ///
    /// Read from the sizes the log already tracks rather than by walking
    /// the directory, so asking every partition on a broker how big it is
    /// costs no filesystem calls at all.
    pub fn size_bytes(&self) -> u64 {
        self.segments.iter().map(|segment| segment.size).sum()
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
        // `max`, not assignment: an explicit DeleteRecords may have moved
        // the start past this segment's base already, and retention must
        // not hand those records back.
        //
        // Only when this pass actually deleted something. Compaction can
        // leave the first surviving segment above the start offset, and
        // that must not be read as "retention deleted those records" —
        // they were superseded, and a fetch from the start has to return
        // what survived rather than fail.
        let start = self.segments[0].base_offset.max(self.start_offset);
        if deleted > 0 && start != self.start_offset {
            self.start_offset = start;
            write_log_start(&self.dir, start)?;
            self.transactions.prune_below(start)?;
        }
        Ok(deleted)
    }

    /// Hide every record below `target` and reclaim the segments that hold
    /// only such records. Returns the resulting log start offset.
    ///
    /// This is Kafka's `DeleteRecords`, and it is the only way to reclaim
    /// space on a topic that retention will not touch — a compacted topic,
    /// or one whose retention is deliberately long — and the only answer to
    /// "delete this data now" that does not mean deleting the topic.
    ///
    /// The target is clamped to the committed range. Above the high
    /// watermark it would discard records that replicas have not all
    /// acknowledged, which is data loss dressed up as an admin operation;
    /// below the current start it would claim to undelete, which nothing
    /// can honour once the segments are gone.
    ///
    /// Records between the new start and the base of the segment holding it
    /// stay on disk until retention or a later delete claims their whole
    /// segment. They are unreadable from that moment: a fetch below the log
    /// start is out of range, exactly as it is for retention-deleted data.
    pub fn delete_records_before(&mut self, target: i64) -> Result<i64, StorageError> {
        let target = target.clamp(self.start_offset, self.high_watermark);
        if target == self.start_offset {
            return Ok(self.start_offset);
        }

        // Drop whole sealed segments that end at or before the new start.
        // The next segment's base offset is this one's exclusive end, and
        // the active segment is never removed.
        while self.segments.len() > 1 && self.segments[1].base_offset <= target {
            let segment = self.segments.remove(0);
            segment.delete()?;
        }
        self.start_offset = target;
        write_log_start(&self.dir, target)?;
        // The records of a transaction that ended below the new start are
        // gone, so nothing will ever need to skip them again.
        self.transactions.prune_below(target)?;
        Ok(self.start_offset)
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
            out.extend(
                batch
                    .iter()
                    .map(|(o, r)| (o, r.value.clone().unwrap_or_default())),
            );
        }
        out
    }

    /// Segments large enough that transactions are not split across them,
    /// and a watermark that reaches disk on every advance so a restart in
    /// these tests resumes where it left off rather than at zero.
    fn txn_config() -> LogConfig {
        LogConfig {
            hwm_checkpoint_interval_ms: 0,
            ..LogConfig::default()
        }
    }

    /// Append `batch` through the producer path, returning its base offset.
    fn append_raw(log: &mut Log, batch: &RecordBatch) -> i64 {
        log.append_producer_batch(&batch.encode(), 0).unwrap().0
    }

    fn transactional_batch(producer_id: i64, sequence: i32, value: &str) -> RecordBatch {
        let mut batch = RecordBatch::new(0, 0, 1_000, vec![Record::new(value.as_bytes().to_vec())]);
        batch.producer = Some(brahmaputra_protocol::ProducerMetadata {
            producer_id,
            producer_epoch: 0,
            base_sequence: sequence,
        });
        batch.transactional = true;
        batch
    }

    fn values(batches: &[Bytes]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|raw| {
                let batch = RecordBatch::decode(&mut raw.clone()).unwrap();
                batch
                    .records
                    .into_iter()
                    .map(|record| String::from_utf8(record.payload().to_vec()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn a_committed_read_stops_at_the_open_transaction_and_skips_aborted_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), txn_config()).unwrap();

        // A plain record everyone can see.
        append_raw(
            &mut log,
            &RecordBatch::new(0, 0, 1_000, vec![Record::new(b"plain".to_vec())]),
        );

        // Producer 1 writes and aborts.
        append_raw(&mut log, &transactional_batch(1, 0, "doomed"));
        let abort_at = append_raw(
            &mut log,
            &brahmaputra_protocol::control_batch(
                brahmaputra_protocol::ProducerMetadata {
                    producer_id: 1,
                    producer_epoch: 0,
                    base_sequence: 0,
                },
                ControlMarker::Abort,
                1_000,
            ),
        );

        // Producer 2 writes and commits.
        append_raw(&mut log, &transactional_batch(2, 0, "kept"));
        append_raw(
            &mut log,
            &brahmaputra_protocol::control_batch(
                brahmaputra_protocol::ProducerMetadata {
                    producer_id: 2,
                    producer_epoch: 0,
                    base_sequence: 0,
                },
                ControlMarker::Commit,
                1_000,
            ),
        );

        // Producer 3 writes and leaves the transaction open.
        let open_at = append_raw(&mut log, &transactional_batch(3, 0, "undecided"));

        let end = log.log_end_offset();
        log.set_high_watermark(end).unwrap();

        // The open transaction, not the high watermark, is what bounds a
        // committed read.
        assert_eq!(log.last_stable_offset(), open_at);
        assert!(log.has_ongoing_transactions());
        assert!(abort_at < open_at);

        let committed = values(&log.read_committed(0, 64 * 1024).unwrap());
        assert_eq!(
            committed,
            vec!["plain", "kept"],
            "aborted records, control markers and undecided records must all be withheld"
        );

        // read_uncommitted sees everything the log holds, markers included.
        let raw = values(&log.read(0, 64 * 1024).unwrap());
        assert!(raw.contains(&"doomed".to_owned()));
        assert!(raw.contains(&"undecided".to_owned()));

        // Committing the open transaction releases its records and returns
        // the stable point to the high watermark.
        append_raw(
            &mut log,
            &brahmaputra_protocol::control_batch(
                brahmaputra_protocol::ProducerMetadata {
                    producer_id: 3,
                    producer_epoch: 0,
                    base_sequence: 0,
                },
                ControlMarker::Commit,
                1_000,
            ),
        );
        let end = log.log_end_offset();
        log.set_high_watermark(end).unwrap();
        assert_eq!(log.last_stable_offset(), end);
        assert_eq!(
            values(&log.read_committed(0, 64 * 1024).unwrap()),
            vec!["plain", "kept", "undecided"]
        );

        // And the abort is still remembered after a restart: an aborted
        // record that reappears is the failure this whole mechanism exists
        // to prevent.
        drop(log);
        let reopened = Log::open(dir.path(), txn_config()).unwrap();
        assert_eq!(
            values(&reopened.read_committed(0, 64 * 1024).unwrap()),
            vec!["plain", "kept", "undecided"]
        );
    }

    #[test]
    fn a_replica_derives_the_same_transaction_state_from_the_same_bytes() {
        let leader_dir = tempfile::tempdir().unwrap();
        let follower_dir = tempfile::tempdir().unwrap();
        let mut leader = Log::open(leader_dir.path(), LogConfig::default()).unwrap();
        let mut follower = Log::open(follower_dir.path(), LogConfig::default()).unwrap();

        append_raw(&mut leader, &transactional_batch(1, 0, "a"));
        append_raw(&mut leader, &transactional_batch(2, 0, "b"));
        append_raw(
            &mut leader,
            &brahmaputra_protocol::control_batch(
                brahmaputra_protocol::ProducerMetadata {
                    producer_id: 2,
                    producer_epoch: 0,
                    base_sequence: 0,
                },
                ControlMarker::Abort,
                1_000,
            ),
        );

        // Replicate byte for byte, exactly as the follower fetch path does.
        for raw in leader.read(0, 1024 * 1024).unwrap() {
            follower.append_replica_batch(raw).unwrap();
        }
        let end = leader.log_end_offset();
        leader.set_high_watermark(end).unwrap();
        follower.set_high_watermark(end).unwrap();

        // If the two disagreed here, a failover would change what a
        // read_committed consumer is allowed to see.
        assert_eq!(follower.last_stable_offset(), leader.last_stable_offset());
        assert_eq!(
            values(&follower.read_committed(0, 64 * 1024).unwrap()),
            values(&leader.read_committed(0, 64 * 1024).unwrap())
        );
    }

    #[test]
    fn deleting_records_hides_them_reclaims_segments_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..50 {
            log.append(batch(2, i * 2, 1_000 + i as i64)).unwrap();
        }
        let end = log.log_end_offset();
        let segments_before = log.segment_count();
        assert!(segments_before > 2, "need several segments to reclaim any");

        // Only offset 40 is committed so far. Asking to delete everything
        // must stop at the high watermark: an admin command must not
        // discard records the cluster has not committed.
        log.set_high_watermark(40).unwrap();
        assert_eq!(log.delete_records_before(end).unwrap(), 40);
        log.set_high_watermark(end).unwrap();

        assert_eq!(log.log_start_offset(), 40);
        assert!(
            log.segment_count() < segments_before,
            "segments below the new start should have been reclaimed"
        );

        // Below the new start is a no-op rather than an undelete.
        assert_eq!(log.delete_records_before(0).unwrap(), 40);
        assert_eq!(log.log_start_offset(), 40);

        // Hidden records are out of range, not merely absent.
        assert!(matches!(
            log.read(0, 4096),
            Err(StorageError::OffsetOutOfRange { .. })
        ));
        // ...and everything at or after the new start still reads.
        let outcome = log.read(40, 64 * 1024).unwrap();
        assert!(!outcome.is_empty());

        drop(log);
        let reopened = Log::open(dir.path(), test_config()).unwrap();
        assert_eq!(
            reopened.log_start_offset(),
            40,
            "a restart must not resurrect deleted records"
        );
        assert_eq!(reopened.log_end_offset(), end);
    }

    #[test]
    fn retention_never_hands_back_records_an_operator_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        for i in 0..50 {
            log.append(batch(2, i * 2, 1_000 + i as i64)).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        log.delete_records_before(40).unwrap();
        assert_eq!(log.log_start_offset(), 40);

        // Retention derives the start from the oldest surviving segment,
        // whose base is below 40; it must not move the start backwards.
        log.apply_retention().unwrap();
        assert_eq!(log.log_start_offset(), 40);
    }

    #[test]
    fn timestamp_lookup_narrows_the_scan_without_moving_the_answer() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), test_config()).unwrap();
        // 50 batches of 2 records at 1000, 1001, ... spread over several
        // segments (512-byte segments in `test_config`).
        for i in 0..50 {
            log.append(batch(2, i * 2, 1_000 + i as i64)).unwrap();
        }
        assert!(
            log.segments.len() > 1,
            "the point of this test is skipping whole segments"
        );

        // The answer a full scan would give: the first batch whose
        // max_timestamp is >= target, which for batch `i` is offset `i * 2`.
        for target in [1_000, 1_001, 1_017, 1_033, 1_049] {
            let scan_start = log
                .scan_start_for_timestamp(target)
                .expect("a record reaches this timestamp");
            let answer = (target - 1_000) * 2;
            assert!(
                scan_start <= answer,
                "scan start {scan_start} skipped past the answer {answer} for {target}"
            );
        }

        // Starting the scan later than offset 0 is the whole benefit; a
        // lookup near the tail must not begin at the log start.
        assert!(log.scan_start_for_timestamp(1_049).unwrap() > 0);

        // Older than everything: scan from the very beginning.
        assert_eq!(log.scan_start_for_timestamp(1).unwrap(), 0);
        // Newer than everything: nothing to scan at all.
        assert_eq!(log.scan_start_for_timestamp(9_999), None);
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

    /// The journal is append-only, so without folding it back it grew by
    /// 36 bytes per checkpoint for the life of the partition and was read
    /// whole on every open. Past the threshold it collapses to its prefix,
    /// and what it says survives a reopen.
    #[test]
    fn the_checkpoint_journal_is_folded_once_it_grows() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = dir.path().join(HWM_FILE);
        let advances = 2 * HighWatermarkCheckpoint::COMPACT_ABOVE_BYTES / HWM_RECORD_LEN;
        {
            let mut log = Log::open(dir.path(), config(0)).unwrap();
            for value in 0..advances {
                log.append(one_batch(value)).unwrap();
                log.set_high_watermark(log.log_end_offset()).unwrap();
            }
            let written = fs::metadata(&checkpoint).unwrap().len() as usize;
            assert!(
                written < HighWatermarkCheckpoint::COMPACT_ABOVE_BYTES + HWM_RECORD_LEN,
                "checkpoint grew to {written} bytes without being folded"
            );
        }
        let log = Log::open(dir.path(), config(0)).unwrap();
        assert_eq!(log.high_watermark(), advances as i64);
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

/// What one compaction pass did, for logging and for the tests that have
/// to distinguish "nothing was removable" from "the pass did not run".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
    /// Superseded records dropped because a later record has the same key.
    pub records_removed: usize,
    /// Tombstones dropped because they aged past `delete.retention.ms`.
    pub tombstones_removed: usize,
    /// Bytes of sealed log before and after the rewrite.
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// False when the pass declined to run — nothing sealed, or not enough
    /// of the log is dirty to be worth rewriting.
    pub ran: bool,
}

/// The shape of the batch a record came out of.
///
/// Survivors are regrouped into batches, and two records may only share an
/// output batch if every one of these matches: they are what the batch
/// header says about all of its records at once, so mixing them would
/// change what the records mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchShape {
    leader_epoch: i32,
    compression: Compression,
    /// Producer id and epoch. The base sequence is per output batch and is
    /// derived from the first survivor's position, so it is not part of the
    /// identity.
    producer: Option<(i64, i16)>,
    transactional: bool,
}

/// One record that survived, with everything needed to re-emit it at its
/// original offset.
struct Survivor {
    offset: i64,
    timestamp: i64,
    /// The producer sequence this record had, or -1 when its batch carried
    /// none.
    sequence: i32,
    record: Record,
}

/// Accumulates survivors into segments under a staging directory.
///
/// Rolls at `segment_bytes` rather than writing one segment for the whole
/// log: a compacted partition that has been running for a year is still a
/// partition whose segments retention, fetch and recovery expect to be
/// bounded.
struct CompactionWriter {
    dir: PathBuf,
    segment_bytes: u64,
    index_interval_bytes: u64,
    current: Option<Segment>,
    bases: Vec<i64>,
    bytes_written: u64,
}

impl CompactionWriter {
    fn new(dir: PathBuf, segment_bytes: u64, index_interval_bytes: u64) -> Self {
        CompactionWriter {
            dir,
            segment_bytes,
            index_interval_bytes,
            current: None,
            bases: Vec::new(),
            bytes_written: 0,
        }
    }

    fn push(
        &mut self,
        base_offset: i64,
        bytes: &[u8],
        max_timestamp: i64,
    ) -> Result<(), StorageError> {
        let roll = match &self.current {
            None => true,
            Some(segment) => {
                segment.size > 0 && segment.size + bytes.len() as u64 > self.segment_bytes
            }
        };
        if roll {
            if let Some(segment) = self.current.take() {
                segment.sync()?;
            }
            let segment = Segment::open(&self.dir, base_offset, self.index_interval_bytes)?;
            self.bases.push(base_offset);
            self.current = Some(segment);
        }
        let segment = self.current.as_mut().expect("a segment was just opened");
        segment.append_batch(base_offset, bytes, max_timestamp)?;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<i64>, StorageError> {
        if let Some(segment) = self.current.take() {
            segment.sync()?;
        }
        Ok(self.bases)
    }
}

/// Name of the marker that says a compaction pass got far enough that its
/// output must be kept.
///
/// Everything before this file exists is discardable: the original
/// segments are still whole. Everything after it is a swap that has to be
/// finished, because the originals are being deleted. Recovery therefore
/// never has to decide which copy is authoritative — the marker decides.
const COMPACTION_COMMIT_FILE: &str = "compaction.commit";
/// Where a pass builds its output before any original is touched.
const COMPACTION_STAGING_DIR: &str = "compaction";
/// Suffix an output file carries while the originals are being removed.
const SWAP_SUFFIX: &str = ".swap";
/// Where the first dirty offset is remembered, so `min.cleanable.dirty.ratio`
/// survives a restart instead of resetting to "everything is dirty".
const CLEANER_CHECKPOINT_FILE: &str = "cleaner";

impl Log {
    /// Keep only the most recent record for each key among the sealed
    /// segments, discarding the versions it supersedes and the deletions
    /// that have outlived their grace period.
    ///
    /// This is what stops a keyed topic — `__consumer_offsets` above all —
    /// from growing without bound. A group that commits every five seconds
    /// writes the same key forever; without compaction the disk fills, and
    /// coordinator failover gets slower without limit because it replays
    /// every superseded commit.
    ///
    /// Offsets are preserved exactly: a survivor keeps the offset it was
    /// written at, so compaction leaves gaps rather than renumbering
    /// anything and a consumer's committed offset still means what it
    /// meant. Contiguous survivors are re-emitted as one batch, which is
    /// why this does not quietly convert a compressed, batched log into a
    /// stream of single-record batches.
    ///
    /// Only sealed segments below the high watermark are touched: the
    /// active segment is still being appended to, and uncommitted records
    /// are not ours to discard.
    pub fn compact(&mut self) -> Result<CompactionOutcome, StorageError> {
        let now = now_ms();
        let mut outcome = CompactionOutcome::default();
        let Some((cleanable_end, cleanable_segments)) = self.cleanable_range(now) else {
            return Ok(outcome);
        };
        let first_dirty = self
            .first_dirty_offset
            .clamp(self.segments[0].base_offset, cleanable_end);
        if !self.worth_compacting(first_dirty, cleanable_end, cleanable_segments, now) {
            return Ok(outcome);
        }
        outcome.ran = true;
        outcome.bytes_before = self.segments[..cleanable_segments]
            .iter()
            .map(|segment| segment.size)
            .sum();

        // Pass one: the offset of the last record written for each key in
        // the dirty range. Only the dirty range, because everything below
        // it was deduplicated by an earlier pass — which is what keeps this
        // map proportional to what has arrived since, not to the log.
        let mut latest: std::collections::HashMap<Vec<u8>, i64> = std::collections::HashMap::new();
        // A batch from a transaction that aborted holds records a committed
        // reader never saw and never will. They are neither the latest
        // value for their key nor worth keeping, so both passes drop them
        // — the same rule Kafka's cleaner applies.
        let aborted = self
            .transactions
            .aborted_in_range(self.segments[0].base_offset, cleanable_end);
        let is_aborted = |batch: &RecordBatch| -> bool {
            let Some(producer) = batch.producer.as_ref() else {
                return false;
            };
            batch.transactional
                && aborted.iter().any(|txn| {
                    txn.producer_id == producer.producer_id
                        && txn.first_offset <= batch.base_offset
                        && batch.base_offset <= txn.last_offset
                })
        };
        for index in 0..cleanable_segments {
            Self::for_each_batch(&self.segments[index], |_, batch| {
                if batch.control || is_aborted(&batch) {
                    return Ok(());
                }
                for (offset, record) in batch.iter() {
                    if offset < first_dirty || offset >= cleanable_end {
                        continue;
                    }
                    if let Some(key) = record.key.as_ref() {
                        let entry = latest.entry(key.to_vec()).or_insert(offset);
                        *entry = (*entry).max(offset);
                    }
                }
                Ok(())
            })?;
        }

        // Pass two: rewrite the cleanable segments, dropping what a later
        // record supersedes and the tombstones that have done their job.
        let staging = self.dir.join(COMPACTION_STAGING_DIR);
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        let mut writer = CompactionWriter::new(
            staging.clone(),
            self.config.segment_bytes,
            self.config.index_interval_bytes,
        );
        let delete_horizon = now.saturating_sub(self.config.delete_retention_ms as i64);
        let mut run: Vec<Survivor> = Vec::new();
        let mut run_shape: Option<BatchShape> = None;

        for index in 0..cleanable_segments {
            let mut error: Option<StorageError> = None;
            Self::for_each_batch(&self.segments[index], |raw, batch| {
                // A control batch is a transaction marker. It is what tells
                // a committed reader that the records around it resolved,
                // and the partition's transaction index is written in terms
                // of its offset, so it is copied through untouched.
                if batch.control {
                    if let Err(e) = Self::flush_run(&mut writer, &mut run, &mut run_shape) {
                        error = Some(e);
                        return Ok(());
                    }
                    if let Err(e) = writer.push(batch.base_offset, &raw, batch.max_timestamp) {
                        error = Some(e);
                    }
                    return Ok(());
                }
                if is_aborted(&batch) {
                    // Counted as removed, or a pass that dropped nothing
                    // else would be treated as a no-op and its output
                    // discarded — with the aborted records still in place.
                    outcome.records_removed += batch.records.len();
                    return Ok(());
                }
                let shape = BatchShape {
                    leader_epoch: batch.leader_epoch,
                    compression: batch.compression,
                    producer: batch
                        .producer
                        .map(|producer| (producer.producer_id, producer.producer_epoch)),
                    transactional: batch.transactional,
                };
                let base_sequence = batch.producer.map_or(-1, |producer| producer.base_sequence);
                let batch_base = batch.base_offset;
                let max_timestamp = batch.max_timestamp;
                for (position, record) in batch.records.into_iter().enumerate() {
                    let offset = batch_base + position as i64;
                    let superseded = record
                        .key
                        .as_ref()
                        .and_then(|key| latest.get(key.as_ref() as &[u8]))
                        .is_some_and(|newest| *newest > offset);
                    let timestamp = record.timestamp(max_timestamp);
                    // A tombstone is kept until it has been visible long
                    // enough for a consumer to have seen the deletion —
                    // that grace period is the whole contract of
                    // `delete.retention.ms`. After it, the tombstone goes
                    // too, which is the only way a compacted topic's key
                    // space ever shrinks.
                    let expired_tombstone = record.is_tombstone() && timestamp <= delete_horizon;
                    if superseded {
                        outcome.records_removed += 1;
                        continue;
                    }
                    if expired_tombstone {
                        outcome.records_removed += 1;
                        outcome.tombstones_removed += 1;
                        continue;
                    }
                    let contiguous = run
                        .last()
                        .is_none_or(|previous| previous.offset + 1 == offset);
                    if !contiguous || run_shape.is_some_and(|current| current != shape) {
                        if let Err(e) = Self::flush_run(&mut writer, &mut run, &mut run_shape) {
                            error = Some(e);
                            return Ok(());
                        }
                    }
                    run_shape = Some(shape);
                    run.push(Survivor {
                        offset,
                        timestamp,
                        sequence: if base_sequence < 0 {
                            -1
                        } else {
                            base_sequence.saturating_add(position as i32)
                        },
                        record,
                    });
                }
                Ok(())
            })?;
            if let Some(error) = error {
                return Err(error);
            }
        }
        Self::flush_run(&mut writer, &mut run, &mut run_shape)?;
        let new_bases = writer.finish()?;

        if outcome.records_removed == 0 {
            // Nothing came out: the rewrite would be byte-for-byte what is
            // already there, so throw the copy away rather than swap it in
            // and pay for the fsyncs.
            fs::remove_dir_all(&staging)?;
            self.set_first_dirty_offset(cleanable_end)?;
            outcome.bytes_after = outcome.bytes_before;
            return Ok(outcome);
        }

        // The swap. Everything up to the marker is discardable; everything
        // after it must be finished, and `recover_compaction` finishes it
        // if this process does not survive to.
        let first_base = self.segments[0].base_offset;
        Self::stage_swap_files(&staging, &self.dir, &new_bases)?;
        write_compaction_marker(&self.dir, first_base, cleanable_end, &new_bases)?;
        for index in (0..cleanable_segments).rev() {
            let segment = self.segments.remove(index);
            segment.delete()?;
        }
        finish_swap(&self.dir, &new_bases)?;
        fs::remove_dir_all(&staging)?;
        fs::remove_file(self.dir.join(COMPACTION_COMMIT_FILE))?;

        // Reopen what was written and splice it in ahead of the segments
        // the pass never touched.
        let mut reopened = Vec::with_capacity(new_bases.len());
        for &base in &new_bases {
            let mut segment = Segment::open(&self.dir, base, self.config.index_interval_bytes)?;
            segment.max_timestamp = segment.scan_max_timestamp()?;
            reopened.push(segment);
        }
        outcome.bytes_after = reopened.iter().map(|segment| segment.size).sum();
        for (position, segment) in reopened.into_iter().enumerate() {
            self.segments.insert(position, segment);
        }

        // The log start offset deliberately does *not* move. Compaction
        // removing the record at offset 0 does not make offset 0 out of
        // range — a consumer reading a compacted topic from the beginning
        // must get the oldest record that still exists, not an error. This
        // is Kafka's rule too: only retention and `DeleteRecords` move the
        // start, because only those two say records are gone rather than
        // superseded.
        self.set_first_dirty_offset(cleanable_end)?;
        Ok(outcome)
    }

    /// How far compaction may clean, and how many segments that covers.
    ///
    /// Never the active segment (it is still being appended to), never
    /// above the high watermark (uncommitted records are not ours to
    /// discard), and never a segment younger than
    /// `min.compaction.lag.ms` — that last one is what lets a consumer be
    /// promised it will see every update to a key if it stays within the
    /// lag, rather than only the ones compaction happened not to have
    /// reached yet.
    fn cleanable_range(&self, now: i64) -> Option<(i64, usize)> {
        if !self.config.compact || self.segments.len() < 2 {
            return None;
        }
        let mut count = self.segments.len() - 1;
        let min_lag = self.config.min_compaction_lag_ms as i64;
        if min_lag > 0 {
            while count > 0 {
                let too_young = self.segments[count - 1]
                    .max_timestamp
                    .is_some_and(|timestamp| now.saturating_sub(timestamp) < min_lag);
                if too_young {
                    count -= 1;
                } else {
                    break;
                }
            }
        }
        // Bounded by the last stable offset, not the high watermark: a
        // record inside an open transaction is not yet a value at all, and
        // letting it supersede the committed one would delete the value
        // that is real for one that may be retracted.
        let stable = self.last_stable_offset();
        while count > 0 && self.segments[count].base_offset > stable {
            count -= 1;
        }
        if count == 0 {
            return None;
        }
        let end = self.segments[count].base_offset;
        if end <= self.segments[0].base_offset {
            return None;
        }
        Some((end, count))
    }

    /// Whether enough of the cleanable range is dirty to be worth
    /// rewriting it.
    ///
    /// Without this gate a pass runs on every maintenance tick and rewrites
    /// the whole cleanable log to remove a handful of records — which is
    /// how compaction turns into the dominant write load on a partition
    /// that is barely changing. `max.compaction.lag.ms` overrides it, so a
    /// slow-moving topic still gets cleaned eventually.
    fn worth_compacting(&self, first_dirty: i64, end: i64, segments: usize, now: i64) -> bool {
        let dirty: u64 = self.segments[..segments]
            .iter()
            .filter(|segment| segment.base_offset >= first_dirty)
            .map(|segment| segment.size)
            .sum();
        let total: u64 = self.segments[..segments]
            .iter()
            .map(|segment| segment.size)
            .sum();
        if total == 0 || end <= first_dirty {
            return false;
        }
        if dirty as f64 / total as f64 >= self.config.min_cleanable_dirty_ratio {
            return true;
        }
        self.config.max_compaction_lag_ms.is_some_and(|lag| {
            self.segments[..segments]
                .iter()
                .filter(|segment| segment.base_offset >= first_dirty)
                .filter_map(|segment| segment.max_timestamp)
                .any(|timestamp| now.saturating_sub(timestamp) >= lag as i64)
        })
    }

    /// Emit the accumulated run of contiguous survivors as one batch.
    fn flush_run(
        writer: &mut CompactionWriter,
        run: &mut Vec<Survivor>,
        shape: &mut Option<BatchShape>,
    ) -> Result<(), StorageError> {
        if run.is_empty() {
            *shape = None;
            return Ok(());
        }
        let shape = shape.take().expect("a non-empty run has a shape");
        let base_offset = run[0].offset;
        let base_sequence = run[0].sequence;
        let timestamped: Vec<(Record, i64)> = run
            .drain(..)
            .map(|survivor| (survivor.record, survivor.timestamp))
            .collect();
        let mut batch =
            RecordBatch::from_timestamped(base_offset, shape.leader_epoch, timestamped, 0)
                .with_compression(shape.compression);
        if let Some((producer_id, producer_epoch)) = shape.producer {
            batch = batch.with_producer(producer_id, producer_epoch, base_sequence);
        }
        batch.transactional = shape.transactional;
        let max_timestamp = batch.max_timestamp;
        let bytes = batch.encode();
        writer.push(base_offset, &bytes, max_timestamp)
    }

    /// Walk every whole batch in a segment, in order.
    fn for_each_batch<F>(segment: &Segment, mut visit: F) -> Result<(), StorageError>
    where
        F: FnMut(Bytes, RecordBatch) -> Result<(), StorageError>,
    {
        let mut position = 0u64;
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
            let raw = buf.freeze();
            let mut cursor = raw.clone();
            let batch = RecordBatch::decode(&mut cursor)?;
            visit(raw, batch)?;
            position += total_len as u64;
        }
        Ok(())
    }

    /// Move the staged output alongside the originals under `.swap` names,
    /// which cannot collide with them.
    fn stage_swap_files(staging: &Path, dir: &Path, bases: &[i64]) -> Result<(), StorageError> {
        for &base in bases {
            for suffix in ["log", "index", "timeindex"] {
                let from = staging.join(format!("{base:020}.{suffix}"));
                if from.exists() {
                    let to = dir.join(format!("{base:020}.{suffix}{SWAP_SUFFIX}"));
                    fs::rename(&from, &to)?;
                }
            }
        }
        sync_dir(dir)
    }

    /// Remember where the clean prefix ends, so a restart does not decide
    /// the whole log is dirty and rewrite it.
    fn set_first_dirty_offset(&mut self, offset: i64) -> Result<(), StorageError> {
        if offset == self.first_dirty_offset {
            return Ok(());
        }
        self.first_dirty_offset = offset;
        write_cleaner_checkpoint(&self.dir, offset)
    }
}

/// Record that a compaction pass has reached the point of no return.
///
/// Names both the range being replaced and the segments replacing it. The
/// second half is what lets recovery tell a segment this pass *produced*
/// from a stale original at a base offset inside the same range — once a
/// swap file has been renamed the two look identical on disk, and deleting
/// the wrong one loses every record the pass kept.
fn write_compaction_marker(
    dir: &Path,
    from: i64,
    to: i64,
    output: &[i64],
) -> Result<(), StorageError> {
    let path = dir.join(COMPACTION_COMMIT_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let bases: Vec<String> = output.iter().map(|base| base.to_string()).collect();
    file.write_all(format!("{from} {to} {}\n", bases.join(",")).as_bytes())?;
    file.sync_all()?;
    sync_dir(dir)
}

/// Put the `.swap` output into its final place.
fn finish_swap(dir: &Path, bases: &[i64]) -> Result<(), StorageError> {
    for &base in bases {
        for suffix in ["log", "index", "timeindex"] {
            let from = dir.join(format!("{base:020}.{suffix}{SWAP_SUFFIX}"));
            if from.exists() {
                fs::rename(&from, dir.join(format!("{base:020}.{suffix}")))?;
            }
        }
    }
    sync_dir(dir)
}

/// Finish or discard a compaction pass interrupted by a crash.
///
/// With no marker the pass never committed: the originals are whole and the
/// staged output is thrown away. With a marker, the originals in the
/// compacted range are on their way out — some may already be gone — so the
/// only consistent state is the one the pass was heading for, and the swap
/// is completed.
fn recover_compaction(dir: &Path) -> Result<(), StorageError> {
    let marker = dir.join(COMPACTION_COMMIT_FILE);
    let staging = dir.join(COMPACTION_STAGING_DIR);
    if !marker.exists() {
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path
                .to_str()
                .is_some_and(|name| name.ends_with(SWAP_SUFFIX))
            {
                fs::remove_file(path)?;
            }
        }
        return Ok(());
    }

    let (from, to, output) = read_compaction_marker(&marker)?;

    // Which base offsets in the compacted range belong to the *output* is
    // read from the marker rather than inferred from what is on disk,
    // because once a swap file has been renamed the two are
    // indistinguishable: both are plain segments at a base offset inside
    // the range. Inferring would delete the output of a pass that had
    // already finished, which is the one interruption point that must not
    // lose data.
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".log") else {
            continue;
        };
        let Ok(base) = stem.parse::<i64>() else {
            continue;
        };
        // A stale original: inside the range this pass replaced, and not
        // one of the segments it produced.
        if base >= from && base < to && !output.contains(&base) {
            for suffix in ["log", "index", "timeindex"] {
                let path = dir.join(format!("{base:020}.{suffix}"));
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
    }
    // A swap file whose base collides with a stale original it is replacing
    // needs that original gone first, which the loop above has done.
    for &base in &output {
        for suffix in ["log", "index", "timeindex"] {
            let swap = dir.join(format!("{base:020}.{suffix}{SWAP_SUFFIX}"));
            if swap.exists() {
                let final_path = dir.join(format!("{base:020}.{suffix}"));
                if final_path.exists() {
                    fs::remove_file(&final_path)?;
                }
            }
        }
    }
    finish_swap(dir, &output)?;
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::remove_file(&marker)?;
    Ok(())
}

/// Read a commit marker: the replaced range and the segments produced.
fn read_compaction_marker(path: &Path) -> Result<(i64, i64, Vec<i64>), StorageError> {
    let contents = fs::read_to_string(path)?;
    let mut parts = contents.split_whitespace();
    let from: i64 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
    let to: i64 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
    let output: Vec<i64> = parts
        .next()
        .map(|bases| {
            bases
                .split(',')
                .filter_map(|base| base.parse::<i64>().ok())
                .collect()
        })
        .unwrap_or_default();
    Ok((from, to, output))
}

fn read_cleaner_checkpoint(dir: &Path) -> Result<Option<i64>, StorageError> {
    match fs::read_to_string(dir.join(CLEANER_CHECKPOINT_FILE)) {
        Ok(contents) => Ok(contents.trim().parse::<i64>().ok()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_cleaner_checkpoint(dir: &Path, offset: i64) -> Result<(), StorageError> {
    let path = dir.join(CLEANER_CHECKPOINT_FILE);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    file.write_all(format!("{offset}\n").as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// fsync a directory so a rename in it is durable.
///
/// Renaming a file is not enough on its own: the directory entry that
/// points at the new name is itself a write, and a crash can lose it. On
/// Windows there is no directory handle to sync, and `ReplaceFile`-style
/// renames are already ordered, so this is a no-op there.
fn sync_dir(dir: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
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
            compact: true,
            // These tests are about what compaction decides, not about when
            // it decides to run, so let every pass run.
            min_cleanable_dirty_ratio: 0.0,
            ..LogConfig::default()
        }
    }

    /// A deletion written now, so `delete.retention.ms` has not run out on
    /// it yet.
    fn tombstone(key: &str) -> RecordBatch {
        RecordBatch::new(
            0,
            0,
            now_ms(),
            vec![Record::tombstone(Bytes::from(key.to_string()), 0)],
        )
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

    /// Offset, key and value; both key and value are `None` when absent,
    /// and a `None` value is a tombstone.
    type ReadRecord = (i64, Option<Vec<u8>>, Option<Vec<u8>>);

    fn read_all(log: &Log) -> Vec<ReadRecord> {
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
                        record.value.map(|value| value.to_vec()),
                    ));
                }
            }
        }
        out
    }

    fn transactional_keyed(producer_id: i64, key: &str, value: &str) -> RecordBatch {
        let mut batch = keyed(key, value);
        batch.producer = Some(brahmaputra_protocol::ProducerMetadata {
            producer_id,
            producer_epoch: 0,
            base_sequence: 0,
        });
        batch.transactional = true;
        batch
    }

    fn marker(producer_id: i64, marker: brahmaputra_protocol::ControlMarker) -> RecordBatch {
        brahmaputra_protocol::control_batch(
            brahmaputra_protocol::ProducerMetadata {
                producer_id,
                producer_epoch: 0,
                base_sequence: 0,
            },
            marker,
            1,
        )
    }

    /// A value written inside a transaction that aborted was never a value
    /// at all. It must neither supersede the committed one nor survive —
    /// on `__consumer_offsets` that is the difference between a group
    /// keeping its committed position and losing it to an aborted commit.
    #[test]
    fn aborted_transactional_records_neither_supersede_nor_survive() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        log.append(keyed("group", "committed-100")).unwrap();
        for filler in 0..8 {
            log.append(keyed(&format!("pad-{filler}"), "x")).unwrap();
        }
        // A transactional write of the same key, then its abort marker.
        log.append_producer_batch(&transactional_keyed(7, "group", "aborted-200").encode(), 0)
            .unwrap();
        log.append_producer_batch(
            &marker(7, brahmaputra_protocol::ControlMarker::Abort).encode(),
            0,
        )
        .unwrap();
        for filler in 0..8 {
            log.append(keyed(&format!("tail-{filler}"), "y")).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(!log.has_ongoing_transactions());

        log.compact().unwrap();

        let values: Vec<String> = read_all(&log)
            .into_iter()
            .filter(|(_, key, _)| key.as_deref() == Some(b"group".as_slice()))
            .map(|(_, _, value)| String::from_utf8(value.unwrap()).unwrap())
            .collect();
        assert_eq!(
            values,
            vec!["committed-100".to_string()],
            "the committed value must survive and the aborted one must not"
        );
    }

    /// A record inside a transaction still open is not decided, so the
    /// range it starts is not cleanable yet: the committed value must not
    /// be removed in favour of one that may be retracted.
    #[test]
    fn an_open_transaction_bounds_what_compaction_may_clean() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        log.append(keyed("group", "committed-100")).unwrap();
        for filler in 0..8 {
            log.append(keyed(&format!("pad-{filler}"), "x")).unwrap();
        }
        log.append_producer_batch(
            &transactional_keyed(9, "group", "undecided-200").encode(),
            0,
        )
        .unwrap();
        for filler in 0..8 {
            log.append(keyed(&format!("tail-{filler}"), "y")).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(log.has_ongoing_transactions());

        log.compact().unwrap();

        let values: Vec<String> = read_all(&log)
            .into_iter()
            .filter(|(_, key, _)| key.as_deref() == Some(b"group".as_slice()))
            .map(|(_, _, value)| String::from_utf8(value.unwrap()).unwrap())
            .collect();
        assert!(
            values.contains(&"committed-100".to_string()),
            "the committed value was removed while its successor was undecided: {values:?}"
        );
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

        let outcome = log.compact().unwrap();
        assert!(
            outcome.records_removed > 0,
            "compaction must remove superseded records"
        );

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
            .map(|(_, _, value)| match value {
                Some(value) => String::from_utf8_lossy(value).into_owned(),
                None => "<tombstone>".to_owned(),
            })
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

        assert_eq!(log.compact().unwrap().records_removed, 0);
        assert_eq!(read_all(&log), before);
    }

    /// The half of compaction that did not exist before: a null value
    /// deletes its key, and once the tombstone itself ages out, the key is
    /// gone from the log entirely.
    #[test]
    fn a_tombstone_deletes_its_key_and_then_itself() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..6 {
            log.append(keyed("doomed", &format!("v{round}"))).unwrap();
            log.append(keyed("kept", &format!("w{round}"))).unwrap();
        }
        log.append(tombstone("doomed")).unwrap();
        // Enough afterwards that the tombstone lands in a sealed segment.
        for round in 6..12 {
            log.append(keyed("kept", &format!("w{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        // Within the grace period the tombstone stays, and it is the only
        // thing left of its key: a consumer reading the log now sees the
        // deletion.
        log.compact().unwrap();
        let after = read_all(&log);
        let doomed: Vec<_> = after
            .iter()
            .filter(|(_, key, _)| key.as_deref() == Some(&b"doomed"[..]))
            .collect();
        assert_eq!(doomed.len(), 1, "only the tombstone may survive: {after:?}");
        assert_eq!(doomed[0].2, None, "and it must still be a tombstone");

        // Past the grace period the tombstone goes too, and the key stops
        // occupying the log at all.
        //
        // The pass that removes it is the next one that has something to
        // do — as in Kafka, a tombstone is dropped while cleaning the range
        // it sits in, not by a sweep of its own, so a topic nobody writes
        // to keeps its tombstones until it is written to again.
        let mut config = config();
        config.delete_retention_ms = 0;
        let mut log = Log::open(dir.path(), config).unwrap();
        for round in 12..20 {
            log.append(keyed("kept", &format!("w{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        log.compact().unwrap();
        let after = read_all(&log);
        assert!(
            !after
                .iter()
                .any(|(_, key, _)| key.as_deref() == Some(&b"doomed"[..])),
            "the tombstone must age out: {after:?}"
        );
        assert!(
            after
                .iter()
                .any(|(_, key, _)| key.as_deref() == Some(&b"kept"[..])),
            "and must take nothing else with it"
        );
    }

    /// An empty value is a value. Only a null one deletes.
    #[test]
    fn an_empty_value_is_not_a_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.delete_retention_ms = 0;
        let mut log = Log::open(dir.path(), config).unwrap();
        for round in 0..8 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.append(keyed("k", "")).unwrap();
        for round in 0..8 {
            log.append(keyed("filler", &format!("f{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        log.compact().unwrap();

        let surviving: Vec<_> = read_all(&log)
            .into_iter()
            .filter(|(_, key, _)| key.as_deref() == Some(&b"k"[..]))
            .collect();
        assert_eq!(surviving.len(), 1);
        assert_eq!(
            surviving[0].2,
            Some(Vec::new()),
            "an empty value survives as an empty value"
        );
    }

    /// Compaction must not quietly convert a batched, compressed log into
    /// a stream of single-record batches: that is a throughput and a disk
    /// regression on every partition it touches.
    #[test]
    fn contiguous_survivors_stay_in_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        // One segment big enough to hold everything, so the whole run can
        // be re-emitted together.
        config.segment_bytes = 1 << 20;
        let mut log = Log::open(dir.path(), config).unwrap();
        // Ten distinct keys in one compressed batch: nothing is removable,
        // and they must come back out as one batch.
        let records: Vec<Record> = (0..10)
            .map(|index| {
                Record::with_key(
                    Bytes::from(format!("k{index}")),
                    Bytes::from(format!("v{index}")),
                    0,
                )
            })
            .collect();
        log.append(RecordBatch::new(0, 0, 1, records).with_compression(Compression::Lz4))
            .unwrap();
        // One superseded key, so the pass actually rewrites.
        log.append(keyed("k0", "old")).unwrap();
        log.append(keyed("k0", "new")).unwrap();
        log.roll_segment().unwrap();
        log.append(keyed("tail", "t")).unwrap();
        log.set_high_watermark(log.log_end_offset()).unwrap();

        assert!(log.compact().unwrap().records_removed > 0);

        let batches = log.read(0, 1 << 20).unwrap();
        let mut decoded = Vec::new();
        for raw in batches {
            let mut bytes = raw;
            decoded.push(RecordBatch::decode(&mut bytes).unwrap());
        }
        let first = &decoded[0];
        assert!(
            first.records.len() > 1,
            "contiguous survivors must share a batch, got {:?}",
            decoded.iter().map(|b| b.records.len()).collect::<Vec<_>>()
        );
        assert_eq!(
            first.compression,
            Compression::Lz4,
            "and must keep the codec they were written with"
        );
    }

    /// A pass that rewrites the whole cleanable log to remove a handful of
    /// records is how compaction becomes the dominant write load on a
    /// partition that is barely changing.
    #[test]
    fn a_mostly_clean_log_is_left_alone_until_it_is_dirty_enough() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.min_cleanable_dirty_ratio = 0.5;
        let mut log = Log::open(dir.path(), config).unwrap();
        for round in 0..24 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();

        // Everything is dirty on the first pass, so it runs.
        assert!(log.compact().unwrap().ran);
        // Immediately afterwards nothing is dirty, so it must not.
        assert!(!log.compact().unwrap().ran);

        // A trickle of new records is still not worth a rewrite.
        log.append(keyed("k", "trickle")).unwrap();
        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(!log.compact().unwrap().ran);
    }

    /// The dirty point has to survive a restart, or every restart makes the
    /// whole log dirty again and the ratio gate stops meaning anything.
    #[test]
    fn the_clean_prefix_is_remembered_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.min_cleanable_dirty_ratio = 0.5;
        {
            let mut log = Log::open(dir.path(), config.clone()).unwrap();
            for round in 0..24 {
                log.append(keyed("k", &format!("v{round}"))).unwrap();
            }
            log.set_high_watermark(log.log_end_offset()).unwrap();
            assert!(log.compact().unwrap().ran);
        }
        let mut log = Log::open(dir.path(), config).unwrap();
        log.set_high_watermark(log.log_end_offset()).unwrap();
        assert!(
            !log.compact().unwrap().ran,
            "a restart must not make the clean prefix dirty again"
        );
    }

    /// A compaction pass interrupted before it committed must leave the
    /// log exactly as it found it.
    #[test]
    fn an_uncommitted_pass_is_discarded_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let before = read_all(&log);
        drop(log);

        // Half-written output, no marker: this is what a crash during the
        // rewrite leaves behind.
        let staging = dir.path().join(COMPACTION_STAGING_DIR);
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("00000000000000000000.log"), b"garbage").unwrap();
        fs::write(dir.path().join("00000000000000000000.log.swap"), b"garbage").unwrap();

        let log = Log::open(dir.path(), config()).unwrap();
        assert_eq!(read_all(&log), before, "the original log must be intact");
        assert!(!staging.exists(), "the staged output must be gone");
    }

    /// One interrupted after it committed must be finished, because the
    /// originals it replaces are already being deleted.
    #[test]
    fn a_committed_pass_is_finished_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let active_base = log.segments.last().unwrap().base_offset;
        drop(log);

        // Stand in for a pass that wrote its output and got as far as the
        // marker: one swap file holding the single surviving record, and
        // every original below the active segment still present.
        let survivor = RecordBatch::new(
            0,
            0,
            1,
            vec![Record::with_key(
                Bytes::from_static(b"k"),
                Bytes::from_static(b"survivor"),
                0,
            )],
        );
        fs::write(
            dir.path().join("00000000000000000000.log.swap"),
            survivor.encode(),
        )
        .unwrap();
        write_compaction_marker(dir.path(), 0, active_base, &[0]).unwrap();

        let log = Log::open(dir.path(), config()).unwrap();
        let after = read_all(&log);
        assert_eq!(
            after[0].2.as_deref(),
            Some(&b"survivor"[..]),
            "the committed output must be what survives: {after:?}"
        );
        assert!(
            !dir.path().join(COMPACTION_COMMIT_FILE).exists(),
            "and the marker must be cleared"
        );
    }

    /// The interruption point that is easiest to get wrong: the swap has
    /// *finished* and only the marker is still there.
    ///
    /// At that moment the output segments are ordinary segments at base
    /// offsets inside the range the marker says was replaced, so recovery
    /// that inferred "inside the range means stale" would delete every
    /// record the pass kept. Naming the output in the marker is what makes
    /// the two distinguishable.
    #[test]
    fn a_finished_swap_with_only_the_marker_left_keeps_its_output() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), config()).unwrap();
        for round in 0..10 {
            log.append(keyed("k", &format!("v{round}"))).unwrap();
        }
        log.set_high_watermark(log.log_end_offset()).unwrap();
        let active_base = log.segments.last().unwrap().base_offset;
        drop(log);

        // Stand in for a completed swap: the output is already in place
        // under its final name, no swap files remain, and the crash landed
        // between the last rename and removing the marker.
        let compacted: Vec<i64> = {
            let log = Log::open(dir.path(), config()).unwrap();
            log.segments
                .iter()
                .map(|segment| segment.base_offset)
                .filter(|base| *base < active_base)
                .collect()
        };
        assert!(!compacted.is_empty(), "the fixture needs sealed segments");
        write_compaction_marker(dir.path(), 0, active_base, &compacted).unwrap();

        let log = Log::open(dir.path(), config()).unwrap();
        assert_eq!(
            read_all(&log).len(),
            10,
            "a finished pass must not have its own output deleted by recovery"
        );
        assert!(!dir.path().join(COMPACTION_COMMIT_FILE).exists());
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
