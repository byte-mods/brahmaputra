//! Background writeback for appended log bytes.
//!
//! A segment asks the kernel to start writing its recent appends out every
//! few megabytes (see `Segment::append_batch`). On Linux that is one
//! non-blocking `sync_file_range` call. Not every filesystem honours it —
//! overlayfs, which is what a container's root is, returns an error — and
//! the only portable way to make the kernel write a file out is
//! `fdatasync`, which blocks until it has. That must not happen on the
//! partition actor, so it is handed to one thread whose whole job is to
//! sit in `fdatasync` so nothing else has to.
//!
//! Requests are coalesced per file: a segment that has already queued a
//! sync does not queue another until that one has run. The channel is
//! unbounded, but the coalescing bounds it to one entry per open segment.

use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, OnceLock};

/// One queued request: the file to sync and the flag to clear afterwards,
/// which is what lets the segment queue the next one.
struct Request {
    file: Arc<File>,
    queued: Arc<AtomicBool>,
}

fn flusher() -> &'static Sender<Request> {
    static FLUSHER: OnceLock<Sender<Request>> = OnceLock::new();
    FLUSHER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name("brahmaputra-writeback".into())
            .spawn(move || {
                for request in receiver {
                    // A failed sync here is not a durability event — the
                    // flush policy owns durability — so it is not reported.
                    let _ = request.file.sync_data();
                    request.queued.store(false, Ordering::Release);
                }
            })
            .expect("spawn the writeback thread");
        sender
    })
}

/// Queue `file` for a background `fdatasync` unless one is already queued
/// for it. Returns whether a request was queued.
pub(crate) fn request_sync(file: &Arc<File>, queued: &Arc<AtomicBool>) -> bool {
    if queued
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }
    let request = Request {
        file: Arc::clone(file),
        queued: Arc::clone(queued),
    };
    if flusher().send(request).is_err() {
        queued.store(false, Ordering::Release);
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One request per file at a time: the second ask while one is queued
    /// is dropped, and once the thread has run the sync a new ask queues.
    #[test]
    fn requests_coalesce_per_file_until_the_sync_has_run() {
        let dir = tempfile::tempdir().unwrap();
        let file = Arc::new(File::create(dir.path().join("segment.log")).unwrap());
        let queued = Arc::new(AtomicBool::new(false));

        assert!(request_sync(&file, &queued), "the first request queues");
        // Either the sync already ran (flag cleared) or it is still queued
        // (flag set); in the latter case a second request must coalesce.
        if queued.load(Ordering::Acquire) {
            assert!(
                !request_sync(&file, &queued),
                "a queued file is not queued twice"
            );
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while queued.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            !queued.load(Ordering::Acquire),
            "the sync must run and clear the flag"
        );
        assert!(
            request_sync(&file, &queued),
            "after it has run, the file can be queued again"
        );
    }
}
