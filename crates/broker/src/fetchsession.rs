//! Incremental fetch sessions (KIP-227).
//!
//! A consumer holding a thousand partitions of which three are moving
//! resends all thousand descriptors on every poll, and the broker parses
//! all thousand to discover that nine hundred and ninety-seven of them say
//! exactly what they said last time. The cost is per fetch, so it grows
//! with partition count and with poll rate at the same time — which is
//! precisely the shape of a cluster that is doing well.
//!
//! A session moves that state to the broker. The first fetch sends
//! everything and is answered with a session id; every fetch after it sends
//! only the partitions whose fetch offset actually moved, plus any the
//! consumer has stopped holding.
//!
//! # What makes this safe
//!
//! The client remains the authority on where it is reading. The session is
//! a cache of what it last said, never a source of truth: a broker that
//! loses a session answers `FETCH_SESSION_ID_NOT_FOUND`, and the client
//! sends a full fetch again. The failure mode is therefore a wasted round
//! trip, not a consumer reading from the wrong offset.
//!
//! Epochs are what keep the two in step. Every response carries the epoch
//! the next request must present; a request that presents any other epoch
//! is refused, because it means one of the two sides has a view of the
//! session the other does not share — a retried request, a response that
//! was lost, or two consumers using one session id.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Epoch a client sends to open a session.
pub(crate) const INITIAL_EPOCH: i32 = 0;
/// Epoch a client sends to close one.
pub(crate) const CLOSE_EPOCH: i32 = -1;

/// How many sessions a broker will hold before evicting the least recently
/// used one.
///
/// Bounded because a session is memory a *client* causes the broker to
/// allocate: without a cap, opening sessions and abandoning them is a way
/// to exhaust a broker from the outside. Eviction is not a failure — the
/// evicted client gets `FETCH_SESSION_ID_NOT_FOUND` and starts again.
const MAX_SESSIONS: usize = 1_000;

/// How long an untouched session is kept.
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How often idle sessions are actually swept for. Far shorter than the
/// timeout, and far longer than the interval between fetches.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(10);

/// What the broker remembers between two fetches of one session.
struct Session {
    principal: Option<String>,
    epoch: i32,
    /// Fetch offset and byte allowance per partition, exactly as the
    /// client last stated them.
    partitions: HashMap<(String, i32), (i64, i32)>,
    last_used: Instant,
}

/// Every open fetch session on this broker.
#[derive(Default)]
pub(crate) struct FetchSessions {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<i32, Session>,
    next_id: i32,
    /// When idle sessions were last swept.
    ///
    /// The sweep is O(sessions) and this lock is taken on *every* fetch, so
    /// running it each time would put a thousand-entry scan on the hottest
    /// path in the broker to reclaim memory that is in no hurry to be
    /// reclaimed.
    last_expiry: Option<Instant>,
}

/// What a session lookup decided.
pub(crate) enum SessionOutcome {
    /// No session: fetch exactly what the request listed.
    None,
    /// A session's full partition set, and the epoch to answer with.
    Resolved {
        session_id: i32,
        session_epoch: i32,
        partitions: Vec<(String, i32, i64, i32)>,
    },
    /// The session named does not exist, or its epoch is out of step.
    Invalid(i32),
}

impl FetchSessions {
    /// Apply one request to the session cache and return what to fetch.
    ///
    /// `updates` are the partitions the client sent this time, `forgotten`
    /// the ones it has stopped holding.
    #[cfg(test)]
    pub(crate) fn resolve(
        &self,
        session_id: i32,
        session_epoch: i32,
        updates: &[(String, i32, i64, i32)],
        forgotten: &[(String, i32)],
    ) -> SessionOutcome {
        self.resolve_owned(None, session_id, session_epoch, updates, forgotten)
    }

    pub(crate) fn resolve_owned(
        &self,
        principal: Option<&str>,
        session_id: i32,
        session_epoch: i32,
        updates: &[(String, i32, i64, i32)],
        forgotten: &[(String, i32)],
    ) -> SessionOutcome {
        let mut inner = self.inner.lock().expect("fetch sessions");
        inner.expire();

        // Check ownership before any mutation, including close and stale
        // epochs. A guessed id must neither expose nor evict another
        // principal's cached partitions.
        if inner
            .sessions
            .get(&session_id)
            .is_some_and(|session| session.principal.as_deref() != principal)
        {
            return SessionOutcome::Invalid(session_id);
        }

        if session_epoch == CLOSE_EPOCH {
            inner.sessions.remove(&session_id);
            return SessionOutcome::None;
        }
        if session_epoch == INITIAL_EPOCH && session_id == 0 {
            // Not asking for a session at all.
            if updates.is_empty() {
                return SessionOutcome::None;
            }
            return SessionOutcome::None;
        }
        if session_epoch == INITIAL_EPOCH {
            // `session_id` of -1 is the request to open one; anything else
            // with epoch 0 is a client that has lost track of its session,
            // and starting it a new one is the recovery it wants anyway.
            let id = inner.allocate();
            let partitions = updates
                .iter()
                .map(|(topic, partition, offset, max_bytes)| {
                    ((topic.clone(), *partition), (*offset, *max_bytes))
                })
                .collect();
            inner.sessions.insert(
                id,
                Session {
                    principal: principal.map(str::to_owned),
                    epoch: 1,
                    partitions,
                    last_used: Instant::now(),
                },
            );
            return SessionOutcome::Resolved {
                session_id: id,
                session_epoch: 1,
                partitions: updates.to_vec(),
            };
        }

        let Some(session) = inner.sessions.get_mut(&session_id) else {
            return SessionOutcome::Invalid(session_id);
        };
        if session.epoch != session_epoch {
            // Out of step: one side has seen something the other has not.
            // Dropping the session is the only state both can agree on.
            inner.sessions.remove(&session_id);
            return SessionOutcome::Invalid(session_id);
        }
        for (topic, partition, offset, max_bytes) in updates {
            session
                .partitions
                .insert((topic.clone(), *partition), (*offset, *max_bytes));
        }
        for (topic, partition) in forgotten {
            session.partitions.remove(&(topic.clone(), *partition));
        }
        session.epoch = session.epoch.wrapping_add(1).max(1);
        session.last_used = Instant::now();
        let mut partitions: Vec<(String, i32, i64, i32)> = session
            .partitions
            .iter()
            .map(|((topic, partition), (offset, max_bytes))| {
                (topic.clone(), *partition, *offset, *max_bytes)
            })
            .collect();
        // Stable order, so a response's results appear in the same order
        // every time and a client can match them positionally if it wants.
        partitions.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        SessionOutcome::Resolved {
            session_id,
            session_epoch: session.epoch,
            partitions,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().expect("fetch sessions").sessions.len()
    }
}

impl Inner {
    fn allocate(&mut self) -> i32 {
        if self.sessions.len() >= MAX_SESSIONS {
            // Least recently used, so an active consumer is never the one
            // evicted by an idle one.
            if let Some((&victim, _)) = self
                .sessions
                .iter()
                .min_by_key(|(_, session)| session.last_used)
            {
                self.sessions.remove(&victim);
            }
        }
        // Session ids are positive and never zero: zero is how a request
        // says it has no session, so issuing it would be indistinguishable
        // from issuing none.
        self.next_id = self.next_id.wrapping_add(1).max(1);
        while self.sessions.contains_key(&self.next_id) {
            self.next_id = self.next_id.wrapping_add(1).max(1);
        }
        self.next_id
    }

    fn expire(&mut self) {
        let now = Instant::now();
        if self
            .last_expiry
            .is_some_and(|last| now.duration_since(last) < EXPIRY_SWEEP_INTERVAL)
        {
            return;
        }
        self.last_expiry = Some(now);
        self.sessions
            .retain(|_, session| now.duration_since(session.last_used) < SESSION_IDLE_TIMEOUT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn updates() -> Vec<(String, i32, i64, i32)> {
        vec![
            ("orders".to_owned(), 0, 10, 1024),
            ("orders".to_owned(), 1, 20, 1024),
        ]
    }

    /// The first fetch establishes a session and is answered in full.
    #[test]
    fn opening_a_session_returns_everything_it_was_given() {
        let sessions = FetchSessions::default();
        match sessions.resolve(-1, INITIAL_EPOCH, &updates(), &[]) {
            SessionOutcome::Resolved {
                session_id,
                session_epoch,
                partitions,
            } => {
                assert!(session_id > 0);
                assert_eq!(session_epoch, 1);
                assert_eq!(partitions.len(), 2);
            }
            _ => panic!("a new session must resolve"),
        }
    }

    /// The point of the feature: a later fetch that mentions one partition
    /// still fetches both.
    #[test]
    fn an_incremental_fetch_covers_the_partitions_it_did_not_mention() {
        let sessions = FetchSessions::default();
        let SessionOutcome::Resolved { session_id, .. } =
            sessions.resolve(-1, INITIAL_EPOCH, &updates(), &[])
        else {
            panic!("a new session must resolve");
        };
        let moved = vec![("orders".to_owned(), 0, 11, 1024)];
        match sessions.resolve(session_id, 1, &moved, &[]) {
            SessionOutcome::Resolved {
                session_epoch,
                partitions,
                ..
            } => {
                assert_eq!(session_epoch, 2);
                assert_eq!(partitions.len(), 2, "both partitions are still fetched");
                let first = partitions
                    .iter()
                    .find(|(_, partition, _, _)| *partition == 0)
                    .unwrap();
                assert_eq!(first.2, 11, "the moved offset is the one that was sent");
                let second = partitions
                    .iter()
                    .find(|(_, partition, _, _)| *partition == 1)
                    .unwrap();
                assert_eq!(second.2, 20, "and the other keeps what it had");
            }
            _ => panic!("an in-step session must resolve"),
        }
    }

    /// A rebalance takes partitions away, and the session has to follow or
    /// the broker keeps reading partitions nobody holds.
    #[test]
    fn a_forgotten_partition_leaves_the_session() {
        let sessions = FetchSessions::default();
        let SessionOutcome::Resolved { session_id, .. } =
            sessions.resolve(-1, INITIAL_EPOCH, &updates(), &[])
        else {
            panic!("a new session must resolve");
        };
        match sessions.resolve(session_id, 1, &[], &[("orders".to_owned(), 1)]) {
            SessionOutcome::Resolved { partitions, .. } => {
                assert_eq!(partitions.len(), 1);
                assert_eq!(partitions[0].1, 0);
            }
            _ => panic!("an in-step session must resolve"),
        }
    }

    /// An epoch that does not match means the two sides disagree about what
    /// has happened. Refusing is what forces a clean restart.
    #[test]
    fn a_stale_epoch_invalidates_the_session() {
        let sessions = FetchSessions::default();
        let SessionOutcome::Resolved { session_id, .. } =
            sessions.resolve(-1, INITIAL_EPOCH, &updates(), &[])
        else {
            panic!("a new session must resolve");
        };
        assert!(matches!(
            sessions.resolve(session_id, 7, &[], &[]),
            SessionOutcome::Invalid(_)
        ));
        // And the session is gone, so the retry is a clean start rather
        // than a second failure.
        assert!(matches!(
            sessions.resolve(session_id, 1, &[], &[]),
            SessionOutcome::Invalid(_)
        ));
    }

    /// An unknown session is refused rather than silently treated as a
    /// full fetch, so a client cannot keep using an id the broker forgot.
    #[test]
    fn an_unknown_session_is_refused() {
        let sessions = FetchSessions::default();
        assert!(matches!(
            sessions.resolve(4242, 1, &updates(), &[]),
            SessionOutcome::Invalid(4242)
        ));
    }

    /// Closing releases the memory the client caused the broker to hold.
    #[test]
    fn closing_a_session_frees_it() {
        let sessions = FetchSessions::default();
        let SessionOutcome::Resolved { session_id, .. } =
            sessions.resolve(-1, INITIAL_EPOCH, &updates(), &[])
        else {
            panic!("a new session must resolve");
        };
        assert_eq!(sessions.len(), 1);
        assert!(matches!(
            sessions.resolve(session_id, CLOSE_EPOCH, &[], &[]),
            SessionOutcome::None
        ));
        assert_eq!(sessions.len(), 0);
    }

    /// A client that asks for nothing gets no session, and the broker
    /// allocates nothing for it.
    #[test]
    fn another_principal_cannot_read_modify_or_close_a_session() {
        let sessions = FetchSessions::default();
        let SessionOutcome::Resolved { session_id, .. } =
            sessions.resolve_owned(Some("alice"), -1, INITIAL_EPOCH, &updates(), &[])
        else {
            panic!("new session");
        };
        for principal in [None, Some("bob")] {
            for epoch in [1, 7, CLOSE_EPOCH] {
                assert!(matches!(
                    sessions.resolve_owned(principal, session_id, epoch, &[], &[]),
                    SessionOutcome::Invalid(_)
                ));
            }
        }
        assert!(matches!(
            sessions.resolve_owned(Some("alice"), session_id, 1, &[], &[]),
            SessionOutcome::Resolved {
                session_epoch: 2,
                ..
            }
        ));
    }

    #[test]
    fn a_fetch_without_a_session_allocates_nothing() {
        let sessions = FetchSessions::default();
        assert!(matches!(
            sessions.resolve(0, INITIAL_EPOCH, &updates(), &[]),
            SessionOutcome::None
        ));
        assert_eq!(sessions.len(), 0);
    }
}
