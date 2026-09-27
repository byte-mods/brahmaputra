//! Durable broker-namespaced producer identity allocation.
//!
//! The append-only journal is fsynced before an identity/epoch is returned.
//! Each broker owns the high 31 bits of the positive i64 producer id and a
//! persistent u32 counter owns the low bits, so restarts cannot reuse an id
//! and distinct broker ids cannot collide.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const JOURNAL_FILE: &str = "producer-ids";
const EVENT_LEN: usize = 1 + 8 + 2;
const EVENT_ALLOCATE: u8 = 0;
const EVENT_BUMP: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProducerIdError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("unknown producer id {0}")]
    Unknown(i64),
    #[error("producer epoch {requested} is fenced by epoch {current}")]
    Fenced { requested: i16, current: i16 },
    #[error("producer epoch exhausted for id {0}")]
    EpochExhausted(i64),
    #[error("producer id space exhausted for broker")]
    IdExhausted,
}

#[derive(Debug)]
struct State {
    next_counter: u32,
    epochs: HashMap<i64, i16>,
}

#[derive(Debug)]
pub(crate) struct ProducerIdManager {
    broker_id: i32,
    path: PathBuf,
    state: Mutex<State>,
}

impl ProducerIdManager {
    pub(crate) fn open(data_dir: &Path, broker_id: i32) -> Result<Self, ProducerIdError> {
        if broker_id < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "broker_id must be non-negative for producer id allocation",
            )
            .into());
        }
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(JOURNAL_FILE);
        let mut bytes = Vec::new();
        match File::open(&path) {
            Ok(mut file) => {
                file.read_to_end(&mut bytes)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        // A crash can leave one partial tail event. Only complete, fsynced
        // events are authoritative; trim that tail before future appends.
        let complete_len = bytes.len() / EVENT_LEN * EVENT_LEN;
        if complete_len != bytes.len() {
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)?
                .set_len(complete_len as u64)?;
            bytes.truncate(complete_len);
        }

        let mut epochs = HashMap::new();
        let mut max_counter = 0_u32;
        for event in bytes.as_chunks::<EVENT_LEN>().0 {
            let kind = event[0];
            let producer_id = i64::from_be_bytes(event[1..9].try_into().expect("event id"));
            let epoch = i16::from_be_bytes(event[9..11].try_into().expect("event epoch"));
            if producer_id < 0 || epoch < 0 || !matches!(kind, EVENT_ALLOCATE | EVENT_BUMP) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "producer id journal contains an invalid event",
                )
                .into());
            }
            let namespace = (producer_id as u64 >> 32) as u32;
            if namespace == broker_id as u32 {
                max_counter = max_counter.max(producer_id as u32);
            }
            match kind {
                EVENT_ALLOCATE if epoch == 0 => {
                    epochs.entry(producer_id).or_insert(0);
                }
                EVENT_BUMP => {
                    let current = epochs.get_mut(&producer_id).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "producer epoch bump precedes allocation",
                        )
                    })?;
                    if epoch <= *current {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "producer epochs are not monotonic",
                        )
                        .into());
                    }
                    *current = epoch;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "producer allocation must start at epoch zero",
                    )
                    .into());
                }
            }
        }
        let next_counter = if max_counter == 0 {
            1
        } else {
            max_counter.checked_add(1).unwrap_or(0)
        };
        Ok(Self {
            broker_id,
            path,
            state: Mutex::new(State {
                next_counter,
                epochs,
            }),
        })
    }

    pub(crate) fn allocate(&self) -> Result<(i64, i16), ProducerIdError> {
        let mut state = self.state.lock().expect("producer id state");
        let counter = state.next_counter;
        if counter == 0 {
            return Err(ProducerIdError::IdExhausted);
        }
        let producer_id = (((self.broker_id as u64) << 32) | u64::from(counter)) as i64;
        self.append_event(EVENT_ALLOCATE, producer_id, 0)?;
        state.epochs.insert(producer_id, 0);
        state.next_counter = counter.checked_add(1).unwrap_or(0);
        Ok((producer_id, 0))
    }

    pub(crate) fn bump(
        &self,
        producer_id: i64,
        requested_epoch: i16,
    ) -> Result<(i64, i16), ProducerIdError> {
        let mut state = self.state.lock().expect("producer id state");
        let current = *state
            .epochs
            .get(&producer_id)
            .ok_or(ProducerIdError::Unknown(producer_id))?;
        if requested_epoch != current {
            return Err(ProducerIdError::Fenced {
                requested: requested_epoch,
                current,
            });
        }
        let next = current
            .checked_add(1)
            .ok_or(ProducerIdError::EpochExhausted(producer_id))?;
        self.append_event(EVENT_BUMP, producer_id, next)?;
        state.epochs.insert(producer_id, next);
        Ok((producer_id, next))
    }

    fn append_event(&self, kind: u8, producer_id: i64, epoch: i16) -> io::Result<()> {
        let mut event = [0_u8; EVENT_LEN];
        event[0] = kind;
        event[1..9].copy_from_slice(&producer_id.to_be_bytes());
        event[9..11].copy_from_slice(&epoch.to_be_bytes());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&event)?;
        file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_are_namespaced_persistent_and_epochs_fence() {
        let dir = tempfile::tempdir().unwrap();
        let first = ProducerIdManager::open(dir.path(), 7).unwrap();
        let (id, epoch) = first.allocate().unwrap();
        assert_eq!((id as u64) >> 32, 7);
        assert_eq!(epoch, 0);
        assert_eq!(first.bump(id, epoch).unwrap(), (id, 1));
        assert!(matches!(
            first.bump(id, 0),
            Err(ProducerIdError::Fenced {
                requested: 0,
                current: 1
            })
        ));
        drop(first);

        let reopened = ProducerIdManager::open(dir.path(), 7).unwrap();
        assert_eq!(reopened.bump(id, 1).unwrap(), (id, 2));
        let (next_id, next_epoch) = reopened.allocate().unwrap();
        assert_ne!(next_id, id);
        assert_eq!(next_epoch, 0);

        let other = ProducerIdManager::open(&dir.path().join("other"), 8).unwrap();
        assert_ne!(other.allocate().unwrap().0, next_id);
    }

    #[test]
    fn partial_tail_is_discarded_on_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ProducerIdManager::open(dir.path(), 1).unwrap();
        let first = manager.allocate().unwrap().0;
        drop(manager);
        let path = dir.path().join(JOURNAL_FILE);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[EVENT_ALLOCATE, 1, 2])
            .unwrap();
        let reopened = ProducerIdManager::open(dir.path(), 1).unwrap();
        assert_ne!(reopened.allocate().unwrap().0, first);
        assert_eq!(fs::metadata(path).unwrap().len() as usize % EVENT_LEN, 0);
    }
}
