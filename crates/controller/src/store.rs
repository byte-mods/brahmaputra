//! Durable OpenRaft storage backed by transactional `redb` tables.
//!
//! Vote/commit metadata, log entries, the state machine, and the current
//! snapshot are separate records. A heartbeat therefore appends one log row
//! instead of rewriting an ever-growing log image, while every storage callback
//! still commits atomically with immediate durability before returning.

// OpenRaft fixes the storage error type, including for blocking write closures.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftStorage, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, Durability, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::NodeId;

const STORE_FILE_NAME: &str = "controller-raft.redb";
const META_KEY: &str = "meta-v1";
const STATE_MACHINE_KEY: &str = "state-machine-v1";
const SNAPSHOT_KEY: &str = "snapshot-v1";
const STORE_FORMAT_VERSION: u32 = 1;
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const STATE_MACHINE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("raft_state_machine");
const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_snapshot");

/// The replicated controller request recorded in the Raft log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRequest {
    pub client: String,
    pub serial: u64,
    pub status: String,
}

/// Response retained for OpenRaft's client de-duplication contract.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientResponse(pub Option<String>);

openraft::declare_raft_types!(
    /// OpenRaft type configuration used by controller nodes.
    pub TypeConfig:
        D = ClientRequest,
        R = ClientResponse,
        Node = (),
);

/// The complete application state machine represented by controller metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ControllerStateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, ()>,
    pub client_serial_responses: HashMap<String, (u64, Option<String>)>,
    pub client_status: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, ()>,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedMeta {
    format_version: u32,
    cluster_id: String,
    node_id: NodeId,
    last_purged_log_id: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    snapshot_sequence: u64,
    vote: Option<Vote<NodeId>>,
}

impl PersistedMeta {
    fn pristine(cluster_id: String, node_id: NodeId) -> Self {
        Self {
            format_version: STORE_FORMAT_VERSION,
            cluster_id,
            node_id,
            last_purged_log_id: None,
            committed: None,
            snapshot_sequence: 0,
            vote: None,
        }
    }

    fn validate_identity(&self, cluster_id: &str, node_id: NodeId) -> Result<(), StoreOpenError> {
        if self.format_version != STORE_FORMAT_VERSION {
            return Err(StoreOpenError::Incompatible(format!(
                "controller store format {} is unsupported (expected {})",
                self.format_version, STORE_FORMAT_VERSION
            )));
        }
        if self.cluster_id != cluster_id {
            return Err(StoreOpenError::Incompatible(format!(
                "controller store belongs to cluster {:?}, not {:?}",
                self.cluster_id, cluster_id
            )));
        }
        if self.node_id != node_id {
            return Err(StoreOpenError::Incompatible(format!(
                "controller store belongs to node {}, not {}",
                self.node_id, node_id
            )));
        }
        Ok(())
    }
}

struct StoreState {
    meta: PersistedMeta,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    state_machine: ControllerStateMachine,
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreOpenError {
    #[error("failed to create controller data directory: {0}")]
    CreateDirectory(#[source] std::io::Error),
    #[error("failed to open controller database: {0}")]
    Database(#[source] Box<redb::DatabaseError>),
    #[error("failed to start controller database transaction: {0}")]
    Transaction(#[source] Box<redb::TransactionError>),
    #[error("failed to open controller table: {0}")]
    Table(#[source] Box<redb::TableError>),
    #[error("failed to access controller state: {0}")]
    Storage(#[source] Box<redb::StorageError>),
    #[error("failed to commit controller state: {0}")]
    Commit(#[source] Box<redb::CommitError>),
    #[error("controller state is corrupt: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("controller state is incompatible: {0}")]
    Incompatible(String),
}

/// A restart-safe OpenRaft store.
pub(crate) struct DurableStore {
    database: Database,
    database_path: PathBuf,
    state: Arc<RwLock<StoreState>>,
}

impl DurableStore {
    pub(crate) async fn open(
        data_dir: &Path,
        cluster_id: &str,
        node_id: NodeId,
    ) -> Result<Arc<Self>, StoreOpenError> {
        std::fs::create_dir_all(data_dir).map_err(StoreOpenError::CreateDirectory)?;
        let database_path = data_dir.join(STORE_FILE_NAME);
        let database = Database::create(&database_path)
            .map_err(|error| StoreOpenError::Database(Box::new(error)))?;

        let mut transaction = database
            .begin_write()
            .map_err(|error| StoreOpenError::Transaction(Box::new(error)))?;
        transaction.set_durability(Durability::Immediate);

        let meta = {
            let mut table = transaction
                .open_table(META_TABLE)
                .map_err(|error| StoreOpenError::Table(Box::new(error)))?;
            let encoded = table
                .get(META_KEY)
                .map_err(|error| StoreOpenError::Storage(Box::new(error)))?
                .map(|value| value.value().to_vec());
            match encoded {
                Some(encoded) => {
                    let meta: PersistedMeta =
                        serde_json::from_slice(&encoded).map_err(StoreOpenError::Decode)?;
                    meta.validate_identity(cluster_id, node_id)?;
                    meta
                }
                None => {
                    let meta = PersistedMeta::pristine(cluster_id.to_owned(), node_id);
                    let encoded = serde_json::to_vec(&meta).map_err(StoreOpenError::Decode)?;
                    table
                        .insert(META_KEY, encoded.as_slice())
                        .map_err(|error| StoreOpenError::Storage(Box::new(error)))?;
                    meta
                }
            }
        };

        let state_machine = {
            let mut table = transaction
                .open_table(STATE_MACHINE_TABLE)
                .map_err(|error| StoreOpenError::Table(Box::new(error)))?;
            let encoded = table
                .get(STATE_MACHINE_KEY)
                .map_err(|error| StoreOpenError::Storage(Box::new(error)))?
                .map(|value| value.value().to_vec());
            match encoded {
                Some(encoded) => {
                    serde_json::from_slice(&encoded).map_err(StoreOpenError::Decode)?
                }
                None => {
                    let state_machine = ControllerStateMachine::default();
                    let encoded =
                        serde_json::to_vec(&state_machine).map_err(StoreOpenError::Decode)?;
                    table
                        .insert(STATE_MACHINE_KEY, encoded.as_slice())
                        .map_err(|error| StoreOpenError::Storage(Box::new(error)))?;
                    state_machine
                }
            }
        };

        let current_snapshot = {
            let table = transaction
                .open_table(SNAPSHOT_TABLE)
                .map_err(|error| StoreOpenError::Table(Box::new(error)))?;
            let encoded = table
                .get(SNAPSHOT_KEY)
                .map_err(|error| StoreOpenError::Storage(Box::new(error)))?
                .map(|value| value.value().to_vec());
            encoded
                .map(|value| serde_json::from_slice(&value))
                .transpose()
                .map_err(StoreOpenError::Decode)?
        };

        let log = {
            let table = transaction
                .open_table(LOG_TABLE)
                .map_err(|error| StoreOpenError::Table(Box::new(error)))?;
            let mut log = BTreeMap::new();
            for row in table
                .iter()
                .map_err(|error| StoreOpenError::Storage(Box::new(error)))?
            {
                let (index, encoded) =
                    row.map_err(|error| StoreOpenError::Storage(Box::new(error)))?;
                let entry =
                    serde_json::from_slice(encoded.value()).map_err(StoreOpenError::Decode)?;
                log.insert(index.value(), entry);
            }
            log
        };

        transaction
            .commit()
            .map_err(|error| StoreOpenError::Commit(Box::new(error)))?;

        Ok(Arc::new(Self {
            database,
            database_path,
            state: Arc::new(RwLock::new(StoreState {
                meta,
                log,
                state_machine,
                current_snapshot,
            })),
        }))
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) async fn state_machine(&self) -> ControllerStateMachine {
        self.state.read().await.state_machine.clone()
    }

    #[allow(clippy::result_large_err)] // OpenRaft fixes the storage error type.
    fn begin_write(&self) -> Result<WriteTransaction, StorageError<NodeId>> {
        let mut transaction = self
            .database
            .begin_write()
            .map_err(|error| StorageIOError::write(&error))?;
        transaction.set_durability(Durability::Immediate);
        Ok(transaction)
    }

    #[allow(clippy::result_large_err)] // OpenRaft fixes the storage error type.
    fn commit(transaction: WriteTransaction) -> Result<(), StorageError<NodeId>> {
        transaction
            .commit()
            .map_err(|error| StorageIOError::write(&error))?;
        Ok(())
    }

    #[allow(clippy::result_large_err)] // OpenRaft fixes the storage error type.
    fn write_json<K, V>(
        transaction: &WriteTransaction,
        table_definition: TableDefinition<K, &[u8]>,
        key: K::SelfType<'_>,
        value: &V,
    ) -> Result<(), StorageError<NodeId>>
    where
        K: redb::Key + 'static,
        V: Serialize,
    {
        let encoded = serde_json::to_vec(value)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let mut table = transaction
            .open_table(table_definition)
            .map_err(|error| StorageIOError::write(&error))?;
        table
            .insert(key, encoded.as_slice())
            .map_err(|error| StorageIOError::write(&error))?;
        Ok(())
    }

    async fn update_meta(
        self: &Arc<Self>,
        mutation: impl FnOnce(&mut PersistedMeta) + Send + 'static,
    ) -> Result<(), StorageError<NodeId>> {
        self.run_blocking_write(move |store, state| {
            let mut next = state.meta.clone();
            mutation(&mut next);
            let transaction = store.begin_write()?;
            Self::write_json(&transaction, META_TABLE, META_KEY, &next)?;
            Self::commit(transaction)?;
            state.meta = next;
            Ok(())
        })
        .await
    }

    /// redb transactions and immediate durability can block on disk. Keep that
    /// work off the executor that runs Raft RPCs, broker leases and data I/O.
    /// The owned operation continues through commit/publication if its caller
    /// is cancelled, preserving the store's durable-before-visible ordering.
    async fn run_blocking_write<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl FnOnce(&Self, &mut StoreState) -> Result<T, StorageError<NodeId>>
            + Send
            + 'static,
    ) -> Result<T, StorageError<NodeId>> {
        let store = self.clone();
        let mut state = self.state.clone().write_owned().await;
        tokio::task::spawn_blocking(move || operation(&store, &mut state))
            .await
            .map_err(|error| StorageIOError::write(&std::io::Error::other(error.to_string())))?
    }
}

impl RaftLogReader<TypeConfig> for Arc<DurableStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let state = self.state.read().await;
        Ok(state
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<DurableStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        self.run_blocking_write(move |store, state| {
            let mut next_meta = state.meta.clone();
            next_meta.snapshot_sequence = next_meta.snapshot_sequence.saturating_add(1);
            let data = serde_json::to_vec(&state.state_machine)
                .map_err(|error| StorageIOError::read_state_machine(&error))?;
            let snapshot_id = match state.state_machine.last_applied_log {
                Some(last) => format!(
                    "{}-{}-{}",
                    last.leader_id, last.index, next_meta.snapshot_sequence
                ),
                None => format!("--{}", next_meta.snapshot_sequence),
            };
            let snapshot = StoredSnapshot {
                meta: SnapshotMeta {
                    last_log_id: state.state_machine.last_applied_log,
                    last_membership: state.state_machine.last_membership.clone(),
                    snapshot_id,
                },
                data,
            };

            let transaction = store.begin_write()?;
            DurableStore::write_json(&transaction, META_TABLE, META_KEY, &next_meta)?;
            DurableStore::write_json(&transaction, SNAPSHOT_TABLE, SNAPSHOT_KEY, &snapshot)?;
            DurableStore::commit(transaction)?;
            state.meta = next_meta;
            state.current_snapshot = Some(snapshot.clone());

            Ok(Snapshot {
                meta: snapshot.meta,
                snapshot: Box::new(Cursor::new(snapshot.data)),
            })
        })
        .await
    }
}

impl RaftStorage<TypeConfig> for Arc<DurableStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let vote = *vote;
        self.update_meta(move |meta| meta.vote = Some(vote)).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.state.read().await.meta.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.update_meta(move |meta| meta.committed = committed)
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.state.read().await.meta.committed)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let state = self.state.read().await;
        let last_log_id = state
            .log
            .last_key_value()
            .map(|(_, entry)| *entry.get_log_id())
            .or(state.meta.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: state.meta.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.run_blocking_write(move |store, state| {
            let transaction = store.begin_write()?;
            {
                let mut table = transaction
                    .open_table(LOG_TABLE)
                    .map_err(|error| StorageIOError::write_logs(&error))?;
                for entry in &entries {
                    let encoded = serde_json::to_vec(entry)
                        .map_err(|error| StorageIOError::write_log_entry(entry.log_id, &error))?;
                    table
                        .insert(&entry.log_id.index, encoded.as_slice())
                        .map_err(|error| StorageIOError::write_log_entry(entry.log_id, &error))?;
                }
            }
            DurableStore::commit(transaction)?;
            for entry in entries {
                state.log.insert(entry.log_id.index, entry);
            }
            Ok(())
        })
        .await
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.run_blocking_write(move |store, state| {
            let indexes: Vec<_> = state
                .log
                .range(log_id.index..)
                .map(|(index, _)| *index)
                .collect();
            let transaction = store.begin_write()?;
            {
                let mut table = transaction
                    .open_table(LOG_TABLE)
                    .map_err(|error| StorageIOError::write_logs(&error))?;
                for index in &indexes {
                    table
                        .remove(index)
                        .map_err(|error| StorageIOError::write_logs(&error))?;
                }
            }
            DurableStore::commit(transaction)?;
            for index in indexes {
                state.log.remove(&index);
            }
            Ok(())
        })
        .await
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.run_blocking_write(move |store, state| {
            let indexes: Vec<_> = state
                .log
                .range(..=log_id.index)
                .map(|(index, _)| *index)
                .collect();
            let mut next_meta = state.meta.clone();
            next_meta.last_purged_log_id = Some(log_id);
            let transaction = store.begin_write()?;
            DurableStore::write_json(&transaction, META_TABLE, META_KEY, &next_meta)?;
            {
                let mut table = transaction
                    .open_table(LOG_TABLE)
                    .map_err(|error| StorageIOError::write_logs(&error))?;
                for index in &indexes {
                    table
                        .remove(index)
                        .map_err(|error| StorageIOError::write_logs(&error))?;
                }
            }
            DurableStore::commit(transaction)?;
            state.meta = next_meta;
            for index in indexes {
                state.log.remove(&index);
            }
            Ok(())
        })
        .await
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, ()>), StorageError<NodeId>> {
        let state = self.state.read().await;
        Ok((
            state.state_machine.last_applied_log,
            state.state_machine.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
        let entries = entries.to_vec();
        self.run_blocking_write(move |store, state| {
            let mut next = state.state_machine.clone();
            let mut responses = Vec::with_capacity(entries.len());
            for entry in &entries {
                next.last_applied_log = Some(entry.log_id);
                match &entry.payload {
                    EntryPayload::Blank => responses.push(ClientResponse(None)),
                    EntryPayload::Normal(data) => {
                        if let Some((serial, response)) =
                            next.client_serial_responses.get(&data.client)
                        {
                            if *serial == data.serial {
                                responses.push(ClientResponse(response.clone()));
                                continue;
                            }
                        }
                        let previous = next
                            .client_status
                            .insert(data.client.clone(), data.status.clone());
                        next.client_serial_responses
                            .insert(data.client.clone(), (data.serial, previous.clone()));
                        responses.push(ClientResponse(previous));
                    }
                    EntryPayload::Membership(membership) => {
                        next.last_membership =
                            StoredMembership::new(Some(entry.log_id), membership.clone());
                        responses.push(ClientResponse(None));
                    }
                }
            }

            let transaction = store.begin_write()?;
            DurableStore::write_json(&transaction, STATE_MACHINE_TABLE, STATE_MACHINE_KEY, &next)?;
            DurableStore::commit(transaction)?;
            state.state_machine = next;
            Ok(responses)
        })
        .await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ()>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };
        let state_machine: ControllerStateMachine = serde_json::from_slice(&snapshot.data)
            .map_err(|error| {
                StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &error)
            })?;
        self.run_blocking_write(move |store, state| {
            let transaction = store.begin_write()?;
            DurableStore::write_json(
                &transaction,
                STATE_MACHINE_TABLE,
                STATE_MACHINE_KEY,
                &state_machine,
            )?;
            DurableStore::write_json(&transaction, SNAPSHOT_TABLE, SNAPSHOT_KEY, &snapshot)?;
            DurableStore::commit(transaction)?;
            state.state_machine = state_machine;
            state.current_snapshot = Some(snapshot);
            Ok(())
        })
        .await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .state
            .read()
            .await
            .current_snapshot
            .clone()
            .map(|snapshot| Snapshot {
                meta: snapshot.meta,
                snapshot: Box::new(Cursor::new(snapshot.data)),
            }))
    }
}

#[cfg(test)]
mod tests {
    use openraft::storage::Adaptor;
    use openraft::testing::{StoreBuilder, Suite};

    use super::*;

    struct DurableStoreBuilder;

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_disk_write_yields_the_executor_and_survives_caller_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let store = DurableStore::open(directory.path(), "blocked-disk", 1)
            .await
            .unwrap();
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker_store = store.clone();
        let blocker = std::thread::spawn(move || {
            let transaction = blocker_store.database.begin_write().unwrap();
            locked_tx.send(()).unwrap();
            // A hard timeout releases the disk lock even if a regression blocks
            // this test's single-thread executor, so the failure cannot hang CI.
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(2));
            transaction.abort().unwrap();
        });
        locked_rx.await.unwrap();
        let mut writer_store = store.clone();
        let started = std::time::Instant::now();
        let write = tokio::spawn(async move { writer_store.save_vote(&Vote::new(7, 1)).await });
        // Wait until the operation owns the state lock and has been submitted.
        while store.state.try_read().is_ok() && !write.is_finished() {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let responsive = started.elapsed() < std::time::Duration::from_secs(1);
        write.abort();
        let _ = write.await;
        let _ = release_tx.send(());
        blocker.join().unwrap();
        assert!(
            responsive,
            "a blocked disk transaction monopolized the async executor"
        );
        // Cancellation cannot split durable commit from in-memory publication.
        assert_eq!(store.state.read().await.meta.vote, Some(Vote::new(7, 1)));
        drop(store);
        let mut reopened = DurableStore::open(directory.path(), "blocked-disk", 1)
            .await
            .unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(7, 1)));
    }

    impl
        StoreBuilder<
            TypeConfig,
            Adaptor<TypeConfig, Arc<DurableStore>>,
            Adaptor<TypeConfig, Arc<DurableStore>>,
            tempfile::TempDir,
        > for DurableStoreBuilder
    {
        async fn build(
            &self,
        ) -> Result<
            (
                tempfile::TempDir,
                Adaptor<TypeConfig, Arc<DurableStore>>,
                Adaptor<TypeConfig, Arc<DurableStore>>,
            ),
            StorageError<NodeId>,
        > {
            let directory = tempfile::tempdir().map_err(|error| StorageIOError::write(&error))?;
            let store = DurableStore::open(directory.path(), "storage-suite", 0)
                .await
                .map_err(|error| {
                    let error = std::io::Error::other(error.to_string());
                    StorageIOError::write(&error)
                })?;
            let (log_store, state_machine) = Adaptor::new(store);
            Ok((directory, log_store, state_machine))
        }
    }

    #[test]
    fn passes_openraft_storage_suite() {
        Suite::test_all(DurableStoreBuilder).expect("durable store must satisfy OpenRaft's suite");
    }
}
