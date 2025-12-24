use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use parking_lot::RwLock;

use crate::{BasicNode, LogEntry, LogResponse, NodeId, StorageBackend, TypeConfig};

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryStateMachineData {
    pub last_applied: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub data: BTreeMap<String, Vec<u8>>,
}

#[derive(Debug, Default)]
pub struct MemoryStateMachine {
    data: Arc<RwLock<MemoryStateMachineData>>,
}

impl MemoryStateMachine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(MemoryStateMachineData::default())),
        }
    }
}

#[async_trait]
impl RaftStateMachine<TypeConfig> for MemoryStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let data = self.data.read();
        Ok((data.last_applied, data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LogResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut responses = Vec::new();
        let mut data = self.data.write();

        for entry in entries {
            data.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(LogResponse { value: None });
                }
                EntryPayload::Normal(log_entry) => {
                    responses.push(LogResponse {
                        value: Some(log_entry.data.clone()),
                    });
                }
                EntryPayload::Membership(mem) => {
                    data.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    responses.push(LogResponse { value: None });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            data: Arc::clone(&self.data),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_data: MemoryStateMachineData =
            serde_json::from_slice(snapshot.get_ref()).map_err(|e| {
                StorageError::read_snapshot(Some(meta.signature()), &e)
            })?;

        let mut data = self.data.write();
        *data = new_data;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}

#[async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for MemoryStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data = self.data.read();
        let snapshot_data = serde_json::to_vec(&*data).map_err(|e| {
            StorageError::write_snapshot(None, &e)
        })?;

        let last_applied = data.last_applied;
        let last_membership = data.last_membership.clone();
        drop(data);

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied.map(|l| l.leader_id.term).unwrap_or(0),
            last_applied.map(|l| l.leader_id.node_id).unwrap_or(0),
            last_applied.map(|l| l.index).unwrap_or(0)
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        Ok(openraft::Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

#[derive(Debug, Default)]
pub struct MemoryLogStore {
    vote: RwLock<Option<Vote<NodeId>>>,
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RaftLogReader<TypeConfig> for MemoryLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read();
        let entries = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read())
    }
}

#[async_trait]
impl RaftLogStorage<TypeConfig> for MemoryLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read();
        let last = log.iter().next_back().map(|(_, e)| e.log_id);
        let last_purged = None;
        Ok(openraft::storage::LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            vote: RwLock::new(*self.vote.read()),
            log: RwLock::new(self.log.read().clone()),
            committed: RwLock::new(*self.committed.read()),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        *self.vote.write() = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        *self.committed.write() = committed;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut log = self.log.write();
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write();
        let keys_to_remove: Vec<_> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write();
        let keys_to_remove: Vec<_> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            log.remove(&key);
        }
        Ok(())
    }
}

pub struct MemoryStorage {
    log_store: MemoryLogStore,
    state_machine: MemoryStateMachine,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            log_store: MemoryLogStore::new(),
            state_machine: MemoryStateMachine::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    type LogStorage = MemoryLogStore;
    type StateMachine = MemoryStateMachine;

    async fn create(_path: &str) -> k8s_operator_core::Result<Self> {
        Ok(Self::new())
    }

    fn log_storage(&self) -> &Self::LogStorage {
        &self.log_store
    }

    fn log_storage_mut(&mut self) -> &mut Self::LogStorage {
        &mut self.log_store
    }

    fn state_machine(&self) -> &Self::StateMachine {
        &self.state_machine
    }

    fn state_machine_mut(&mut self) -> &mut Self::StateMachine {
        &mut self.state_machine
    }

    async fn close(&mut self) -> k8s_operator_core::Result<()> {
        Ok(())
    }
}
