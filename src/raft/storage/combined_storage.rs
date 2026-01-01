use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::RaftStorage;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use openraft::{ErrorSubject, ErrorVerb};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::types::{KeyValueStateMachine, RaftNode, StateMachine, TypeConfig};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "SM::Snapshot: Serialize",
    deserialize = "SM::Snapshot: for<'a> Deserialize<'a>"
))]
pub struct StateMachineData<SM: StateMachine> {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, RaftNode>,
    pub snapshot: SM::Snapshot,
}

impl<SM: StateMachine> Default for StateMachineData<SM> {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            snapshot: SM::Snapshot::default(),
        }
    }
}

pub struct MemStoreInner<SM: StateMachine> {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<TypeConfig<SM>>>,
    last_purged_log_id: Option<LogId<u64>>,
    state_machine_data: StateMachineData<SM>,
    machine: SM,
    snapshot: Option<(SnapshotMeta<u64, RaftNode>, Vec<u8>)>,
}

impl<SM: StateMachine> Default for MemStoreInner<SM> {
    fn default() -> Self {
        Self {
            vote: None,
            log: BTreeMap::new(),
            last_purged_log_id: None,
            state_machine_data: StateMachineData::default(),
            machine: SM::default(),
            snapshot: None,
        }
    }
}

#[derive(Clone)]
pub struct MemStore<SM: StateMachine = KeyValueStateMachine> {
    inner: Arc<RwLock<MemStoreInner<SM>>>,
}

impl<SM: StateMachine> Default for MemStore<SM> {
    fn default() -> Self {
        Self::new()
    }
}

impl<SM: StateMachine> MemStore<SM> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStoreInner::default())),
        }
    }

    pub fn with_state_machine(machine: SM) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStoreInner {
                machine,
                ..Default::default()
            })),
        }
    }

    pub async fn snapshot(&self) -> SM::Snapshot {
        self.inner.read().await.state_machine_data.snapshot.clone()
    }

    pub async fn state_machine_data(&self) -> StateMachineData<SM> {
        self.inner.read().await.state_machine_data.clone()
    }
}

impl MemStore<KeyValueStateMachine> {
    pub async fn get(&self, key: &str) -> Option<String> {
        self.inner.read().await.machine.data.get(key).cloned()
    }

    pub async fn data(&self) -> std::collections::HashMap<String, String> {
        self.inner.read().await.machine.data.clone()
    }
}

impl<SM: StateMachine> RaftLogReader<TypeConfig<SM>> for Arc<MemStore<SM>> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig<SM>>>, StorageError<u64>> {
        let inner = self.inner.read().await;
        let entries: Vec<_> = inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(entries)
    }
}

fn io_error<E: std::error::Error + Send + Sync + 'static>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, openraft::AnyError::new(&e)).into()
}

impl<SM: StateMachine> RaftSnapshotBuilder<TypeConfig<SM>> for Arc<MemStore<SM>> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig<SM>>, StorageError<u64>> {
        let inner = self.inner.read().await;
        let data = serde_json::to_vec(&inner.state_machine_data)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;

        let meta = SnapshotMeta {
            last_log_id: inner.state_machine_data.last_applied_log,
            last_membership: inner.state_machine_data.last_membership.clone(),
            snapshot_id: format!("{:?}", inner.state_machine_data.last_applied_log),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl<SM: StateMachine> RaftStorage<TypeConfig<SM>> for Arc<MemStore<SM>> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.write().await.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().await.vote.clone())
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig<SM>>, StorageError<u64>> {
        let inner = self.inner.read().await;
        let last_log_id = inner.log.iter().next_back().map(|(_, entry)| entry.log_id);
        let last_purged = inner.last_purged_log_id;

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log_id.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig<SM>>> + OptionalSend,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().await;
        let keys_to_remove: Vec<_> = inner
            .log
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().await;
        inner.last_purged_log_id = Some(log_id);
        let keys_to_remove: Vec<_> = inner
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, RaftNode>), StorageError<u64>> {
        let inner = self.inner.read().await;
        Ok((
            inner.state_machine_data.last_applied_log,
            inner.state_machine_data.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig<SM>>],
    ) -> Result<Vec<SM::Response>, StorageError<u64>> {
        let mut responses = Vec::new();
        let mut inner = self.inner.write().await;

        for entry in entries {
            inner.state_machine_data.last_applied_log = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(SM::Response::default());
                }
                EntryPayload::Normal(req) => {
                    let response = inner.machine.apply(req);
                    responses.push(response);
                }
                EntryPayload::Membership(mem) => {
                    inner.state_machine_data.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
                    responses.push(SM::Response::default());
                }
            }
        }

        inner.state_machine_data.snapshot = inner.machine.snapshot();

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data: StateMachineData<SM> = serde_json::from_slice(snapshot.get_ref())
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;

        let mut inner = self.inner.write().await;
        inner.machine.restore(data.snapshot.clone());
        inner.state_machine_data = data;
        inner.snapshot = Some((meta.clone(), snapshot.into_inner()));

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig<SM>>>, StorageError<u64>> {
        let inner = self.inner.read().await;
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}
