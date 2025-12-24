use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use parking_lot::RwLock;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::{BasicNode, LogEntry, LogResponse, NodeId, StorageBackend, TypeConfig};

const CF_LOGS: &str = "logs";
const CF_STATE: &str = "state";
const CF_META: &str = "meta";

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_APPLIED: &[u8] = b"last_applied";
const KEY_LAST_MEMBERSHIP: &[u8] = b"last_membership";

pub struct RocksDbStorage {
    db: Arc<DB>,
    state_machine: RocksDbStateMachine,
    log_store: RocksDbLogStore,
}

impl RocksDbStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> k8s_operator_core::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_opts = Options::default();
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_LOGS, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_STATE, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_META, cf_opts),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| k8s_operator_core::OperatorError::StorageError(e.to_string()))?;
        let db = Arc::new(db);

        Ok(Self {
            db: Arc::clone(&db),
            state_machine: RocksDbStateMachine::new(Arc::clone(&db)),
            log_store: RocksDbLogStore::new(db),
        })
    }
}

#[async_trait]
impl StorageBackend for RocksDbStorage {
    type LogStorage = RocksDbLogStore;
    type StateMachine = RocksDbStateMachine;

    async fn create(path: &str) -> k8s_operator_core::Result<Self> {
        Self::new(path)
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

pub struct RocksDbStateMachine {
    db: Arc<DB>,
    last_applied: RwLock<Option<LogId<NodeId>>>,
    last_membership: RwLock<StoredMembership<NodeId, BasicNode>>,
}

impl RocksDbStateMachine {
    fn new(db: Arc<DB>) -> Self {
        let cf = db.cf_handle(CF_META).unwrap();

        let last_applied = db
            .get_cf(&cf, KEY_LAST_APPLIED)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok());

        let last_membership = db
            .get_cf(&cf, KEY_LAST_MEMBERSHIP)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or_default();

        Self {
            db,
            last_applied: RwLock::new(last_applied),
            last_membership: RwLock::new(last_membership),
        }
    }

    fn save_last_applied(&self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        let data = serde_json::to_vec(&log_id)
            .map_err(|e| StorageError::write(&e))?;
        self.db
            .put_cf(&cf, KEY_LAST_APPLIED, &data)
            .map_err(|e| StorageError::write(&e))?;
        *self.last_applied.write() = Some(log_id);
        Ok(())
    }

    fn save_membership(
        &self,
        membership: StoredMembership<NodeId, BasicNode>,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        let data = serde_json::to_vec(&membership)
            .map_err(|e| StorageError::write(&e))?;
        self.db
            .put_cf(&cf, KEY_LAST_MEMBERSHIP, &data)
            .map_err(|e| StorageError::write(&e))?;
        *self.last_membership.write() = membership;
        Ok(())
    }
}

#[async_trait]
impl RaftStateMachine<TypeConfig> for RocksDbStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        Ok((*self.last_applied.read(), self.last_membership.read().clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LogResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut responses = Vec::new();
        let cf = self.db.cf_handle(CF_STATE).unwrap();

        for entry in entries {
            self.save_last_applied(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(LogResponse { value: None });
                }
                EntryPayload::Normal(log_entry) => {
                    let key = format!("data:{}", entry.log_id.index);
                    self.db
                        .put_cf(&cf, key.as_bytes(), &log_entry.data)
                        .map_err(|e| StorageError::write(&e))?;

                    responses.push(LogResponse {
                        value: Some(log_entry.data.clone()),
                    });
                }
                EntryPayload::Membership(mem) => {
                    let stored = StoredMembership::new(Some(entry.log_id), mem);
                    self.save_membership(stored)?;
                    responses.push(LogResponse { value: None });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            db: Arc::clone(&self.db),
            last_applied: RwLock::new(*self.last_applied.read()),
            last_membership: RwLock::new(self.last_membership.read().clone()),
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
        #[derive(serde::Deserialize)]
        struct SnapshotData {
            last_applied: Option<LogId<NodeId>>,
            last_membership: StoredMembership<NodeId, BasicNode>,
            entries: Vec<(String, Vec<u8>)>,
        }

        let data: SnapshotData =
            serde_json::from_slice(snapshot.get_ref())
                .map_err(|e| StorageError::read_snapshot(Some(meta.signature()), &e))?;

        if let Some(log_id) = data.last_applied {
            self.save_last_applied(log_id)?;
        }
        self.save_membership(data.last_membership)?;

        let cf = self.db.cf_handle(CF_STATE).unwrap();
        for (key, value) in data.entries {
            self.db
                .put_cf(&cf, key.as_bytes(), &value)
                .map_err(|e| StorageError::write(&e))?;
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}

#[async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for RocksDbStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<TypeConfig>, StorageError<NodeId>> {
        let last_applied = *self.last_applied.read();
        let last_membership = self.last_membership.read().clone();

        let cf = self.db.cf_handle(CF_STATE).unwrap();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let entries: Vec<(String, Vec<u8>)> = iter
            .filter_map(|r| r.ok())
            .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v.to_vec()))
            .collect();

        #[derive(serde::Serialize)]
        struct SnapshotData {
            last_applied: Option<LogId<NodeId>>,
            last_membership: StoredMembership<NodeId, BasicNode>,
            entries: Vec<(String, Vec<u8>)>,
        }

        let data = SnapshotData {
            last_applied,
            last_membership: last_membership.clone(),
            entries,
        };

        let snapshot_data = serde_json::to_vec(&data)
            .map_err(|e| StorageError::write_snapshot(None, &e))?;

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

pub struct RocksDbLogStore {
    db: Arc<DB>,
}

impl RocksDbLogStore {
    fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    fn log_key(index: u64) -> Vec<u8> {
        format!("log:{:020}", index).into_bytes()
    }
}

#[async_trait]
impl RaftLogReader<TypeConfig> for RocksDbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_LOGS).unwrap();

        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n + 1),
            std::ops::Bound::Excluded(&n) => Some(n),
            std::ops::Bound::Unbounded => None,
        };

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&Self::log_key(start), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::read(&e))?;
            let key_str = String::from_utf8_lossy(&key);

            if !key_str.starts_with("log:") {
                continue;
            }

            let index: u64 = key_str
                .strip_prefix("log:")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if let Some(end_idx) = end {
                if index >= end_idx {
                    break;
                }
            }

            let entry: Entry<TypeConfig> =
                serde_json::from_slice(&value).map_err(|e| StorageError::read(&e))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        let vote = self
            .db
            .get_cf(&cf, KEY_VOTE)
            .map_err(|e| StorageError::read(&e))?
            .and_then(|v| serde_json::from_slice(&v).ok());
        Ok(vote)
    }
}

#[async_trait]
impl RaftLogStorage<TypeConfig> for RocksDbLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<TypeConfig>, StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_LOGS).unwrap();

        let last = self
            .db
            .iterator_cf(&cf, rocksdb::IteratorMode::End)
            .next()
            .and_then(|r| r.ok())
            .and_then(|(_, v)| serde_json::from_slice::<Entry<TypeConfig>>(&v).ok())
            .map(|e| e.log_id);

        Ok(openraft::storage::LogState {
            last_purged_log_id: None,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            db: Arc::clone(&self.db),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        let data = serde_json::to_vec(vote).map_err(|e| StorageError::write(&e))?;
        self.db
            .put_cf(&cf, KEY_VOTE, &data)
            .map_err(|e| StorageError::write(&e))?;
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        match committed {
            Some(log_id) => {
                let data = serde_json::to_vec(&log_id).map_err(|e| StorageError::write(&e))?;
                self.db
                    .put_cf(&cf, KEY_COMMITTED, &data)
                    .map_err(|e| StorageError::write(&e))?;
            }
            None => {
                self.db
                    .delete_cf(&cf, KEY_COMMITTED)
                    .map_err(|e| StorageError::write(&e))?;
            }
        }
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
        let cf = self.db.cf_handle(CF_LOGS).unwrap();

        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry).map_err(|e| StorageError::write(&e))?;
            self.db
                .put_cf(&cf, &key, &value)
                .map_err(|e| StorageError::write(&e))?;
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_LOGS).unwrap();

        let keys_to_delete: Vec<_> = self
            .db
            .iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(
                    &Self::log_key(log_id.index),
                    rocksdb::Direction::Forward,
                ),
            )
            .filter_map(|r| r.ok())
            .map(|(k, _)| k.to_vec())
            .collect();

        for key in keys_to_delete {
            self.db
                .delete_cf(&cf, &key)
                .map_err(|e| StorageError::write(&e))?;
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.db.cf_handle(CF_LOGS).unwrap();

        let keys_to_delete: Vec<_> = self
            .db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .filter_map(|r| r.ok())
            .take_while(|(k, _)| {
                let key_str = String::from_utf8_lossy(k);
                key_str
                    .strip_prefix("log:")
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|idx| idx <= log_id.index)
                    .unwrap_or(false)
            })
            .map(|(k, _)| k.to_vec())
            .collect();

        for key in keys_to_delete {
            self.db
                .delete_cf(&cf, &key)
                .map_err(|e| StorageError::write(&e))?;
        }

        Ok(())
    }
}
