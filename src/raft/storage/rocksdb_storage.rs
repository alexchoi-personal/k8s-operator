use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::RaftStorage;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use openraft::{ErrorSubject, ErrorVerb};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::types::{RaftNode, RaftResponse, TypeConfig};

const CF_META: &str = "meta";
const CF_LOGS: &str = "logs";
const CF_STATE: &str = "state";

const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_STATE_MACHINE: &[u8] = b"state_machine";
const KEY_SNAPSHOT_META: &[u8] = b"snapshot_meta";
const KEY_SNAPSHOT_DATA: &[u8] = b"snapshot_data";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, RaftNode>,
    pub data: HashMap<String, String>,
}

pub struct RocksDbStore {
    db: Arc<DB>,
    state_machine_cache: RwLock<StateMachineData>,
}

impl RocksDbStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_meta = ColumnFamilyDescriptor::new(CF_META, Options::default());
        let cf_logs = ColumnFamilyDescriptor::new(CF_LOGS, Options::default());
        let cf_state = ColumnFamilyDescriptor::new(CF_STATE, Options::default());

        let db = DB::open_cf_descriptors(&opts, path, vec![cf_meta, cf_logs, cf_state])?;
        let db = Arc::new(db);

        let state_machine = Self::load_state_machine(&db).unwrap_or_default();

        Ok(Self {
            db,
            state_machine_cache: RwLock::new(state_machine),
        })
    }

    fn load_state_machine(db: &DB) -> Option<StateMachineData> {
        let cf = db.cf_handle(CF_STATE)?;
        let bytes = db.get_cf(cf, KEY_STATE_MACHINE).ok()??;
        serde_json::from_slice(&bytes).ok()
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.state_machine_cache.read().await.data.get(key).cloned()
    }

    pub async fn data(&self) -> StateMachineData {
        self.state_machine_cache.read().await.clone()
    }

    fn log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

fn io_error<E: std::error::Error + Send + Sync + 'static>(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, openraft::AnyError::new(&e)).into()
}

impl RaftLogReader<TypeConfig> for Arc<RocksDbStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let cf = self
            .db
            .cf_handle(CF_LOGS)
            .ok_or_else(|| io_error(ErrorSubject::Logs, ErrorVerb::Read, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "logs column family not found",
            )))?;

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
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::From(
            &RocksDbStore::log_key(start),
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            let (key, value) = item.map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;

            if key.len() != 8 {
                continue;
            }

            let index = u64::from_be_bytes(key.as_ref().try_into().unwrap());

            if let Some(end_idx) = end {
                if index >= end_idx {
                    break;
                }
            }

            let entry: Entry<TypeConfig> = serde_json::from_slice(&value)
                .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<RocksDbStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let state_machine = self.state_machine_cache.read().await.clone();
        let data = serde_json::to_vec(&state_machine)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;

        let meta = SnapshotMeta {
            last_log_id: state_machine.last_applied_log,
            last_membership: state_machine.last_membership.clone(),
            snapshot_id: format!("{:?}", state_machine.last_applied_log),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<RocksDbStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| io_error(ErrorSubject::Vote, ErrorVerb::Write, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "meta column family not found",
            )))?;

        let bytes = serde_json::to_vec(vote)
            .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf, KEY_VOTE, &bytes)
            .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| io_error(ErrorSubject::Vote, ErrorVerb::Read, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "meta column family not found",
            )))?;

        match self.db.get_cf(cf, KEY_VOTE) {
            Ok(Some(bytes)) => {
                let vote: Vote<u64> = serde_json::from_slice(&bytes)
                    .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Read, e))?;
                Ok(Some(vote))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io_error(ErrorSubject::Vote, ErrorVerb::Read, e)),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let cf_logs = self.db.cf_handle(CF_LOGS).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Read, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "logs column family not found",
            ))
        })?;

        let cf_meta = self.db.cf_handle(CF_META).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Read, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "meta column family not found",
            ))
        })?;

        let last_purged_log_id: Option<LogId<u64>> = self
            .db
            .get_cf(cf_meta, KEY_LAST_PURGED)
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Read, e))?
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());

        let mut last_log_id: Option<LogId<u64>> = None;
        let iter = self.db.iterator_cf(cf_logs, rocksdb::IteratorMode::End);

        for item in iter {
            if let Ok((_, value)) = item {
                if let Ok(entry) = serde_json::from_slice::<Entry<TypeConfig>>(&value) {
                    last_log_id = Some(entry.log_id);
                }
            }
            break;
        }

        Ok(LogState {
            last_purged_log_id,
            last_log_id: last_log_id.or(last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let cf = self.db.cf_handle(CF_LOGS).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Write, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "logs column family not found",
            ))
        })?;

        for entry in entries {
            let key = RocksDbStore::log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry)
                .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

            self.db
                .put_cf(cf, key, &value)
                .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        }

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let cf = self.db.cf_handle(CF_LOGS).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Delete, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "logs column family not found",
            ))
        })?;

        let start_key = RocksDbStore::log_key(log_id.index);
        let end_key = RocksDbStore::log_key(u64::MAX);

        self.db
            .delete_range_cf(cf, &start_key, &end_key)
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, e))?;

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, e))?;

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let cf_logs = self.db.cf_handle(CF_LOGS).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Delete, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "logs column family not found",
            ))
        })?;

        let cf_meta = self.db.cf_handle(CF_META).ok_or_else(|| {
            io_error(ErrorSubject::Logs, ErrorVerb::Write, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "meta column family not found",
            ))
        })?;

        let last_purged_bytes = serde_json::to_vec(&log_id)
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf_meta, KEY_LAST_PURGED, &last_purged_bytes)
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

        let start_key = RocksDbStore::log_key(0);
        let end_key = RocksDbStore::log_key(log_id.index + 1);

        self.db
            .delete_range_cf(cf_logs, &start_key, &end_key)
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, e))?;

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, e))?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, RaftNode>), StorageError<u64>> {
        let sm = self.state_machine_cache.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<RaftResponse>, StorageError<u64>> {
        let mut responses = Vec::new();
        let mut sm = self.state_machine_cache.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(RaftResponse {
                        success: true,
                        value: None,
                    });
                }
                EntryPayload::Normal(req) => {
                    let old_value = sm.data.insert(req.key.clone(), req.value.clone());
                    responses.push(RaftResponse {
                        success: true,
                        value: old_value,
                    });
                }
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    responses.push(RaftResponse {
                        success: true,
                        value: None,
                    });
                }
            }
        }

        let cf = self.db.cf_handle(CF_STATE).ok_or_else(|| {
            io_error(ErrorSubject::StateMachine, ErrorVerb::Write, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "state column family not found",
            ))
        })?;

        let sm_bytes = serde_json::to_vec(&*sm)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf, KEY_STATE_MACHINE, &sm_bytes)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data: StateMachineData = serde_json::from_slice(snapshot.get_ref())
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;

        let cf = self.db.cf_handle(CF_STATE).ok_or_else(|| {
            io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "state column family not found",
            ))
        })?;

        let meta_bytes = serde_json::to_vec(meta)
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf, KEY_SNAPSHOT_META, &meta_bytes)
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf, KEY_SNAPSHOT_DATA, snapshot.get_ref())
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        let sm_bytes = serde_json::to_vec(&data)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        self.db
            .put_cf(cf, KEY_STATE_MACHINE, &sm_bytes)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

        self.db
            .flush()
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;

        *self.state_machine_cache.write().await = data;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let cf = self.db.cf_handle(CF_STATE).ok_or_else(|| {
            io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "state column family not found",
            ))
        })?;

        let meta_bytes = match self.db.get_cf(cf, KEY_SNAPSHOT_META) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(e) => return Err(io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)),
        };

        let data_bytes = match self.db.get_cf(cf, KEY_SNAPSHOT_DATA) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(None),
            Err(e) => return Err(io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)),
        };

        let meta: SnapshotMeta<u64, RaftNode> = serde_json::from_slice(&meta_bytes)
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?;

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data_bytes.to_vec())),
        }))
    }
}
