mod combined_storage;

#[cfg(feature = "rocksdb")]
mod rocksdb_storage;

pub use combined_storage::{MemStore, StateMachineData};

#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::RocksDbStore;

use std::sync::Arc;
use openraft::storage::Adaptor;
use crate::raft::types::{KeyValueStateMachine, StateMachine, TypeConfig};

pub type MemLogStorage<SM = KeyValueStateMachine> = Adaptor<TypeConfig<SM>, Arc<MemStore<SM>>>;
pub type MemStateMachine<SM = KeyValueStateMachine> = Adaptor<TypeConfig<SM>, Arc<MemStore<SM>>>;

#[cfg(feature = "rocksdb")]
pub type RocksDbLogStorage<SM = KeyValueStateMachine> = Adaptor<TypeConfig<SM>, Arc<RocksDbStore<SM>>>;
#[cfg(feature = "rocksdb")]
pub type RocksDbStateMachine<SM = KeyValueStateMachine> = Adaptor<TypeConfig<SM>, Arc<RocksDbStore<SM>>>;
