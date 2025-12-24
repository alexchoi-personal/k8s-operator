mod combined_storage;

#[cfg(feature = "rocksdb")]
mod rocksdb_storage;

pub use combined_storage::MemStore;

#[cfg(feature = "rocksdb")]
pub use rocksdb_storage::RocksDbStore;

use std::sync::Arc;
use openraft::storage::Adaptor;
use crate::raft::types::TypeConfig;

pub type MemLogStorage = Adaptor<TypeConfig, Arc<MemStore>>;
pub type MemStateMachine = Adaptor<TypeConfig, Arc<MemStore>>;

#[cfg(feature = "rocksdb")]
pub type RocksDbLogStorage = Adaptor<TypeConfig, Arc<RocksDbStore>>;
#[cfg(feature = "rocksdb")]
pub type RocksDbStateMachine = Adaptor<TypeConfig, Arc<RocksDbStore>>;
