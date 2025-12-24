mod combined_storage;

pub use combined_storage::MemStore;

use std::sync::Arc;
use openraft::storage::Adaptor;
use crate::raft::types::TypeConfig;

pub type MemLogStorage = Adaptor<TypeConfig, Arc<MemStore>>;
pub type MemStateMachine = Adaptor<TypeConfig, Arc<MemStore>>;
