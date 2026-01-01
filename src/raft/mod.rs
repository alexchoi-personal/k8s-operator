mod cert_reload;
mod compaction;
mod config;
mod discovery;
mod health;
mod leader;
mod shutdown;
mod types;

pub mod cluster;
pub mod network;
pub mod node;
pub mod storage;

pub use cert_reload::CertReloader;
pub use cluster::ClusterManager;
pub use compaction::CompactionManager;
pub use config::{CompactionConfig, RaftConfig, TlsConfig, TlsMode};
pub use discovery::HeadlessServiceDiscovery;
pub use health::{HealthService, HealthState};
pub use leader::{LeaderElection, LeaderGuard};
pub use node::{RaftNodeManager, StorageBackend};
pub use shutdown::{GracefulShutdown, ShutdownCoordinator};
pub use storage::{MemLogStorage, MemStateMachine, MemStore, StateMachineData};
pub use types::{
    KeyValueStateMachine, NodeId, RaftNode, RaftRequest, RaftResponse, RaftTypeConfig,
    StateMachine, TypeConfig,
};

#[cfg(feature = "rocksdb")]
pub use storage::{RocksDbLogStorage, RocksDbStateMachine, RocksDbStore};
