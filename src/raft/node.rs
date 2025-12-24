use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{Config, Raft, ServerState};
use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::raft::config::RaftConfig;
use crate::raft::leader::LeaderElection;
use crate::raft::network::{start_raft_server, GrpcNetworkFactory};
use crate::raft::storage::MemStore;
#[cfg(feature = "rocksdb")]
use crate::raft::storage::RocksDbStore;
use crate::raft::types::{RaftNode, TypeConfig};

pub enum StorageBackend {
    Memory(Arc<MemStore>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<RocksDbStore>),
}

pub struct RaftNodeManager {
    config: RaftConfig,
    raft: Arc<Raft<TypeConfig>>,
    leader_election: Arc<LeaderElection>,
    storage: StorageBackend,
    shutdown_tx: watch::Sender<bool>,
}

impl RaftNodeManager {
    pub async fn new(config: RaftConfig) -> Result<Self> {
        let raft_config = Arc::new(Config {
            cluster_name: config.cluster_name.clone(),
            election_timeout_min: config.election_timeout.as_millis() as u64,
            election_timeout_max: config.election_timeout.as_millis() as u64 * 2,
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            ..Default::default()
        });

        let store = Arc::new(MemStore::new());
        let (log_storage, state_machine) = Adaptor::new(store.clone());
        let network = GrpcNetworkFactory::with_tls(config.tls.clone());

        let raft = Raft::new(
            config.node_id,
            raft_config,
            network,
            log_storage,
            state_machine,
        )
        .await
        .map_err(|e| Error::Other(format!("Failed to create Raft node: {:?}", e)))?;

        let raft = Arc::new(raft);
        let leader_election = Arc::new(LeaderElection::new(config.node_id));
        let (shutdown_tx, _) = watch::channel(false);

        Ok(Self {
            config,
            raft,
            leader_election,
            storage: StorageBackend::Memory(store),
            shutdown_tx,
        })
    }

    #[cfg(feature = "rocksdb")]
    pub async fn new_with_rocksdb<P: AsRef<std::path::Path>>(
        config: RaftConfig,
        data_dir: P,
    ) -> Result<Self> {
        let raft_config = Arc::new(Config {
            cluster_name: config.cluster_name.clone(),
            election_timeout_min: config.election_timeout.as_millis() as u64,
            election_timeout_max: config.election_timeout.as_millis() as u64 * 2,
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            ..Default::default()
        });

        let store = Arc::new(
            RocksDbStore::new(data_dir)
                .map_err(|e| Error::Other(format!("Failed to open RocksDB: {:?}", e)))?,
        );
        let (log_storage, state_machine) = Adaptor::new(store.clone());
        let network = GrpcNetworkFactory::with_tls(config.tls.clone());

        let raft = Raft::new(
            config.node_id,
            raft_config,
            network,
            log_storage,
            state_machine,
        )
        .await
        .map_err(|e| Error::Other(format!("Failed to create Raft node: {:?}", e)))?;

        let raft = Arc::new(raft);
        let leader_election = Arc::new(LeaderElection::new(config.node_id));
        let (shutdown_tx, _) = watch::channel(false);

        Ok(Self {
            config,
            raft,
            leader_election,
            storage: StorageBackend::RocksDb(store),
            shutdown_tx,
        })
    }

    pub fn start_leadership_watcher(&self) {
        let raft = self.raft.clone();
        let leader_election = self.leader_election.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut metrics_rx = raft.metrics();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    Ok(()) = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow();
                        let is_leader = matches!(metrics.state, ServerState::Leader);
                        let was_leader = leader_election.is_leader();

                        if is_leader != was_leader {
                            leader_election.set_leader(is_leader);
                            if is_leader {
                                tracing::info!("This node became the leader");
                            } else {
                                tracing::info!("This node lost leadership");
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn start_grpc_server(&self, port: u16) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .map_err(|e| Error::Other(format!("Invalid address: {}", e)))?;

        let raft = self.raft.clone();
        tokio::spawn(async move {
            if let Err(e) = start_raft_server(raft, addr).await {
                tracing::error!("Raft gRPC server error: {:?}", e);
            }
        });

        Ok(())
    }

    pub async fn bootstrap_or_join(&self) -> Result<()> {
        let is_initialized = self
            .raft
            .is_initialized()
            .await
            .map_err(|e| Error::Other(e.to_string()))?;

        if is_initialized {
            tracing::info!("Raft node already initialized, skipping bootstrap");
            return Ok(());
        }

        tracing::info!("Bootstrapping new cluster with node {}", self.config.node_id);
        self.bootstrap_cluster().await
    }

    async fn bootstrap_cluster(&self) -> Result<()> {
        let mut members = BTreeMap::new();
        members.insert(
            self.config.node_id,
            RaftNode {
                addr: self.get_node_addr(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Other(format!("Failed to bootstrap cluster: {:?}", e)))?;

        Ok(())
    }

    fn get_node_addr(&self) -> String {
        let pod_name = std::env::var("POD_NAME").unwrap_or_else(|_| format!("pod-{}", self.config.node_id));
        format!(
            "{}.{}.{}.svc.cluster.local:8080",
            pod_name, self.config.service_name, self.config.namespace
        )
    }

    pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
        &self.raft
    }

    pub fn leader_election(&self) -> &Arc<LeaderElection> {
        &self.leader_election
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn mem_store(&self) -> Option<&Arc<MemStore>> {
        match &self.storage {
            StorageBackend::Memory(store) => Some(store),
            #[cfg(feature = "rocksdb")]
            _ => None,
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn rocksdb_store(&self) -> Option<&Arc<RocksDbStore>> {
        match &self.storage {
            StorageBackend::RocksDb(store) => Some(store),
            _ => None,
        }
    }

    pub async fn add_member(&self, node_id: u64, addr: String) -> Result<()> {
        let node = RaftNode { addr };

        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(|e| Error::Other(format!("Failed to add learner: {:?}", e)))?;

        let members: BTreeMap<u64, ()> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .map(|id| (id, ()))
            .chain(std::iter::once((node_id, ())))
            .collect();

        self.raft
            .change_membership(members.keys().cloned().collect::<Vec<_>>(), false)
            .await
            .map_err(|e| Error::Other(format!("Failed to change membership: {:?}", e)))?;

        Ok(())
    }

    pub async fn remove_member(&self, node_id: u64) -> Result<()> {
        let members: Vec<u64> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .filter(|id| *id != node_id)
            .collect();

        self.raft
            .change_membership(members, false)
            .await
            .map_err(|e| Error::Other(format!("Failed to remove member: {:?}", e)))?;

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);

        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Other(format!("Shutdown error: {:?}", e)))?;

        Ok(())
    }

    pub fn node_id_from_hostname() -> Result<u64> {
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("POD_NAME"))
            .map_err(|_| Error::Other("HOSTNAME or POD_NAME not set".into()))?;

        let parts: Vec<&str> = hostname.rsplitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(Error::Other(format!(
                "Invalid hostname format: {}",
                hostname
            )));
        }

        parts[0]
            .parse()
            .map_err(|_| Error::Other(format!("Failed to parse node ID from {}", hostname)))
    }
}
