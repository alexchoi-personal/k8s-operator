use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{Config, Raft, ServerState};

use crate::error::{Error, Result};
use crate::raft::cert_reload::CertReloader;
use crate::raft::compaction::CompactionManager;
use crate::raft::config::RaftConfig;
use crate::raft::leader::LeaderElection;
use crate::raft::network::{start_raft_server_with_health, GrpcNetworkFactory};
use crate::raft::shutdown::GracefulShutdown;
use crate::raft::storage::MemStore;
#[cfg(feature = "rocksdb")]
use crate::raft::storage::RocksDbStore;
use crate::raft::types::{KeyValueStateMachine, RaftNode, StateMachine, TypeConfig};

#[derive(Clone)]
pub enum StorageBackend<SM: StateMachine = KeyValueStateMachine> {
    Memory(Arc<MemStore<SM>>),
    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<RocksDbStore<SM>>),
}

pub struct RaftNodeManager<SM: StateMachine = KeyValueStateMachine> {
    config: RaftConfig,
    raft: Arc<Raft<TypeConfig<SM>>>,
    leader_election: Arc<LeaderElection>,
    storage: StorageBackend<SM>,
    graceful_shutdown: GracefulShutdown,
    _marker: PhantomData<SM>,
}

impl RaftNodeManager<KeyValueStateMachine> {
    pub async fn new(config: RaftConfig) -> Result<Self> {
        let raft_config = Arc::new(Config {
            cluster_name: config.cluster_name.clone(),
            election_timeout_min: config.election_timeout.as_millis() as u64,
            election_timeout_max: config.election_timeout.as_millis() as u64 * 2,
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            ..Default::default()
        });

        let store = Arc::new(MemStore::<KeyValueStateMachine>::new());
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
        let graceful_shutdown = GracefulShutdown::new(config.drain_timeout);

        Ok(Self {
            config,
            raft,
            leader_election,
            storage: StorageBackend::Memory(store),
            graceful_shutdown,
            _marker: PhantomData,
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
            RocksDbStore::with_state_machine(data_dir, KeyValueStateMachine::default())
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
        let graceful_shutdown = GracefulShutdown::new(config.drain_timeout);

        Ok(Self {
            config,
            raft,
            leader_election,
            storage: StorageBackend::RocksDb(store),
            graceful_shutdown,
            _marker: PhantomData,
        })
    }

    pub async fn start_grpc_server(&self, port: u16) -> Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .map_err(|e| Error::Other(format!("Invalid address: {}", e)))?;

        let raft = self.raft.clone();
        let leader_election = self.leader_election.clone();
        let node_id = self.config.node_id;
        let tls = self.config.tls.clone();

        tokio::spawn(async move {
            if let Err(e) =
                start_raft_server_with_health(raft, leader_election, node_id, addr, tls).await
            {
                tracing::error!("Raft gRPC server error: {:?}", e);
            }
        });

        Ok(())
    }

    pub fn start_compaction_manager(&self) {
        let compaction_manager = Arc::new(CompactionManager::new(
            self.raft.clone(),
            self.storage.clone(),
            self.config.compaction.clone(),
        ));

        let shutdown_rx = self.graceful_shutdown.subscribe();
        tokio::spawn(async move {
            compaction_manager.run(shutdown_rx).await;
        });
    }

    pub async fn bootstrap(&self) -> Result<()> {
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
        let pod_name =
            std::env::var("POD_NAME").unwrap_or_else(|_| format!("pod-{}", self.config.node_id));
        format!(
            "{}.{}.{}.svc.cluster.local:8080",
            pod_name, self.config.service_name, self.config.namespace
        )
    }
}

impl<SM: StateMachine> RaftNodeManager<SM> {
    pub fn start_leadership_watcher(&self) {
        let raft = self.raft.clone();
        let leader_election = self.leader_election.clone();
        let mut shutdown_rx = self.graceful_shutdown.subscribe();

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

    pub fn start_signal_handler(&self) {
        self.graceful_shutdown.start_signal_handler();
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

        tracing::info!(
            "Bootstrapping new cluster with node {}",
            self.config.node_id
        );

        let mut members = BTreeMap::new();
        let pod_name =
            std::env::var("POD_NAME").unwrap_or_else(|_| format!("pod-{}", self.config.node_id));
        let addr = format!(
            "{}.{}.{}.svc.cluster.local:8080",
            pod_name, self.config.service_name, self.config.namespace
        );
        members.insert(self.config.node_id, RaftNode { addr });

        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Other(format!("Failed to bootstrap cluster: {:?}", e)))?;

        Ok(())
    }

    pub fn raft(&self) -> &Arc<Raft<TypeConfig<SM>>> {
        &self.raft
    }

    pub fn leader_election(&self) -> &Arc<LeaderElection> {
        &self.leader_election
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn mem_store(&self) -> Option<&Arc<MemStore<SM>>> {
        match &self.storage {
            StorageBackend::Memory(store) => Some(store),
            #[cfg(feature = "rocksdb")]
            _ => None,
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn rocksdb_store(&self) -> Option<&Arc<RocksDbStore<SM>>> {
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
        tracing::info!("Starting graceful shutdown sequence");

        if self.leader_election.is_leader() {
            tracing::info!("Node is leader, attempting to step down before shutdown");
        }

        self.graceful_shutdown
            .coordinator()
            .initiate_shutdown()
            .await;

        if let Err(e) = self.remove_self_from_cluster().await {
            tracing::warn!("Failed to remove self from cluster: {:?}", e);
        }

        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Other(format!("Shutdown error: {:?}", e)))?;

        tracing::info!("Graceful shutdown complete");
        Ok(())
    }

    pub async fn remove_self_from_cluster(&self) -> Result<()> {
        let metrics = self.raft.metrics().borrow().clone();
        let is_voter = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == self.config.node_id);

        if is_voter {
            tracing::info!("Removing self (node {}) from cluster", self.config.node_id);
            self.remove_member(self.config.node_id).await?;
        }
        Ok(())
    }

    pub fn graceful_shutdown(&self) -> &GracefulShutdown {
        &self.graceful_shutdown
    }

    pub fn start_cert_reloader(&self) {
        if !self.config.tls.is_enabled() {
            return;
        }

        let cert_reloader = Arc::new(CertReloader::new(
            self.config.tls.clone(),
            self.config.cert_reload_interval,
        ));

        let shutdown_rx = self.graceful_shutdown.subscribe();
        tokio::spawn(async move {
            cert_reloader.run(shutdown_rx).await;
        });
    }

    pub fn storage(&self) -> &StorageBackend<SM> {
        &self.storage
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
