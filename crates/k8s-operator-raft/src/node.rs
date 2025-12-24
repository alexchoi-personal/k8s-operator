use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::error::InitializeError;
use openraft::{Config, Raft};
use parking_lot::RwLock;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::{debug, error, info, warn};

use k8s_operator_core::{ClusterState, NodeInfo, NodeRole, OperatorError, StateMachine};
use k8s_operator_storage::{BasicNode, NodeId, StorageBackend, TypeConfig};

use crate::config::RaftConfig;
use crate::membership::MembershipManager;
use crate::network::RaftNetworkManager;
use crate::raft::raft_service_server::RaftServiceServer;
use crate::state_machine::OperatorStateMachine;

pub type RaftInstance<SM> = Raft<TypeConfig>;

pub struct RaftNode<S, SM>
where
    S: StorageBackend,
    SM: StateMachine,
{
    node_id: NodeId,
    config: RaftConfig,
    raft: Arc<RaftInstance<SM>>,
    membership: MembershipManager,
    state_machine: Arc<RwLock<OperatorStateMachine<SM>>>,
    role_tx: watch::Sender<NodeRole>,
    role_rx: watch::Receiver<NodeRole>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<S, SM> RaftNode<S, SM>
where
    S: StorageBackend,
    SM: StateMachine,
{
    pub async fn new(
        config: RaftConfig,
        storage: S,
        state_machine: SM,
    ) -> k8s_operator_core::Result<Self> {
        let node_id = config.node_id;
        let openraft_config = Arc::new(config.to_openraft_config());

        let operator_sm = OperatorStateMachine::new(state_machine);
        let sm_arc = Arc::new(RwLock::new(operator_sm.clone()));

        let network = RaftNetworkManager::new(node_id);

        let raft = Raft::new(
            node_id,
            openraft_config,
            network,
            storage.log_storage_mut().clone(),
            operator_sm,
        )
        .await
        .map_err(|e| OperatorError::RaftError(e.to_string()))?;

        let (role_tx, role_rx) = watch::channel(NodeRole::Follower);
        let membership = MembershipManager::new(node_id);

        Ok(Self {
            node_id,
            config,
            raft: Arc::new(raft),
            membership,
            state_machine: sm_arc,
            role_tx,
            role_rx,
            shutdown_tx: None,
        })
    }

    pub async fn initialize_cluster(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> k8s_operator_core::Result<()> {
        info!("Initializing Raft cluster with {} members", members.len());

        for (id, node) in &members {
            self.membership.add_voter(*id, node.clone());
        }

        self.raft
            .initialize(members)
            .await
            .map_err(|e| match e {
                InitializeError::NotAllowed(_) => {
                    warn!("Cluster already initialized");
                    OperatorError::RaftError("Cluster already initialized".to_string())
                }
                InitializeError::NotInMembers(_) => {
                    OperatorError::RaftError("Local node not in members".to_string())
                }
            })?;

        info!("Cluster initialized successfully");
        Ok(())
    }

    pub async fn add_learner(&self, node_id: NodeId, node: BasicNode) -> k8s_operator_core::Result<()> {
        info!("Adding learner {} at {}", node_id, node.addr);

        self.raft
            .add_learner(node_id, node.clone(), true)
            .await
            .map_err(|e| OperatorError::RaftError(e.to_string()))?;

        self.membership.add_learner(node_id, node);
        Ok(())
    }

    pub async fn promote_learner(&self, node_id: NodeId) -> k8s_operator_core::Result<()> {
        info!("Promoting learner {} to voter", node_id);

        let membership = self.membership.get_state();
        let mut new_voters = membership.voters().clone();
        new_voters.insert(node_id);

        self.raft
            .change_membership(new_voters, false)
            .await
            .map_err(|e| OperatorError::RaftError(e.to_string()))?;

        self.membership.promote_to_voter(node_id);
        Ok(())
    }

    pub async fn remove_node(&self, node_id: NodeId) -> k8s_operator_core::Result<()> {
        info!("Removing node {}", node_id);

        let membership = self.membership.get_state();
        let mut new_voters = membership.voters().clone();
        new_voters.remove(&node_id);

        if !new_voters.is_empty() {
            self.raft
                .change_membership(new_voters, false)
                .await
                .map_err(|e| OperatorError::RaftError(e.to_string()))?;
        }

        self.membership.remove_node(node_id);
        Ok(())
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn is_leader(&self) -> bool {
        matches!(*self.role_rx.borrow(), NodeRole::Leader)
    }

    pub fn role(&self) -> NodeRole {
        *self.role_rx.borrow()
    }

    pub fn subscribe_role(&self) -> watch::Receiver<NodeRole> {
        self.role_rx.clone()
    }

    pub fn cluster_state(&self) -> ClusterState {
        let metrics = self.raft.metrics().borrow().clone();

        let leader_id = metrics.current_leader;
        let term = metrics.vote.leader_id().term;

        let membership_state = self.membership.get_state();
        let nodes: std::collections::HashMap<u64, NodeInfo> = membership_state
            .all_nodes()
            .iter()
            .map(|(id, node)| {
                let role = if Some(*id) == leader_id {
                    NodeRole::Leader
                } else if membership_state.voters().contains(id) {
                    NodeRole::Follower
                } else {
                    NodeRole::Learner
                };

                (
                    *id,
                    NodeInfo {
                        id: *id,
                        address: node.addr.clone(),
                        role,
                        is_healthy: true,
                    },
                )
            })
            .collect();

        ClusterState {
            leader_id,
            term,
            nodes,
            committed_index: metrics.committed.map(|l| l.index).unwrap_or(0),
            applied_index: metrics.last_applied.map(|l| l.index).unwrap_or(0),
        }
    }

    pub fn raft(&self) -> &Arc<RaftInstance<SM>> {
        &self.raft
    }

    pub async fn start_server(&mut self) -> k8s_operator_core::Result<()> {
        let addr: SocketAddr = self
            .config
            .listen_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| OperatorError::ConfigError(e.to_string()))?;

        info!("Starting Raft gRPC server on {}", addr);

        let service = RaftGrpcService::new(Arc::clone(&self.raft));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let server = Server::builder()
            .add_service(RaftServiceServer::new(service))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            });

        tokio::spawn(async move {
            if let Err(e) = server.await {
                error!("Raft gRPC server error: {}", e);
            }
        });

        self.start_metrics_watcher();

        Ok(())
    }

    fn start_metrics_watcher(&self) {
        let raft = Arc::clone(&self.raft);
        let role_tx = self.role_tx.clone();

        tokio::spawn(async move {
            let mut metrics_rx = raft.metrics();

            loop {
                if metrics_rx.changed().await.is_err() {
                    break;
                }

                let metrics = metrics_rx.borrow().clone();
                let new_role = if Some(metrics.id) == metrics.current_leader {
                    NodeRole::Leader
                } else if metrics.vote.leader_id().node_id == metrics.id {
                    NodeRole::Candidate
                } else {
                    NodeRole::Follower
                };

                let _ = role_tx.send(new_role);
            }
        });
    }

    pub async fn shutdown(&mut self) -> k8s_operator_core::Result<()> {
        info!("Shutting down Raft node {}", self.node_id);

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        self.raft
            .shutdown()
            .await
            .map_err(|e| OperatorError::RaftError(e.to_string()))?;

        Ok(())
    }
}

struct RaftGrpcService<SM>
where
    SM: StateMachine,
{
    raft: Arc<RaftInstance<SM>>,
}

impl<SM> RaftGrpcService<SM>
where
    SM: StateMachine,
{
    fn new(raft: Arc<RaftInstance<SM>>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl<SM> crate::raft::raft_service_server::RaftService for RaftGrpcService<SM>
where
    SM: StateMachine,
{
    async fn vote(
        &self,
        request: tonic::Request<crate::raft::VoteRequest>,
    ) -> Result<tonic::Response<crate::raft::VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        let vote_req = openraft::raft::VoteRequest {
            vote: openraft::Vote::new(req.term, req.candidate_id),
            last_log_id: if req.last_log_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(req.last_log_term, req.candidate_id),
                    req.last_log_index,
                ))
            } else {
                None
            },
        };

        let response = self
            .raft
            .vote(vote_req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(crate::raft::VoteResponse {
            term: response.vote.leader_id().term,
            vote_granted: response.vote_granted,
            last_log_index: response.last_log_id.map(|l| l.index).unwrap_or(0),
        }))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<crate::raft::AppendEntriesRequest>,
    ) -> Result<tonic::Response<crate::raft::AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();

        let entries: Vec<openraft::Entry<TypeConfig>> = req
            .entries
            .into_iter()
            .map(|e| {
                let payload = if e.data.is_empty() {
                    openraft::EntryPayload::Blank
                } else {
                    openraft::EntryPayload::Normal(k8s_operator_storage::LogEntry { data: e.data })
                };

                openraft::Entry {
                    log_id: openraft::LogId::new(
                        openraft::CommittedLeaderId::new(e.term, req.leader_id),
                        e.index,
                    ),
                    payload,
                }
            })
            .collect();

        let append_req = openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new(req.term, req.leader_id),
            prev_log_id: if req.prev_log_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(req.prev_log_term, req.leader_id),
                    req.prev_log_index,
                ))
            } else {
                None
            },
            entries,
            leader_commit: if req.leader_commit > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(req.term, req.leader_id),
                    req.leader_commit,
                ))
            } else {
                None
            },
        };

        let response = self
            .raft
            .append_entries(append_req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let (success, term) = match response {
            openraft::raft::AppendEntriesResponse::Success => (true, req.term),
            openraft::raft::AppendEntriesResponse::HigherVote(vote) => {
                (false, vote.leader_id().term)
            }
            _ => (false, req.term),
        };

        Ok(tonic::Response::new(crate::raft::AppendEntriesResponse {
            term,
            success,
            match_index: 0,
            conflict_index: 0,
            conflict_term: 0,
        }))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<crate::raft::InstallSnapshotRequest>>,
    ) -> Result<tonic::Response<crate::raft::InstallSnapshotResponse>, tonic::Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut data = Vec::new();
        let mut meta: Option<crate::raft::InstallSnapshotRequest> = None;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if meta.is_none() {
                meta = Some(chunk.clone());
            }
            data.extend(chunk.data);

            if chunk.done {
                break;
            }
        }

        let meta = meta.ok_or_else(|| tonic::Status::invalid_argument("Empty snapshot stream"))?;

        let snapshot_meta = openraft::SnapshotMeta {
            last_log_id: if meta.last_included_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(meta.last_included_term, meta.leader_id),
                    meta.last_included_index,
                ))
            } else {
                None
            },
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: format!("{}-{}", meta.last_included_term, meta.last_included_index),
        };

        let snapshot = openraft::Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(std::io::Cursor::new(data)),
        };

        let install_req = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote::new(meta.term, meta.leader_id),
            snapshot,
        };

        let response = self
            .raft
            .install_snapshot(install_req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(crate::raft::InstallSnapshotResponse {
            term: response.vote.leader_id().term,
        }))
    }
}
