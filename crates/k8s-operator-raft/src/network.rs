use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

use k8s_operator_storage::{BasicNode, NodeId, TypeConfig};

use crate::raft::raft_service_client::RaftServiceClient;

pub struct RaftNetworkManager {
    local_node_id: NodeId,
    connections: Arc<RwLock<HashMap<NodeId, Channel>>>,
}

impl RaftNetworkManager {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn connect(&self, node_id: NodeId, addr: &str) -> Result<Channel, tonic::transport::Error> {
        let endpoint = format!("http://{}", addr);
        let channel = Channel::from_shared(endpoint)?.connect().await?;

        self.connections.write().insert(node_id, channel.clone());
        Ok(channel)
    }

    pub fn get_connection(&self, node_id: NodeId) -> Option<Channel> {
        self.connections.read().get(&node_id).cloned()
    }

    pub fn remove_connection(&self, node_id: NodeId) {
        self.connections.write().remove(&node_id);
    }
}

impl Clone for RaftNetworkManager {
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id,
            connections: Arc::clone(&self.connections),
        }
    }
}

pub struct RaftNetworkConnection {
    target: NodeId,
    target_node: BasicNode,
    manager: RaftNetworkManager,
}

impl RaftNetworkConnection {
    async fn get_client(&self) -> Result<RaftServiceClient<Channel>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let channel = if let Some(ch) = self.manager.get_connection(self.target) {
            ch
        } else {
            self.manager
                .connect(self.target, &self.target_node.addr)
                .await
                .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?
        };

        Ok(RaftServiceClient::new(channel))
    }
}

#[async_trait]
impl RaftNetwork<TypeConfig> for RaftNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let mut client = self.get_client().await?;

        let proto_req = crate::raft::AppendEntriesRequest {
            term: req.vote.leader_id().term,
            leader_id: req.vote.leader_id().node_id,
            prev_log_index: req.prev_log_id.map(|l| l.index).unwrap_or(0),
            prev_log_term: req.prev_log_id.map(|l| l.leader_id.term).unwrap_or(0),
            entries: req
                .entries
                .iter()
                .map(|e| crate::raft::LogEntry {
                    term: e.log_id.leader_id.term,
                    index: e.log_id.index,
                    data: serde_json::to_vec(&e.payload).unwrap_or_default(),
                    entry_type: crate::raft::EntryType::Normal as i32,
                })
                .collect(),
            leader_commit: req.leader_commit.map(|l| l.index).unwrap_or(0),
        };

        let response = client
            .append_entries(proto_req)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = response.into_inner();

        if resp.success {
            Ok(AppendEntriesResponse::Success)
        } else {
            Ok(AppendEntriesResponse::HigherVote(openraft::Vote::new(
                resp.term,
                self.target,
            )))
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut client = self.get_client().await.map_err(|e| match e {
            RPCError::Network(n) => RPCError::Network(n),
            RPCError::RemoteError(r) => RPCError::RemoteError(RemoteError::new(r.target, r.target_node, RaftError::Fatal(openraft::error::Fatal::Panicked))),
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
            RPCError::Timeout(t) => RPCError::Timeout(t),
        })?;

        let snapshot_data = req.snapshot.snapshot.into_inner();
        let chunks = snapshot_data.chunks(64 * 1024);

        let stream = async_stream::stream! {
            let mut offset = 0u64;
            let total_chunks: Vec<_> = chunks.collect();
            let num_chunks = total_chunks.len();

            for (idx, chunk) in total_chunks.into_iter().enumerate() {
                yield crate::raft::InstallSnapshotRequest {
                    term: req.vote.leader_id().term,
                    leader_id: req.vote.leader_id().node_id,
                    last_included_index: req.snapshot.meta.last_log_id.map(|l| l.index).unwrap_or(0),
                    last_included_term: req.snapshot.meta.last_log_id.map(|l| l.leader_id.term).unwrap_or(0),
                    offset,
                    data: chunk.to_vec(),
                    done: idx == num_chunks - 1,
                };
                offset += chunk.len() as u64;
            }
        };

        let response = client
            .install_snapshot(stream)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = response.into_inner();

        Ok(InstallSnapshotResponse {
            vote: openraft::Vote::new(resp.term, self.target),
        })
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let mut client = self.get_client().await?;

        let proto_req = crate::raft::VoteRequest {
            term: req.vote.leader_id().term,
            candidate_id: req.vote.leader_id().node_id,
            last_log_index: req.last_log_id.map(|l| l.index).unwrap_or(0),
            last_log_term: req.last_log_id.map(|l| l.leader_id.term).unwrap_or(0),
        };

        let response = client
            .vote(proto_req)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = response.into_inner();

        Ok(VoteResponse {
            vote: openraft::Vote::new(resp.term, self.target),
            vote_granted: resp.vote_granted,
            last_log_id: if resp.last_log_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(resp.term, self.target),
                    resp.last_log_index,
                ))
            } else {
                None
            },
        })
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for RaftNetworkManager {
    type Network = RaftNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        RaftNetworkConnection {
            target,
            target_node: node.clone(),
            manager: self.clone(),
        }
    }
}
