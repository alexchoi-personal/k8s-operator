use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{NodeInfo, NodeRole};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterState {
    pub leader_id: Option<u64>,
    pub term: u64,
    pub nodes: HashMap<u64, NodeInfo>,
    pub committed_index: u64,
    pub applied_index: u64,
}

impl ClusterState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_leader(&self, node_id: u64) -> bool {
        self.leader_id == Some(node_id)
    }

    pub fn leader(&self) -> Option<&NodeInfo> {
        self.leader_id.and_then(|id| self.nodes.get(&id))
    }

    pub fn healthy_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values().filter(|n| n.is_healthy)
    }

    pub fn has_quorum(&self) -> bool {
        let healthy_count = self.healthy_nodes().count();
        let total = self.nodes.len();
        healthy_count > total / 2
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorState<S = ()>
where
    S: Serialize + Clone,
{
    pub cluster: ClusterState,
    pub local_node_id: u64,
    pub local_role: NodeRole,
    pub custom: S,
}

impl<S> Default for OperatorState<S>
where
    S: Serialize + Clone + Default,
{
    fn default() -> Self {
        Self {
            cluster: ClusterState::default(),
            local_node_id: 0,
            local_role: NodeRole::Follower,
            custom: S::default(),
        }
    }
}

impl<S> OperatorState<S>
where
    S: Serialize + Clone,
{
    pub fn new(node_id: u64, custom: S) -> Self {
        Self {
            cluster: ClusterState::default(),
            local_node_id: node_id,
            local_role: NodeRole::Follower,
            custom,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.cluster.is_leader(self.local_node_id)
    }

    pub fn set_role(&mut self, role: NodeRole) {
        self.local_role = role;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineCommand {
    pub id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineResponse {
    pub success: bool,
    pub data: Option<Vec<u8>>,
    pub error: Option<String>,
}

impl StateMachineResponse {
    pub fn ok(data: Option<Vec<u8>>) -> Self {
        Self {
            success: true,
            data,
            error: None,
        }
    }

    pub fn err(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}
