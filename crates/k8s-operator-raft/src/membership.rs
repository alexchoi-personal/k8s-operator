use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::Membership;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use k8s_operator_storage::{BasicNode, NodeId};

#[derive(Debug, Clone)]
pub struct MembershipState {
    nodes: BTreeMap<NodeId, BasicNode>,
    voters: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
}

impl MembershipState {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
        }
    }

    pub fn add_voter(&mut self, node_id: NodeId, node: BasicNode) {
        self.nodes.insert(node_id, node);
        self.voters.insert(node_id);
        self.learners.remove(&node_id);
    }

    pub fn add_learner(&mut self, node_id: NodeId, node: BasicNode) {
        self.nodes.insert(node_id, node);
        self.learners.insert(node_id);
    }

    pub fn promote_to_voter(&mut self, node_id: NodeId) -> bool {
        if self.learners.remove(&node_id) {
            self.voters.insert(node_id);
            true
        } else {
            false
        }
    }

    pub fn demote_to_learner(&mut self, node_id: NodeId) -> bool {
        if self.voters.remove(&node_id) {
            self.learners.insert(node_id);
            true
        } else {
            false
        }
    }

    pub fn remove_node(&mut self, node_id: NodeId) -> Option<BasicNode> {
        self.voters.remove(&node_id);
        self.learners.remove(&node_id);
        self.nodes.remove(&node_id)
    }

    pub fn get_node(&self, node_id: NodeId) -> Option<&BasicNode> {
        self.nodes.get(&node_id)
    }

    pub fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    pub fn learners(&self) -> &BTreeSet<NodeId> {
        &self.learners
    }

    pub fn all_nodes(&self) -> &BTreeMap<NodeId, BasicNode> {
        &self.nodes
    }

    pub fn to_membership(&self) -> Membership<NodeId, BasicNode> {
        Membership::new(vec![self.voters.clone()], Some(self.learners.clone()))
    }

    pub fn from_membership(membership: &Membership<NodeId, BasicNode>, nodes: BTreeMap<NodeId, BasicNode>) -> Self {
        let voters: BTreeSet<NodeId> = membership
            .voter_ids()
            .collect();
        let learners: BTreeSet<NodeId> = membership
            .learner_ids()
            .collect();

        Self {
            nodes,
            voters,
            learners,
        }
    }
}

impl Default for MembershipState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MembershipManager {
    state: Arc<RwLock<MembershipState>>,
    local_node_id: NodeId,
}

impl MembershipManager {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            state: Arc::new(RwLock::new(MembershipState::new())),
            local_node_id,
        }
    }

    pub fn with_initial_members(local_node_id: NodeId, members: BTreeMap<NodeId, BasicNode>) -> Self {
        let mut state = MembershipState::new();
        for (id, node) in members {
            state.add_voter(id, node);
        }
        Self {
            state: Arc::new(RwLock::new(state)),
            local_node_id,
        }
    }

    pub fn add_voter(&self, node_id: NodeId, node: BasicNode) {
        info!("Adding voter: {} at {}", node_id, node.addr);
        self.state.write().add_voter(node_id, node);
    }

    pub fn add_learner(&self, node_id: NodeId, node: BasicNode) {
        info!("Adding learner: {} at {}", node_id, node.addr);
        self.state.write().add_learner(node_id, node);
    }

    pub fn promote_to_voter(&self, node_id: NodeId) -> bool {
        let result = self.state.write().promote_to_voter(node_id);
        if result {
            info!("Promoted {} to voter", node_id);
        } else {
            warn!("Failed to promote {} to voter (not a learner)", node_id);
        }
        result
    }

    pub fn demote_to_learner(&self, node_id: NodeId) -> bool {
        let result = self.state.write().demote_to_learner(node_id);
        if result {
            info!("Demoted {} to learner", node_id);
        } else {
            warn!("Failed to demote {} to learner (not a voter)", node_id);
        }
        result
    }

    pub fn remove_node(&self, node_id: NodeId) -> Option<BasicNode> {
        info!("Removing node: {}", node_id);
        self.state.write().remove_node(node_id)
    }

    pub fn get_node(&self, node_id: NodeId) -> Option<BasicNode> {
        self.state.read().get_node(node_id).cloned()
    }

    pub fn get_state(&self) -> MembershipState {
        self.state.read().clone()
    }

    pub fn update_from_membership(&self, membership: &Membership<NodeId, BasicNode>, nodes: BTreeMap<NodeId, BasicNode>) {
        let new_state = MembershipState::from_membership(membership, nodes);
        *self.state.write() = new_state;
        debug!("Updated membership state from Raft membership");
    }

    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    pub fn is_voter(&self) -> bool {
        self.state.read().voters().contains(&self.local_node_id)
    }

    pub fn voter_count(&self) -> usize {
        self.state.read().voters().len()
    }

    pub fn total_count(&self) -> usize {
        self.state.read().all_nodes().len()
    }
}

impl Clone for MembershipManager {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            local_node_id: self.local_node_id,
        }
    }
}
