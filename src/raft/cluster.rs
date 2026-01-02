use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::raft::discovery::HeadlessServiceDiscovery;
use crate::raft::node::RaftNodeManager;

pub struct ClusterManager {
    node_manager: Arc<RaftNodeManager>,
    discovery: HeadlessServiceDiscovery,
    poll_interval: Duration,
}

impl ClusterManager {
    pub fn new(node_manager: Arc<RaftNodeManager>, discovery: HeadlessServiceDiscovery) -> Self {
        Self {
            node_manager,
            discovery,
            poll_interval: Duration::from_secs(30),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn start_membership_watcher(self: Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            this.watch_membership_changes().await;
        });
    }

    async fn watch_membership_changes(&self) {
        let mut known_peers: BTreeSet<u64> = BTreeSet::new();

        loop {
            tokio::time::sleep(self.poll_interval).await;

            if !self.node_manager.leader_election().is_leader() {
                continue;
            }

            match self.discovery.discover_peers().await {
                Ok(current_peers) => {
                    let new_peers: Vec<u64> =
                        current_peers.difference(&known_peers).cloned().collect();

                    let removed_peers: Vec<u64> =
                        known_peers.difference(&current_peers).cloned().collect();

                    for peer_id in new_peers {
                        if let Err(e) = self.handle_new_peer(peer_id).await {
                            tracing::warn!("Failed to add new peer {}: {:?}", peer_id, e);
                        }
                    }

                    for peer_id in removed_peers {
                        if let Err(e) = self.handle_removed_peer(peer_id).await {
                            tracing::warn!("Failed to remove peer {}: {:?}", peer_id, e);
                        }
                    }

                    known_peers = current_peers;
                }
                Err(e) => {
                    tracing::warn!("Failed to discover peers: {:?}", e);
                }
            }
        }
    }

    async fn handle_new_peer(&self, peer_id: u64) -> crate::Result<()> {
        tracing::info!("New peer detected: {}", peer_id);
        let addr = self.get_peer_address(peer_id);
        self.node_manager.add_member(peer_id, addr).await
    }

    async fn handle_removed_peer(&self, peer_id: u64) -> crate::Result<()> {
        tracing::info!("Peer removed: {}", peer_id);
        self.node_manager.remove_member(peer_id).await
    }

    fn get_peer_address(&self, peer_id: u64) -> String {
        let base_name = self
            .discovery
            .service_name()
            .strip_suffix("-headless")
            .unwrap_or(self.discovery.service_name());

        format!(
            "{}-{}.{}.{}.svc.cluster.local:{}",
            base_name,
            peer_id,
            self.discovery.service_name(),
            self.discovery.namespace(),
            self.discovery.port()
        )
    }
}
