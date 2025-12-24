use openraft::network::RaftNetworkFactory;

use super::client::GrpcRaftClient;
use crate::raft::types::{RaftNode, TypeConfig};

#[derive(Clone)]
pub struct GrpcNetworkFactory {
    use_tls: bool,
}

impl Default for GrpcNetworkFactory {
    fn default() -> Self {
        Self { use_tls: false }
    }
}

impl GrpcNetworkFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tls(use_tls: bool) -> Self {
        Self { use_tls }
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftClient;

    async fn new_client(&mut self, target: u64, node: &RaftNode) -> Self::Network {
        GrpcRaftClient::new(target, node, self.use_tls)
    }
}
