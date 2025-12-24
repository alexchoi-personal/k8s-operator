use openraft::network::RaftNetworkFactory;

use super::client::GrpcRaftClient;
use crate::raft::types::{RaftNode, TypeConfig};

#[derive(Clone, Default)]
pub struct GrpcNetworkFactory;

impl GrpcNetworkFactory {
    pub fn new() -> Self {
        Self
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftClient;

    async fn new_client(&mut self, target: u64, node: &RaftNode) -> Self::Network {
        GrpcRaftClient::new(target, node)
    }
}
