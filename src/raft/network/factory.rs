use openraft::network::RaftNetworkFactory;

use super::client::GrpcRaftClient;
use crate::raft::config::TlsConfig;
use crate::raft::types::{RaftNode, TypeConfig};

#[derive(Clone)]
pub struct GrpcNetworkFactory {
    tls: TlsConfig,
}

impl Default for GrpcNetworkFactory {
    fn default() -> Self {
        Self {
            tls: TlsConfig::disabled(),
        }
    }
}

impl GrpcNetworkFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tls(tls: TlsConfig) -> Self {
        Self { tls }
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcRaftClient;

    async fn new_client(&mut self, target: u64, node: &RaftNode) -> Self::Network {
        GrpcRaftClient::new(target, node, self.tls.clone())
    }
}
