use std::marker::PhantomData;
use openraft::network::RaftNetworkFactory;

use super::client::GrpcRaftClient;
use crate::raft::config::TlsConfig;
use crate::raft::types::{KeyValueStateMachine, RaftNode, StateMachine, TypeConfig};

#[derive(Clone)]
pub struct GrpcNetworkFactory<SM: StateMachine = KeyValueStateMachine> {
    tls: TlsConfig,
    _marker: PhantomData<SM>,
}

impl<SM: StateMachine> Default for GrpcNetworkFactory<SM> {
    fn default() -> Self {
        Self {
            tls: TlsConfig::disabled(),
            _marker: PhantomData,
        }
    }
}

impl<SM: StateMachine> GrpcNetworkFactory<SM> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_tls(tls: TlsConfig) -> Self {
        Self { tls, _marker: PhantomData }
    }
}

impl<SM: StateMachine> RaftNetworkFactory<TypeConfig<SM>> for GrpcNetworkFactory<SM> {
    type Network = GrpcRaftClient<SM>;

    async fn new_client(&mut self, target: u64, node: &RaftNode) -> Self::Network {
        GrpcRaftClient::new(target, node, self.tls.clone())
    }
}
