mod client;
mod factory;
mod server;

pub use client::GrpcRaftClient;
pub use factory::GrpcNetworkFactory;
pub use server::{RaftGrpcServer, start_raft_server, start_raft_server_with_tls};

pub mod proto {
    tonic::include_proto!("raft");
}
