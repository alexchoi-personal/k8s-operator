use openraft::Config;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TypeConfig;

pub type RaftTypeConfig = TypeConfig;

impl openraft::RaftTypeConfig for RaftTypeConfig {
    type D = RaftRequest;
    type R = RaftResponse;
    type Node = RaftNode;
    type NodeId = NodeId;
    type Entry = openraft::Entry<RaftTypeConfig>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftResponse {
    pub success: bool,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftNode {
    pub addr: String,
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

#[allow(dead_code)]
pub fn default_raft_config() -> Config {
    Config {
        election_timeout_min: 500,
        election_timeout_max: 1000,
        heartbeat_interval: 100,
        ..Default::default()
    }
}
