use async_trait::async_trait;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::RaftTypeConfig;

pub type NodeId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = LogEntry;
    type R = LogResponse;
    type Node = BasicNode;
    type NodeId = NodeId;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = tokio::io::Cursor<Vec<u8>>;
    type Responder = openraft::impls::OneshotResponder<Self>;
    type AsyncRuntime = openraft::TokioRuntime;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct BasicNode {
    pub addr: String,
}

impl std::fmt::Display for BasicNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

impl openraft::Node for BasicNode {
    fn addr(&self) -> &str {
        &self.addr
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct LogResponse {
    pub value: Option<Vec<u8>>,
}

#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    type LogStorage: RaftLogStorage<TypeConfig>;
    type StateMachine: RaftStateMachine<TypeConfig>;

    async fn create(path: &str) -> k8s_operator_core::Result<Self>
    where
        Self: Sized;

    fn log_storage(&self) -> &Self::LogStorage;
    fn log_storage_mut(&mut self) -> &mut Self::LogStorage;

    fn state_machine(&self) -> &Self::StateMachine;
    fn state_machine_mut(&mut self) -> &mut Self::StateMachine;

    async fn close(&mut self) -> k8s_operator_core::Result<()>;
}
