use thiserror::Error;

#[derive(Error, Debug)]
pub enum OperatorError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Raft error: {0}")]
    RaftError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Node not leader")]
    NotLeader,

    #[error("Cluster not ready")]
    ClusterNotReady,

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum ReconcileError {
    #[error("Resource not found")]
    NotFound,

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Invalid resource: {0}")]
    Invalid(String),

    #[error("Temporary failure: {0}")]
    Temporary(String),

    #[error("Permanent failure: {0}")]
    Permanent(String),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Not leader, skipping reconciliation")]
    NotLeader,
}

impl ReconcileError {
    pub fn is_retriable(&self) -> bool {
        matches!(self, ReconcileError::Temporary(_) | ReconcileError::NotLeader)
    }
}

pub type Result<T> = std::result::Result<T, OperatorError>;
