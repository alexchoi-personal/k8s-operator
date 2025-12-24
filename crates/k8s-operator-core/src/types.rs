use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum ReconcileResult {
    Ok(Action),
    Err(crate::ReconcileError),
}

impl From<Action> for ReconcileResult {
    fn from(action: Action) -> Self {
        ReconcileResult::Ok(action)
    }
}

impl<E: Into<crate::ReconcileError>> From<std::result::Result<Action, E>> for ReconcileResult {
    fn from(result: std::result::Result<Action, E>) -> Self {
        match result {
            Ok(action) => ReconcileResult::Ok(action),
            Err(e) => ReconcileResult::Err(e.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Action {
    Requeue(Duration),
    RequeueAfter(Duration),
    Done,
}

impl Action {
    pub fn requeue(duration: Duration) -> Self {
        Action::Requeue(duration)
    }

    pub fn requeue_after(duration: Duration) -> Self {
        Action::RequeueAfter(duration)
    }

    pub fn done() -> Self {
        Action::Done
    }

    pub fn await_change() -> Self {
        Action::Requeue(Duration::from_secs(300))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorConfig {
    pub node_id: u64,
    pub cluster_name: String,
    pub namespace: String,
    pub service_name: String,
    pub raft_port: u16,
    pub metrics_port: u16,
    pub data_dir: String,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster_name: "operator".to_string(),
            namespace: "default".to_string(),
            service_name: "operator-headless".to_string(),
            raft_port: 5000,
            metrics_port: 8080,
            data_dir: "/data/raft".to_string(),
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 300,
            snapshot_threshold: 10000,
            max_payload_entries: 100,
        }
    }
}

impl OperatorConfig {
    pub fn from_env() -> crate::Result<Self> {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("NODE_ID") {
            config.node_id = val.parse().map_err(|_| {
                crate::OperatorError::ConfigError("Invalid NODE_ID".to_string())
            })?;
        }

        if let Ok(val) = std::env::var("CLUSTER_NAME") {
            config.cluster_name = val;
        }

        if let Ok(val) = std::env::var("NAMESPACE") {
            config.namespace = val;
        }

        if let Ok(val) = std::env::var("SERVICE_NAME") {
            config.service_name = val;
        }

        if let Ok(val) = std::env::var("RAFT_PORT") {
            config.raft_port = val.parse().map_err(|_| {
                crate::OperatorError::ConfigError("Invalid RAFT_PORT".to_string())
            })?;
        }

        if let Ok(val) = std::env::var("DATA_DIR") {
            config.data_dir = val;
        }

        Ok(config)
    }

    pub fn peer_address(&self, node_id: u64) -> String {
        format!(
            "{}-{}.{}.{}.svc.cluster.local:{}",
            self.cluster_name, node_id, self.service_name, self.namespace, self.raft_port
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
    Learner,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Follower
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
    pub role: NodeRole,
    pub is_healthy: bool,
}
