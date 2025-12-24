use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub cluster_name: String,
    pub listen_addr: String,
    pub advertise_addr: String,
    pub peer_addresses: Vec<String>,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
    pub install_snapshot_timeout: Duration,
    pub send_timeout: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster_name: "operator".to_string(),
            listen_addr: "0.0.0.0:5000".to_string(),
            advertise_addr: "localhost:5000".to_string(),
            peer_addresses: Vec::new(),
            election_timeout: Duration::from_millis(1000),
            heartbeat_interval: Duration::from_millis(300),
            snapshot_threshold: 10000,
            max_payload_entries: 100,
            install_snapshot_timeout: Duration::from_secs(60),
            send_timeout: Duration::from_secs(5),
        }
    }
}

impl RaftConfig {
    pub fn builder() -> RaftConfigBuilder {
        RaftConfigBuilder::default()
    }

    pub fn from_env() -> k8s_operator_core::Result<Self> {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("RAFT_NODE_ID") {
            config.node_id = val.parse().map_err(|_| {
                k8s_operator_core::OperatorError::ConfigError("Invalid RAFT_NODE_ID".to_string())
            })?;
        }

        if let Ok(val) = std::env::var("RAFT_CLUSTER_NAME") {
            config.cluster_name = val;
        }

        if let Ok(val) = std::env::var("RAFT_LISTEN_ADDR") {
            config.listen_addr = val;
        }

        if let Ok(val) = std::env::var("RAFT_ADVERTISE_ADDR") {
            config.advertise_addr = val;
        }

        if let Ok(val) = std::env::var("RAFT_PEERS") {
            config.peer_addresses = val.split(',').map(|s| s.trim().to_string()).collect();
        }

        Ok(config)
    }

    pub fn to_openraft_config(&self) -> openraft::Config {
        openraft::Config {
            cluster_name: self.cluster_name.clone(),
            election_timeout_min: self.election_timeout.as_millis() as u64,
            election_timeout_max: (self.election_timeout.as_millis() as u64) * 2,
            heartbeat_interval: self.heartbeat_interval.as_millis() as u64,
            install_snapshot_timeout: self.install_snapshot_timeout.as_millis() as u64,
            send_snapshot_timeout: self.send_timeout.as_millis() as u64,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(self.snapshot_threshold as u32),
            max_payload_entries: self.max_payload_entries as usize,
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct RaftConfigBuilder {
    config: RaftConfig,
}

impl RaftConfigBuilder {
    pub fn node_id(mut self, id: u64) -> Self {
        self.config.node_id = id;
        self
    }

    pub fn cluster_name(mut self, name: impl Into<String>) -> Self {
        self.config.cluster_name = name.into();
        self
    }

    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.listen_addr = addr.into();
        self
    }

    pub fn advertise_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.advertise_addr = addr.into();
        self
    }

    pub fn peer(mut self, addr: impl Into<String>) -> Self {
        self.config.peer_addresses.push(addr.into());
        self
    }

    pub fn peers(mut self, addrs: Vec<String>) -> Self {
        self.config.peer_addresses = addrs;
        self
    }

    pub fn election_timeout(mut self, timeout: Duration) -> Self {
        self.config.election_timeout = timeout;
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    pub fn snapshot_threshold(mut self, threshold: u64) -> Self {
        self.config.snapshot_threshold = threshold;
        self
    }

    pub fn build(self) -> RaftConfig {
        self.config
    }
}
