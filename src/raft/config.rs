use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub node_id: u64,
    pub cluster_name: String,
    pub service_name: String,
    pub namespace: String,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub snapshot_threshold: u64,
    pub use_tls: bool,
}

impl RaftConfig {
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            node_id: 0,
            cluster_name: cluster_name.into(),
            service_name: String::new(),
            namespace: String::from("default"),
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(100),
            snapshot_threshold: 1000,
            use_tls: false,
        }
    }

    pub fn node_id(mut self, id: u64) -> Self {
        self.node_id = id;
        self
    }

    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = ns.into();
        self
    }

    pub fn election_timeout(mut self, timeout: Duration) -> Self {
        self.election_timeout = timeout;
        self
    }

    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn snapshot_threshold(mut self, threshold: u64) -> Self {
        self.snapshot_threshold = threshold;
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }
}
