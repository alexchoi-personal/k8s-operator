use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub log_entries_threshold: u64,
    pub time_threshold: Duration,
    pub check_interval: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            log_entries_threshold: 1000,
            time_threshold: Duration::from_secs(3600),
            check_interval: Duration::from_secs(60),
        }
    }
}

impl CompactionConfig {
    pub fn log_entries_threshold(mut self, threshold: u64) -> Self {
        self.log_entries_threshold = threshold;
        self
    }

    pub fn time_threshold(mut self, threshold: Duration) -> Self {
        self.time_threshold = threshold;
        self
    }

    pub fn check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub enum TlsMode {
    #[default]
    Disabled,
    Tls,
    Mtls,
}

#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    pub mode: TlsMode,
    pub ca_cert: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

impl TlsConfig {
    pub fn disabled() -> Self {
        Self {
            mode: TlsMode::Disabled,
            ..Default::default()
        }
    }

    pub fn tls(ca_cert: impl Into<PathBuf>) -> Self {
        Self {
            mode: TlsMode::Tls,
            ca_cert: Some(ca_cert.into()),
            cert: None,
            key: None,
        }
    }

    pub fn mtls(
        ca_cert: impl Into<PathBuf>,
        cert: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
    ) -> Self {
        Self {
            mode: TlsMode::Mtls,
            ca_cert: Some(ca_cert.into()),
            cert: Some(cert.into()),
            key: Some(key.into()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, TlsMode::Disabled)
    }

    pub fn is_mtls(&self) -> bool {
        matches!(self.mode, TlsMode::Mtls)
    }
}

#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub node_id: u64,
    pub cluster_name: String,
    pub service_name: String,
    pub namespace: String,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub snapshot_threshold: u64,
    pub tls: TlsConfig,
    pub drain_timeout: Duration,
    pub compaction: CompactionConfig,
    pub cert_reload_interval: Duration,
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
            tls: TlsConfig::disabled(),
            drain_timeout: Duration::from_secs(5),
            compaction: CompactionConfig::default(),
            cert_reload_interval: Duration::from_secs(30),
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

    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    pub fn compaction(mut self, config: CompactionConfig) -> Self {
        self.compaction = config;
        self
    }

    pub fn cert_reload_interval(mut self, interval: Duration) -> Self {
        self.cert_reload_interval = interval;
        self
    }

    #[deprecated(since = "0.4.0", note = "Use .tls(TlsConfig::tls(...)) instead")]
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        if use_tls {
            self.tls = TlsConfig {
                mode: TlsMode::Tls,
                ..Default::default()
            };
        }
        self
    }
}
