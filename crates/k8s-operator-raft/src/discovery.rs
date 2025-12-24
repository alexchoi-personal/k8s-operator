use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::time::Duration;

use tokio::time::interval;
use tracing::{debug, error, info, warn};

use k8s_operator_storage::{BasicNode, NodeId};

pub struct HeadlessServiceDiscovery {
    service_name: String,
    namespace: String,
    port: u16,
    refresh_interval: Duration,
}

impl HeadlessServiceDiscovery {
    pub fn new(service_name: &str, namespace: &str, port: u16) -> Self {
        Self {
            service_name: service_name.to_string(),
            namespace: namespace.to_string(),
            port,
            refresh_interval: Duration::from_secs(10),
        }
    }

    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    pub fn dns_name(&self) -> String {
        format!("{}.{}.svc.cluster.local", self.service_name, self.namespace)
    }

    pub fn pod_dns_name(&self, ordinal: u64) -> String {
        format!(
            "{}-{}.{}.{}.svc.cluster.local",
            self.service_name.trim_end_matches("-headless"),
            ordinal,
            self.service_name,
            self.namespace
        )
    }

    pub async fn discover_peers(&self) -> k8s_operator_core::Result<HashMap<NodeId, BasicNode>> {
        let dns_name = self.dns_name();
        debug!("Discovering peers via DNS: {}", dns_name);

        let addr = format!("{}:{}", dns_name, self.port);

        let addresses: Vec<_> = match addr.to_socket_addrs() {
            Ok(addrs) => addrs.collect(),
            Err(e) => {
                warn!("DNS resolution failed for {}: {}", dns_name, e);
                return Ok(HashMap::new());
            }
        };

        let mut peers = HashMap::new();
        for (idx, addr) in addresses.iter().enumerate() {
            let node_id = idx as NodeId;
            let node = BasicNode {
                addr: format!("{}:{}", addr.ip(), self.port),
            };
            peers.insert(node_id, node);
        }

        info!("Discovered {} peers via DNS", peers.len());
        Ok(peers)
    }

    pub async fn discover_by_ordinal(&self, num_replicas: u64) -> HashMap<NodeId, BasicNode> {
        let mut peers = HashMap::new();

        for ordinal in 0..num_replicas {
            let dns_name = self.pod_dns_name(ordinal);
            let addr = format!("{}:{}", dns_name, self.port);

            if let Ok(mut addrs) = addr.to_socket_addrs() {
                if let Some(socket_addr) = addrs.next() {
                    let node = BasicNode {
                        addr: format!("{}:{}", socket_addr.ip(), self.port),
                    };
                    peers.insert(ordinal, node);
                    debug!("Discovered peer {}: {}", ordinal, dns_name);
                }
            } else {
                debug!("Could not resolve peer {}: {}", ordinal, dns_name);
            }
        }

        peers
    }

    pub fn start_discovery_loop<F>(
        &self,
        num_replicas: u64,
        callback: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(HashMap<NodeId, BasicNode>) + Send + 'static,
    {
        let service_name = self.service_name.clone();
        let namespace = self.namespace.clone();
        let port = self.port;
        let refresh_interval = self.refresh_interval;

        tokio::spawn(async move {
            let discovery = HeadlessServiceDiscovery::new(&service_name, &namespace, port);
            let mut ticker = interval(refresh_interval);

            loop {
                ticker.tick().await;

                let peers = discovery.discover_by_ordinal(num_replicas).await;
                if !peers.is_empty() {
                    callback(peers);
                }
            }
        })
    }
}

pub struct StaticDiscovery {
    peers: HashMap<NodeId, BasicNode>,
}

impl StaticDiscovery {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn add_peer(mut self, node_id: NodeId, addr: impl Into<String>) -> Self {
        self.peers.insert(
            node_id,
            BasicNode {
                addr: addr.into(),
            },
        );
        self
    }

    pub fn from_addresses(addresses: Vec<String>) -> Self {
        let mut discovery = Self::new();
        for (idx, addr) in addresses.into_iter().enumerate() {
            discovery.peers.insert(
                idx as NodeId,
                BasicNode { addr },
            );
        }
        discovery
    }

    pub fn peers(&self) -> &HashMap<NodeId, BasicNode> {
        &self.peers
    }

    pub fn into_peers(self) -> HashMap<NodeId, BasicNode> {
        self.peers
    }
}

impl Default for StaticDiscovery {
    fn default() -> Self {
        Self::new()
    }
}
