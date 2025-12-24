use crate::error::Result;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct HeadlessServiceDiscovery {
    service_name: String,
    namespace: String,
    port: u16,
}

impl HeadlessServiceDiscovery {
    pub fn new(service_name: impl Into<String>, namespace: impl Into<String>, port: u16) -> Self {
        Self {
            service_name: service_name.into(),
            namespace: namespace.into(),
            port,
        }
    }

    pub async fn discover_peers(&self) -> Result<BTreeSet<u64>> {
        let dns_name = format!(
            "{}.{}.svc.cluster.local",
            self.service_name, self.namespace
        );

        let mut peers = BTreeSet::new();

        match tokio::net::lookup_host(format!("{}:{}", dns_name, self.port)).await {
            Ok(addrs) => {
                for (idx, _addr) in addrs.enumerate() {
                    peers.insert(idx as u64);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to discover peers via DNS: {}", e);
            }
        }

        Ok(peers)
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}
