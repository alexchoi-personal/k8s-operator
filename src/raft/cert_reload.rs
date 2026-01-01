use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use tokio::sync::{broadcast, watch};

use crate::raft::config::TlsConfig;

pub struct CertReloader {
    tls_config: TlsConfig,
    check_interval: Duration,
    last_modified: RwLock<Option<SystemTime>>,
    reload_notify: broadcast::Sender<()>,
}

impl CertReloader {
    pub fn new(tls_config: TlsConfig, check_interval: Duration) -> Self {
        let (reload_notify, _) = broadcast::channel(16);
        Self {
            tls_config,
            check_interval,
            last_modified: RwLock::new(None),
            reload_notify,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.reload_notify.subscribe()
    }

    pub async fn run(self: Arc<Self>, mut shutdown_rx: watch::Receiver<bool>) {
        if !self.tls_config.is_enabled() {
            tracing::debug!("TLS disabled, cert reloader not starting");
            return;
        }

        tracing::info!(
            "Starting certificate reloader (interval: {:?})",
            self.check_interval
        );

        if let Err(e) = self.initialize_mtime().await {
            tracing::warn!("Failed to initialize certificate mtime: {:?}", e);
        }

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Certificate reloader shutting down");
                        break;
                    }
                }
                _ = tokio::time::sleep(self.check_interval) => {
                    if let Err(e) = self.check_for_changes().await {
                        tracing::warn!("Certificate check failed: {:?}", e);
                    }
                }
            }
        }
    }

    async fn initialize_mtime(&self) -> Result<(), std::io::Error> {
        let mtime = self.get_latest_mtime().await?;
        *self.last_modified.write() = mtime;
        Ok(())
    }

    async fn get_latest_mtime(&self) -> Result<Option<SystemTime>, std::io::Error> {
        let mut latest: Option<SystemTime> = None;

        if let Some(cert_path) = &self.tls_config.cert {
            if let Ok(metadata) = tokio::fs::metadata(cert_path).await {
                if let Ok(mtime) = metadata.modified() {
                    latest = Some(latest.map_or(mtime, |l| std::cmp::max(l, mtime)));
                }
            }
        }

        if let Some(key_path) = &self.tls_config.key {
            if let Ok(metadata) = tokio::fs::metadata(key_path).await {
                if let Ok(mtime) = metadata.modified() {
                    latest = Some(latest.map_or(mtime, |l| std::cmp::max(l, mtime)));
                }
            }
        }

        if let Some(ca_path) = &self.tls_config.ca_cert {
            if let Ok(metadata) = tokio::fs::metadata(ca_path).await {
                if let Ok(mtime) = metadata.modified() {
                    latest = Some(latest.map_or(mtime, |l| std::cmp::max(l, mtime)));
                }
            }
        }

        Ok(latest)
    }

    async fn check_for_changes(&self) -> Result<(), std::io::Error> {
        let current_mtime = self.get_latest_mtime().await?;

        let last_mtime = *self.last_modified.read();

        if let (Some(current), Some(last)) = (current_mtime, last_mtime) {
            if current > last {
                tracing::info!("Certificate files changed, triggering reload");
                *self.last_modified.write() = Some(current);
                let _ = self.reload_notify.send(());
            }
        } else if current_mtime.is_some() && last_mtime.is_none() {
            *self.last_modified.write() = current_mtime;
        }

        Ok(())
    }
}
