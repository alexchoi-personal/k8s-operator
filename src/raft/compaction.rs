use std::sync::Arc;
use std::time::Instant;

use openraft::{Raft, ServerState};
use parking_lot::RwLock;
use tokio::sync::watch;

use crate::raft::config::CompactionConfig;
use crate::raft::node::StorageBackend;
use crate::raft::types::TypeConfig;

pub struct CompactionManager {
    raft: Arc<Raft<TypeConfig>>,
    storage: StorageBackend,
    config: CompactionConfig,
    last_compaction: RwLock<Instant>,
}

impl CompactionManager {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        storage: StorageBackend,
        config: CompactionConfig,
    ) -> Self {
        Self {
            raft,
            storage,
            config,
            last_compaction: RwLock::new(Instant::now()),
        }
    }

    pub async fn run(self: Arc<Self>, mut shutdown_rx: watch::Receiver<bool>) {
        tracing::info!(
            "Starting compaction manager (threshold: {} entries, interval: {:?})",
            self.config.log_entries_threshold,
            self.config.check_interval
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("Compaction manager shutting down");
                        break;
                    }
                }
                _ = tokio::time::sleep(self.config.check_interval) => {
                    if let Err(e) = self.maybe_compact().await {
                        tracing::warn!("Compaction check failed: {:?}", e);
                    }
                }
            }
        }
    }

    async fn maybe_compact(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metrics = self.raft.metrics().borrow().clone();

        let last_log = metrics.last_log_index.unwrap_or(0);
        let last_applied = metrics.last_applied.map(|l| l.index).unwrap_or(0);
        let log_entries = last_log.saturating_sub(last_applied);

        let time_since_last = self.last_compaction.read().elapsed();

        let should_compact = log_entries >= self.config.log_entries_threshold
            || time_since_last >= self.config.time_threshold;

        if !should_compact {
            return Ok(());
        }

        tracing::info!(
            "Initiating compaction: {} log entries, {:?} since last compaction",
            log_entries,
            time_since_last
        );

        self.perform_compaction().await?;
        *self.last_compaction.write() = Instant::now();

        Ok(())
    }

    async fn perform_compaction(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use openraft::storage::{RaftSnapshotBuilder, RaftStorage};

        let snapshot_result = match &self.storage {
            StorageBackend::Memory(store) => {
                let mut builder = store.clone();
                builder.build_snapshot().await
            }
            #[cfg(feature = "rocksdb")]
            StorageBackend::RocksDb(store) => {
                let mut builder = store.clone();
                builder.build_snapshot().await
            }
        };

        let snapshot = snapshot_result.map_err(|e| format!("Snapshot build failed: {:?}", e))?;

        if let Some(last_log_id) = snapshot.meta.last_log_id {
            tracing::info!("Snapshot created up to log {:?}", last_log_id);

            match &self.storage {
                StorageBackend::Memory(store) => {
                    let mut storage = store.clone();
                    storage
                        .purge_logs_upto(last_log_id)
                        .await
                        .map_err(|e| format!("Log purge failed: {:?}", e))?;
                }
                #[cfg(feature = "rocksdb")]
                StorageBackend::RocksDb(store) => {
                    let mut storage = store.clone();
                    storage
                        .purge_logs_upto(last_log_id)
                        .await
                        .map_err(|e| format!("Log purge failed: {:?}", e))?;
                }
            }

            tracing::info!("Purged logs up to {:?}", last_log_id);
        }

        Ok(())
    }
}
