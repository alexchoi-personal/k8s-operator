use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

pub struct ShutdownCoordinator {
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    drain_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(drain_timeout: Duration) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            shutdown_rx,
            drain_timeout,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    pub fn shutdown_tx(&self) -> &watch::Sender<bool> {
        &self.shutdown_tx
    }

    pub fn is_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    pub async fn wait_for_shutdown(&mut self) {
        loop {
            if self.shutdown_rx.changed().await.is_err() {
                break;
            }
            if *self.shutdown_rx.borrow() {
                break;
            }
        }
    }

    pub async fn initiate_shutdown(&self) {
        tracing::info!("Initiating graceful shutdown");
        let _ = self.shutdown_tx.send(true);

        tracing::info!(
            "Waiting for in-flight requests to complete (timeout: {:?})",
            self.drain_timeout
        );
        tokio::time::sleep(self.drain_timeout).await;
    }
}

pub async fn run_signal_handler(coordinator: Arc<ShutdownCoordinator>) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT, initiating graceful shutdown");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, initiating graceful shutdown");
        }
    }

    coordinator.initiate_shutdown().await;
}

pub struct GracefulShutdown {
    coordinator: Arc<ShutdownCoordinator>,
}

impl GracefulShutdown {
    pub fn new(drain_timeout: Duration) -> Self {
        Self {
            coordinator: Arc::new(ShutdownCoordinator::new(drain_timeout)),
        }
    }

    pub fn coordinator(&self) -> &Arc<ShutdownCoordinator> {
        &self.coordinator
    }

    pub fn start_signal_handler(&self) {
        let coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            run_signal_handler(coordinator).await;
        });
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.coordinator.subscribe()
    }
}
