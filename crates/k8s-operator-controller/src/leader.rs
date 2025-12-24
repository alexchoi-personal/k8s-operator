use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

use k8s_operator_core::{NodeRole, ReconcileError, ReconcileResult};

pub struct LeaderGuard {
    role_rx: watch::Receiver<NodeRole>,
}

impl LeaderGuard {
    pub fn new(role_rx: watch::Receiver<NodeRole>) -> Self {
        Self { role_rx }
    }

    pub fn is_leader(&self) -> bool {
        matches!(*self.role_rx.borrow(), NodeRole::Leader)
    }

    pub fn current_role(&self) -> NodeRole {
        *self.role_rx.borrow()
    }

    pub async fn wait_for_leadership(&mut self) -> NodeRole {
        loop {
            let role = *self.role_rx.borrow_and_update();
            if role == NodeRole::Leader {
                return role;
            }

            if self.role_rx.changed().await.is_err() {
                return NodeRole::Follower;
            }
        }
    }

    pub async fn wait_for_leadership_with_timeout(&mut self, timeout: Duration) -> Option<NodeRole> {
        tokio::select! {
            role = self.wait_for_leadership() => Some(role),
            _ = tokio::time::sleep(timeout) => None,
        }
    }

    pub fn check(&self) -> Result<(), ReconcileError> {
        if self.is_leader() {
            Ok(())
        } else {
            Err(ReconcileError::NotLeader)
        }
    }

    pub fn ensure<F, R>(&self, f: F) -> ReconcileResult
    where
        F: FnOnce() -> R,
        R: Into<ReconcileResult>,
    {
        if self.is_leader() {
            f().into()
        } else {
            ReconcileResult::Err(ReconcileError::NotLeader)
        }
    }
}

impl Clone for LeaderGuard {
    fn clone(&self) -> Self {
        Self {
            role_rx: self.role_rx.clone(),
        }
    }
}

pub struct LeaderElection {
    role_tx: watch::Sender<NodeRole>,
    role_rx: watch::Receiver<NodeRole>,
}

impl LeaderElection {
    pub fn new() -> Self {
        let (role_tx, role_rx) = watch::channel(NodeRole::Follower);
        Self { role_tx, role_rx }
    }

    pub fn set_role(&self, role: NodeRole) {
        let _ = self.role_tx.send(role);
        match role {
            NodeRole::Leader => info!("This node is now the leader"),
            NodeRole::Follower => debug!("This node is now a follower"),
            NodeRole::Candidate => debug!("This node is now a candidate"),
            NodeRole::Learner => debug!("This node is now a learner"),
        }
    }

    pub fn guard(&self) -> LeaderGuard {
        LeaderGuard::new(self.role_rx.clone())
    }

    pub fn subscribe(&self) -> watch::Receiver<NodeRole> {
        self.role_rx.clone()
    }
}

impl Default for LeaderElection {
    fn default() -> Self {
        Self::new()
    }
}

pub fn leader_only<F, T>(guard: &LeaderGuard, f: F) -> Result<T, ReconcileError>
where
    F: FnOnce() -> T,
{
    guard.check()?;
    Ok(f())
}

pub async fn leader_only_async<F, Fut, T>(guard: &LeaderGuard, f: F) -> Result<T, ReconcileError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    guard.check()?;
    Ok(f().await)
}
