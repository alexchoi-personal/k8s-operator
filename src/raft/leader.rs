use crate::error::Result;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct LeaderElection {
    is_leader: Arc<RwLock<bool>>,
    node_id: u64,
}

impl LeaderElection {
    pub fn new(node_id: u64) -> Self {
        Self {
            is_leader: Arc::new(RwLock::new(false)),
            node_id,
        }
    }

    pub fn is_leader(&self) -> bool {
        *self.is_leader.read()
    }

    pub fn set_leader(&self, is_leader: bool) {
        *self.is_leader.write() = is_leader;
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn guard(&self) -> Option<LeaderGuard> {
        if self.is_leader() {
            Some(LeaderGuard {
                _election: self.is_leader.clone(),
            })
        } else {
            None
        }
    }

    pub async fn wait_for_leadership(&self) -> Result<LeaderGuard> {
        loop {
            if self.is_leader() {
                return Ok(LeaderGuard {
                    _election: self.is_leader.clone(),
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

pub struct LeaderGuard {
    _election: Arc<RwLock<bool>>,
}

impl LeaderGuard {
    pub fn is_valid(&self) -> bool {
        *self._election.read()
    }
}
