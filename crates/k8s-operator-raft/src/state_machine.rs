use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::storage::RaftStateMachine;
use openraft::{Entry, EntryPayload, LogId, OptionalSend, RaftLogId, SnapshotMeta, StorageError, StoredMembership};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use k8s_operator_core::{StateMachine, StateMachineCommand, StateMachineResponse};
use k8s_operator_storage::{BasicNode, LogEntry, LogResponse, NodeId, TypeConfig};

pub struct OperatorStateMachine<S>
where
    S: StateMachine,
{
    inner: Arc<RwLock<S>>,
    last_applied: RwLock<Option<LogId<NodeId>>>,
    last_membership: RwLock<StoredMembership<NodeId, BasicNode>>,
}

impl<S> OperatorStateMachine<S>
where
    S: StateMachine,
{
    pub fn new(state_machine: S) -> Self {
        Self {
            inner: Arc::new(RwLock::new(state_machine)),
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
        }
    }

    pub fn state(&self) -> S::State {
        self.inner.read().state().clone()
    }

    pub async fn apply_command(&self, command: StateMachineCommand) -> StateMachineResponse {
        self.inner.write().apply(command).await
    }
}

impl<S> Clone for OperatorStateMachine<S>
where
    S: StateMachine,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            last_applied: RwLock::new(*self.last_applied.read()),
            last_membership: RwLock::new(self.last_membership.read().clone()),
        }
    }
}

#[async_trait]
impl<S> RaftStateMachine<TypeConfig> for OperatorStateMachine<S>
where
    S: StateMachine,
{
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        Ok((*self.last_applied.read(), self.last_membership.read().clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LogResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            *self.last_applied.write() = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    debug!("Applied blank entry at index {}", entry.log_id.index);
                    responses.push(LogResponse { value: None });
                }
                EntryPayload::Normal(log_entry) => {
                    let command = StateMachineCommand {
                        id: entry.log_id.index,
                        payload: log_entry.data.clone(),
                    };

                    let response = self.inner.write().apply(command).await;

                    debug!(
                        "Applied command at index {}: success={}",
                        entry.log_id.index, response.success
                    );

                    responses.push(LogResponse {
                        value: response.data,
                    });
                }
                EntryPayload::Membership(mem) => {
                    *self.last_membership.write() = StoredMembership::new(Some(entry.log_id), mem);
                    info!("Applied membership change at index {}", entry.log_id.index);
                    responses.push(LogResponse { value: None });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.get_ref();

        self.inner
            .write()
            .restore(data)
            .await
            .map_err(|e| StorageError::read_snapshot(Some(meta.signature()), &std::io::Error::other(e.to_string())))?;

        *self.last_applied.write() = meta.last_log_id;
        *self.last_membership.write() = meta.last_membership.clone();

        info!("Installed snapshot at index {:?}", meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}

#[async_trait]
impl<S> openraft::storage::RaftSnapshotBuilder<TypeConfig> for OperatorStateMachine<S>
where
    S: StateMachine,
{
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<TypeConfig>, StorageError<NodeId>> {
        let snapshot_data = self
            .inner
            .read()
            .snapshot()
            .await
            .map_err(|e| StorageError::write_snapshot(None, &std::io::Error::other(e.to_string())))?;

        let last_applied = *self.last_applied.read();
        let last_membership = self.last_membership.read().clone();

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied.map(|l| l.leader_id.term).unwrap_or(0),
            last_applied.map(|l| l.leader_id.node_id).unwrap_or(0),
            last_applied.map(|l| l.index).unwrap_or(0)
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        info!("Built snapshot at index {:?}", last_applied);

        Ok(openraft::Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_data)),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoOpState;

pub struct NoOpStateMachine {
    state: NoOpState,
}

impl NoOpStateMachine {
    pub fn new() -> Self {
        Self {
            state: NoOpState::default(),
        }
    }
}

impl Default for NoOpStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateMachine for NoOpStateMachine {
    type State = NoOpState;

    async fn apply(&mut self, command: StateMachineCommand) -> StateMachineResponse {
        StateMachineResponse::ok(Some(command.payload))
    }

    async fn snapshot(&self) -> k8s_operator_core::Result<Vec<u8>> {
        serde_json::to_vec(&self.state).map_err(|e| e.into())
    }

    async fn restore(&mut self, data: &[u8]) -> k8s_operator_core::Result<()> {
        self.state = serde_json::from_slice(data)?;
        Ok(())
    }

    fn state(&self) -> &Self::State {
        &self.state
    }
}
