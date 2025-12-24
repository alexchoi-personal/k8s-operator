use async_trait::async_trait;
use kube::api::Resource;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{ReconcileResult, StateMachineCommand, StateMachineResponse};

#[async_trait]
pub trait Reconciler<K>: Send + Sync + 'static
where
    K: Resource + Clone + Debug + Send + Sync + 'static,
    K::DynamicType: Default,
{
    async fn reconcile(&self, resource: Arc<K>) -> ReconcileResult;

    async fn on_delete(&self, _resource: Arc<K>) -> ReconcileResult {
        ReconcileResult::Ok(crate::Action::Done)
    }

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

#[async_trait]
pub trait StateMachine: Send + Sync + 'static {
    type State: Serialize + DeserializeOwned + Clone + Default + Send + Sync;

    async fn apply(&mut self, command: StateMachineCommand) -> StateMachineResponse;

    async fn snapshot(&self) -> crate::Result<Vec<u8>>;

    async fn restore(&mut self, data: &[u8]) -> crate::Result<()>;

    fn state(&self) -> &Self::State;
}

pub trait OperatorResource: Send + Sync + 'static {
    fn resource_name(&self) -> &str;
}
