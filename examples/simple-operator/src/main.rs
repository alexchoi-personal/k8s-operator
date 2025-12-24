use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_operator::prelude::*;
use kube::api::Resource;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "example.com",
    version = "v1",
    kind = "SimpleApp",
    namespaced,
    status = "SimpleAppStatus",
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready", "type":"string", "jsonPath":".status.ready"}"#
)]
pub struct SimpleAppSpec {
    pub replicas: i32,
    pub image: String,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Default)]
pub struct SimpleAppStatus {
    pub ready: bool,
    pub replicas: i32,
    pub message: String,
}

struct SimpleAppReconciler;

#[async_trait]
impl Reconciler<SimpleApp> for SimpleAppReconciler {
    async fn reconcile(&self, resource: Arc<SimpleApp>) -> ReconcileResult {
        let name = resource.meta().name.clone().unwrap_or_default();
        let namespace = resource.meta().namespace.clone().unwrap_or_default();

        info!(
            "Reconciling SimpleApp: {}/{} with {} replicas",
            namespace, name, resource.spec.replicas
        );

        if let Some(msg) = &resource.spec.message {
            info!("Message: {}", msg);
        }

        ReconcileResult::Ok(Action::requeue(Duration::from_secs(60)))
    }

    async fn on_delete(&self, resource: Arc<SimpleApp>) -> ReconcileResult {
        let name = resource.meta().name.clone().unwrap_or_default();
        info!("SimpleApp {} is being deleted", name);
        ReconcileResult::Ok(Action::Done)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct AppState {
    total_reconciles: u64,
}

struct AppStateMachine {
    state: AppState,
}

impl AppStateMachine {
    fn new() -> Self {
        Self {
            state: AppState::default(),
        }
    }
}

#[async_trait]
impl k8s_operator::StateMachine for AppStateMachine {
    type State = AppState;

    async fn apply(
        &mut self,
        command: k8s_operator::StateMachineCommand,
    ) -> k8s_operator::StateMachineResponse {
        self.state.total_reconciles += 1;
        k8s_operator::StateMachineResponse::ok(Some(command.payload))
    }

    async fn snapshot(&self) -> k8s_operator::Result<Vec<u8>> {
        serde_json::to_vec(&self.state).map_err(|e| e.into())
    }

    async fn restore(&mut self, data: &[u8]) -> k8s_operator::Result<()> {
        self.state = serde_json::from_slice(data)?;
        Ok(())
    }

    fn state(&self) -> &Self::State {
        &self.state
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting SimpleApp operator");

    let config = OperatorConfig::from_env().unwrap_or_default();
    let raft_config = RaftConfig::from_env().unwrap_or_default();

    let storage = MemoryStorage::new();
    let state_machine = AppStateMachine::new();

    let mut initial_members = BTreeMap::new();
    initial_members.insert(
        0,
        k8s_operator::BasicNode {
            addr: "localhost:5000".to_string(),
        },
    );

    let operator = Operator::<SimpleApp, SimpleAppReconciler, MemoryStorage, AppStateMachine>::builder()
        .with_config(config)
        .with_raft_config(raft_config)
        .with_reconciler(SimpleAppReconciler)
        .with_storage(storage)
        .with_state_machine(state_machine)
        .with_initial_members(initial_members)
        .build()
        .await?;

    info!("Operator built successfully, starting...");
    operator.run().await?;

    Ok(())
}
