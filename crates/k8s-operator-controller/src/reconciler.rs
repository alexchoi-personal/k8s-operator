use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::Resource;
use kube::runtime::controller::Action as KubeAction;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::Controller;
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use tracing::{debug, error, info, warn};

use k8s_operator_core::{Action, NodeRole, OperatorError, ReconcileError, ReconcileResult, Reconciler, StateMachine};
use k8s_operator_raft::RaftNode;
use k8s_operator_storage::StorageBackend;

pub async fn run_controller<K, R, S, SM>(
    client: Client,
    namespace: String,
    reconciler: Arc<R>,
    raft_node: &RaftNode<S, SM>,
) -> k8s_operator_core::Result<()>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + DeserializeOwned + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    let api: Api<K> = if namespace.is_empty() || namespace == "*" {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), &namespace)
    };

    let watcher_config = WatcherConfig::default();
    let mut role_rx = raft_node.subscribe_role();

    info!(
        "Starting controller for {}",
        K::kind(&K::DynamicType::default())
    );

    let reconciler_clone = reconciler.clone();

    let controller = Controller::new(api.clone(), watcher_config)
        .run(
            move |resource, _ctx| {
                let reconciler = reconciler_clone.clone();
                let mut role_rx = role_rx.clone();

                async move {
                    let current_role = *role_rx.borrow_and_update();

                    if current_role != NodeRole::Leader {
                        debug!(
                            "Skipping reconciliation for {:?} - not leader (role: {:?})",
                            resource.meta().name,
                            current_role
                        );
                        return Ok(KubeAction::requeue(Duration::from_secs(30)));
                    }

                    let resource_name = resource.meta().name.clone().unwrap_or_default();
                    debug!("Reconciling: {}", resource_name);

                    let result = reconciler.reconcile(Arc::new(resource)).await;

                    match result {
                        ReconcileResult::Ok(action) => {
                            debug!("Reconciliation successful for {}", resource_name);
                            Ok(action_to_kube_action(action))
                        }
                        ReconcileResult::Err(e) => {
                            if e.is_retriable() {
                                warn!("Retriable error for {}: {}", resource_name, e);
                                Ok(KubeAction::requeue(Duration::from_secs(10)))
                            } else {
                                error!("Permanent error for {}: {}", resource_name, e);
                                Err(e)
                            }
                        }
                    }
                }
            },
            |_resource, error, _ctx| {
                error!("Controller error: {:?}", error);
                KubeAction::requeue(Duration::from_secs(60))
            },
            Arc::new(()),
        )
        .for_each(|result| async move {
            match result {
                Ok(_) => {}
                Err(e) => error!("Controller reconciliation error: {:?}", e),
            }
        });

    controller.await;
    Ok(())
}

fn action_to_kube_action(action: Action) -> KubeAction {
    match action {
        Action::Requeue(duration) => KubeAction::requeue(duration),
        Action::RequeueAfter(duration) => KubeAction::requeue(duration),
        Action::Done => KubeAction::requeue(Duration::from_secs(300)),
    }
}

pub struct ReconcileContext<K>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default,
{
    pub resource: Arc<K>,
    pub client: Client,
    pub namespace: String,
}

impl<K> ReconcileContext<K>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default,
{
    pub fn new(resource: Arc<K>, client: Client, namespace: String) -> Self {
        Self {
            resource,
            client,
            namespace,
        }
    }

    pub fn name(&self) -> String {
        self.resource.meta().name.clone().unwrap_or_default()
    }

    pub fn namespace(&self) -> String {
        self.resource
            .meta()
            .namespace
            .clone()
            .unwrap_or_else(|| self.namespace.clone())
    }
}
