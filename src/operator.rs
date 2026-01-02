use crate::context::Context;
use crate::error::{Error, Result};
use futures::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::core::NamespaceResourceScope;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Client, Resource, ResourceExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

#[cfg(feature = "ha")]
use crate::raft::{
    ClusterManager, HeadlessServiceDiscovery, LeaderElection, RaftConfig, RaftNodeManager,
};

pub type ReconcileFn<R> =
    Box<dyn Fn(Context<R>) -> Pin<Box<dyn Future<Output = Result<Action>> + Send>> + Send + Sync>;

pub struct Operator<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    reconcile_fn: Option<ReconcileFn<R>>,
    delete_fn: Option<ReconcileFn<R>>,
    finalizer: Option<String>,
    requeue_after: Duration,
    error_requeue: Duration,
    #[cfg(feature = "ha")]
    raft_config: Option<RaftConfig>,
    _marker: std::marker::PhantomData<R>,
}

impl<R> Default for Operator<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> Operator<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new() -> Self {
        Self {
            reconcile_fn: None,
            delete_fn: None,
            finalizer: None,
            requeue_after: Duration::from_secs(300),
            error_requeue: Duration::from_secs(60),
            #[cfg(feature = "ha")]
            raft_config: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn reconcile<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Context<R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Action>> + Send + 'static,
    {
        self.reconcile_fn = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn on_delete<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Context<R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Action>> + Send + 'static,
    {
        self.delete_fn = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn with_finalizer(mut self) -> Self {
        self.finalizer = Some(format!(
            "{}.{}/finalizer",
            R::kind(&()).to_lowercase(),
            R::group(&())
        ));
        self
    }

    pub fn with_custom_finalizer(mut self, name: impl Into<String>) -> Self {
        self.finalizer = Some(name.into());
        self
    }

    pub fn requeue_after(mut self, duration: Duration) -> Self {
        self.requeue_after = duration;
        self
    }

    pub fn error_requeue(mut self, duration: Duration) -> Self {
        self.error_requeue = duration;
        self
    }

    #[cfg(feature = "ha")]
    pub fn with_leader_election(mut self, config: RaftConfig) -> Self {
        self.raft_config = Some(config);
        self
    }

    pub async fn run(self) -> Result<()> {
        let client = Client::try_default().await.map_err(Error::Kube)?;

        info!("Starting operator for {}/{}", R::group(&()), R::kind(&()));

        let api: Api<R> = Api::all(client.clone());

        let reconcile_fn = Arc::new(
            self.reconcile_fn
                .ok_or(Error::MissingField("reconcile_fn"))?,
        );
        let delete_fn = self.delete_fn.map(Arc::new);
        let finalizer = self.finalizer.clone();
        let default_requeue = self.requeue_after;
        let error_requeue = self.error_requeue;

        #[cfg(feature = "ha")]
        let leader_election = if let Some(config) = self.raft_config {
            info!("Initializing Raft cluster for node {}", config.node_id);

            let node_manager = RaftNodeManager::new(config.clone())
                .await
                .map_err(|e| Error::Other(format!("Failed to create Raft node: {:?}", e)))?;

            node_manager
                .start_grpc_server(8080)
                .await
                .map_err(|e| Error::Other(format!("Failed to start gRPC server: {:?}", e)))?;

            node_manager
                .bootstrap_or_join()
                .await
                .map_err(|e| Error::Other(format!("Failed to bootstrap Raft: {:?}", e)))?;

            node_manager.start_leadership_watcher();

            let node_manager = Arc::new(node_manager);

            let discovery =
                HeadlessServiceDiscovery::new(&config.service_name, &config.namespace, 8080);
            let cluster_manager = Arc::new(ClusterManager::new(node_manager.clone(), discovery));
            cluster_manager.start_membership_watcher();

            Some(node_manager.leader_election().clone())
        } else {
            None
        };

        let controller_ctx = ControllerContext {
            client: client.clone(),
            reconcile_fn,
            delete_fn,
            finalizer,
            default_requeue,
            error_requeue,
            #[cfg(feature = "ha")]
            leader_election,
        };

        Controller::new(api, WatcherConfig::default())
            .run(
                move |resource, ctx| {
                    let ctx = ctx.as_ref().clone();
                    async move { reconcile_wrapper(resource, ctx).await }
                },
                |resource, err, ctx| {
                    let ctx = Arc::clone(&ctx);
                    error_policy(resource, err, ctx)
                },
                Arc::new(controller_ctx),
            )
            .for_each(|result| async move {
                match result {
                    Ok((resource, action)) => {
                        info!("Reconciled {} - {:?}", resource.name, action);
                    }
                    Err(e) => {
                        error!("Reconciliation error: {:?}", e);
                    }
                }
            })
            .await;

        Ok(())
    }
}

struct ControllerContext<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    client: Client,
    reconcile_fn: Arc<ReconcileFn<R>>,
    delete_fn: Option<Arc<ReconcileFn<R>>>,
    finalizer: Option<String>,
    #[allow(dead_code)]
    default_requeue: Duration,
    error_requeue: Duration,
    #[cfg(feature = "ha")]
    leader_election: Option<Arc<LeaderElection>>,
}

impl<R> Clone for ControllerContext<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            reconcile_fn: Arc::clone(&self.reconcile_fn),
            delete_fn: self.delete_fn.as_ref().map(Arc::clone),
            finalizer: self.finalizer.clone(),
            default_requeue: self.default_requeue,
            error_requeue: self.error_requeue,
            #[cfg(feature = "ha")]
            leader_election: self.leader_election.as_ref().map(Arc::clone),
        }
    }
}

async fn reconcile_wrapper<R>(
    resource: Arc<R>,
    ctx: ControllerContext<R>,
) -> std::result::Result<Action, Error>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    #[cfg(feature = "ha")]
    if let Some(ref leader_election) = ctx.leader_election {
        if !leader_election.is_leader() {
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
    }

    let namespace = resource.namespace().unwrap_or_default();
    let name = resource.name_any();

    info!(
        "Reconciling {}/{} in namespace {}",
        R::kind(&()),
        name,
        namespace
    );

    let is_deleted = resource.meta().deletion_timestamp.is_some();

    if let Some(ref finalizer_name) = ctx.finalizer {
        let has_finalizer = resource.finalizers().iter().any(|f| f == finalizer_name);

        if is_deleted {
            if has_finalizer {
                if let Some(ref delete_fn) = ctx.delete_fn {
                    let context = Context::new(Arc::clone(&resource), ctx.client.clone());
                    delete_fn(context).await?;
                }

                remove_finalizer(&ctx.client, resource.as_ref(), &namespace, finalizer_name)
                    .await?;
            }
            return Ok(Action::await_change());
        } else if !has_finalizer {
            add_finalizer(&ctx.client, resource.as_ref(), &namespace, finalizer_name).await?;
        }
    } else if is_deleted {
        if let Some(ref delete_fn) = ctx.delete_fn {
            let context = Context::new(Arc::clone(&resource), ctx.client.clone());
            return delete_fn(context).await;
        }
        return Ok(Action::await_change());
    }

    let context = Context::new(resource, ctx.client.clone());
    (ctx.reconcile_fn)(context).await
}

fn error_policy<R>(resource: Arc<R>, error: &Error, ctx: Arc<ControllerContext<R>>) -> Action
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let name = resource.name_any();
    error!("Error reconciling {}: {:?}", name, error);
    Action::requeue(ctx.error_requeue)
}

async fn add_finalizer<R>(
    client: &Client,
    resource: &R,
    namespace: &str,
    finalizer: &str,
) -> Result<()>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let api: Api<R> = Api::namespaced(client.clone(), namespace);
    let name = resource.name_any();

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [finalizer]
        }
    });

    api.patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}

async fn remove_finalizer<R>(
    client: &Client,
    resource: &R,
    namespace: &str,
    finalizer: &str,
) -> Result<()>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let api: Api<R> = Api::namespaced(client.clone(), namespace);
    let name = resource.name_any();

    let finalizers: Vec<String> = resource
        .finalizers()
        .iter()
        .filter(|f| *f != finalizer)
        .cloned()
        .collect();

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": finalizers
        }
    });

    api.patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .map_err(Error::Kube)?;

    Ok(())
}
