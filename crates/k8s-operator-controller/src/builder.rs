use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use kube::api::Resource;
use kube::Client;
use serde::de::DeserializeOwned;
use tracing::info;

use k8s_operator_core::{OperatorConfig, OperatorError, Reconciler, StateMachine};
use k8s_operator_raft::{RaftConfig, RaftNode};
use k8s_operator_storage::{BasicNode, NodeId, StorageBackend};

pub struct OperatorBuilder<K, R, S, SM>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    config: OperatorConfig,
    raft_config: Option<RaftConfig>,
    reconciler: Option<R>,
    storage: Option<S>,
    state_machine: Option<SM>,
    initial_members: BTreeMap<NodeId, BasicNode>,
    _phantom: PhantomData<K>,
}

impl<K, R, S, SM> OperatorBuilder<K, R, S, SM>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + DeserializeOwned + 'static,
    K::DynamicType: Default,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    pub fn new() -> Self {
        Self {
            config: OperatorConfig::default(),
            raft_config: None,
            reconciler: None,
            storage: None,
            state_machine: None,
            initial_members: BTreeMap::new(),
            _phantom: PhantomData,
        }
    }

    pub fn with_config(mut self, config: OperatorConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_raft_config(mut self, config: RaftConfig) -> Self {
        self.raft_config = Some(config);
        self
    }

    pub fn with_reconciler(mut self, reconciler: R) -> Self {
        self.reconciler = Some(reconciler);
        self
    }

    pub fn with_storage(mut self, storage: S) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn with_state_machine(mut self, state_machine: SM) -> Self {
        self.state_machine = Some(state_machine);
        self
    }

    pub fn with_initial_member(mut self, node_id: NodeId, addr: impl Into<String>) -> Self {
        self.initial_members.insert(
            node_id,
            BasicNode {
                addr: addr.into(),
            },
        );
        self
    }

    pub fn with_initial_members(mut self, members: BTreeMap<NodeId, BasicNode>) -> Self {
        self.initial_members = members;
        self
    }

    pub async fn build(self) -> k8s_operator_core::Result<Operator<K, R, S, SM>> {
        let reconciler = self
            .reconciler
            .ok_or_else(|| OperatorError::ConfigError("Reconciler is required".to_string()))?;

        let storage = self
            .storage
            .ok_or_else(|| OperatorError::ConfigError("Storage is required".to_string()))?;

        let state_machine = self
            .state_machine
            .ok_or_else(|| OperatorError::ConfigError("State machine is required".to_string()))?;

        let raft_config = self.raft_config.unwrap_or_else(|| {
            RaftConfig::builder()
                .node_id(self.config.node_id)
                .cluster_name(&self.config.cluster_name)
                .listen_addr(format!("0.0.0.0:{}", self.config.raft_port))
                .advertise_addr(self.config.peer_address(self.config.node_id))
                .build()
        });

        let raft_node = RaftNode::new(raft_config, storage, state_machine).await?;

        let client = Client::try_default()
            .await
            .map_err(|e| OperatorError::KubeError(e))?;

        info!(
            "Operator built for resource: {}",
            K::kind(&K::DynamicType::default())
        );

        Ok(Operator {
            config: self.config,
            reconciler: Arc::new(reconciler),
            raft_node,
            initial_members: self.initial_members,
            client,
            _phantom: PhantomData,
        })
    }
}

impl<K, R, S, SM> Default for OperatorBuilder<K, R, S, SM>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + DeserializeOwned + 'static,
    K::DynamicType: Default,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct Operator<K, R, S, SM>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    K::DynamicType: Default,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    config: OperatorConfig,
    reconciler: Arc<R>,
    raft_node: RaftNode<S, SM>,
    initial_members: BTreeMap<NodeId, BasicNode>,
    client: Client,
    _phantom: PhantomData<K>,
}

impl<K, R, S, SM> Operator<K, R, S, SM>
where
    K: Resource + Clone + std::fmt::Debug + Send + Sync + DeserializeOwned + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
    R: Reconciler<K>,
    S: StorageBackend,
    SM: StateMachine,
{
    pub fn builder() -> OperatorBuilder<K, R, S, SM> {
        OperatorBuilder::new()
    }

    pub async fn run(mut self) -> k8s_operator_core::Result<()> {
        info!("Starting operator");

        self.raft_node.start_server().await?;

        if !self.initial_members.is_empty() {
            if let Err(e) = self.raft_node.initialize_cluster(self.initial_members.clone()).await {
                info!("Cluster initialization: {}", e);
            }
        }

        let reconciler = self.reconciler.clone();
        let raft_node = &self.raft_node;
        let client = self.client.clone();
        let namespace = self.config.namespace.clone();

        crate::reconciler::run_controller(client, namespace, reconciler, raft_node).await?;

        Ok(())
    }

    pub fn raft_node(&self) -> &RaftNode<S, SM> {
        &self.raft_node
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn config(&self) -> &OperatorConfig {
        &self.config
    }
}
