use kube::api::{Patch, PatchParams, Resource};
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::{debug, info};

use k8s_operator_core::OperatorError;

pub const DEFAULT_FINALIZER: &str = "k8s-operator.io/finalizer";

pub async fn add_finalizer<K>(client: &Client, resource: &K, finalizer: &str) -> k8s_operator_core::Result<()>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    K::DynamicType: Default,
{
    let name = resource.meta().name.clone().ok_or_else(|| {
        OperatorError::Internal("Resource has no name".to_string())
    })?;

    let namespace = resource.meta().namespace.clone();

    let api: Api<K> = if let Some(ns) = namespace {
        Api::namespaced(client.clone(), &ns)
    } else {
        Api::all(client.clone())
    };

    let patch = json!({
        "metadata": {
            "finalizers": [finalizer]
        }
    });

    api.patch(&name, &PatchParams::apply("k8s-operator"), &Patch::Merge(&patch))
        .await
        .map_err(|e| OperatorError::KubeError(e))?;

    info!("Added finalizer {} to {}", finalizer, name);
    Ok(())
}

pub async fn remove_finalizer<K>(client: &Client, resource: &K, finalizer: &str) -> k8s_operator_core::Result<()>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    K::DynamicType: Default,
{
    let name = resource.meta().name.clone().ok_or_else(|| {
        OperatorError::Internal("Resource has no name".to_string())
    })?;

    let namespace = resource.meta().namespace.clone();

    let api: Api<K> = if let Some(ns) = namespace {
        Api::namespaced(client.clone(), &ns)
    } else {
        Api::all(client.clone())
    };

    let current_finalizers: Vec<String> = resource
        .meta()
        .finalizers
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter(|f| f != finalizer)
        .collect();

    let patch = json!({
        "metadata": {
            "finalizers": current_finalizers
        }
    });

    api.patch(&name, &PatchParams::apply("k8s-operator"), &Patch::Merge(&patch))
        .await
        .map_err(|e| OperatorError::KubeError(e))?;

    info!("Removed finalizer {} from {}", finalizer, name);
    Ok(())
}

pub fn has_finalizer<K>(resource: &K, finalizer: &str) -> bool
where
    K: Resource,
{
    resource
        .meta()
        .finalizers
        .as_ref()
        .map(|f| f.contains(&finalizer.to_string()))
        .unwrap_or(false)
}

pub fn is_being_deleted<K>(resource: &K) -> bool
where
    K: Resource,
{
    resource.meta().deletion_timestamp.is_some()
}

pub struct FinalizerGuard<'a, K>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    K::DynamicType: Default,
{
    client: &'a Client,
    resource: &'a K,
    finalizer: String,
}

impl<'a, K> FinalizerGuard<'a, K>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    K::DynamicType: Default,
{
    pub fn new(client: &'a Client, resource: &'a K) -> Self {
        Self {
            client,
            resource,
            finalizer: DEFAULT_FINALIZER.to_string(),
        }
    }

    pub fn with_finalizer(mut self, finalizer: impl Into<String>) -> Self {
        self.finalizer = finalizer.into();
        self
    }

    pub async fn ensure(&self) -> k8s_operator_core::Result<()> {
        if !has_finalizer(self.resource, &self.finalizer) {
            add_finalizer(self.client, self.resource, &self.finalizer).await?;
        }
        Ok(())
    }

    pub fn is_being_deleted(&self) -> bool {
        is_being_deleted(self.resource)
    }

    pub fn has_finalizer(&self) -> bool {
        has_finalizer(self.resource, &self.finalizer)
    }

    pub async fn cleanup_complete(&self) -> k8s_operator_core::Result<()> {
        if has_finalizer(self.resource, &self.finalizer) {
            remove_finalizer(self.client, self.resource, &self.finalizer).await?;
        }
        Ok(())
    }
}
