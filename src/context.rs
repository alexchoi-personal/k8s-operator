use crate::error::{Error, Result};
use crate::types::{Annotations, ChildResource, Labels};
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time};
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::core::NamespaceResourceScope;
use kube::runtime::controller::Action;
use kube::{Client, Resource, ResourceExt};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

pub struct Context<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope> + Clone,
{
    resource: Arc<R>,
    client: Client,
    namespace: String,
    name: String,
}

impl<R> Context<R>
where
    R: Resource<DynamicType = (), Scope = NamespaceResourceScope> + Clone + std::fmt::Debug,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new(resource: Arc<R>, client: Client) -> Self {
        let namespace = resource.namespace().unwrap_or_default();
        let name = resource.name_any();
        Self {
            resource,
            client,
            namespace,
            name,
        }
    }

    pub fn resource(&self) -> &R {
        &self.resource
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn labels(&self) -> Labels {
        Labels(
            self.resource
                .labels()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }

    pub fn annotations(&self) -> Annotations {
        Annotations(
            self.resource
                .annotations()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        )
    }

    pub fn owner_reference(&self) -> OwnerReference {
        OwnerReference {
            api_version: R::api_version(&()).to_string(),
            kind: R::kind(&()).to_string(),
            name: self.name.clone(),
            uid: self.resource.uid().unwrap_or_default(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    pub async fn apply<T: ChildResource>(&self, resource: T) -> Result<T::K8sType>
    where
        T::K8sType: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>,
    {
        let k8s_resource = resource.into_k8s(&self.namespace, Some(self.owner_reference()));
        let api: Api<T::K8sType> = Api::namespaced(self.client.clone(), &self.namespace);

        let name = k8s_resource.name_any();
        let patch = Patch::Apply(&k8s_resource);
        let params = PatchParams::apply("k8s-operator").force();

        api.patch(&name, &params, &patch).await.map_err(Error::Kube)
    }

    pub async fn delete<T>(&self, name: &str) -> Result<()>
    where
        T: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>
            + Clone
            + std::fmt::Debug,
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let api: Api<T> = Api::namespaced(self.client.clone(), &self.namespace);

        match api.delete(name, &Default::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(Error::Kube(e)),
        }
    }

    pub async fn get<T>(&self, name: &str) -> Result<Option<T>>
    where
        T: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>
            + Clone
            + std::fmt::Debug,
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let api: Api<T> = Api::namespaced(self.client.clone(), &self.namespace);

        match api.get(name).await {
            Ok(resource) => Ok(Some(resource)),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(Error::Kube(e)),
        }
    }

    pub async fn list<T>(&self, selector: Option<&str>) -> Result<Vec<T>>
    where
        T: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>
            + Clone
            + std::fmt::Debug,
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let api: Api<T> = Api::namespaced(self.client.clone(), &self.namespace);

        let list_params = if let Some(sel) = selector {
            kube::api::ListParams::default().labels(sel)
        } else {
            kube::api::ListParams::default()
        };

        let list = api.list(&list_params).await.map_err(Error::Kube)?;
        Ok(list.items)
    }

    pub async fn set_status<S>(&self, status: S) -> Result<()>
    where
        S: Serialize,
    {
        let api: Api<R> = Api::namespaced(self.client.clone(), &self.namespace);

        let status_json = serde_json::json!({
            "status": status
        });

        let patch = Patch::Merge(&status_json);
        let params = PatchParams::default();

        api.patch_status(&self.name, &params, &patch)
            .await
            .map_err(Error::Kube)?;

        Ok(())
    }

    pub async fn set_condition(&self, condition: Condition) -> Result<()> {
        let api: Api<R> = Api::namespaced(self.client.clone(), &self.namespace);

        let status_json = serde_json::json!({
            "status": {
                "conditions": [condition]
            }
        });

        let patch = Patch::Merge(&status_json);
        let params = PatchParams::default();

        api.patch_status(&self.name, &params, &patch)
            .await
            .map_err(Error::Kube)?;

        Ok(())
    }

    pub async fn event(&self, reason: &str, message: &str) -> Result<()> {
        self.record_event("Normal", reason, message).await
    }

    pub async fn warning(&self, reason: &str, message: &str) -> Result<()> {
        self.record_event("Warning", reason, message).await
    }

    async fn record_event(&self, type_: &str, reason: &str, message: &str) -> Result<()> {
        let api: Api<Event> = Api::namespaced(self.client.clone(), &self.namespace);

        let now = chrono::Utc::now();
        let event_name = format!(
            "{}.{:x}",
            self.name,
            now.timestamp_nanos_opt().unwrap_or_default()
        );

        let event = Event {
            metadata: ObjectMeta {
                name: Some(event_name),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            type_: Some(type_.to_string()),
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
            involved_object: k8s_openapi::api::core::v1::ObjectReference {
                api_version: Some(R::api_version(&()).to_string()),
                kind: Some(R::kind(&()).to_string()),
                name: Some(self.name.clone()),
                namespace: Some(self.namespace.clone()),
                uid: self.resource.uid(),
                ..Default::default()
            },
            first_timestamp: Some(Time(now)),
            last_timestamp: Some(Time(now)),
            count: Some(1),
            reporting_component: Some("k8s-operator".to_string()),
            reporting_instance: Some(
                std::env::var("POD_NAME").unwrap_or_else(|_| "operator".to_string()),
            ),
            ..Default::default()
        };

        api.create(&PostParams::default(), &event)
            .await
            .map_err(Error::Kube)?;

        Ok(())
    }

    pub fn requeue_after(&self, duration: Duration) -> Action {
        Action::requeue(duration)
    }

    pub fn done(&self) -> Action {
        Action::await_change()
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
}

impl Condition {
    pub fn ready(status: bool) -> Self {
        Self {
            type_: "Ready".to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            reason: if status { "Ready" } else { "NotReady" }.to_string(),
            message: String::new(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn available(status: bool) -> Self {
        Self {
            type_: "Available".to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            reason: if status { "Available" } else { "Unavailable" }.to_string(),
            message: String::new(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn progressing(status: bool) -> Self {
        Self {
            type_: "Progressing".to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            reason: if status { "Progressing" } else { "Complete" }.to_string(),
            message: String::new(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn custom(type_: impl Into<String>, status: bool) -> Self {
        Self {
            type_: type_.into(),
            status: if status { "True" } else { "False" }.to_string(),
            reason: String::new(),
            message: String::new(),
            last_transition_time: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }
}
