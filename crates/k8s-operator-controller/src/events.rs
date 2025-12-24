use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::chrono::Utc;
use kube::api::{ObjectMeta, PostParams, Resource};
use kube::{Api, Client};
use tracing::debug;

use k8s_operator_core::OperatorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Normal,
    Warning,
}

impl EventType {
    fn as_str(&self) -> &'static str {
        match self {
            EventType::Normal => "Normal",
            EventType::Warning => "Warning",
        }
    }
}

pub struct EventRecorder {
    client: Client,
    component: String,
}

impl EventRecorder {
    pub fn new(client: Client, component: impl Into<String>) -> Self {
        Self {
            client,
            component: component.into(),
        }
    }

    pub async fn record<K>(
        &self,
        resource: &K,
        event_type: EventType,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> k8s_operator_core::Result<()>
    where
        K: Resource,
    {
        let reason = reason.into();
        let message = message.into();

        let namespace = resource
            .meta()
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        let name = resource.meta().name.clone().unwrap_or_default();
        let uid = resource.meta().uid.clone().unwrap_or_default();

        let event_name = format!(
            "{}.{}.{}",
            name,
            self.component.replace('/', "-"),
            Utc::now().timestamp_millis()
        );

        let event = Event {
            metadata: ObjectMeta {
                name: Some(event_name),
                namespace: Some(namespace.clone()),
                ..Default::default()
            },
            involved_object: k8s_openapi::api::core::v1::ObjectReference {
                api_version: Some(K::api_version(&K::DynamicType::default()).to_string()),
                kind: Some(K::kind(&K::DynamicType::default()).to_string()),
                name: Some(name.clone()),
                namespace: Some(namespace.clone()),
                uid: Some(uid),
                ..Default::default()
            },
            reason: Some(reason),
            message: Some(message),
            type_: Some(event_type.as_str().to_string()),
            first_timestamp: Some(Time(Utc::now())),
            last_timestamp: Some(Time(Utc::now())),
            count: Some(1),
            reporting_component: Some(self.component.clone()),
            reporting_instance: Some(self.component.clone()),
            ..Default::default()
        };

        let events: Api<Event> = Api::namespaced(self.client.clone(), &namespace);
        events
            .create(&PostParams::default(), &event)
            .await
            .map_err(|e| OperatorError::KubeError(e))?;

        debug!(
            "Recorded event: {} - {} for {}",
            event_type.as_str(),
            event.reason.unwrap_or_default(),
            name
        );

        Ok(())
    }

    pub async fn normal<K>(
        &self,
        resource: &K,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> k8s_operator_core::Result<()>
    where
        K: Resource,
    {
        self.record(resource, EventType::Normal, reason, message)
            .await
    }

    pub async fn warning<K>(
        &self,
        resource: &K,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> k8s_operator_core::Result<()>
    where
        K: Resource,
    {
        self.record(resource, EventType::Warning, reason, message)
            .await
    }
}

pub struct EventBuilder<'a, K>
where
    K: Resource,
{
    recorder: &'a EventRecorder,
    resource: &'a K,
    event_type: EventType,
    reason: Option<String>,
    message: Option<String>,
}

impl<'a, K> EventBuilder<'a, K>
where
    K: Resource,
{
    pub fn new(recorder: &'a EventRecorder, resource: &'a K) -> Self {
        Self {
            recorder,
            resource,
            event_type: EventType::Normal,
            reason: None,
            message: None,
        }
    }

    pub fn event_type(mut self, event_type: EventType) -> Self {
        self.event_type = event_type;
        self
    }

    pub fn normal(mut self) -> Self {
        self.event_type = EventType::Normal;
        self
    }

    pub fn warning(mut self) -> Self {
        self.event_type = EventType::Warning;
        self
    }

    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub async fn emit(self) -> k8s_operator_core::Result<()> {
        let reason = self.reason.unwrap_or_else(|| "Unknown".to_string());
        let message = self.message.unwrap_or_default();

        self.recorder
            .record(self.resource, self.event_type, reason, message)
            .await
    }
}
