use kube::api::{Patch, PatchParams, Resource};
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::debug;

use k8s_operator_core::OperatorError;

pub async fn update_status<K, S>(
    client: &Client,
    resource: &K,
    status: S,
) -> k8s_operator_core::Result<()>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    K::DynamicType: Default,
    S: Serialize,
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
        "status": status
    });

    api.patch_status(&name, &PatchParams::apply("k8s-operator"), &Patch::Merge(&patch))
        .await
        .map_err(|e| OperatorError::KubeError(e))?;

    debug!("Updated status for {}", name);
    Ok(())
}

pub struct StatusPatch {
    values: serde_json::Map<String, serde_json::Value>,
}

impl StatusPatch {
    pub fn new() -> Self {
        Self {
            values: serde_json::Map::new(),
        }
    }

    pub fn set<V: Serialize>(mut self, key: impl Into<String>, value: V) -> Self {
        if let Ok(v) = serde_json::to_value(value) {
            self.values.insert(key.into(), v);
        }
        self
    }

    pub fn condition(mut self, condition: StatusCondition) -> Self {
        let conditions = self
            .values
            .entry("conditions".to_string())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()));

        if let serde_json::Value::Array(ref mut arr) = conditions {
            if let Ok(v) = serde_json::to_value(&condition) {
                let existing_idx = arr.iter().position(|c| {
                    c.get("type")
                        .and_then(|t| t.as_str())
                        .map(|t| t == condition.type_)
                        .unwrap_or(false)
                });

                if let Some(idx) = existing_idx {
                    arr[idx] = v;
                } else {
                    arr.push(v);
                }
            }
        }

        self
    }

    pub async fn apply<K>(self, client: &Client, resource: &K) -> k8s_operator_core::Result<()>
    where
        K: Resource + Clone + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
        K::DynamicType: Default,
    {
        update_status(client, resource, serde_json::Value::Object(self.values)).await
    }
}

impl Default for StatusPatch {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: ConditionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_transition_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

impl StatusCondition {
    pub fn new(type_: impl Into<String>, status: ConditionStatus) -> Self {
        Self {
            type_: type_.into(),
            status,
            reason: None,
            message: None,
            last_transition_time: chrono::Utc::now().to_rfc3339(),
            observed_generation: None,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_generation(mut self, generation: i64) -> Self {
        self.observed_generation = Some(generation);
        self
    }

    pub fn ready(ready: bool) -> Self {
        Self::new(
            "Ready",
            if ready {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
        )
    }

    pub fn available(available: bool) -> Self {
        Self::new(
            "Available",
            if available {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
        )
    }

    pub fn progressing(progressing: bool) -> Self {
        Self::new(
            "Progressing",
            if progressing {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
        )
    }

    pub fn degraded(degraded: bool) -> Self {
        Self::new(
            "Degraded",
            if degraded {
                ConditionStatus::True
            } else {
                ConditionStatus::False
            },
        )
    }
}
