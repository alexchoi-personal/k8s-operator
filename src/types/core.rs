use super::{Annotations, Container, Labels, Selector, Volume};
use crate::types::ChildResource;
use k8s_openapi::api::core::v1 as k8s;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Pod {
    pub name: String,
    pub labels: Labels,
    pub annotations: Annotations,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub service_account: Option<String>,
    pub restart_policy: Option<String>,
    pub node_selector: BTreeMap<String, String>,
}

impl Pod {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            annotations: Annotations::new(),
            containers: Vec::new(),
            init_containers: Vec::new(),
            volumes: Vec::new(),
            service_account: None,
            restart_policy: None,
            node_selector: BTreeMap::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn annotations(mut self, annotations: Annotations) -> Self {
        self.annotations = annotations;
        self
    }

    pub fn container(mut self, container: Container) -> Self {
        self.containers.push(container);
        self
    }

    pub fn init_container(mut self, container: Container) -> Self {
        self.init_containers.push(container);
        self
    }

    pub fn volume(mut self, volume: Volume) -> Self {
        self.volumes.push(volume);
        self
    }

    pub fn service_account(mut self, name: impl Into<String>) -> Self {
        self.service_account = Some(name.into());
        self
    }

    pub fn restart_always(mut self) -> Self {
        self.restart_policy = Some("Always".to_string());
        self
    }

    pub fn restart_never(mut self) -> Self {
        self.restart_policy = Some("Never".to_string());
        self
    }

    pub fn restart_on_failure(mut self) -> Self {
        self.restart_policy = Some("OnFailure".to_string());
        self
    }

    pub fn node_selector(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.node_selector.insert(key.into(), value.into());
        self
    }
}

impl ChildResource for Pod {
    type K8sType = k8s::Pod;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::Pod {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                annotations: if self.annotations.0.is_empty() {
                    None
                } else {
                    Some(self.annotations.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(k8s::PodSpec {
                containers: self.containers.into_iter().map(|c| c.into_k8s()).collect(),
                init_containers: if self.init_containers.is_empty() {
                    None
                } else {
                    Some(
                        self.init_containers
                            .into_iter()
                            .map(|c| c.into_k8s())
                            .collect(),
                    )
                },
                volumes: if self.volumes.is_empty() {
                    None
                } else {
                    Some(self.volumes.into_iter().map(|v| v.into_k8s()).collect())
                },
                service_account_name: self.service_account,
                restart_policy: self.restart_policy,
                node_selector: if self.node_selector.is_empty() {
                    None
                } else {
                    Some(self.node_selector)
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConfigMap {
    pub name: String,
    pub labels: Labels,
    pub data: BTreeMap<String, String>,
    pub binary_data: BTreeMap<String, Vec<u8>>,
}

impl ConfigMap {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            data: BTreeMap::new(),
            binary_data: BTreeMap::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn data(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.data.insert(key.into(), value.into());
        self
    }

    pub fn binary_data(mut self, key: impl Into<String>, value: Vec<u8>) -> Self {
        self.binary_data.insert(key.into(), value);
        self
    }

    pub fn from_file(mut self, key: impl Into<String>, content: impl Into<String>) -> Self {
        self.data.insert(key.into(), content.into());
        self
    }
}

impl ChildResource for ConfigMap {
    type K8sType = k8s::ConfigMap;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        use k8s_openapi::ByteString;

        k8s::ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            data: if self.data.is_empty() {
                None
            } else {
                Some(self.data)
            },
            binary_data: if self.binary_data.is_empty() {
                None
            } else {
                Some(
                    self.binary_data
                        .into_iter()
                        .map(|(k, v)| (k, ByteString(v)))
                        .collect(),
                )
            },
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct Secret {
    pub name: String,
    pub labels: Labels,
    pub type_: Option<String>,
    pub data: BTreeMap<String, Vec<u8>>,
    pub string_data: BTreeMap<String, String>,
}

impl Secret {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            type_: None,
            data: BTreeMap::new(),
            string_data: BTreeMap::new(),
        }
    }

    pub fn opaque(name: impl Into<String>) -> Self {
        Self::new(name).type_("Opaque")
    }

    pub fn docker_config(name: impl Into<String>) -> Self {
        Self::new(name).type_("kubernetes.io/dockerconfigjson")
    }

    pub fn tls(name: impl Into<String>) -> Self {
        Self::new(name).type_("kubernetes.io/tls")
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn type_(mut self, t: impl Into<String>) -> Self {
        self.type_ = Some(t.into());
        self
    }

    pub fn data(mut self, key: impl Into<String>, value: Vec<u8>) -> Self {
        self.data.insert(key.into(), value);
        self
    }

    pub fn string_data(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.string_data.insert(key.into(), value.into());
        self
    }
}

impl ChildResource for Secret {
    type K8sType = k8s::Secret;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        use k8s_openapi::ByteString;

        k8s::Secret {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            type_: self.type_,
            data: if self.data.is_empty() {
                None
            } else {
                Some(
                    self.data
                        .into_iter()
                        .map(|(k, v)| (k, ByteString(v)))
                        .collect(),
                )
            },
            string_data: if self.string_data.is_empty() {
                None
            } else {
                Some(self.string_data)
            },
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct Service {
    pub name: String,
    pub labels: Labels,
    pub selector: Selector,
    pub ports: Vec<ServicePort>,
    pub type_: Option<String>,
    pub cluster_ip: Option<String>,
    pub external_traffic_policy: Option<String>,
}

impl Service {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            labels: Labels::new().insert("app", &name),
            selector: Selector::new().match_labels("app", &name),
            ports: Vec::new(),
            type_: None,
            cluster_ip: None,
            external_traffic_policy: None,
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn selector(mut self, selector: Selector) -> Self {
        self.selector = selector;
        self
    }

    pub fn port(mut self, port: i32) -> Self {
        self.ports.push(ServicePort {
            port,
            target_port: None,
            name: None,
            protocol: None,
            node_port: None,
        });
        self
    }

    pub fn named_port(mut self, name: impl Into<String>, port: i32, target_port: i32) -> Self {
        self.ports.push(ServicePort {
            port,
            target_port: Some(target_port),
            name: Some(name.into()),
            protocol: None,
            node_port: None,
        });
        self
    }

    pub fn port_with_target(mut self, port: i32, target_port: i32) -> Self {
        self.ports.push(ServicePort {
            port,
            target_port: Some(target_port),
            name: None,
            protocol: None,
            node_port: None,
        });
        self
    }

    pub fn cluster_ip(mut self) -> Self {
        self.type_ = Some("ClusterIP".to_string());
        self
    }

    pub fn node_port(mut self) -> Self {
        self.type_ = Some("NodePort".to_string());
        self
    }

    pub fn load_balancer(mut self) -> Self {
        self.type_ = Some("LoadBalancer".to_string());
        self
    }

    pub fn headless(mut self) -> Self {
        self.type_ = Some("ClusterIP".to_string());
        self.cluster_ip = Some("None".to_string());
        self
    }

    pub fn external_traffic_local(mut self) -> Self {
        self.external_traffic_policy = Some("Local".to_string());
        self
    }
}

impl ChildResource for Service {
    type K8sType = k8s::Service;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::Service {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(self.labels.into_inner()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(k8s::ServiceSpec {
                selector: Some(self.selector.into_inner()),
                ports: if self.ports.is_empty() {
                    None
                } else {
                    Some(self.ports.into_iter().map(|p| p.into_k8s()).collect())
                },
                type_: self.type_,
                cluster_ip: self.cluster_ip,
                external_traffic_policy: self.external_traffic_policy,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServicePort {
    pub port: i32,
    pub target_port: Option<i32>,
    pub name: Option<String>,
    pub protocol: Option<String>,
    pub node_port: Option<i32>,
}

impl ServicePort {
    pub fn into_k8s(self) -> k8s::ServicePort {
        k8s::ServicePort {
            port: self.port,
            target_port: self.target_port.map(IntOrString::Int),
            name: self.name,
            protocol: self.protocol,
            node_port: self.node_port,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServiceAccount {
    pub name: String,
    pub labels: Labels,
    pub automount_service_account_token: Option<bool>,
    pub image_pull_secrets: Vec<String>,
}

impl ServiceAccount {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            automount_service_account_token: None,
            image_pull_secrets: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn automount_token(mut self, value: bool) -> Self {
        self.automount_service_account_token = Some(value);
        self
    }

    pub fn image_pull_secret(mut self, name: impl Into<String>) -> Self {
        self.image_pull_secrets.push(name.into());
        self
    }
}

impl ChildResource for ServiceAccount {
    type K8sType = k8s::ServiceAccount;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::ServiceAccount {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            automount_service_account_token: self.automount_service_account_token,
            image_pull_secrets: if self.image_pull_secrets.is_empty() {
                None
            } else {
                Some(
                    self.image_pull_secrets
                        .into_iter()
                        .map(|n| k8s::LocalObjectReference { name: n })
                        .collect(),
                )
            },
            ..Default::default()
        }
    }
}
