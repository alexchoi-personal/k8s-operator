use super::{Annotations, Labels, Selector};
use crate::types::ChildResource;
use k8s_openapi::api::networking::v1 as k8s;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

#[derive(Clone, Debug)]
pub struct Ingress {
    pub name: String,
    pub labels: Labels,
    pub annotations: Annotations,
    pub ingress_class: Option<String>,
    pub rules: Vec<IngressRule>,
    pub tls: Vec<IngressTls>,
}

impl Ingress {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            annotations: Annotations::new(),
            ingress_class: None,
            rules: Vec::new(),
            tls: Vec::new(),
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

    pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations = self.annotations.insert(key, value);
        self
    }

    pub fn ingress_class(mut self, class: impl Into<String>) -> Self {
        self.ingress_class = Some(class.into());
        self
    }

    pub fn rule(mut self, rule: IngressRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn tls(mut self, hosts: Vec<impl Into<String>>, secret_name: impl Into<String>) -> Self {
        self.tls.push(IngressTls {
            hosts: hosts.into_iter().map(Into::into).collect(),
            secret_name: secret_name.into(),
        });
        self
    }
}

impl ChildResource for Ingress {
    type K8sType = k8s::Ingress;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::Ingress {
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
            spec: Some(k8s::IngressSpec {
                ingress_class_name: self.ingress_class,
                rules: if self.rules.is_empty() {
                    None
                } else {
                    Some(self.rules.into_iter().map(|r| r.into_k8s()).collect())
                },
                tls: if self.tls.is_empty() {
                    None
                } else {
                    Some(self.tls.into_iter().map(|t| t.into_k8s()).collect())
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct IngressRule {
    pub host: Option<String>,
    pub paths: Vec<IngressPath>,
}

impl IngressRule {
    pub fn new() -> Self {
        Self {
            host: None,
            paths: Vec::new(),
        }
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn path(
        mut self,
        path: impl Into<String>,
        service_name: impl Into<String>,
        service_port: i32,
    ) -> Self {
        self.paths.push(IngressPath {
            path: path.into(),
            path_type: "Prefix".to_string(),
            service_name: service_name.into(),
            service_port,
        });
        self
    }

    pub fn exact_path(
        mut self,
        path: impl Into<String>,
        service_name: impl Into<String>,
        service_port: i32,
    ) -> Self {
        self.paths.push(IngressPath {
            path: path.into(),
            path_type: "Exact".to_string(),
            service_name: service_name.into(),
            service_port,
        });
        self
    }

    pub fn into_k8s(self) -> k8s::IngressRule {
        k8s::IngressRule {
            host: self.host,
            http: Some(k8s::HTTPIngressRuleValue {
                paths: self.paths.into_iter().map(|p| p.into_k8s()).collect(),
            }),
        }
    }
}

impl Default for IngressRule {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct IngressPath {
    pub path: String,
    pub path_type: String,
    pub service_name: String,
    pub service_port: i32,
}

impl IngressPath {
    pub fn into_k8s(self) -> k8s::HTTPIngressPath {
        k8s::HTTPIngressPath {
            path: Some(self.path),
            path_type: self.path_type,
            backend: k8s::IngressBackend {
                service: Some(k8s::IngressServiceBackend {
                    name: self.service_name,
                    port: Some(k8s::ServiceBackendPort {
                        number: Some(self.service_port),
                        name: None,
                    }),
                }),
                resource: None,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct IngressTls {
    pub hosts: Vec<String>,
    pub secret_name: String,
}

impl IngressTls {
    pub fn into_k8s(self) -> k8s::IngressTLS {
        k8s::IngressTLS {
            hosts: Some(self.hosts),
            secret_name: Some(self.secret_name),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetworkPolicy {
    pub name: String,
    pub labels: Labels,
    pub pod_selector: Selector,
    pub policy_types: Vec<String>,
    pub ingress: Vec<NetworkPolicyIngress>,
    pub egress: Vec<NetworkPolicyEgress>,
}

impl NetworkPolicy {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            pod_selector: Selector::new(),
            policy_types: Vec::new(),
            ingress: Vec::new(),
            egress: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn pod_selector(mut self, selector: Selector) -> Self {
        self.pod_selector = selector;
        self
    }

    pub fn allow_ingress(mut self, rule: NetworkPolicyIngress) -> Self {
        if !self.policy_types.contains(&"Ingress".to_string()) {
            self.policy_types.push("Ingress".to_string());
        }
        self.ingress.push(rule);
        self
    }

    pub fn allow_egress(mut self, rule: NetworkPolicyEgress) -> Self {
        if !self.policy_types.contains(&"Egress".to_string()) {
            self.policy_types.push("Egress".to_string());
        }
        self.egress.push(rule);
        self
    }

    pub fn deny_all_ingress(mut self) -> Self {
        self.policy_types.push("Ingress".to_string());
        self
    }

    pub fn deny_all_egress(mut self) -> Self {
        self.policy_types.push("Egress".to_string());
        self
    }
}

impl ChildResource for NetworkPolicy {
    type K8sType = k8s::NetworkPolicy;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::NetworkPolicy {
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
            spec: Some(k8s::NetworkPolicySpec {
                pod_selector: Some(LabelSelector {
                    match_labels: if self.pod_selector.0.is_empty() {
                        None
                    } else {
                        Some(self.pod_selector.into_inner())
                    },
                    match_expressions: None,
                }),
                policy_types: if self.policy_types.is_empty() {
                    None
                } else {
                    Some(self.policy_types)
                },
                ingress: if self.ingress.is_empty() {
                    None
                } else {
                    Some(self.ingress.into_iter().map(|i| i.into_k8s()).collect())
                },
                egress: if self.egress.is_empty() {
                    None
                } else {
                    Some(self.egress.into_iter().map(|e| e.into_k8s()).collect())
                },
            }),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NetworkPolicyIngress {
    pub from: Vec<NetworkPolicyPeer>,
    pub ports: Vec<NetworkPolicyPort>,
}

impl NetworkPolicyIngress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_pod_selector(mut self, selector: Selector) -> Self {
        self.from.push(NetworkPolicyPeer::PodSelector(selector));
        self
    }

    pub fn from_namespace_selector(mut self, selector: Selector) -> Self {
        self.from
            .push(NetworkPolicyPeer::NamespaceSelector(selector));
        self
    }

    pub fn from_ip_block(mut self, cidr: impl Into<String>) -> Self {
        self.from.push(NetworkPolicyPeer::IpBlock {
            cidr: cidr.into(),
            except: Vec::new(),
        });
        self
    }

    pub fn port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: None,
        });
        self
    }

    pub fn tcp_port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: Some("TCP".to_string()),
        });
        self
    }

    pub fn udp_port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: Some("UDP".to_string()),
        });
        self
    }

    pub fn into_k8s(self) -> k8s::NetworkPolicyIngressRule {
        k8s::NetworkPolicyIngressRule {
            from: if self.from.is_empty() {
                None
            } else {
                Some(self.from.into_iter().map(|p| p.into_k8s()).collect())
            },
            ports: if self.ports.is_empty() {
                None
            } else {
                Some(self.ports.into_iter().map(|p| p.into_k8s()).collect())
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NetworkPolicyEgress {
    pub to: Vec<NetworkPolicyPeer>,
    pub ports: Vec<NetworkPolicyPort>,
}

impl NetworkPolicyEgress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_pod_selector(mut self, selector: Selector) -> Self {
        self.to.push(NetworkPolicyPeer::PodSelector(selector));
        self
    }

    pub fn to_namespace_selector(mut self, selector: Selector) -> Self {
        self.to.push(NetworkPolicyPeer::NamespaceSelector(selector));
        self
    }

    pub fn to_ip_block(mut self, cidr: impl Into<String>) -> Self {
        self.to.push(NetworkPolicyPeer::IpBlock {
            cidr: cidr.into(),
            except: Vec::new(),
        });
        self
    }

    pub fn port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: None,
        });
        self
    }

    pub fn tcp_port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: Some("TCP".to_string()),
        });
        self
    }

    pub fn udp_port(mut self, port: i32) -> Self {
        self.ports.push(NetworkPolicyPort {
            port,
            protocol: Some("UDP".to_string()),
        });
        self
    }

    pub fn into_k8s(self) -> k8s::NetworkPolicyEgressRule {
        k8s::NetworkPolicyEgressRule {
            to: if self.to.is_empty() {
                None
            } else {
                Some(self.to.into_iter().map(|p| p.into_k8s()).collect())
            },
            ports: if self.ports.is_empty() {
                None
            } else {
                Some(self.ports.into_iter().map(|p| p.into_k8s()).collect())
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum NetworkPolicyPeer {
    PodSelector(Selector),
    NamespaceSelector(Selector),
    IpBlock { cidr: String, except: Vec<String> },
}

impl NetworkPolicyPeer {
    pub fn into_k8s(self) -> k8s::NetworkPolicyPeer {
        match self {
            NetworkPolicyPeer::PodSelector(selector) => k8s::NetworkPolicyPeer {
                pod_selector: Some(LabelSelector {
                    match_labels: Some(selector.into_inner()),
                    match_expressions: None,
                }),
                namespace_selector: None,
                ip_block: None,
            },
            NetworkPolicyPeer::NamespaceSelector(selector) => k8s::NetworkPolicyPeer {
                pod_selector: None,
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(selector.into_inner()),
                    match_expressions: None,
                }),
                ip_block: None,
            },
            NetworkPolicyPeer::IpBlock { cidr, except } => k8s::NetworkPolicyPeer {
                pod_selector: None,
                namespace_selector: None,
                ip_block: Some(k8s::IPBlock {
                    cidr,
                    except: if except.is_empty() {
                        None
                    } else {
                        Some(except)
                    },
                }),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct NetworkPolicyPort {
    pub port: i32,
    pub protocol: Option<String>,
}

impl NetworkPolicyPort {
    pub fn into_k8s(self) -> k8s::NetworkPolicyPort {
        k8s::NetworkPolicyPort {
            port: Some(IntOrString::Int(self.port)),
            protocol: self.protocol,
            end_port: None,
        }
    }
}
