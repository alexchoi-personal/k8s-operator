use super::Labels;
use crate::types::ChildResource;
use k8s_openapi::api::rbac::v1 as k8s;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

#[derive(Clone, Debug)]
pub struct Role {
    pub name: String,
    pub labels: Labels,
    pub rules: Vec<PolicyRule>,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            rules: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn rule(mut self, rule: PolicyRule) -> Self {
        self.rules.push(rule);
        self
    }
}

impl ChildResource for Role {
    type K8sType = k8s::Role;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::Role {
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
            rules: if self.rules.is_empty() {
                None
            } else {
                Some(self.rules.into_iter().map(|r| r.into_k8s()).collect())
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClusterRole {
    pub name: String,
    pub labels: Labels,
    pub rules: Vec<PolicyRule>,
}

impl ClusterRole {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            rules: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn rule(mut self, rule: PolicyRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn into_k8s(self, owner_ref: Option<OwnerReference>) -> k8s::ClusterRole {
        k8s::ClusterRole {
            metadata: ObjectMeta {
                name: Some(self.name),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            rules: if self.rules.is_empty() {
                None
            } else {
                Some(self.rules.into_iter().map(|r| r.into_k8s()).collect())
            },
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoleBinding {
    pub name: String,
    pub labels: Labels,
    pub role_ref: RoleRef,
    pub subjects: Vec<Subject>,
}

impl RoleBinding {
    pub fn new(name: impl Into<String>, role_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            role_ref: RoleRef {
                kind: "Role".to_string(),
                name: role_name.into(),
                api_group: "rbac.authorization.k8s.io".to_string(),
            },
            subjects: Vec::new(),
        }
    }

    pub fn to_cluster_role(name: impl Into<String>, cluster_role: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            role_ref: RoleRef {
                kind: "ClusterRole".to_string(),
                name: cluster_role.into(),
                api_group: "rbac.authorization.k8s.io".to_string(),
            },
            subjects: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn service_account(
        mut self,
        name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        self.subjects.push(Subject {
            kind: "ServiceAccount".to_string(),
            name: name.into(),
            namespace: Some(namespace.into()),
            api_group: None,
        });
        self
    }

    pub fn user(mut self, name: impl Into<String>) -> Self {
        self.subjects.push(Subject {
            kind: "User".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        });
        self
    }

    pub fn group(mut self, name: impl Into<String>) -> Self {
        self.subjects.push(Subject {
            kind: "Group".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        });
        self
    }
}

impl ChildResource for RoleBinding {
    type K8sType = k8s::RoleBinding;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        k8s::RoleBinding {
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
            role_ref: self.role_ref.into_k8s(),
            subjects: if self.subjects.is_empty() {
                None
            } else {
                Some(self.subjects.into_iter().map(|s| s.into_k8s()).collect())
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClusterRoleBinding {
    pub name: String,
    pub labels: Labels,
    pub role_ref: RoleRef,
    pub subjects: Vec<Subject>,
}

impl ClusterRoleBinding {
    pub fn new(name: impl Into<String>, cluster_role: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            role_ref: RoleRef {
                kind: "ClusterRole".to_string(),
                name: cluster_role.into(),
                api_group: "rbac.authorization.k8s.io".to_string(),
            },
            subjects: Vec::new(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn service_account(
        mut self,
        name: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        self.subjects.push(Subject {
            kind: "ServiceAccount".to_string(),
            name: name.into(),
            namespace: Some(namespace.into()),
            api_group: None,
        });
        self
    }

    pub fn user(mut self, name: impl Into<String>) -> Self {
        self.subjects.push(Subject {
            kind: "User".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        });
        self
    }

    pub fn group(mut self, name: impl Into<String>) -> Self {
        self.subjects.push(Subject {
            kind: "Group".to_string(),
            name: name.into(),
            namespace: None,
            api_group: Some("rbac.authorization.k8s.io".to_string()),
        });
        self
    }

    pub fn into_k8s(self, owner_ref: Option<OwnerReference>) -> k8s::ClusterRoleBinding {
        k8s::ClusterRoleBinding {
            metadata: ObjectMeta {
                name: Some(self.name),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            role_ref: self.role_ref.into_k8s(),
            subjects: if self.subjects.is_empty() {
                None
            } else {
                Some(self.subjects.into_iter().map(|s| s.into_k8s()).collect())
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PolicyRule {
    pub api_groups: Vec<String>,
    pub resources: Vec<String>,
    pub verbs: Vec<String>,
    pub resource_names: Vec<String>,
}

impl PolicyRule {
    pub fn new() -> Self {
        Self {
            api_groups: Vec::new(),
            resources: Vec::new(),
            verbs: Vec::new(),
            resource_names: Vec::new(),
        }
    }

    pub fn api_groups(mut self, groups: Vec<impl Into<String>>) -> Self {
        self.api_groups = groups.into_iter().map(Into::into).collect();
        self
    }

    pub fn core_api(mut self) -> Self {
        self.api_groups.push("".to_string());
        self
    }

    pub fn apps_api(mut self) -> Self {
        self.api_groups.push("apps".to_string());
        self
    }

    pub fn resources(mut self, resources: Vec<impl Into<String>>) -> Self {
        self.resources = resources.into_iter().map(Into::into).collect();
        self
    }

    pub fn verbs(mut self, verbs: Vec<impl Into<String>>) -> Self {
        self.verbs = verbs.into_iter().map(Into::into).collect();
        self
    }

    pub fn all_verbs(mut self) -> Self {
        self.verbs = vec!["*".to_string()];
        self
    }

    pub fn read_only(mut self) -> Self {
        self.verbs = vec!["get".to_string(), "list".to_string(), "watch".to_string()];
        self
    }

    pub fn read_write(mut self) -> Self {
        self.verbs = vec![
            "get".to_string(),
            "list".to_string(),
            "watch".to_string(),
            "create".to_string(),
            "update".to_string(),
            "patch".to_string(),
            "delete".to_string(),
        ];
        self
    }

    pub fn resource_names(mut self, names: Vec<impl Into<String>>) -> Self {
        self.resource_names = names.into_iter().map(Into::into).collect();
        self
    }

    pub fn into_k8s(self) -> k8s::PolicyRule {
        k8s::PolicyRule {
            api_groups: if self.api_groups.is_empty() {
                None
            } else {
                Some(self.api_groups)
            },
            resources: if self.resources.is_empty() {
                None
            } else {
                Some(self.resources)
            },
            verbs: self.verbs,
            resource_names: if self.resource_names.is_empty() {
                None
            } else {
                Some(self.resource_names)
            },
            non_resource_urls: None,
        }
    }
}

impl Default for PolicyRule {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct RoleRef {
    pub kind: String,
    pub name: String,
    pub api_group: String,
}

impl RoleRef {
    pub fn into_k8s(self) -> k8s::RoleRef {
        k8s::RoleRef {
            kind: self.kind,
            name: self.name,
            api_group: self.api_group,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Subject {
    pub kind: String,
    pub name: String,
    pub namespace: Option<String>,
    pub api_group: Option<String>,
}

impl Subject {
    pub fn into_k8s(self) -> k8s::Subject {
        k8s::Subject {
            kind: self.kind,
            name: self.name,
            namespace: self.namespace,
            api_group: self.api_group,
        }
    }
}
