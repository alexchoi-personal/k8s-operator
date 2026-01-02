use super::{Container, Labels, Selector, Volume};
use crate::types::ChildResource;
use k8s_openapi::api::apps::v1 as apps;
use k8s_openapi::api::batch::v1 as batch;
use k8s_openapi::api::core::v1 as core;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Deployment {
    pub name: String,
    pub replicas: i32,
    pub labels: Labels,
    pub selector: Selector,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub service_account: Option<String>,
    pub node_selector: BTreeMap<String, String>,
    pub restart_policy: Option<String>,
    pub image_pull_secrets: Vec<String>,
}

impl Deployment {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            replicas: 1,
            labels: Labels::new().insert("app", &name),
            selector: Selector::new().match_labels("app", &name),
            containers: Vec::new(),
            init_containers: Vec::new(),
            volumes: Vec::new(),
            service_account: None,
            node_selector: BTreeMap::new(),
            restart_policy: None,
            image_pull_secrets: Vec::new(),
        }
    }

    pub fn replicas(mut self, n: i32) -> Self {
        self.replicas = n;
        self
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn selector(mut self, selector: Selector) -> Self {
        self.selector = selector;
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

    pub fn node_selector(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.node_selector.insert(key.into(), value.into());
        self
    }

    pub fn image_pull_secret(mut self, name: impl Into<String>) -> Self {
        self.image_pull_secrets.push(name.into());
        self
    }
}

impl ChildResource for Deployment {
    type K8sType = apps::Deployment;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        let labels_map = self.labels.into_inner();
        apps::Deployment {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(labels_map.clone()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(apps::DeploymentSpec {
                replicas: Some(self.replicas),
                selector: LabelSelector {
                    match_labels: Some(self.selector.into_inner()),
                    match_expressions: None,
                },
                template: core::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels_map),
                        ..Default::default()
                    }),
                    spec: Some(core::PodSpec {
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
                        node_selector: if self.node_selector.is_empty() {
                            None
                        } else {
                            Some(self.node_selector)
                        },
                        restart_policy: self.restart_policy,
                        image_pull_secrets: if self.image_pull_secrets.is_empty() {
                            None
                        } else {
                            Some(
                                self.image_pull_secrets
                                    .into_iter()
                                    .map(|n| core::LocalObjectReference { name: n })
                                    .collect(),
                            )
                        },
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct StatefulSet {
    pub name: String,
    pub replicas: i32,
    pub labels: Labels,
    pub selector: Selector,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub volume_claim_templates: Vec<PersistentVolumeClaim>,
    pub service_name: String,
    pub service_account: Option<String>,
    pub pod_management_policy: Option<String>,
}

impl StatefulSet {
    pub fn new(name: impl Into<String>, service_name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            replicas: 1,
            labels: Labels::new().insert("app", &name),
            selector: Selector::new().match_labels("app", &name),
            containers: Vec::new(),
            init_containers: Vec::new(),
            volumes: Vec::new(),
            volume_claim_templates: Vec::new(),
            service_name: service_name.into(),
            service_account: None,
            pod_management_policy: None,
        }
    }

    pub fn replicas(mut self, n: i32) -> Self {
        self.replicas = n;
        self
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn selector(mut self, selector: Selector) -> Self {
        self.selector = selector;
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

    pub fn volume_claim_template(mut self, pvc: PersistentVolumeClaim) -> Self {
        self.volume_claim_templates.push(pvc);
        self
    }

    pub fn service_account(mut self, name: impl Into<String>) -> Self {
        self.service_account = Some(name.into());
        self
    }

    pub fn parallel_pod_management(mut self) -> Self {
        self.pod_management_policy = Some("Parallel".to_string());
        self
    }
}

impl ChildResource for StatefulSet {
    type K8sType = apps::StatefulSet;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        apps::StatefulSet {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(self.labels.clone().into_inner()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(apps::StatefulSetSpec {
                replicas: Some(self.replicas),
                selector: LabelSelector {
                    match_labels: Some(self.selector.into_inner()),
                    match_expressions: None,
                },
                service_name: Some(self.service_name),
                pod_management_policy: self.pod_management_policy,
                template: core::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(self.labels.into_inner()),
                        ..Default::default()
                    }),
                    spec: Some(core::PodSpec {
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
                        ..Default::default()
                    }),
                },
                volume_claim_templates: if self.volume_claim_templates.is_empty() {
                    None
                } else {
                    Some(
                        self.volume_claim_templates
                            .into_iter()
                            .map(|p| p.into_k8s_template())
                            .collect(),
                    )
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct DaemonSet {
    pub name: String,
    pub labels: Labels,
    pub selector: Selector,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub service_account: Option<String>,
    pub node_selector: BTreeMap<String, String>,
}

impl DaemonSet {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            labels: Labels::new().insert("app", &name),
            selector: Selector::new().match_labels("app", &name),
            containers: Vec::new(),
            init_containers: Vec::new(),
            volumes: Vec::new(),
            service_account: None,
            node_selector: BTreeMap::new(),
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

    pub fn node_selector(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.node_selector.insert(key.into(), value.into());
        self
    }
}

impl ChildResource for DaemonSet {
    type K8sType = apps::DaemonSet;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        apps::DaemonSet {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(self.labels.clone().into_inner()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(apps::DaemonSetSpec {
                selector: LabelSelector {
                    match_labels: Some(self.selector.into_inner()),
                    match_expressions: None,
                },
                template: core::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(self.labels.into_inner()),
                        ..Default::default()
                    }),
                    spec: Some(core::PodSpec {
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
                        node_selector: if self.node_selector.is_empty() {
                            None
                        } else {
                            Some(self.node_selector)
                        },
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct Job {
    pub name: String,
    pub labels: Labels,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
    pub volumes: Vec<Volume>,
    pub restart_policy: String,
    pub backoff_limit: Option<i32>,
    pub completions: Option<i32>,
    pub parallelism: Option<i32>,
    pub active_deadline_seconds: Option<i64>,
    pub ttl_seconds_after_finished: Option<i32>,
    pub service_account: Option<String>,
}

impl Job {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            labels: Labels::new().insert("app", &name),
            containers: Vec::new(),
            init_containers: Vec::new(),
            volumes: Vec::new(),
            restart_policy: "Never".to_string(),
            backoff_limit: None,
            completions: None,
            parallelism: None,
            active_deadline_seconds: None,
            ttl_seconds_after_finished: None,
            service_account: None,
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
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

    pub fn restart_on_failure(mut self) -> Self {
        self.restart_policy = "OnFailure".to_string();
        self
    }

    pub fn backoff_limit(mut self, limit: i32) -> Self {
        self.backoff_limit = Some(limit);
        self
    }

    pub fn completions(mut self, n: i32) -> Self {
        self.completions = Some(n);
        self
    }

    pub fn parallelism(mut self, n: i32) -> Self {
        self.parallelism = Some(n);
        self
    }

    pub fn active_deadline(mut self, seconds: i64) -> Self {
        self.active_deadline_seconds = Some(seconds);
        self
    }

    pub fn ttl_after_finished(mut self, seconds: i32) -> Self {
        self.ttl_seconds_after_finished = Some(seconds);
        self
    }

    pub fn service_account(mut self, name: impl Into<String>) -> Self {
        self.service_account = Some(name.into());
        self
    }
}

impl ChildResource for Job {
    type K8sType = batch::Job;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        batch::Job {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(self.labels.clone().into_inner()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(batch::JobSpec {
                backoff_limit: self.backoff_limit,
                completions: self.completions,
                parallelism: self.parallelism,
                active_deadline_seconds: self.active_deadline_seconds,
                ttl_seconds_after_finished: self.ttl_seconds_after_finished,
                template: core::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(self.labels.into_inner()),
                        ..Default::default()
                    }),
                    spec: Some(core::PodSpec {
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
                        restart_policy: Some(self.restart_policy),
                        service_account_name: self.service_account,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct CronJob {
    pub name: String,
    pub schedule: String,
    pub labels: Labels,
    pub job_template: Job,
    pub concurrency_policy: Option<String>,
    pub suspend: Option<bool>,
    pub successful_jobs_history_limit: Option<i32>,
    pub failed_jobs_history_limit: Option<i32>,
}

impl CronJob {
    pub fn new(name: impl Into<String>, schedule: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            schedule: schedule.into(),
            labels: Labels::new().insert("app", &name),
            job_template: Job::new(&name),
            concurrency_policy: None,
            suspend: None,
            successful_jobs_history_limit: None,
            failed_jobs_history_limit: None,
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn container(mut self, container: Container) -> Self {
        self.job_template = self.job_template.container(container);
        self
    }

    pub fn volume(mut self, volume: Volume) -> Self {
        self.job_template = self.job_template.volume(volume);
        self
    }

    pub fn forbid_concurrent(mut self) -> Self {
        self.concurrency_policy = Some("Forbid".to_string());
        self
    }

    pub fn replace_concurrent(mut self) -> Self {
        self.concurrency_policy = Some("Replace".to_string());
        self
    }

    pub fn suspend(mut self, value: bool) -> Self {
        self.suspend = Some(value);
        self
    }

    pub fn history_limits(mut self, success: i32, failed: i32) -> Self {
        self.successful_jobs_history_limit = Some(success);
        self.failed_jobs_history_limit = Some(failed);
        self
    }

    pub fn service_account(mut self, name: impl Into<String>) -> Self {
        self.job_template = self.job_template.service_account(name);
        self
    }
}

impl ChildResource for CronJob {
    type K8sType = batch::CronJob;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        batch::CronJob {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(namespace.to_string()),
                labels: Some(self.labels.into_inner()),
                owner_references: owner_ref.map(|r| vec![r]),
                ..Default::default()
            },
            spec: Some(batch::CronJobSpec {
                schedule: self.schedule,
                concurrency_policy: self.concurrency_policy,
                suspend: self.suspend,
                successful_jobs_history_limit: self.successful_jobs_history_limit,
                failed_jobs_history_limit: self.failed_jobs_history_limit,
                job_template: batch::JobTemplateSpec {
                    metadata: None,
                    spec: Some(self.job_template.into_k8s(namespace, None).spec.unwrap()),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersistentVolumeClaim {
    pub name: String,
    pub labels: Labels,
    pub storage_class: Option<String>,
    pub access_modes: Vec<String>,
    pub storage: String,
}

impl PersistentVolumeClaim {
    pub fn new(name: impl Into<String>, storage: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: Labels::new(),
            storage_class: None,
            access_modes: vec!["ReadWriteOnce".to_string()],
            storage: storage.into(),
        }
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn storage_class(mut self, class: impl Into<String>) -> Self {
        self.storage_class = Some(class.into());
        self
    }

    pub fn read_write_many(mut self) -> Self {
        self.access_modes = vec!["ReadWriteMany".to_string()];
        self
    }

    pub fn read_only_many(mut self) -> Self {
        self.access_modes = vec!["ReadOnlyMany".to_string()];
        self
    }

    pub fn into_k8s_template(self) -> core::PersistentVolumeClaim {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        core::PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(self.name),
                labels: if self.labels.0.is_empty() {
                    None
                } else {
                    Some(self.labels.into_inner())
                },
                ..Default::default()
            },
            spec: Some(core::PersistentVolumeClaimSpec {
                access_modes: Some(self.access_modes),
                storage_class_name: self.storage_class,
                resources: Some(core::VolumeResourceRequirements {
                    requests: Some(
                        [("storage".to_string(), Quantity(self.storage))]
                            .into_iter()
                            .collect(),
                    ),
                    limits: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

impl ChildResource for PersistentVolumeClaim {
    type K8sType = core::PersistentVolumeClaim;

    fn name(&self) -> &str {
        &self.name
    }

    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        core::PersistentVolumeClaim {
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
            spec: Some(core::PersistentVolumeClaimSpec {
                access_modes: Some(self.access_modes),
                storage_class_name: self.storage_class,
                resources: Some(core::VolumeResourceRequirements {
                    requests: Some(
                        [("storage".to_string(), Quantity(self.storage))]
                            .into_iter()
                            .collect(),
                    ),
                    limits: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}
