use k8s_openapi::api::core::v1 as k8s;

#[derive(Clone, Debug)]
pub struct Volume {
    pub name: String,
    pub source: VolumeSource,
}

impl Volume {
    pub fn empty_dir(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::EmptyDir { medium: None },
        }
    }

    pub fn empty_dir_memory(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::EmptyDir {
                medium: Some("Memory".to_string()),
            },
        }
    }

    pub fn secret(name: impl Into<String>, secret_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::Secret {
                secret_name: secret_name.into(),
                optional: None,
                default_mode: None,
            },
        }
    }

    pub fn configmap(name: impl Into<String>, configmap_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::ConfigMap {
                name: configmap_name.into(),
                optional: None,
                default_mode: None,
            },
        }
    }

    pub fn pvc(name: impl Into<String>, claim_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::PersistentVolumeClaim {
                claim_name: claim_name.into(),
                read_only: false,
            },
        }
    }

    pub fn host_path(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::HostPath {
                path: path.into(),
                type_: None,
            },
        }
    }

    pub fn projected(name: impl Into<String>, sources: Vec<ProjectedVolumeSource>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::Projected {
                sources,
                default_mode: None,
            },
        }
    }

    pub fn downward_api(name: impl Into<String>, items: Vec<DownwardApiItem>) -> Self {
        Self {
            name: name.into(),
            source: VolumeSource::DownwardApi {
                items,
                default_mode: None,
            },
        }
    }

    pub fn default_mode(mut self, mode: i32) -> Self {
        match &mut self.source {
            VolumeSource::Secret { default_mode, .. } => *default_mode = Some(mode),
            VolumeSource::ConfigMap { default_mode, .. } => *default_mode = Some(mode),
            VolumeSource::Projected { default_mode, .. } => *default_mode = Some(mode),
            VolumeSource::DownwardApi { default_mode, .. } => *default_mode = Some(mode),
            _ => {}
        }
        self
    }

    pub fn optional(mut self, opt: bool) -> Self {
        match &mut self.source {
            VolumeSource::Secret { optional, .. } => *optional = Some(opt),
            VolumeSource::ConfigMap { optional, .. } => *optional = Some(opt),
            _ => {}
        }
        self
    }

    pub fn into_k8s(self) -> k8s::Volume {
        let source = self.source.into_k8s();
        k8s::Volume {
            name: self.name,
            empty_dir: source.empty_dir,
            secret: source.secret,
            config_map: source.config_map,
            persistent_volume_claim: source.persistent_volume_claim,
            host_path: source.host_path,
            projected: source.projected,
            downward_api: source.downward_api,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub enum VolumeSource {
    EmptyDir {
        medium: Option<String>,
    },
    Secret {
        secret_name: String,
        optional: Option<bool>,
        default_mode: Option<i32>,
    },
    ConfigMap {
        name: String,
        optional: Option<bool>,
        default_mode: Option<i32>,
    },
    PersistentVolumeClaim {
        claim_name: String,
        read_only: bool,
    },
    HostPath {
        path: String,
        type_: Option<String>,
    },
    Projected {
        sources: Vec<ProjectedVolumeSource>,
        default_mode: Option<i32>,
    },
    DownwardApi {
        items: Vec<DownwardApiItem>,
        default_mode: Option<i32>,
    },
}

struct VolumeSourceK8s {
    empty_dir: Option<k8s::EmptyDirVolumeSource>,
    secret: Option<k8s::SecretVolumeSource>,
    config_map: Option<k8s::ConfigMapVolumeSource>,
    persistent_volume_claim: Option<k8s::PersistentVolumeClaimVolumeSource>,
    host_path: Option<k8s::HostPathVolumeSource>,
    projected: Option<k8s::ProjectedVolumeSource>,
    downward_api: Option<k8s::DownwardAPIVolumeSource>,
}

impl VolumeSource {
    fn into_k8s(self) -> VolumeSourceK8s {
        match self {
            VolumeSource::EmptyDir { medium } => VolumeSourceK8s {
                empty_dir: Some(k8s::EmptyDirVolumeSource {
                    medium,
                    size_limit: None,
                }),
                secret: None,
                config_map: None,
                persistent_volume_claim: None,
                host_path: None,
                projected: None,
                downward_api: None,
            },
            VolumeSource::Secret {
                secret_name,
                optional,
                default_mode,
            } => VolumeSourceK8s {
                empty_dir: None,
                secret: Some(k8s::SecretVolumeSource {
                    secret_name: Some(secret_name),
                    optional,
                    default_mode,
                    items: None,
                }),
                config_map: None,
                persistent_volume_claim: None,
                host_path: None,
                projected: None,
                downward_api: None,
            },
            VolumeSource::ConfigMap {
                name,
                optional,
                default_mode,
            } => VolumeSourceK8s {
                empty_dir: None,
                secret: None,
                config_map: Some(k8s::ConfigMapVolumeSource {
                    name,
                    optional,
                    default_mode,
                    items: None,
                }),
                persistent_volume_claim: None,
                host_path: None,
                projected: None,
                downward_api: None,
            },
            VolumeSource::PersistentVolumeClaim {
                claim_name,
                read_only,
            } => VolumeSourceK8s {
                empty_dir: None,
                secret: None,
                config_map: None,
                persistent_volume_claim: Some(k8s::PersistentVolumeClaimVolumeSource {
                    claim_name,
                    read_only: Some(read_only),
                }),
                host_path: None,
                projected: None,
                downward_api: None,
            },
            VolumeSource::HostPath { path, type_ } => VolumeSourceK8s {
                empty_dir: None,
                secret: None,
                config_map: None,
                persistent_volume_claim: None,
                host_path: Some(k8s::HostPathVolumeSource { path, type_ }),
                projected: None,
                downward_api: None,
            },
            VolumeSource::Projected {
                sources,
                default_mode,
            } => VolumeSourceK8s {
                empty_dir: None,
                secret: None,
                config_map: None,
                persistent_volume_claim: None,
                host_path: None,
                projected: Some(k8s::ProjectedVolumeSource {
                    sources: Some(sources.into_iter().map(|s| s.into_k8s()).collect()),
                    default_mode,
                }),
                downward_api: None,
            },
            VolumeSource::DownwardApi {
                items,
                default_mode,
            } => VolumeSourceK8s {
                empty_dir: None,
                secret: None,
                config_map: None,
                persistent_volume_claim: None,
                host_path: None,
                projected: None,
                downward_api: Some(k8s::DownwardAPIVolumeSource {
                    items: Some(items.into_iter().map(|i| i.into_k8s()).collect()),
                    default_mode,
                }),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum ProjectedVolumeSource {
    Secret {
        name: String,
    },
    ConfigMap {
        name: String,
    },
    ServiceAccountToken {
        path: String,
        expiration_seconds: Option<i64>,
    },
}

impl ProjectedVolumeSource {
    pub fn into_k8s(self) -> k8s::VolumeProjection {
        match self {
            ProjectedVolumeSource::Secret { name } => k8s::VolumeProjection {
                secret: Some(k8s::SecretProjection {
                    name,
                    items: None,
                    optional: None,
                }),
                ..Default::default()
            },
            ProjectedVolumeSource::ConfigMap { name } => k8s::VolumeProjection {
                config_map: Some(k8s::ConfigMapProjection {
                    name,
                    items: None,
                    optional: None,
                }),
                ..Default::default()
            },
            ProjectedVolumeSource::ServiceAccountToken {
                path,
                expiration_seconds,
            } => k8s::VolumeProjection {
                service_account_token: Some(k8s::ServiceAccountTokenProjection {
                    path,
                    expiration_seconds,
                    audience: None,
                }),
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum DownwardApiItem {
    FieldRef {
        path: String,
        field_path: String,
    },
    ResourceFieldRef {
        path: String,
        container_name: String,
        resource: String,
    },
}

impl DownwardApiItem {
    pub fn into_k8s(self) -> k8s::DownwardAPIVolumeFile {
        match self {
            DownwardApiItem::FieldRef { path, field_path } => k8s::DownwardAPIVolumeFile {
                path,
                field_ref: Some(k8s::ObjectFieldSelector {
                    field_path,
                    api_version: None,
                }),
                resource_field_ref: None,
                mode: None,
            },
            DownwardApiItem::ResourceFieldRef {
                path,
                container_name,
                resource,
            } => k8s::DownwardAPIVolumeFile {
                path,
                field_ref: None,
                resource_field_ref: Some(k8s::ResourceFieldSelector {
                    container_name: Some(container_name),
                    resource,
                    divisor: None,
                }),
                mode: None,
            },
        }
    }
}
