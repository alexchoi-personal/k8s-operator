use k8s_openapi::api::core::v1 as k8s;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub image_pull_policy: Option<String>,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: Option<String>,
    pub ports: Vec<ContainerPort>,
    pub env: Vec<EnvVar>,
    pub env_from: Vec<EnvFromSource>,
    pub resources: Option<Resources>,
    pub volume_mounts: Vec<VolumeMount>,
    pub liveness_probe: Option<Probe>,
    pub readiness_probe: Option<Probe>,
    pub startup_probe: Option<Probe>,
    pub security_context: Option<SecurityContext>,
}

impl Container {
    pub fn new(name: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            image: image.into(),
            image_pull_policy: None,
            command: Vec::new(),
            args: Vec::new(),
            working_dir: None,
            ports: Vec::new(),
            env: Vec::new(),
            env_from: Vec::new(),
            resources: None,
            volume_mounts: Vec::new(),
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
        }
    }

    pub fn image_pull_policy(mut self, policy: impl Into<String>) -> Self {
        self.image_pull_policy = Some(policy.into());
        self
    }

    pub fn command(mut self, cmd: Vec<impl Into<String>>) -> Self {
        self.command = cmd.into_iter().map(Into::into).collect();
        self
    }

    pub fn args(mut self, args: Vec<impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn working_dir(mut self, dir: impl Into<String>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    pub fn port(mut self, port: i32) -> Self {
        self.ports.push(ContainerPort {
            container_port: port,
            name: None,
            protocol: None,
        });
        self
    }

    pub fn named_port(mut self, name: impl Into<String>, port: i32) -> Self {
        self.ports.push(ContainerPort {
            container_port: port,
            name: Some(name.into()),
            protocol: None,
        });
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push(EnvVar::Value {
            name: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn env_from_secret(
        mut self,
        name: impl Into<String>,
        secret_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        self.env.push(EnvVar::SecretRef {
            name: name.into(),
            secret_name: secret_name.into(),
            key: key.into(),
        });
        self
    }

    pub fn env_from_configmap(
        mut self,
        name: impl Into<String>,
        configmap_name: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        self.env.push(EnvVar::ConfigMapRef {
            name: name.into(),
            configmap_name: configmap_name.into(),
            key: key.into(),
        });
        self
    }

    pub fn env_from_field(
        mut self,
        name: impl Into<String>,
        field_path: impl Into<String>,
    ) -> Self {
        self.env.push(EnvVar::FieldRef {
            name: name.into(),
            field_path: field_path.into(),
        });
        self
    }

    pub fn env_all_from_secret(mut self, secret_name: impl Into<String>) -> Self {
        self.env_from.push(EnvFromSource::Secret(secret_name.into()));
        self
    }

    pub fn env_all_from_configmap(mut self, configmap_name: impl Into<String>) -> Self {
        self.env_from.push(EnvFromSource::ConfigMap(configmap_name.into()));
        self
    }

    pub fn resources(mut self, resources: Resources) -> Self {
        self.resources = Some(resources);
        self
    }

    pub fn volume_mount(
        mut self,
        name: impl Into<String>,
        mount_path: impl Into<String>,
    ) -> Self {
        self.volume_mounts.push(VolumeMount {
            name: name.into(),
            mount_path: mount_path.into(),
            sub_path: None,
            read_only: false,
        });
        self
    }

    pub fn volume_mount_readonly(
        mut self,
        name: impl Into<String>,
        mount_path: impl Into<String>,
    ) -> Self {
        self.volume_mounts.push(VolumeMount {
            name: name.into(),
            mount_path: mount_path.into(),
            sub_path: None,
            read_only: true,
        });
        self
    }

    pub fn liveness_probe(mut self, probe: Probe) -> Self {
        self.liveness_probe = Some(probe);
        self
    }

    pub fn readiness_probe(mut self, probe: Probe) -> Self {
        self.readiness_probe = Some(probe);
        self
    }

    pub fn startup_probe(mut self, probe: Probe) -> Self {
        self.startup_probe = Some(probe);
        self
    }

    pub fn security_context(mut self, ctx: SecurityContext) -> Self {
        self.security_context = Some(ctx);
        self
    }

    pub fn into_k8s(self) -> k8s::Container {
        k8s::Container {
            name: self.name,
            image: Some(self.image),
            image_pull_policy: self.image_pull_policy,
            command: if self.command.is_empty() {
                None
            } else {
                Some(self.command)
            },
            args: if self.args.is_empty() { None } else { Some(self.args) },
            working_dir: self.working_dir,
            ports: if self.ports.is_empty() {
                None
            } else {
                Some(self.ports.into_iter().map(|p| p.into_k8s()).collect())
            },
            env: if self.env.is_empty() {
                None
            } else {
                Some(self.env.into_iter().map(|e| e.into_k8s()).collect())
            },
            env_from: if self.env_from.is_empty() {
                None
            } else {
                Some(self.env_from.into_iter().map(|e| e.into_k8s()).collect())
            },
            resources: self.resources.map(|r| r.into_k8s()),
            volume_mounts: if self.volume_mounts.is_empty() {
                None
            } else {
                Some(
                    self.volume_mounts
                        .into_iter()
                        .map(|v| v.into_k8s())
                        .collect(),
                )
            },
            liveness_probe: self.liveness_probe.map(|p| p.into_k8s()),
            readiness_probe: self.readiness_probe.map(|p| p.into_k8s()),
            startup_probe: self.startup_probe.map(|p| p.into_k8s()),
            security_context: self.security_context.map(|s| s.into_k8s()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContainerPort {
    pub container_port: i32,
    pub name: Option<String>,
    pub protocol: Option<String>,
}

impl ContainerPort {
    pub fn into_k8s(self) -> k8s::ContainerPort {
        k8s::ContainerPort {
            container_port: self.container_port,
            name: self.name,
            protocol: self.protocol,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub enum EnvVar {
    Value {
        name: String,
        value: String,
    },
    SecretRef {
        name: String,
        secret_name: String,
        key: String,
    },
    ConfigMapRef {
        name: String,
        configmap_name: String,
        key: String,
    },
    FieldRef {
        name: String,
        field_path: String,
    },
}

impl EnvVar {
    pub fn into_k8s(self) -> k8s::EnvVar {
        match self {
            EnvVar::Value { name, value } => k8s::EnvVar {
                name,
                value: Some(value),
                value_from: None,
            },
            EnvVar::SecretRef {
                name,
                secret_name,
                key,
            } => k8s::EnvVar {
                name,
                value: None,
                value_from: Some(k8s::EnvVarSource {
                    secret_key_ref: Some(k8s::SecretKeySelector {
                        name: secret_name,
                        key,
                        optional: None,
                    }),
                    ..Default::default()
                }),
            },
            EnvVar::ConfigMapRef {
                name,
                configmap_name,
                key,
            } => k8s::EnvVar {
                name,
                value: None,
                value_from: Some(k8s::EnvVarSource {
                    config_map_key_ref: Some(k8s::ConfigMapKeySelector {
                        name: configmap_name,
                        key,
                        optional: None,
                    }),
                    ..Default::default()
                }),
            },
            EnvVar::FieldRef { name, field_path } => k8s::EnvVar {
                name,
                value: None,
                value_from: Some(k8s::EnvVarSource {
                    field_ref: Some(k8s::ObjectFieldSelector {
                        field_path,
                        api_version: None,
                    }),
                    ..Default::default()
                }),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum EnvFromSource {
    Secret(String),
    ConfigMap(String),
}

impl EnvFromSource {
    pub fn into_k8s(self) -> k8s::EnvFromSource {
        match self {
            EnvFromSource::Secret(name) => k8s::EnvFromSource {
                secret_ref: Some(k8s::SecretEnvSource {
                    name,
                    optional: None,
                }),
                ..Default::default()
            },
            EnvFromSource::ConfigMap(name) => k8s::EnvFromSource {
                config_map_ref: Some(k8s::ConfigMapEnvSource {
                    name,
                    optional: None,
                }),
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Resources {
    pub requests: BTreeMap<String, String>,
    pub limits: BTreeMap<String, String>,
}

impl Resources {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cpu_request(mut self, cpu: impl Into<String>) -> Self {
        self.requests.insert("cpu".to_string(), cpu.into());
        self
    }

    pub fn cpu_limit(mut self, cpu: impl Into<String>) -> Self {
        self.limits.insert("cpu".to_string(), cpu.into());
        self
    }

    pub fn memory_request(mut self, memory: impl Into<String>) -> Self {
        self.requests.insert("memory".to_string(), memory.into());
        self
    }

    pub fn memory_limit(mut self, memory: impl Into<String>) -> Self {
        self.limits.insert("memory".to_string(), memory.into());
        self
    }

    pub fn cpu(self, request: impl Into<String>, limit: impl Into<String>) -> Self {
        self.cpu_request(request).cpu_limit(limit)
    }

    pub fn memory(self, request: impl Into<String>, limit: impl Into<String>) -> Self {
        self.memory_request(request).memory_limit(limit)
    }

    pub fn into_k8s(self) -> k8s::ResourceRequirements {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        k8s::ResourceRequirements {
            requests: if self.requests.is_empty() {
                None
            } else {
                Some(
                    self.requests
                        .into_iter()
                        .map(|(k, v)| (k, Quantity(v)))
                        .collect(),
                )
            },
            limits: if self.limits.is_empty() {
                None
            } else {
                Some(
                    self.limits
                        .into_iter()
                        .map(|(k, v)| (k, Quantity(v)))
                        .collect(),
                )
            },
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    pub sub_path: Option<String>,
    pub read_only: bool,
}

impl VolumeMount {
    pub fn into_k8s(self) -> k8s::VolumeMount {
        k8s::VolumeMount {
            name: self.name,
            mount_path: self.mount_path,
            sub_path: self.sub_path,
            read_only: Some(self.read_only),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct Probe {
    pub action: ProbeAction,
    pub initial_delay_seconds: Option<i32>,
    pub period_seconds: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub success_threshold: Option<i32>,
    pub failure_threshold: Option<i32>,
}

#[derive(Clone, Debug)]
pub enum ProbeAction {
    Http { path: String, port: i32 },
    Tcp { port: i32 },
    Exec { command: Vec<String> },
    Grpc { port: i32 },
}

impl Probe {
    pub fn http(path: impl Into<String>, port: i32) -> Self {
        Self {
            action: ProbeAction::Http {
                path: path.into(),
                port,
            },
            initial_delay_seconds: None,
            period_seconds: None,
            timeout_seconds: None,
            success_threshold: None,
            failure_threshold: None,
        }
    }

    pub fn tcp(port: i32) -> Self {
        Self {
            action: ProbeAction::Tcp { port },
            initial_delay_seconds: None,
            period_seconds: None,
            timeout_seconds: None,
            success_threshold: None,
            failure_threshold: None,
        }
    }

    pub fn exec(command: Vec<impl Into<String>>) -> Self {
        Self {
            action: ProbeAction::Exec {
                command: command.into_iter().map(Into::into).collect(),
            },
            initial_delay_seconds: None,
            period_seconds: None,
            timeout_seconds: None,
            success_threshold: None,
            failure_threshold: None,
        }
    }

    pub fn grpc(port: i32) -> Self {
        Self {
            action: ProbeAction::Grpc { port },
            initial_delay_seconds: None,
            period_seconds: None,
            timeout_seconds: None,
            success_threshold: None,
            failure_threshold: None,
        }
    }

    pub fn initial_delay(mut self, seconds: i32) -> Self {
        self.initial_delay_seconds = Some(seconds);
        self
    }

    pub fn period(mut self, seconds: i32) -> Self {
        self.period_seconds = Some(seconds);
        self
    }

    pub fn timeout(mut self, seconds: i32) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    pub fn success_threshold(mut self, threshold: i32) -> Self {
        self.success_threshold = Some(threshold);
        self
    }

    pub fn failure_threshold(mut self, threshold: i32) -> Self {
        self.failure_threshold = Some(threshold);
        self
    }

    pub fn into_k8s(self) -> k8s::Probe {
        let (http_get, tcp_socket, exec, grpc) = match self.action {
            ProbeAction::Http { path, port } => (
                Some(k8s::HTTPGetAction {
                    path: Some(path),
                    port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(port),
                    ..Default::default()
                }),
                None,
                None,
                None,
            ),
            ProbeAction::Tcp { port } => (
                None,
                Some(k8s::TCPSocketAction {
                    port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(port),
                    ..Default::default()
                }),
                None,
                None,
            ),
            ProbeAction::Exec { command } => (
                None,
                None,
                Some(k8s::ExecAction {
                    command: Some(command),
                }),
                None,
            ),
            ProbeAction::Grpc { port } => (
                None,
                None,
                None,
                Some(k8s::GRPCAction {
                    port,
                    service: None,
                }),
            ),
        };

        k8s::Probe {
            http_get,
            tcp_socket,
            exec,
            grpc,
            initial_delay_seconds: self.initial_delay_seconds,
            period_seconds: self.period_seconds,
            timeout_seconds: self.timeout_seconds,
            success_threshold: self.success_threshold,
            failure_threshold: self.failure_threshold,
            termination_grace_period_seconds: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SecurityContext {
    pub run_as_user: Option<i64>,
    pub run_as_group: Option<i64>,
    pub run_as_non_root: Option<bool>,
    pub read_only_root_filesystem: Option<bool>,
    pub privileged: Option<bool>,
    pub allow_privilege_escalation: Option<bool>,
}

impl SecurityContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn run_as_user(mut self, uid: i64) -> Self {
        self.run_as_user = Some(uid);
        self
    }

    pub fn run_as_group(mut self, gid: i64) -> Self {
        self.run_as_group = Some(gid);
        self
    }

    pub fn run_as_non_root(mut self, value: bool) -> Self {
        self.run_as_non_root = Some(value);
        self
    }

    pub fn read_only_root_filesystem(mut self, value: bool) -> Self {
        self.read_only_root_filesystem = Some(value);
        self
    }

    pub fn privileged(mut self, value: bool) -> Self {
        self.privileged = Some(value);
        self
    }

    pub fn allow_privilege_escalation(mut self, value: bool) -> Self {
        self.allow_privilege_escalation = Some(value);
        self
    }

    pub fn into_k8s(self) -> k8s::SecurityContext {
        k8s::SecurityContext {
            run_as_user: self.run_as_user,
            run_as_group: self.run_as_group,
            run_as_non_root: self.run_as_non_root,
            read_only_root_filesystem: self.read_only_root_filesystem,
            privileged: self.privileged,
            allow_privilege_escalation: self.allow_privilege_escalation,
            ..Default::default()
        }
    }
}
