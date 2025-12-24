pub mod context;
pub mod error;
pub mod operator;
pub mod types;

#[cfg(feature = "ha")]
pub mod raft;

pub use context::{Condition, Context};
pub use error::{Error, Result};
pub use operator::Operator;
pub use types::*;

#[cfg(feature = "ha")]
pub use raft::{HeadlessServiceDiscovery, LeaderElection, LeaderGuard, RaftConfig};

pub mod prelude {
    pub use crate::context::{Condition, Context};
    pub use crate::error::{Error, Result};
    pub use crate::operator::Operator;

    pub use crate::types::{
        Annotations, ClusterRole, ClusterRoleBinding, ConfigMap, Container,
        CronJob, DaemonSet, Deployment, Ingress, IngressRule, Job, Labels,
        NetworkPolicy, NetworkPolicyEgress, NetworkPolicyIngress,
        PersistentVolumeClaim, Pod, PolicyRule, Probe, Resources, Role,
        RoleBinding, Secret, SecurityContext, Selector, Service,
        ServiceAccount, StatefulSet, Volume,
    };

    pub use kube::runtime::controller::Action;
    pub use kube::CustomResource;
    pub use schemars::JsonSchema;
    pub use serde::{Deserialize, Serialize};

    pub use std::sync::Arc;
    pub use std::time::Duration;
}
