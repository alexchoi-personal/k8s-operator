pub mod container;
pub mod core;
pub mod metadata;
pub mod networking;
pub mod rbac;
pub mod volume;
pub mod workloads;

pub use container::*;
pub use core::*;
pub use metadata::*;
pub use networking::*;
pub use rbac::*;
pub use volume::*;
pub use workloads::*;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

pub trait ChildResource: Send + Sync {
    type K8sType: kube::Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>;

    fn name(&self) -> &str;
    fn into_k8s(self, namespace: &str, owner_ref: Option<OwnerReference>) -> Self::K8sType;
}
