use openraft::Config;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RaftResponse {
    pub success: bool,
    pub value: Option<String>,
}

pub trait StateMachine: Send + Sync + Default + Clone + Debug + 'static {
    type Request: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static;
    type Response: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + Default + 'static;
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + Clone + Default + 'static;

    fn apply(&mut self, request: &Self::Request) -> Self::Response;

    fn snapshot(&self) -> Self::Snapshot;

    fn restore(&mut self, snapshot: Self::Snapshot);
}

#[derive(Debug, Clone, Default)]
pub struct KeyValueStateMachine {
    pub data: HashMap<String, String>,
}

impl StateMachine for KeyValueStateMachine {
    type Request = RaftRequest;
    type Response = RaftResponse;
    type Snapshot = HashMap<String, String>;

    fn apply(&mut self, request: &Self::Request) -> Self::Response {
        let old_value = self.data.insert(request.key.clone(), request.value.clone());
        RaftResponse {
            success: true,
            value: old_value,
        }
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.data.clone()
    }

    fn restore(&mut self, snapshot: Self::Snapshot) {
        self.data = snapshot;
    }
}

pub struct TypeConfig<SM: StateMachine = KeyValueStateMachine>(PhantomData<SM>);

impl<SM: StateMachine> Debug for TypeConfig<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeConfig").finish()
    }
}

impl<SM: StateMachine> Clone for TypeConfig<SM> {
    fn clone(&self) -> Self {
        TypeConfig(PhantomData)
    }
}

impl<SM: StateMachine> Copy for TypeConfig<SM> {}

impl<SM: StateMachine> PartialEq for TypeConfig<SM> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<SM: StateMachine> Eq for TypeConfig<SM> {}

impl<SM: StateMachine> PartialOrd for TypeConfig<SM> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<SM: StateMachine> Ord for TypeConfig<SM> {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl<SM: StateMachine> Default for TypeConfig<SM> {
    fn default() -> Self {
        TypeConfig(PhantomData)
    }
}

impl<SM: StateMachine> Serialize for TypeConfig<SM> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de, SM: StateMachine> Deserialize<'de> for TypeConfig<SM> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor<SM>(PhantomData<SM>);
        impl<'de, SM: StateMachine> serde::de::Visitor<'de> for Visitor<SM> {
            type Value = TypeConfig<SM>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("unit")
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(TypeConfig(PhantomData))
            }
        }
        deserializer.deserialize_unit(Visitor(PhantomData))
    }
}

pub type RaftTypeConfig = TypeConfig<KeyValueStateMachine>;

impl<SM: StateMachine> openraft::RaftTypeConfig for TypeConfig<SM> {
    type D = SM::Request;
    type R = SM::Response;
    type Node = RaftNode;
    type NodeId = NodeId;
    type Entry = openraft::Entry<TypeConfig<SM>>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftNode {
    pub addr: String,
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

#[allow(dead_code)]
pub fn default_raft_config() -> Config {
    Config {
        election_timeout_min: 500,
        election_timeout_max: 1000,
        heartbeat_interval: 100,
        ..Default::default()
    }
}
