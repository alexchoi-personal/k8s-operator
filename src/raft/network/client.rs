use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{Entry, EntryPayload, LogId, Membership, Vote};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use super::proto::raft_service_client::RaftServiceClient;
use super::proto::{self as pb};
use crate::raft::config::TlsConfig;
use crate::raft::types::{KeyValueStateMachine, RaftNode, RaftRequest, StateMachine, TypeConfig};

#[derive(Debug)]
struct ConnectionError(String);

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConnectionError {}

pub struct GrpcRaftClient<SM: StateMachine = KeyValueStateMachine> {
    #[allow(dead_code)]
    target_id: u64,
    addr: String,
    tls: TlsConfig,
    client: Option<RaftServiceClient<Channel>>,
    _marker: PhantomData<SM>,
}

impl<SM: StateMachine> GrpcRaftClient<SM> {
    pub fn new(target_id: u64, node: &RaftNode, tls: TlsConfig) -> Self {
        Self {
            target_id,
            addr: node.addr.clone(),
            tls,
            client: None,
            _marker: PhantomData,
        }
    }

    pub fn invalidate_connection(&mut self) {
        self.client = None;
    }

    async fn get_client(&mut self) -> Result<&mut RaftServiceClient<Channel>, ConnectionError> {
        if self.client.is_none() {
            let scheme = if self.tls.is_enabled() { "https" } else { "http" };
            let endpoint = format!("{}://{}", scheme, self.addr);
            let mut channel_builder = Channel::from_shared(endpoint)
                .map_err(|e| ConnectionError(e.to_string()))?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10));

            if self.tls.is_enabled() {
                let tls_config = self.build_tls_config().await?;
                channel_builder = channel_builder
                    .tls_config(tls_config)
                    .map_err(|e| ConnectionError(format!("TLS config error: {}", e)))?;
            }

            let channel = channel_builder
                .connect()
                .await
                .map_err(|e| ConnectionError(e.to_string()))?;

            self.client = Some(RaftServiceClient::new(channel));
        }
        Ok(self.client.as_mut().unwrap())
    }

    async fn build_tls_config(&self) -> Result<ClientTlsConfig, ConnectionError> {
        let mut tls_config = ClientTlsConfig::new();

        if let Some(ca_path) = &self.tls.ca_cert {
            let ca_cert = tokio::fs::read(ca_path)
                .await
                .map_err(|e| ConnectionError(format!("Failed to read CA cert: {}", e)))?;
            tls_config = tls_config.ca_certificate(Certificate::from_pem(ca_cert));
        }

        if self.tls.is_mtls() {
            let cert_path = self.tls.cert.as_ref()
                .ok_or_else(|| ConnectionError("mTLS requires cert path".into()))?;
            let key_path = self.tls.key.as_ref()
                .ok_or_else(|| ConnectionError("mTLS requires key path".into()))?;

            let cert = tokio::fs::read(cert_path)
                .await
                .map_err(|e| ConnectionError(format!("Failed to read cert: {}", e)))?;
            let key = tokio::fs::read(key_path)
                .await
                .map_err(|e| ConnectionError(format!("Failed to read key: {}", e)))?;

            tls_config = tls_config.identity(Identity::from_pem(cert, key));
        }

        Ok(tls_config)
    }
}

fn vote_to_proto(vote: &Vote<u64>) -> pb::Vote {
    pb::Vote {
        leader_id: vote.leader_id().node_id,
        term: vote.leader_id().term,
        committed: vote.is_committed(),
    }
}

fn vote_from_proto(v: pb::Vote) -> Vote<u64> {
    if v.committed {
        Vote::new_committed(v.term, v.leader_id)
    } else {
        Vote::new(v.term, v.leader_id)
    }
}

fn log_id_to_proto(log_id: &LogId<u64>) -> pb::LogId {
    pb::LogId {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}

fn log_id_from_proto(l: pb::LogId) -> LogId<u64> {
    LogId::new(openraft::LeaderId::new(l.term, 0), l.index)
}

fn entry_to_proto(entry: &Entry<TypeConfig<KeyValueStateMachine>>) -> pb::Entry {
    let payload = match &entry.payload {
        EntryPayload::Blank => pb::EntryPayload {
            payload: Some(pb::entry_payload::Payload::Blank(pb::Empty {})),
        },
        EntryPayload::Normal(req) => pb::EntryPayload {
            payload: Some(pb::entry_payload::Payload::Normal(pb::NormalPayload {
                key: req.key.clone(),
                value: req.value.clone(),
            })),
        },
        EntryPayload::Membership(mem) => {
            let configs: Vec<pb::MembershipConfig> = mem
                .get_joint_config()
                .iter()
                .map(|cfg| {
                    let nodes: HashMap<u64, pb::RaftNode> = cfg
                        .iter()
                        .map(|id| {
                            let node = mem.get_node(id)
                                .map(|n| pb::RaftNode { addr: n.addr.clone() })
                                .unwrap_or_default();
                            (*id, node)
                        })
                        .collect();
                    pb::MembershipConfig { nodes }
                })
                .collect();
            pb::EntryPayload {
                payload: Some(pb::entry_payload::Payload::Membership(pb::MembershipPayload {
                    configs,
                })),
            }
        }
    };

    pb::Entry {
        log_id: Some(log_id_to_proto(&entry.log_id)),
        payload: Some(payload),
    }
}

#[allow(dead_code)]
fn entry_from_proto(e: pb::Entry) -> Entry<TypeConfig<KeyValueStateMachine>> {
    let log_id = e.log_id.map(log_id_from_proto).unwrap_or_default();
    let payload = match e.payload.and_then(|p| p.payload) {
        Some(pb::entry_payload::Payload::Blank(_)) => EntryPayload::Blank,
        Some(pb::entry_payload::Payload::Normal(n)) => EntryPayload::Normal(RaftRequest {
            key: n.key,
            value: n.value,
        }),
        Some(pb::entry_payload::Payload::Membership(m)) => {
            let mut all_nodes: BTreeMap<u64, RaftNode> = BTreeMap::new();
            let configs: Vec<BTreeSet<u64>> = m
                .configs
                .into_iter()
                .map(|cfg| {
                    cfg.nodes
                        .into_iter()
                        .map(|(id, node)| {
                            all_nodes.insert(id, RaftNode { addr: node.addr });
                            id
                        })
                        .collect()
                })
                .collect();
            let membership = if configs.is_empty() {
                Membership::new(vec![BTreeSet::new()], all_nodes)
            } else {
                Membership::new(configs, all_nodes)
            };
            EntryPayload::Membership(membership)
        }
        None => EntryPayload::Blank,
    };

    Entry { log_id, payload }
}

impl RaftNetwork<TypeConfig<KeyValueStateMachine>> for GrpcRaftClient<KeyValueStateMachine> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig<KeyValueStateMachine>>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let request = pb::AppendEntriesRequest {
            vote: Some(vote_to_proto(&rpc.vote)),
            prev_log_id: rpc.prev_log_id.as_ref().map(log_id_to_proto),
            entries: rpc.entries.iter().map(entry_to_proto).collect(),
            leader_commit: rpc.leader_commit.as_ref().map(log_id_to_proto),
        };

        let response = client
            .append_entries(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .into_inner();

        if response.success {
            Ok(AppendEntriesResponse::Success)
        } else if response.conflict.is_some() {
            Ok(AppendEntriesResponse::Conflict)
        } else if let Some(vote) = response.vote {
            Ok(AppendEntriesResponse::HigherVote(vote_from_proto(vote)))
        } else {
            Ok(AppendEntriesResponse::Conflict)
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, RaftNode, RaftError<u64>>> {
        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let request = pb::VoteRequest {
            vote: Some(vote_to_proto(&rpc.vote)),
            last_log_id: rpc.last_log_id.as_ref().map(log_id_to_proto),
        };

        let response = client
            .vote(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .into_inner();

        Ok(VoteResponse {
            vote: response.vote.map(vote_from_proto).unwrap_or_default(),
            vote_granted: response.vote_granted,
            last_log_id: response.last_log_id.map(log_id_from_proto),
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig<KeyValueStateMachine>>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, RaftNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let meta = pb::SnapshotMeta {
            last_log_id: rpc.meta.last_log_id.as_ref().map(log_id_to_proto),
            snapshot_id: rpc.meta.snapshot_id.clone(),
            last_membership: Some(pb::MembershipPayload { configs: vec![] }),
        };

        let request = pb::InstallSnapshotRequest {
            vote: Some(vote_to_proto(&rpc.vote)),
            meta: Some(meta),
            offset: rpc.offset,
            data: rpc.data.clone(),
            done: rpc.done,
        };

        let response = client
            .install_snapshot(request)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .into_inner();

        Ok(InstallSnapshotResponse {
            vote: response.vote.map(vote_from_proto).unwrap_or_default(),
        })
    }
}
