use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, VoteRequest};
use openraft::{Entry, EntryPayload, LogId, Membership, Raft, SnapshotMeta, StoredMembership, Vote};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status};

use super::proto::raft_service_server::{RaftService, RaftServiceServer};
use super::proto::{self as pb};
use crate::raft::config::TlsConfig;
use crate::raft::health::{HealthService, HealthState};
use crate::raft::leader::LeaderElection;
use crate::raft::types::{RaftNode, RaftRequest, TypeConfig};

pub struct RaftGrpcServer {
    raft: Arc<Raft<TypeConfig>>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self { raft }
    }
}

fn vote_from_proto(v: pb::Vote) -> Vote<u64> {
    if v.committed {
        Vote::new_committed(v.term, v.leader_id)
    } else {
        Vote::new(v.term, v.leader_id)
    }
}

fn vote_to_proto(vote: &Vote<u64>) -> pb::Vote {
    pb::Vote {
        leader_id: vote.leader_id().node_id,
        term: vote.leader_id().term,
        committed: vote.is_committed(),
    }
}

fn log_id_from_proto(l: pb::LogId) -> LogId<u64> {
    LogId::new(openraft::LeaderId::new(l.term, 0), l.index)
}

fn log_id_to_proto(log_id: &LogId<u64>) -> pb::LogId {
    pb::LogId {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}

fn entry_from_proto(e: pb::Entry) -> Entry<TypeConfig> {
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

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let req = request.into_inner();

        let vote_req = VoteRequest {
            vote: req.vote.map(vote_from_proto).unwrap_or_default(),
            last_log_id: req.last_log_id.map(log_id_from_proto),
        };

        match self.raft.vote(vote_req).await {
            Ok(resp) => Ok(Response::new(pb::VoteResponse {
                vote: Some(vote_to_proto(&resp.vote)),
                vote_granted: resp.vote_granted,
                last_log_id: resp.last_log_id.map(|l| log_id_to_proto(&l)),
            })),
            Err(e) => Err(Status::internal(format!("Vote error: {:?}", e))),
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let append_req = AppendEntriesRequest {
            vote: req.vote.map(vote_from_proto).unwrap_or_default(),
            prev_log_id: req.prev_log_id.map(log_id_from_proto),
            entries: req.entries.into_iter().map(entry_from_proto).collect(),
            leader_commit: req.leader_commit.map(log_id_from_proto),
        };

        match self.raft.append_entries(append_req).await {
            Ok(resp) => {
                let (success, conflict, vote) = match resp {
                    AppendEntriesResponse::Success => (true, None, None),
                    AppendEntriesResponse::PartialSuccess(log_id) => (true, log_id, None),
                    AppendEntriesResponse::Conflict => (false, None, None),
                    AppendEntriesResponse::HigherVote(v) => (false, None, Some(vote_to_proto(&v))),
                };
                Ok(Response::new(pb::AppendEntriesResponse {
                    vote,
                    success,
                    conflict: conflict.map(|l| log_id_to_proto(&l)),
                }))
            }
            Err(e) => Err(Status::internal(format!("AppendEntries error: {:?}", e))),
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<pb::InstallSnapshotRequest>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();

        let meta = req.meta.ok_or_else(|| Status::invalid_argument("Missing meta"))?;

        let snapshot_req = InstallSnapshotRequest {
            vote: req.vote.map(vote_from_proto).unwrap_or_default(),
            meta: SnapshotMeta {
                last_log_id: meta.last_log_id.map(log_id_from_proto),
                last_membership: StoredMembership::default(),
                snapshot_id: meta.snapshot_id,
            },
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        match self.raft.install_snapshot(snapshot_req).await {
            Ok(resp) => Ok(Response::new(pb::InstallSnapshotResponse {
                vote: Some(vote_to_proto(&resp.vote)),
            })),
            Err(e) => Err(Status::internal(format!("InstallSnapshot error: {:?}", e))),
        }
    }
}

pub async fn start_raft_server(
    raft: Arc<Raft<TypeConfig>>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_raft_server_with_tls(raft, addr, TlsConfig::default()).await
}

pub async fn start_raft_server_with_tls(
    raft: Arc<Raft<TypeConfig>>,
    addr: SocketAddr,
    tls: TlsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = RaftGrpcServer::new(raft);

    tracing::info!("Starting Raft gRPC server on {} (TLS: {})", addr, tls.is_enabled());

    let mut builder = tonic::transport::Server::builder();

    if tls.is_enabled() {
        let tls_config = build_server_tls_config(&tls).await?;
        builder = builder.tls_config(tls_config)?;
    }

    builder
        .add_service(RaftServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

pub async fn start_raft_server_with_health(
    raft: Arc<Raft<TypeConfig>>,
    leader_election: Arc<LeaderElection>,
    node_id: u64,
    addr: SocketAddr,
    tls: TlsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let grpc_server = RaftGrpcServer::new(raft.clone());
    let health_state = HealthState::new(raft, leader_election, node_id);
    let health_service = HealthService::new(health_state);

    tracing::info!(
        "Starting Raft gRPC server with health endpoints on {} (TLS: {})",
        addr,
        tls.is_enabled()
    );

    let mut builder = tonic::transport::Server::builder()
        .accept_http1(true);

    if tls.is_enabled() {
        let tls_config = build_server_tls_config(&tls).await?;
        builder = builder.tls_config(tls_config)?;
    }

    let grpc_service = RaftServiceServer::new(grpc_server);

    builder
        .layer(HealthLayer::new(health_service))
        .add_service(grpc_service)
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Clone)]
pub struct HealthLayer {
    health_service: HealthService,
}

impl HealthLayer {
    pub fn new(health_service: HealthService) -> Self {
        Self { health_service }
    }
}

impl<S> tower::Layer<S> for HealthLayer {
    type Service = HealthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HealthMiddleware {
            inner,
            health_service: self.health_service.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HealthMiddleware<S> {
    inner: S,
    health_service: HealthService,
}

impl<S, B> tower::Service<http::Request<B>> for HealthMiddleware<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<tonic::body::BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let path = req.uri().path();

        if matches!(path, "/health" | "/ready" | "/leader" | "/metrics") {
            let mut health = self.health_service.clone();
            Box::pin(async move { Ok(health.call(req).await.unwrap()) })
        } else {
            let future = self.inner.call(req);
            Box::pin(async move { future.await })
        }
    }
}

async fn build_server_tls_config(
    tls: &TlsConfig,
) -> Result<ServerTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let cert_path = tls
        .cert
        .as_ref()
        .ok_or("TLS requires cert path")?;
    let key_path = tls
        .key
        .as_ref()
        .ok_or("TLS requires key path")?;

    let cert = tokio::fs::read(cert_path).await?;
    let key = tokio::fs::read(key_path).await?;

    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

    if tls.is_mtls() {
        if let Some(ca_path) = &tls.ca_cert {
            let ca_cert = tokio::fs::read(ca_path).await?;
            tls_config = tls_config.client_ca_root(Certificate::from_pem(ca_cert));
        }
    }

    Ok(tls_config)
}
