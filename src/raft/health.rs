use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use http_body::Body;
use openraft::Raft;
use tonic::body::BoxBody;

use crate::raft::leader::LeaderElection;
use crate::raft::types::TypeConfig;

#[derive(Clone)]
pub struct HealthState {
    pub raft: Arc<Raft<TypeConfig>>,
    pub leader_election: Arc<LeaderElection>,
    pub node_id: u64,
}

impl HealthState {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        leader_election: Arc<LeaderElection>,
        node_id: u64,
    ) -> Self {
        Self {
            raft,
            leader_election,
            node_id,
        }
    }
}

#[derive(Clone)]
pub struct HealthService {
    state: HealthState,
}

impl HealthService {
    pub fn new(state: HealthState) -> Self {
        Self { state }
    }

    fn handle_health(&self) -> Response<BoxBody> {
        json_response(StatusCode::OK, r#"{"status":"ok"}"#)
    }

    fn handle_ready(&self) -> Response<BoxBody> {
        let metrics = self.state.raft.metrics().borrow().clone();

        let is_initialized = metrics.running_state.is_ok();
        let has_leader = metrics.current_leader.is_some();

        if is_initialized && has_leader {
            json_response(StatusCode::OK, r#"{"status":"ready"}"#)
        } else {
            let reason = if !is_initialized {
                "raft_not_initialized"
            } else {
                "no_leader"
            };
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!(r#"{{"status":"not_ready","reason":"{}"}}"#, reason),
            )
        }
    }

    fn handle_leader(&self) -> Response<BoxBody> {
        let metrics = self.state.raft.metrics().borrow().clone();

        let leader_id = metrics.current_leader;
        let is_leader = self.state.leader_election.is_leader();
        let current_term = metrics.current_term;
        let state = format!("{:?}", metrics.state);

        let body = format!(
            r#"{{"node_id":{},"is_leader":{},"current_leader":{},"current_term":{},"state":"{}"}}"#,
            self.state.node_id,
            is_leader,
            leader_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "null".to_string()),
            current_term,
            state
        );

        json_response(StatusCode::OK, &body)
    }

    fn handle_metrics(&self) -> Response<BoxBody> {
        let metrics = self.state.raft.metrics().borrow().clone();

        let last_log_index = metrics.last_log_index.unwrap_or(0);
        let last_applied = metrics.last_applied.map(|l| l.index).unwrap_or(0);
        let log_size = last_log_index.saturating_sub(last_applied);
        let snapshot_index = metrics.snapshot.map(|s| s.index).unwrap_or(0);
        let current_term = metrics.current_term;
        let state = format!("{:?}", metrics.state);
        let members = metrics.membership_config.membership().voter_ids().count();

        let body = format!(
            r#"{{"node_id":{},"last_log_index":{},"last_applied":{},"log_size":{},"snapshot_index":{},"current_term":{},"state":"{}","members":{}}}"#,
            self.state.node_id,
            last_log_index,
            last_applied,
            log_size,
            snapshot_index,
            current_term,
            state,
            members
        );

        json_response(StatusCode::OK, &body)
    }

    fn handle_not_found(&self) -> Response<BoxBody> {
        json_response(StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#)
    }
}

fn json_response(status: StatusCode, body: &str) -> Response<BoxBody> {
    let body = tonic::body::boxed(http_body_util::Full::new(bytes::Bytes::from(
        body.to_string(),
    )));
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

impl<B> tower::Service<Request<B>> for HealthService
where
    B: Body + Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path();
        let method = req.method();

        let response = if method == http::Method::GET {
            match path {
                "/health" => self.handle_health(),
                "/ready" => self.handle_ready(),
                "/leader" => self.handle_leader(),
                "/metrics" => self.handle_metrics(),
                _ => self.handle_not_found(),
            }
        } else {
            self.handle_not_found()
        };

        Box::pin(async move { Ok(response) })
    }
}
