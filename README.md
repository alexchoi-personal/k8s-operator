# k8s-operator

A batteries-included Kubernetes operator framework for Rust. Write operators with minimal boilerplate while hiding the complexity of `kube-rs` and `k8s-openapi`.

[![Crates.io](https://img.shields.io/crates/v/k8s-operator.svg)](https://crates.io/crates/k8s-operator)
[![License](https://img.shields.io/crates/l/k8s-operator.svg)](LICENSE)

## Features

- **Zero k8s-openapi exposure** - Create Deployments, Services, ConfigMaps, etc. with typed builders
- **Simple reconciliation** - Just implement an async function
- **Automatic finalizers** - Optional finalizer management with cleanup hooks
- **HA/Raft support** - Optional leader election for high availability (feature flag)
- **Type-safe** - Full Rust type safety for Kubernetes resources

## Installation

```toml
[dependencies]
k8s-operator = "0.3"
tokio = { version = "1", features = ["full"] }
```

For HA/leader election support:

```toml
[dependencies]
k8s-operator = { version = "0.3", features = ["ha"] }
```

## Quick Start

```rust
use k8s_operator::prelude::*;

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(group = "example.com", version = "v1", kind = "WebApp", namespaced)]
pub struct WebAppSpec {
    pub image: String,
    pub replicas: u32,
}

async fn reconcile(ctx: Context<WebApp>) -> Result<Action> {
    let app = ctx.resource();

    ctx.apply(
        Deployment::new(ctx.name())
            .replicas(app.spec.replicas)
            .container(
                Container::new("app")
                    .image(&app.spec.image)
                    .port(80)
            )
    ).await?;

    ctx.apply(
        Service::new(ctx.name())
            .port(80, 80)
            .selector(Selector::new().insert("app", ctx.name()))
    ).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

#[tokio::main]
async fn main() -> Result<()> {
    Operator::<WebApp>::new()
        .reconcile(reconcile)
        .with_finalizer()
        .run()
        .await
}
```

## Core Concepts

### Operator

The `Operator` builder configures and runs your controller:

```rust
Operator::<MyResource>::new()
    .reconcile(reconcile_fn)       // Required: reconciliation function
    .on_delete(cleanup_fn)         // Optional: cleanup on deletion
    .with_finalizer()              // Optional: auto-manage finalizer
    .requeue_after(Duration::from_secs(300))  // Default requeue interval
    .error_requeue(Duration::from_secs(60))   // Requeue on error
    .run()
    .await
```

### Context

The `Context<R>` provides access to the resource and Kubernetes operations:

```rust
async fn reconcile(ctx: Context<MyResource>) -> Result<Action> {
    // Access the resource
    let resource = ctx.resource();
    let name = ctx.name();
    let namespace = ctx.namespace();

    // Apply child resources (creates or updates)
    ctx.apply(Deployment::new("my-deploy")).await?;

    // Delete child resources
    ctx.delete::<Deployment>("old-deploy").await?;

    // Get existing resources
    let deploy: Option<Deployment> = ctx.get("my-deploy").await?;

    // Set status
    ctx.set_status(MyStatus { ready: true }).await?;

    // Emit events
    ctx.event("Reconciled", "Successfully reconciled resource").await?;
    ctx.warning("Issue", "Something needs attention").await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}
```

### Resource Builders

Create Kubernetes resources with typed builders:

#### Deployment

```rust
Deployment::new("my-app")
    .replicas(3)
    .labels(Labels::new().insert("app", "my-app"))
    .container(
        Container::new("main")
            .image("nginx:1.25")
            .port(80)
            .env("LOG_LEVEL", "info")
            .env_from_secret("DB_PASSWORD", "my-secret", "password")
            .env_from_configmap("CONFIG", "my-config", "key")
            .resources(Resources::new()
                .cpu_request("100m")
                .cpu_limit("500m")
                .memory_request("128Mi")
                .memory_limit("512Mi"))
            .liveness_probe(Probe::http("/health", 8080).period(10))
            .readiness_probe(Probe::http("/ready", 8080).initial_delay(5))
            .volume_mount("data", "/data")
            .security_context(SecurityContext::new()
                .read_only_root_filesystem(true)
                .run_as_non_root(true))
    )
    .volume(Volume::pvc("data", "my-pvc"))
    .service_account("my-sa")
```

#### Service

```rust
Service::new("my-service")
    .port(80, 8080)                    // port, targetPort
    .named_port("metrics", 9090, 9090)
    .selector(Selector::new().insert("app", "my-app"))
    .cluster_ip()                      // default
    .node_port(80, 8080, 30080)        // with nodePort
    .load_balancer()                   // type: LoadBalancer
    .headless()                        // clusterIP: None
```

#### ConfigMap & Secret

```rust
ConfigMap::new("my-config")
    .data("config.yaml", "key: value")
    .data("settings.json", "{}")

Secret::new("my-secret")
    .data("password", "secret123")     // auto base64 encoded
    .data("api-key", "abc123")
```

#### Ingress

```rust
Ingress::new("my-ingress")
    .ingress_class("nginx")
    .annotation("nginx.ingress.kubernetes.io/rewrite-target", "/")
    .rule(
        IngressRule::new()
            .host("example.com")
            .path("/api", "api-service", 80)
            .path("/", "web-service", 80)
    )
    .tls(vec!["example.com"], "tls-secret")
```

#### StatefulSet

```rust
StatefulSet::new("my-stateful")
    .replicas(3)
    .service_name("my-headless")
    .container(Container::new("app").image("redis:7"))
    .volume_claim_template(
        PersistentVolumeClaim::new("data")
            .storage("10Gi")
            .access_mode("ReadWriteOnce")
    )
```

#### RBAC

```rust
ServiceAccount::new("my-sa")

Role::new("my-role")
    .rule(PolicyRule::new()
        .api_groups(vec![""])
        .resources(vec!["pods", "services"])
        .verbs(vec!["get", "list", "watch"]))

RoleBinding::new("my-binding")
    .role("my-role")
    .service_account("my-sa", "default")

ClusterRole::new("my-cluster-role")
    .rule(PolicyRule::new()
        .api_groups(vec!["example.com"])
        .resources(vec!["myresources"])
        .verbs(vec!["*"]))

ClusterRoleBinding::new("my-cluster-binding")
    .cluster_role("my-cluster-role")
    .service_account("my-sa", "default")
```

#### Other Resources

```rust
// Jobs
Job::new("my-job")
    .backoff_limit(3)
    .container(Container::new("job").image("busybox").command(vec!["echo", "hello"]))

// CronJobs
CronJob::new("my-cronjob")
    .schedule("0 * * * *")
    .container(Container::new("cron").image("busybox"))

// DaemonSets
DaemonSet::new("my-daemon")
    .container(Container::new("agent").image("datadog/agent"))

// NetworkPolicies
NetworkPolicy::new("my-policy")
    .pod_selector(Selector::new().insert("app", "my-app"))
    .allow_ingress(
        NetworkPolicyIngress::new()
            .from_namespace_selector(Selector::new().insert("env", "prod"))
            .tcp_port(80)
    )
    .deny_all_egress()
```

## High Availability (HA)

Enable the `ha` feature for Raft-based leader election with replicated logs:

```toml
[dependencies]
k8s-operator = { version = "0.3", features = ["ha"] }
```

For persistent storage with RocksDB (survives pod restarts):

```toml
[dependencies]
k8s-operator = { version = "0.3", features = ["ha-persist"] }
```

### Features

- **Raft Consensus**: Full Raft implementation using [openraft](https://github.com/datafuselabs/openraft)
- **gRPC Transport**: Inter-node communication via gRPC (tonic) on port 8080
- **Automatic Leader Election**: ~500ms failover when leader dies
- **Log Replication**: All state replicated across nodes before acknowledgment
- **Autoscaling Support**: Handles horizontal scaling without disruption
- **DNS-based Discovery**: Automatic peer discovery via headless service
- **Persistent Storage**: Optional RocksDB backend for durable state (ha-persist feature)

### Usage

```rust
use k8s_operator::prelude::*;
use k8s_operator::{RaftConfig, RaftNodeManager};

#[tokio::main]
async fn main() -> Result<()> {
    // Automatically parse node ID from StatefulSet pod name (e.g., "my-operator-0" -> 0)
    let node_id = RaftNodeManager::node_id_from_hostname()?;

    Operator::<MyResource>::new()
        .reconcile(reconcile)
        .with_leader_election(
            RaftConfig::new("my-cluster")
                .node_id(node_id)
                .service_name("my-operator-headless")
                .namespace("default")
        )
        .run()
        .await
}
```

### Persistent Storage (RocksDB)

With the `ha-persist` feature, Raft logs and state machine data are persisted to disk using RocksDB:

```rust
use k8s_operator::{RaftConfig, RaftNodeManager};

// Create a Raft node with persistent storage
let node_manager = RaftNodeManager::new_with_rocksdb(
    RaftConfig::new("my-cluster")
        .node_id(node_id)
        .service_name("my-operator-headless")
        .namespace("default"),
    "/data/raft"  // Data directory for RocksDB
).await?;
```

For persistent storage, mount a PersistentVolume to `/data/raft`:

```yaml
volumeClaimTemplates:
  - metadata:
      name: raft-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

### How It Works

1. **Bootstrap**: The first node (node-0) bootstraps a new Raft cluster
2. **Join**: Additional nodes join as learners, then become voters
3. **Leader Election**: Raft elects a leader; only the leader runs reconciliation
4. **Failover**: If leader dies, remaining nodes elect a new leader (~500ms)
5. **Scaling**: `ClusterManager` watches DNS for pod changes and updates membership

### Kubernetes Deployment

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-operator-headless
spec:
  clusterIP: None
  selector:
    app: my-operator
  ports:
    - port: 8080
      name: raft
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: my-operator
spec:
  serviceName: my-operator-headless
  replicas: 3
  selector:
    matchLabels:
      app: my-operator
  template:
    metadata:
      labels:
        app: my-operator
    spec:
      containers:
        - name: operator
          image: my-operator:latest
          ports:
            - containerPort: 8080
              name: raft
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: HOSTNAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
```

### Autoscaling Behavior

| Operation | Behavior |
|-----------|----------|
| Scale up | New pods discovered via DNS, added as learners, then voters |
| Scale down | Removed pods detected, membership updated, logs redistributed |
| Pod restart | Same node ID rejoins, catches up from leader's log |
| Leader dies | New election in ~500ms, reconciliation continues |

### Architecture

```
  ┌────────────────────────────────────────────────┐
  │                 Operator Pod                   │
  │  ┌──────────────┐    ┌──────────────────────┐  │
  │  │  Reconciler  │    │   RaftNodeManager    │  │
  │  │              │    │  ┌────────────────┐  │  │
  │  │ if is_leader │◄───│  │   Raft Node    │  │  │
  │  │   reconcile  │    │  └───────┬────────┘  │  │
  │  └──────────────┘    │          │           │  │
  │                      │  ┌───────▼────────┐  │  │
  │                      │  │  gRPC Server   │  │  │
  │                      │  │    :8080       │  │  │
  │                      │  └────────────────┘  │  │
  │                      └──────────────────────┘  │
  └────────────────────────────────────────────────┘
```

```
  ┌───────────────────────────────────────────────────────────────────┐
  │                       StatefulSet (3 replicas)                    │
  │                                                                   │
  │   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐   │
  │   │   Pod 0     │◄──────►│   Pod 1     │◄──────►│   Pod 2     │   │
  │   │  (LEADER)   │  gRPC  │ (follower)  │  gRPC  │ (follower)  │   │
  │   └──────┬──────┘        └─────────────┘        └─────────────┘   │
  │          │                     │                      │           │
  │          │ reconciles          │ standby              │ standby   │
  │          ▼                     ▼                      ▼           │
  │   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐   │
  │   │  Creates    │        │   Waits     │        │   Waits     │   │
  │   │ Deployments │        │   (noop)    │        │   (noop)    │   │
  │   │  Services   │        │             │        │             │   │
  │   └─────────────┘        └─────────────┘        └─────────────┘   │
  └───────────────────────────────────────────────────────────────────┘
```
```
  ┌────────────────────────────────────────────────────────────────┐
  │                        APPLICATION LAYER                       │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │                    Reconciler Logic                      │  │
  │  │         (only executes on leader, skipped on followers)  │  │
  │  └──────────────────────────────────────────────────────────┘  │
  ├────────────────────────────────────────────────────────────────┤
  │                        CONSENSUS LAYER                         │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │                   Raft (openraft)                        │  │
  │  │    • Leader Election    • Log Replication    • Voting    │  │
  │  └──────────────────────────────────────────────────────────┘  │
  ├────────────────────────────────────────────────────────────────┤
  │                        TRANSPORT LAYER                         │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │                    gRPC (tonic)                          │  │
  │  │              Vote | AppendEntries | Snapshot             │  │
  │  └──────────────────────────────────────────────────────────┘  │
  ├────────────────────────────────────────────────────────────────┤
  │                        DISCOVERY LAYER                         │
  │  ┌──────────────────────────────────────────────────────────┐  │
  │  │              Headless Service DNS Lookup                 │  │
  │  │         my-operator-0.my-operator-headless.svc           │  │
  │  └──────────────────────────────────────────────────────────┘  │
  └────────────────────────────────────────────────────────────────┘
```
```

                      ┌─────────────────┐
                      │    STARTUP      │
                      └────────┬────────┘
                               │
                               ▼
                      ┌─────────────────┐
              ┌──────►│   CANDIDATE     │◄──────┐
              │       │  (requesting    │       │
              │       │    votes)       │       │
              │       └────────┬────────┘       │
              │                │                │
              │   election     │ wins           │ election
              │   timeout      │ election       │ timeout
              │                ▼                │
      ┌───────┴───────┐    ┌─────────────────┐  │
      │   FOLLOWER    │◄───│     LEADER      │──┘
      │               │    │                 │
      │ • Receives    │    │ • Sends         │
      │   heartbeats  │    │   heartbeats    │
      │ • Replicates  │    │ • Runs          │
      │   logs        │    │   reconciler    │
      │ • NO work     │    │ • ALL work      │
      └───────────────┘    └─────────────────┘
              ▲                    │
              │   higher term      │
              │   discovered       │
              └────────────────────┘
```
```

      Pod 0              Pod 1              Pod 2           Kubernetes
        │                  │                  │                 │
        │◄─────────────────┼──────────────────┼─── Start pods ──┤
        │                  │                  │                 │
        ├── Vote Request ──►                  │                 │
        ├── Vote Request ──┼─────────────────►│                 │
        │                  │                  │                 │
        │◄── Vote Granted ─┤                  │                 │
        │◄── Vote Granted ─┼──────────────────┤                 │
        │                  │                  │                 │
        │  ╔═══════════════════════════════╗  │                 │
        │  ║  Pod 0 becomes LEADER         ║  │                 │
        │  ╚═══════════════════════════════╝  │                 │
        │                  │                  │                 │
        ├── Heartbeat ────►│                  │                 │
        ├── Heartbeat ─────┼─────────────────►│                 │
        │                  │                  │                 │
        ├── Reconcile ─────┼──────────────────┼────────────────►│
        │                  │                  │     (creates    │
        │                  │                  │    Deployment)  │
        │                  │                  │                 │
       ─┼─ Pod 0 crashes ──┼──────────────────┼─────────────────┤
        X                  │                  │                 │
                           │◄─ No heartbeat ──┤                 │
                           │   (timeout)      │                 │
                           │                  │                 │
                           ├── Vote Request ──►                 │
                           │◄── Vote Granted ─┤                 │
                           │                  │                 │
                           │  ╔════════════════════════════╗    │
                           │  ║ Pod 1 becomes LEADER       ║    │
                           │  ╚════════════════════════════╝    │
                           │                  │                 │
                           ├── Reconcile ─────┼────────────────►│
                           │                  │                 │

```
```

                      ┌───────────────────────┐
                      │   Kubernetes API      │
                      └───────────┬───────────┘
                                  │
                                  │ only leader writes
                                  │
                ┌─────────────────┼─────────────────┐
                │                 │                 │
          ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
          │  Pod 0    │     │  Pod 1    │     │  Pod 2    │
          │ ┌───────┐ │     │ ┌───────┐ │     │ ┌───────┐ │
          │ │Leader │ │     │ │Follow │ │     │ │Follow │ │
          │ │  ✓    │ │     │ │  ✗    │ │     │ │  ✗    │ │
          │ └───────┘ │     │ └───────┘ │     │ └───────┘ │
          └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
                │                 │                 │
                └────────────┬────┴────────────────┘
                             │
                      Raft consensus
                      (gRPC :8080)
```

## Available Types

| Category | Types |
|----------|-------|
| **Workloads** | `Deployment`, `StatefulSet`, `DaemonSet`, `Job`, `CronJob`, `Pod` |
| **Config** | `ConfigMap`, `Secret` |
| **Networking** | `Service`, `Ingress`, `NetworkPolicy` |
| **Storage** | `PersistentVolumeClaim`, `Volume` |
| **RBAC** | `ServiceAccount`, `Role`, `RoleBinding`, `ClusterRole`, `ClusterRoleBinding` |
| **Helpers** | `Container`, `Labels`, `Annotations`, `Selector`, `Probe`, `Resources`, `SecurityContext` |

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
