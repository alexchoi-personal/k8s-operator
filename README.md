# k8s-operator

A batteries-included Kubernetes operator framework with Raft consensus for high availability.

[![Crates.io](https://img.shields.io/crates/v/k8s-operator.svg)](https://crates.io/crates/k8s-operator)
[![License](https://img.shields.io/crates/l/k8s-operator.svg)](LICENSE)

## Overview

`k8s-operator` is a Rust library for building Kubernetes operators that run as highly-available clusters. It uses [Raft consensus](https://raft.github.io/) via [openraft](https://github.com/databendlabs/openraft) to ensure only one replica performs reconciliation at a time, with automatic leader election and failover.

## Features

- **Batteries Included**: All dependencies re-exported - just add `k8s-operator`
- **High Availability**: Run multiple operator replicas with automatic leader election
- **Raft Consensus**: Built on openraft for distributed consensus
- **Kubernetes Native**: Uses kube-rs for Kubernetes API interactions
- **Headless Service Discovery**: Automatic peer discovery via Kubernetes DNS
- **Controller Runtime**: Finalizers, event recording, and status management

## Installation

```toml
[dependencies]
k8s-operator = "0.2"
```

That's it! No need to add `kube`, `k8s-openapi`, `schemars`, `tokio`, `serde`, etc. - everything is re-exported.

## Crate Structure

| Crate | Description |
|-------|-------------|
| `k8s-operator` | Unified API re-exporting all subcrates |
| `k8s-operator-core` | Core traits and types (`Reconciler`, `ReconcileResult`, etc.) |
| `k8s-operator-raft` | Raft configuration and peer discovery |
| `k8s-operator-storage` | Storage layer (memory-backed via openraft-memstore) |
| `k8s-operator-controller` | Controller components (finalizers, events, status, leader guard) |
| `k8s-operator-derive` | Procedural macros for CRD definitions |

## Quick Start

```rust
use k8s_operator::*;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "example.com", version = "v1", kind = "MyResource", namespaced)]
pub struct MyResourceSpec {
    pub replicas: i32,
}

struct MyReconciler;

#[async_trait::async_trait]
impl Reconciler<MyResource> for MyReconciler {
    async fn reconcile(&self, resource: Arc<MyResource>) -> ReconcileResult {
        println!("Reconciling: {:?}", resource.metadata.name);
        Ok(Action::requeue(std::time::Duration::from_secs(300)))
    }

    async fn on_error(&self, _resource: Arc<MyResource>, error: &ReconcileError) -> Action {
        eprintln!("Error: {:?}", error);
        Action::requeue(std::time::Duration::from_secs(60))
    }
}
```

## Leader Election

Only the leader replica performs reconciliation:

```rust
use k8s_operator::{LeaderElection, LeaderGuard, NodeRole};

let election = LeaderElection::new();
let guard = election.guard();

// Set role based on Raft state
election.set_role(NodeRole::Leader);

// Check if this node is the leader
if guard.is_leader() {
    // Perform reconciliation
}

// Or use the guard to gate operations
guard.check()?; // Returns Err(ReconcileError::NotLeader) if not leader
```

## Peer Discovery

Discover peers via Kubernetes headless service DNS:

```rust
use k8s_operator::HeadlessServiceDiscovery;

let discovery = HeadlessServiceDiscovery::new(
    "my-operator-headless",
    "default",
    5000,
);

// Get DNS name for the service
let dns = discovery.dns_name();
// => "my-operator-headless.default.svc.cluster.local"

// Discover peers by StatefulSet ordinal
let peers = discovery.discover_by_ordinal(3);
// => HashMap with nodes 0, 1, 2
```

## Finalizers

Manage Kubernetes finalizers for cleanup:

```rust
use k8s_operator::{add_finalizer, remove_finalizer, has_finalizer};
use kube::Client;

const FINALIZER: &str = "example.com/cleanup";

// Add finalizer before creating resources
add_finalizer(&client, &resource, FINALIZER).await?;

// Check if finalizer exists
if has_finalizer(&resource, FINALIZER) {
    // Perform cleanup
    remove_finalizer(&client, &resource, FINALIZER).await?;
}
```

## Status Updates

Update resource status with conditions:

```rust
use k8s_operator::{StatusPatch, StatusCondition, ConditionStatus};

StatusPatch::new()
    .set("replicas", 3)
    .set("ready", true)
    .condition(
        StatusCondition::ready(true)
            .with_reason("AllReplicasReady")
            .with_message("All replicas are running")
    )
    .apply(&client, &resource)
    .await?;
```

## Event Recording

Record Kubernetes events:

```rust
use k8s_operator::EventRecorder;

let recorder = EventRecorder::new(client.clone(), "my-operator");

recorder.normal(&resource, "Created", "Resource created successfully").await?;
recorder.warning(&resource, "ScaleDown", "Scaling down replicas").await?;
```

## Kubernetes Deployment

Deploy as a StatefulSet with a headless service for peer discovery:

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
    - port: 5000
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
            - containerPort: 5000
              name: raft
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
