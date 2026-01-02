#![cfg(feature = "ha")]

use std::time::Duration;

use k8s_operator::raft::{RaftConfig, RaftNodeManager};
use serial_test::serial;

fn test_config(node_id: u64) -> RaftConfig {
    RaftConfig::new("test-cluster")
        .node_id(node_id)
        .service_name("test")
        .namespace("default")
        .election_timeout(Duration::from_millis(500))
        .heartbeat_interval(Duration::from_millis(100))
}

#[tokio::test]
#[serial]
async fn single_node_becomes_leader() {
    let node = RaftNodeManager::new(test_config(0)).await.unwrap();
    node.start_grpc_server(19000).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    node.bootstrap_or_join().await.unwrap();
    node.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        node.leader_election().is_leader(),
        "Single node should become leader"
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn three_node_cluster_elects_one_leader() {
    let node0 = RaftNodeManager::new(test_config(0)).await.unwrap();
    let node1 = RaftNodeManager::new(test_config(1)).await.unwrap();
    let node2 = RaftNodeManager::new(test_config(2)).await.unwrap();

    node0.start_grpc_server(19100).await.unwrap();
    node1.start_grpc_server(19101).await.unwrap();
    node2.start_grpc_server(19102).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    node0.bootstrap_or_join().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    node0.add_member(1, "127.0.0.1:19101".into()).await.unwrap();
    node0.add_member(2, "127.0.0.1:19102".into()).await.unwrap();

    node0.start_leadership_watcher();
    node1.start_leadership_watcher();
    node2.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader_count = [&node0, &node1, &node2]
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();

    assert_eq!(leader_count, 1, "Expected exactly one leader");

    node0.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn leader_failure_triggers_new_election() {
    let node0 = RaftNodeManager::new(test_config(0)).await.unwrap();
    let node1 = RaftNodeManager::new(test_config(1)).await.unwrap();
    let node2 = RaftNodeManager::new(test_config(2)).await.unwrap();

    node0.start_grpc_server(19200).await.unwrap();
    node1.start_grpc_server(19201).await.unwrap();
    node2.start_grpc_server(19202).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    node0.bootstrap_or_join().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    node0.add_member(1, "127.0.0.1:19201".into()).await.unwrap();
    node0.add_member(2, "127.0.0.1:19202".into()).await.unwrap();

    node0.start_leadership_watcher();
    node1.start_leadership_watcher();
    node2.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let nodes = vec![node0, node1, node2];
    let initial_leader_idx = nodes
        .iter()
        .position(|n| n.leader_election().is_leader())
        .expect("Should have a leader");

    let old_leader_id = nodes[initial_leader_idx].config().node_id;
    eprintln!("Initial leader is node {}", old_leader_id);

    nodes[initial_leader_idx].shutdown().await.unwrap();
    eprintln!("Shut down leader node {}", old_leader_id);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let remaining: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != initial_leader_idx)
        .map(|(_, n)| n)
        .collect();

    let new_leader_count = remaining
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();

    assert_eq!(
        new_leader_count, 1,
        "New leader should be elected from remaining nodes"
    );

    let new_leader = remaining
        .iter()
        .find(|n| n.leader_election().is_leader())
        .unwrap();

    assert_ne!(
        new_leader.config().node_id,
        old_leader_id,
        "New leader should be different from old leader"
    );

    for node in remaining {
        let _ = node.shutdown().await;
    }
}

#[tokio::test]
#[serial]
async fn node_can_be_added_to_cluster() {
    let node0 = RaftNodeManager::new(test_config(0)).await.unwrap();
    node0.start_grpc_server(19300).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    node0.bootstrap_or_join().await.unwrap();
    node0.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(node0.leader_election().is_leader());

    let node1 = RaftNodeManager::new(test_config(1)).await.unwrap();
    node1.start_grpc_server(19301).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    node0.add_member(1, "127.0.0.1:19301".into()).await.unwrap();

    node1.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(
        node0.leader_election().is_leader() || node1.leader_election().is_leader(),
        "One of the nodes should be leader"
    );

    let leader_count = [&node0, &node1]
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();
    assert_eq!(leader_count, 1);

    node0.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn node_can_be_removed_from_cluster() {
    let node0 = RaftNodeManager::new(test_config(0)).await.unwrap();
    let node1 = RaftNodeManager::new(test_config(1)).await.unwrap();
    let node2 = RaftNodeManager::new(test_config(2)).await.unwrap();

    node0.start_grpc_server(19400).await.unwrap();
    node1.start_grpc_server(19401).await.unwrap();
    node2.start_grpc_server(19402).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    node0.bootstrap_or_join().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    node0.add_member(1, "127.0.0.1:19401".into()).await.unwrap();
    node0.add_member(2, "127.0.0.1:19402".into()).await.unwrap();

    node0.start_leadership_watcher();
    node1.start_leadership_watcher();
    node2.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let nodes = [&node0, &node1, &node2];
    let leader = nodes
        .iter()
        .find(|n| n.leader_election().is_leader())
        .expect("Should have a leader");

    leader.remove_member(2).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let leader_count = [&node0, &node1]
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();
    assert_eq!(leader_count, 1, "Should still have exactly one leader");

    node0.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn node_id_from_hostname_parses_correctly() {
    std::env::set_var("HOSTNAME", "my-operator-0");
    assert_eq!(RaftNodeManager::node_id_from_hostname().unwrap(), 0);

    std::env::set_var("HOSTNAME", "my-operator-42");
    assert_eq!(RaftNodeManager::node_id_from_hostname().unwrap(), 42);

    std::env::set_var("HOSTNAME", "complex-app-name-operator-123");
    assert_eq!(RaftNodeManager::node_id_from_hostname().unwrap(), 123);
}

#[tokio::test]
#[serial]
async fn node_id_from_hostname_errors_on_invalid() {
    std::env::remove_var("HOSTNAME");
    std::env::remove_var("POD_NAME");

    assert!(RaftNodeManager::node_id_from_hostname().is_err());

    std::env::set_var("HOSTNAME", "no-number-suffix");
    assert!(RaftNodeManager::node_id_from_hostname().is_err());

    std::env::set_var("HOSTNAME", "operator-");
    assert!(RaftNodeManager::node_id_from_hostname().is_err());
}
