#![cfg(feature = "ha-persist")]

use std::time::Duration;

use k8s_operator::raft::{RaftConfig, RaftNodeManager};
use serial_test::serial;
use tempfile::tempdir;

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
async fn rocksdb_node_becomes_leader() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("raft");

    let node = RaftNodeManager::new_with_rocksdb(test_config(0), &data_path)
        .await
        .unwrap();

    node.start_grpc_server(19500).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    node.bootstrap_or_join().await.unwrap();
    node.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        node.leader_election().is_leader(),
        "RocksDB node should become leader"
    );

    assert!(
        node.rocksdb_store().is_some(),
        "Should have RocksDB storage backend"
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn rocksdb_state_persists_across_restart() {
    let dir = tempdir().unwrap();
    let data_path = dir.path().join("raft");

    {
        let node = RaftNodeManager::new_with_rocksdb(test_config(0), &data_path)
            .await
            .unwrap();

        node.start_grpc_server(19600).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        node.bootstrap_or_join().await.unwrap();
        node.start_leadership_watcher();

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(node.leader_election().is_leader());

        node.shutdown().await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    {
        let node = RaftNodeManager::new_with_rocksdb(test_config(0), &data_path)
            .await
            .unwrap();

        node.start_grpc_server(19600).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        node.bootstrap_or_join().await.unwrap();
        node.start_leadership_watcher();

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            node.leader_election().is_leader(),
            "Node should recover and become leader after restart"
        );

        node.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn rocksdb_three_node_cluster() {
    let dir0 = tempdir().unwrap();
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();

    let node0 = RaftNodeManager::new_with_rocksdb(test_config(0), dir0.path().join("raft"))
        .await
        .unwrap();
    let node1 = RaftNodeManager::new_with_rocksdb(test_config(1), dir1.path().join("raft"))
        .await
        .unwrap();
    let node2 = RaftNodeManager::new_with_rocksdb(test_config(2), dir2.path().join("raft"))
        .await
        .unwrap();

    node0.start_grpc_server(19700).await.unwrap();
    node1.start_grpc_server(19701).await.unwrap();
    node2.start_grpc_server(19702).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    node0.bootstrap_or_join().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    node0.add_member(1, "127.0.0.1:19701".into()).await.unwrap();
    node0.add_member(2, "127.0.0.1:19702".into()).await.unwrap();

    node0.start_leadership_watcher();
    node1.start_leadership_watcher();
    node2.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader_count = [&node0, &node1, &node2]
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();

    assert_eq!(
        leader_count, 1,
        "RocksDB cluster should have exactly one leader"
    );

    for node in [&node0, &node1, &node2] {
        assert!(
            node.rocksdb_store().is_some(),
            "All nodes should use RocksDB backend"
        );
    }

    node0.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn memory_and_rocksdb_interop() {
    let dir = tempdir().unwrap();

    let mem_node = RaftNodeManager::new(test_config(0)).await.unwrap();
    let rocks_node = RaftNodeManager::new_with_rocksdb(test_config(1), dir.path().join("raft"))
        .await
        .unwrap();

    mem_node.start_grpc_server(19800).await.unwrap();
    rocks_node.start_grpc_server(19801).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    mem_node.bootstrap_or_join().await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    mem_node
        .add_member(1, "127.0.0.1:19801".into())
        .await
        .unwrap();

    mem_node.start_leadership_watcher();
    rocks_node.start_leadership_watcher();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let leader_count = [&mem_node, &rocks_node]
        .iter()
        .filter(|n| n.leader_election().is_leader())
        .count();

    assert_eq!(
        leader_count, 1,
        "Mixed cluster should have exactly one leader"
    );

    assert!(mem_node.mem_store().is_some());
    assert!(rocks_node.rocksdb_store().is_some());

    mem_node.shutdown().await.unwrap();
    rocks_node.shutdown().await.unwrap();
}
