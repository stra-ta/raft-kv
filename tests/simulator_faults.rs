use raft_kv::storage::{load_node, load_snapshot, save_node, save_snapshot};
use raft_kv::{
    ClientRequest, Cluster, FaultPlan, LogEntry, Node, Role, SNAPSHOT_FORMAT_VERSION, Snapshot,
    SnapshotInstallResult, StateSnapshot, check_linearizable,
};
use std::collections::BTreeMap;

fn committed_node() -> Node {
    Node::from_parts(
        0,
        vec![1, 2],
        1,
        None,
        vec![
            raft_kv::LogEntry {
                term: 1,
                command: raft_kv::Command::Set {
                    key: "key".to_string(),
                    value: "one".to_string(),
                },
            },
            raft_kv::LogEntry {
                term: 1,
                command: raft_kv::Command::Set {
                    key: "key".to_string(),
                    value: "two".to_string(),
                },
            },
        ],
        2,
    )
}

#[test]
fn seeded_delay_duplicate_and_reorder_plan_stays_live_and_records_a_checkable_history() {
    let plan = FaultPlan::seeded(0xfeed)
        .with_max_delay_ms(4)
        .with_duplicate_rate_per_mille(350)
        .with_reorder_window(4);
    let mut cluster = Cluster::with_fault_plan(5, plan);
    assert!(cluster.run_until(2_000, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader should be elected");

    assert!(
        cluster
            .propose_as(
                7,
                leader,
                ClientRequest::Set {
                    key: "faults".to_string(),
                    value: "survived".to_string(),
                },
            )
            .success
    );
    assert!(
        cluster
            .propose_as(
                8,
                leader,
                ClientRequest::Get {
                    key: "faults".to_string(),
                },
            )
            .success
    );
    assert!(check_linearizable(cluster.history()).is_ok());
    assert!(
        cluster
            .history()
            .operations()
            .iter()
            .all(|operation| operation.completed_at >= operation.invoked_at)
    );
}

#[test]
fn scheduled_stop_restart_is_deterministic_and_does_not_change_the_public_constructor() {
    let plan = FaultPlan::seeded(11).stop_at(0, 2).restart_at(80, 2);
    let mut first = Cluster::with_fault_plan(3, plan.clone());
    let mut second = Cluster::with_fault_plan(3, plan);
    first.run_for(40);
    second.run_for(40);
    assert!(first.is_stopped(2));
    assert!(second.is_stopped(2));
    first.run_for(60);
    second.run_for(60);
    assert!(!first.is_stopped(2));
    assert!(!second.is_stopped(2));
    assert_eq!(first.now(), second.now());
}

#[test]
fn same_seed_replays_the_same_visible_cluster_state() {
    let plan = FaultPlan::seeded(0x5eed)
        .with_max_delay_ms(4)
        .with_duplicate_rate_per_mille(250)
        .with_reorder_window(4);
    let mut first = Cluster::with_fault_plan(5, plan.clone());
    let mut second = Cluster::with_fault_plan(5, plan);

    assert!(first.run_until(1_000, |cluster| cluster.leader().is_some()));
    assert!(second.run_until(1_000, |cluster| cluster.leader().is_some()));
    let first_leader = first.leader().unwrap();
    let second_leader = second.leader().unwrap();
    assert_eq!(first_leader, second_leader);

    let request = ClientRequest::Set {
        key: "deterministic".to_string(),
        value: "yes".to_string(),
    };
    assert_eq!(
        first.propose_as(7, first_leader, request.clone()),
        second.propose_as(7, second_leader, request)
    );
    first.run_for(500);
    second.run_for(500);

    assert_eq!(first.now(), second.now());
    assert_eq!(first.history(), second.history());
    assert_eq!(visible_state(&first), visible_state(&second));
}

type VisibleNodeState = (Role, u64, Option<usize>, usize, usize, Vec<LogEntry>);

fn visible_state(cluster: &Cluster) -> BTreeMap<usize, VisibleNodeState> {
    cluster
        .nodes()
        .map(|(id, node)| {
            (
                id,
                (
                    node.role(),
                    node.current_term(),
                    node.leader_id(),
                    node.commit_index(),
                    node.last_applied(),
                    node.log().to_vec(),
                ),
            )
        })
        .collect()
}

#[test]
fn compaction_install_snapshot_and_persistence_keep_state_and_reject_stale_data() {
    let mut source = committed_node();
    let snapshot = source.compact_to(2).expect("committed point can compact");
    assert_eq!(source.snapshot_index(), 2);
    assert_eq!(source.log().len(), 1, "only the boundary remains");
    assert_eq!(source.get("key"), Some("two".to_string()));

    let mut follower = Node::new(1, vec![0]);
    assert_eq!(
        follower.install_snapshot(snapshot.clone()).unwrap(),
        SnapshotInstallResult::Installed
    );
    assert_eq!(follower.get("key"), Some("two".to_string()));

    let stale = {
        let old = Node::from_parts(
            2,
            vec![0],
            1,
            None,
            vec![raft_kv::LogEntry {
                term: 1,
                command: raft_kv::Command::Set {
                    key: "key".to_string(),
                    value: "one".to_string(),
                },
            }],
            1,
        );
        old.create_snapshot().unwrap()
    };
    assert!(matches!(
        follower.install_snapshot(stale),
        Ok(SnapshotInstallResult::Stale)
    ));
    assert_eq!(follower.get("key"), Some("two".to_string()));

    let dir = tempfile::tempdir().unwrap();
    let node_path = dir.path().join("node.bin");
    let snapshot_path = dir.path().join("snapshot.bin");
    save_node(&node_path, &source).unwrap();
    save_snapshot(&snapshot_path, &snapshot).unwrap();
    let restored = load_node(&node_path, 0, vec![1, 2]).unwrap();
    assert_eq!(restored.snapshot_index(), 2);
    assert_eq!(restored.get("key"), Some("two".to_string()));
    assert_eq!(load_snapshot(&snapshot_path).unwrap(), snapshot);
}

#[test]
fn zero_boundary_snapshot_is_rejected_without_mutating_the_follower() {
    let mut follower = Node::from_parts(
        1,
        vec![0],
        1,
        None,
        vec![LogEntry {
            term: 1,
            command: raft_kv::Command::Set {
                key: "keep".to_string(),
                value: "me".to_string(),
            },
        }],
        1,
    );
    let malformed = Snapshot {
        version: SNAPSHOT_FORMAT_VERSION,
        last_included_index: 0,
        last_included_term: 0,
        state: StateSnapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            last_applied: 0,
            data: Vec::new(),
        },
    };
    let before_log = follower.log().to_vec();

    let error = follower
        .install_snapshot(malformed)
        .expect_err("zero cannot be a retained log boundary");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(follower.snapshot_index(), 0);
    assert_eq!(follower.log(), before_log.as_slice());
    assert_eq!(follower.get("keep"), Some("me".to_string()));
}

#[test]
fn unknown_public_proposals_return_failed_replies() {
    let mut cluster = Cluster::new(3);

    for request in [
        ClientRequest::Get {
            key: "missing".to_string(),
        },
        ClientRequest::Set {
            key: "key".to_string(),
            value: "value".to_string(),
        },
    ] {
        let reply = cluster.propose(99, request);
        assert!(!reply.success);
        assert_eq!(reply.leader_id, None);
        assert_eq!(reply.response, None);
    }
}

#[test]
fn compacted_leader_transfers_a_snapshot_to_a_restarted_follower() {
    let mut cluster = Cluster::new(3);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().unwrap();
    let follower = cluster
        .node_ids()
        .find(|&id| id != leader)
        .expect("follower");
    cluster.stop(follower);

    let reply = cluster.propose(
        leader,
        ClientRequest::Set {
            key: "offline".to_string(),
            value: "restored".to_string(),
        },
    );
    assert!(reply.success);
    assert!(cluster.run_until(1_500, |cluster| {
        cluster.node(leader).get("offline") == Some("restored".to_string())
    }));
    let index = cluster.node(leader).last_applied();
    cluster.compact_node(leader, index).unwrap();
    cluster.restart(follower);
    assert!(cluster.run_until(3_000, |cluster| {
        cluster.node(follower).get("offline") == Some("restored".to_string())
    }));
}
