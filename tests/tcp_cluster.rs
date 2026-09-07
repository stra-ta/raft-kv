use raft_kv::net::{WireMessage, read_frame, write_frame};
use raft_kv::storage::PersistedState;
use raft_kv::{ClientReply, ClientRequest, NodeId};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn tcp_process_cluster_elects_replicates_and_recovers_after_leader_kill() {
    let dir = tempfile::tempdir().unwrap();
    let peers = reserve_ports(3);
    let metrics = reserve_ports(3);
    let mut children = spawn_cluster(dir.path(), &peers, &metrics);

    let leader = wait_for_leader(&peers, Duration::from_secs(5)).expect("leader elected");
    let reply = send_client(
        &peers[&leader],
        ClientRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    )
    .expect("set through leader");
    assert!(reply.success);
    assert_eq!(reply.leader_id, Some(leader));
    assert_eq!(
        wait_for_get(&peers, "foo", Duration::from_secs(5)),
        Some("bar".to_string())
    );

    children.get_mut(&leader).unwrap().kill().unwrap();
    let _ = children.get_mut(&leader).unwrap().wait();

    let new_leader =
        wait_for_leader_except(&peers, leader, Duration::from_secs(5)).expect("new leader elected");
    let reply = send_client(
        &peers[&new_leader],
        ClientRequest::Set {
            key: "baz".to_string(),
            value: "qux".to_string(),
        },
    )
    .expect("set through new leader");
    assert!(reply.success);
    assert_eq!(
        wait_for_get(&peers, "baz", Duration::from_secs(5)),
        Some("qux".to_string())
    );

    let restarted = spawn_node(dir.path(), leader, &peers, &metrics);
    children.insert(leader, restarted);
    let restarted_reply = wait_for_local_get(&peers[&leader], "foo", Duration::from_secs(5))
        .expect("restarted node should expose local state directly");
    assert!(restarted_reply.success);
    assert_eq!(restarted_reply.response, Some("bar".to_string()));
    assert_eq!(
        wait_for_get(&peers, "foo", Duration::from_secs(5)),
        Some("bar".to_string())
    );

    for (_, mut child) in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn reserve_ports(count: usize) -> HashMap<NodeId, String> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .enumerate()
        .map(|(id, listener)| (id, listener.local_addr().unwrap().to_string()))
        .collect()
}

fn spawn_cluster(
    dir: &Path,
    peers: &HashMap<NodeId, String>,
    metrics: &HashMap<NodeId, String>,
) -> HashMap<NodeId, Child> {
    peers
        .keys()
        .copied()
        .map(|id| (id, spawn_node(dir, id, peers, metrics)))
        .collect()
}

fn spawn_node(
    dir: &Path,
    id: NodeId,
    peers: &HashMap<NodeId, String>,
    metrics: &HashMap<NodeId, String>,
) -> Child {
    spawn_node_with_env(dir, id, peers, metrics, &[])
}

fn spawn_node_with_env(
    dir: &Path,
    id: NodeId,
    peers: &HashMap<NodeId, String>,
    metrics: &HashMap<NodeId, String>,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut args = vec![
        id.to_string(),
        dir.join(format!("node-{id}.bin")).display().to_string(),
    ];
    let mut peer_args: Vec<_> = peers
        .iter()
        .map(|(id, addr)| format!("{id}={addr}"))
        .collect();
    peer_args.sort();
    args.extend(peer_args);
    let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_raft-node"));
    command
        .args(args)
        .env("RAFT_KV_METRICS_ADDR", &metrics[&id])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command.spawn().unwrap()
}

fn wait_for_leader(peers: &HashMap<NodeId, String>, timeout: Duration) -> Option<NodeId> {
    wait_for_leader_where(peers, timeout, |_| true)
}

fn wait_for_leader_except(
    peers: &HashMap<NodeId, String>,
    old: NodeId,
    timeout: Duration,
) -> Option<NodeId> {
    wait_for_leader_where(peers, timeout, |id| id != old)
}

fn wait_for_leader_where(
    peers: &HashMap<NodeId, String>,
    timeout: Duration,
    accept: impl Fn(NodeId) -> bool,
) -> Option<NodeId> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for addr in peers.values() {
            if let Ok(reply) = send_client(
                addr,
                ClientRequest::Get {
                    key: "__probe__".to_string(),
                },
            ) && let Some(id) = reply.leader_id.filter(|&id| accept(id))
            {
                return Some(id);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

fn wait_for_get(peers: &HashMap<NodeId, String>, key: &str, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for addr in peers.values() {
            if let Ok(reply) = send_client(
                addr,
                ClientRequest::Get {
                    key: key.to_string(),
                },
            ) && reply.success
                && reply.response.is_some()
            {
                return reply.response;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

fn wait_for_local_get(addr: &str, key: &str, timeout: Duration) -> Option<ClientReply> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(reply) = send_client(
            addr,
            ClientRequest::LocalGet {
                key: key.to_string(),
            },
        ) && reply.success
            && reply.response.is_some()
        {
            return Some(reply);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

fn send_client(addr: &str, request: ClientRequest) -> io::Result<ClientReply> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write_frame(&mut stream, &WireMessage::ClientRequest(request))?;
    match read_frame(&mut stream)? {
        WireMessage::ClientReply(reply) => Ok(reply),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected client reply",
        )),
    }
}

fn get_metrics(addr: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    io::Write::write_all(&mut stream, b"GET /metrics HTTP/1.1\r\nHost: node\r\n\r\n")?;
    let mut response = String::new();
    io::Read::read_to_string(&mut stream, &mut response)?;
    Ok(response)
}

#[test]
fn cluster_recovers_state_after_full_restart() {
    let dir = tempfile::tempdir().unwrap();
    let peers = reserve_ports(3);
    let metrics = reserve_ports(3);
    let mut children = spawn_cluster(dir.path(), &peers, &metrics);

    let leader = wait_for_leader(&peers, Duration::from_secs(5)).expect("leader elected");
    let reply = send_client(
        &peers[&leader],
        ClientRequest::Set {
            key: "alpha".to_string(),
            value: "bravo".to_string(),
        },
    )
    .expect("set alpha");
    assert!(reply.success);

    assert_eq!(
        wait_for_get(&peers, "alpha", Duration::from_secs(5)),
        Some("bravo".to_string())
    );

    // Kill all nodes.
    for (_, mut child) in children.drain() {
        let _ = child.kill();
        let _ = child.wait();
    }

    // Restart all nodes from the same data directory (state files on disk).
    let children = spawn_cluster(dir.path(), &peers, &metrics);
    let new_leader =
        wait_for_leader(&peers, Duration::from_secs(5)).expect("leader re-elected after restart");

    assert_eq!(
        wait_for_get(&peers, "alpha", Duration::from_secs(5)),
        Some("bravo".to_string()),
        "persisted state should survive full cluster restart"
    );

    // New writes should still work.
    let reply = send_client(
        &peers[&new_leader],
        ClientRequest::Set {
            key: "charlie".to_string(),
            value: "delta".to_string(),
        },
    )
    .expect("set charlie after restart");
    assert!(reply.success);
    assert_eq!(
        wait_for_get(&peers, "charlie", Duration::from_secs(5)),
        Some("delta".to_string())
    );

    for (_, mut child) in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[test]
fn metrics_endpoint_exposes_raft_and_lsm_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let peers = reserve_ports(3);
    let metrics = reserve_ports(3);
    let children = spawn_cluster(dir.path(), &peers, &metrics);

    let leader = wait_for_leader(&peers, Duration::from_secs(5)).expect("leader elected");
    let reply = send_client(
        &peers[&leader],
        ClientRequest::Set {
            key: "metrics".to_string(),
            value: "visible".to_string(),
        },
    )
    .expect("write through leader");
    assert!(reply.success);

    let body = wait_for_metrics(&metrics[&leader], Duration::from_secs(5)).expect("metrics body");
    for name in [
        "raft_term",
        "raft_state",
        "raft_commit_index",
        "raft_last_applied",
        "raft_writes_total",
        "raft_write_latency_seconds",
        "raft_replication_lag",
        "lsm_memtable_size_bytes",
        "lsm_sstable_count",
        "lsm_compactions_total",
    ] {
        assert!(body.contains(name), "missing {name}\n{body}");
    }

    for (_, mut child) in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn wait_for_metrics(addr: &str, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(response) = get_metrics(addr)
            && response.starts_with("HTTP/1.1 200 OK")
        {
            return Some(response);
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

const PERSISTENCE_MAGIC: &[u8; 8] = b"RKVPST02";

fn persisted_snapshot_index(dir: &Path, id: NodeId) -> Option<usize> {
    let bytes = fs::read(dir.join(format!("node-{id}.bin"))).ok()?;
    let payload = bytes.strip_prefix(PERSISTENCE_MAGIC)?;
    let state: PersistedState = bincode::deserialize(payload).ok()?;
    state.snapshot.map(|snapshot| snapshot.last_included_index)
}

#[test]
fn small_snapshot_threshold_auto_compacts_through_persist_if_changed() {
    let dir = tempfile::tempdir().unwrap();
    let peers = reserve_ports(3);
    let metrics = reserve_ports(3);
    let children: HashMap<NodeId, Child> = peers
        .keys()
        .copied()
        .map(|id| {
            (
                id,
                spawn_node_with_env(
                    dir.path(),
                    id,
                    &peers,
                    &metrics,
                    &[("RAFT_KV_SNAPSHOT_THRESHOLD", "2")],
                ),
            )
        })
        .collect();

    let leader = wait_for_leader(&peers, Duration::from_secs(5)).expect("leader elected");
    for i in 0..4 {
        let reply = send_client(
            &peers[&leader],
            ClientRequest::Set {
                key: format!("snap-key-{i}"),
                value: format!("snap-value-{i}"),
            },
        )
        .expect("set through leader");
        assert!(reply.success);
    }
    assert_eq!(
        wait_for_get(&peers, "snap-key-3", Duration::from_secs(5)),
        Some("snap-value-3".to_string())
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    let snapshot_index = loop {
        if let Some(index) = persisted_snapshot_index(dir.path(), leader) {
            break index;
        }
        if Instant::now() > deadline {
            panic!("leader never auto-compacted below the small threshold");
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(
        snapshot_index >= 2,
        "snapshot boundary should advance past the threshold, got {snapshot_index}"
    );

    for (_, mut child) in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}
