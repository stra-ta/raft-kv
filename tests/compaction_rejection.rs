use raft_kv::{
    Command, LogEntry, LogIndex, Node, SNAPSHOT_FORMAT_VERSION, StateMachine, StateSnapshot,
};
use std::io;

#[derive(Clone, Debug)]
struct StubStateMachine {
    last_applied: LogIndex,
    snapshot_index: LogIndex,
}

impl StateMachine for StubStateMachine {
    fn apply(&mut self, index: LogIndex, _command: &Command) -> io::Result<()> {
        self.last_applied = index;
        Ok(())
    }

    fn get(&self, _key: &str) -> io::Result<Option<String>> {
        Ok(None)
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    fn snapshot(&self) -> io::Result<StateSnapshot> {
        Ok(StateSnapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            last_applied: self.snapshot_index,
            data: Vec::new(),
        })
    }
}

fn two_entry_log() -> Vec<LogEntry> {
    vec![
        LogEntry {
            term: 1,
            command: Command::Set {
                key: "k1".to_string(),
                value: "v1".to_string(),
            },
        },
        LogEntry {
            term: 1,
            command: Command::Set {
                key: "k2".to_string(),
                value: "v2".to_string(),
            },
        },
    ]
}

fn committed_node() -> Node {
    Node::from_parts(0, vec![1, 2], 1, None, two_entry_log(), 2)
}

#[test]
fn compact_to_rejects_an_index_at_or_below_the_snapshot_boundary() {
    let mut node = committed_node();
    node.compact_to(2).expect("the committed applied point can compact");
    assert_eq!(node.snapshot_index(), 2);
    assert_eq!(node.last_applied(), 2);
    assert_eq!(node.commit_index(), 2);

    let error = node
        .compact_to(2)
        .expect_err("compaction at the existing snapshot boundary must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn compact_to_rejects_an_index_below_last_applied() {
    let mut node = committed_node();
    assert_eq!(node.snapshot_index(), 0);
    assert_eq!(node.last_applied(), 2);
    assert_eq!(node.commit_index(), 2);

    let error = node
        .compact_to(1)
        .expect_err("compaction below the applied point must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn compact_to_rejects_an_index_beyond_commit_index() {
    let stub = StubStateMachine {
        last_applied: 5,
        snapshot_index: 5,
    };
    let mut node = Node::from_parts_with_state_machine(
        0,
        vec![1, 2],
        1,
        None,
        two_entry_log(),
        2,
        stub,
    );
    assert_eq!(node.snapshot_index(), 0);
    assert_eq!(node.last_applied(), 5);
    assert_eq!(node.commit_index(), 2);

    let error = node
        .compact_to(5)
        .expect_err("compaction beyond the commit index must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn create_snapshot_rejects_a_stale_state_machine_snapshot() {
    let stub = StubStateMachine {
        last_applied: 5,
        snapshot_index: 4,
    };
    let node = Node::new_with_state_machine(0, vec![], stub);
    assert_eq!(node.last_applied(), 5);

    let error = node
        .create_snapshot()
        .expect_err("a state machine snapshot behind last_applied must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "state machine snapshot index is stale",
        "the stale snapshot guard (not Snapshot::validate) must fire first"
    );
}
