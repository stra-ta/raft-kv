use crate::{
    LogEntry, LogIndex, MemoryStateMachine, Node, NodeId, SNAPSHOT_FORMAT_VERSION, Snapshot,
    StateMachine, Term,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const PERSISTENCE_MAGIC: &[u8; 8] = b"RKVPST02";
const SNAPSHOT_MAGIC: &[u8; 8] = b"RKVSNP01";
const PERSISTENCE_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedState {
    pub version: u32,
    pub durable: DurableState,
    pub snapshot: Option<Snapshot>,
}

impl PersistedState {
    pub fn from_node<S: StateMachine>(node: &Node<S>) -> Self {
        Self {
            version: PERSISTENCE_FORMAT_VERSION,
            durable: DurableState::from_node(node),
            snapshot: node.snapshot().cloned(),
        }
    }
}

impl DurableState {
    pub fn from_node<S: StateMachine>(node: &Node<S>) -> Self {
        Self {
            current_term: node.current_term(),
            voted_for: node.voted_for(),
            log: node.log().to_vec(),
            commit_index: node.commit_index(),
        }
    }
}

pub fn load_node(path: &Path, id: NodeId, peers: Vec<NodeId>) -> io::Result<Node> {
    load_node_with_state_machine(path, id, peers, MemoryStateMachine::new())
}

pub fn load_node_with_state_machine<S: StateMachine>(
    path: &Path,
    id: NodeId,
    peers: Vec<NodeId>,
    state_machine: S,
) -> io::Result<Node<S>> {
    match fs::read(path) {
        Ok(bytes) => {
            if let Some(payload) = bytes.strip_prefix(PERSISTENCE_MAGIC) {
                let state: PersistedState = bincode::deserialize(payload)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                if state.version != PERSISTENCE_FORMAT_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported persisted state format version",
                    ));
                }
                Node::from_persisted_parts(
                    id,
                    peers,
                    state.durable.current_term,
                    state.durable.voted_for,
                    state.durable.log,
                    state.durable.commit_index,
                    state.snapshot,
                    state_machine,
                )
            } else {
                let state: DurableState = bincode::deserialize(&bytes)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                Ok(Node::from_parts_with_state_machine(
                    id,
                    peers,
                    state.current_term,
                    state.voted_for,
                    state.log,
                    state.commit_index,
                    state_machine,
                ))
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            Ok(Node::new_with_state_machine(id, peers, state_machine))
        }
        Err(err) => Err(err),
    }
}

pub fn save_node<S: StateMachine>(path: &Path, node: &Node<S>) -> io::Result<()> {
    save_persisted_state(path, &PersistedState::from_node(node))
}

pub fn save_state(path: &Path, state: &DurableState) -> io::Result<()> {
    // This function predates snapshot-aware persistence and is still a public
    // raw-state compatibility boundary. Keep its bytes identical to the
    // legacy `bincode::serialize(DurableState)` representation.
    let bytes = bincode::serialize(state).map_err(io::Error::other)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp_path = tmp_path_for(path);
    write_atomic(path, &tmp_path, parent, &bytes)
}

fn save_persisted_state(path: &Path, state: &PersistedState) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp_path = tmp_path_for(path);
    let mut bytes = PERSISTENCE_MAGIC.to_vec();
    bytes.extend(bincode::serialize(state).map_err(io::Error::other)?);
    write_atomic(path, &tmp_path, parent, &bytes)
}

pub fn save_snapshot(path: &Path, snapshot: &Snapshot) -> io::Result<()> {
    snapshot.validate()?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp_path = tmp_path_for(path);
    let mut bytes = SNAPSHOT_MAGIC.to_vec();
    bytes.extend(bincode::serialize(snapshot).map_err(io::Error::other)?);
    write_atomic(path, &tmp_path, parent, &bytes)
}

pub fn load_snapshot(path: &Path) -> io::Result<Snapshot> {
    let bytes = fs::read(path)?;
    let payload = bytes
        .strip_prefix(SNAPSHOT_MAGIC)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid snapshot magic"))?;
    let snapshot: Snapshot = bincode::deserialize(payload)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    snapshot.validate()?;
    if snapshot.version != SNAPSHOT_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported snapshot format version",
        ));
    }
    Ok(snapshot)
}

fn write_atomic(path: &Path, tmp_path: &Path, parent: &Path, bytes: &[u8]) -> io::Result<()> {
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, path)?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.bin");
    tmp.set_file_name(format!(".{name}.tmp"));
    tmp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;

    #[test]
    fn save_and_load_restores_term_vote_log_and_state_machine() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.bin");
        let node = Node::from_parts(
            0,
            vec![1, 2],
            4,
            Some(2),
            vec![LogEntry {
                term: 4,
                command: Command::Set {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                },
            }],
            1,
        );
        save_node(&path, &node).unwrap();
        let loaded = load_node(&path, 0, vec![1, 2]).unwrap();

        assert_eq!(loaded.current_term(), 4);
        assert_eq!(loaded.voted_for(), Some(2));
        assert_eq!(loaded.log(), node.log());
        assert_eq!(loaded.get("foo"), Some("bar".to_string()));
    }

    #[test]
    fn save_state_preserves_the_legacy_raw_durable_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-node.bin");
        let state = DurableState {
            current_term: 4,
            voted_for: Some(2),
            log: vec![LogEntry {
                term: 4,
                command: Command::Set {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                },
            }],
            commit_index: 1,
        };

        save_state(&path, &state).unwrap();

        assert_eq!(
            fs::read(&path).unwrap(),
            bincode::serialize(&state).unwrap()
        );
        let loaded = load_node(&path, 0, vec![1, 2]).unwrap();
        assert_eq!(loaded.current_term(), state.current_term);
        assert_eq!(loaded.voted_for(), state.voted_for);
        assert_eq!(loaded.log(), state.log.as_slice());
    }
}
