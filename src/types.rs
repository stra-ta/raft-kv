use serde::{Deserialize, Serialize};

pub type NodeId = usize;
pub type Term = u64;
pub type LogIndex = usize;

pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Command {
    Noop,
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

/// A pre-vote probes whether a candidate could win an election without
/// advancing the receiver's persistent term.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StateSnapshot {
    pub version: u32,
    pub last_applied: LogIndex,
    pub data: Vec<(String, String)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Snapshot {
    pub version: u32,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub state: StateSnapshot,
}

impl Snapshot {
    pub fn validate(&self) -> std::io::Result<()> {
        if self.version != SNAPSHOT_FORMAT_VERSION || self.state.version != SNAPSHOT_FORMAT_VERSION
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unsupported snapshot format version",
            ));
        }
        if self.last_included_index == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot boundary must be greater than zero",
            ));
        }
        if self.state.last_applied != self.last_included_index {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot state index does not match snapshot boundary",
            ));
        }
        if self
            .state
            .data
            .windows(2)
            .any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot keys must be unique and sorted",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ClientRequest {
    Get { key: String },
    LocalGet { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientReply {
    pub success: bool,
    pub leader_id: Option<NodeId>,
    pub response: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Rpc {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(Snapshot),
    InstallSnapshotReply(InstallSnapshotReply),
    PreVote(PreVote),
    PreVoteReply(PreVoteReply),
    InstallSnapshotRequest(InstallSnapshotRequest),
    ReadIndex(ReadIndex),
    ReadIndexReply(ReadIndexReply),
    TimeoutNow(TimeoutNow),
    InstallSnapshotChunk(InstallSnapshotChunk),
    InstallSnapshotChunkReply(InstallSnapshotChunkReply),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstallSnapshotReply {
    pub term: Term,
    pub accepted: bool,
    pub last_included_index: LogIndex,
}

/// Metadata-bearing snapshot request used by current process runners. The
/// legacy `InstallSnapshot(Snapshot)` variant remains available for old
/// simulator fixtures and callers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadIndex {
    pub term: Term,
    pub leader_id: NodeId,
    pub request_id: u64,
    pub commit_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadIndexReply {
    pub term: Term,
    pub request_id: u64,
    pub applied_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TimeoutNow {
    pub term: Term,
    pub leader_id: NodeId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstallSnapshotChunk {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot_id: u64,
    pub offset: usize,
    pub total_size: usize,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InstallSnapshotChunkReply {
    pub term: Term,
    pub snapshot_id: u64,
    pub accepted: bool,
    pub next_offset: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub rpc: Rpc,
}
