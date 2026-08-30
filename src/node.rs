use crate::state_machine::{MemoryStateMachine, StateMachine};
use crate::types::*;
use std::collections::{HashMap, HashSet};

const HEARTBEAT_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_SPAN_MS: u64 = 151;
pub const MAX_APPEND_ENTRIES: usize = 64;
pub const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
struct PendingReadState {
    term: Term,
    barrier_index: LogIndex,
    acknowledgements: HashSet<NodeId>,
}

#[derive(Clone, Debug)]
struct PartialSnapshot {
    term: Term,
    leader_id: NodeId,
    total_size: usize,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Node<S: StateMachine = MemoryStateMachine> {
    id: NodeId,
    peers: Vec<NodeId>,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    /// When non-zero, `log[0]` is the snapshot boundary and subsequent
    /// entries start at `log_base_index + 1`. Before compaction the base is
    /// zero and the vector keeps the original 1-based log representation.
    log_base_index: LogIndex,
    snapshot: Option<Snapshot>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    state_machine: S,
    leader_id: Option<NodeId>,
    election_deadline: u64,
    last_heartbeat_at: u64,
    last_leader_contact: u64,
    rng_state: u64,
    votes_granted: usize,
    /// Voters that granted the current election term, including self. Guards
    /// against counting a duplicated vote reply from the same peer twice,
    /// which could otherwise win an election without a real majority.
    granted_voters: HashSet<NodeId>,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    /// Index of the first log entry in the current term (noop). When commit_index
    /// reaches this, the leader has proved it is still the leader and can serve reads.
    term_start_index: LogIndex,
    /// Pre-vote state is deliberately separate from a real election. A
    /// pre-vote never changes `current_term` or `voted_for`.
    pre_vote_term: Option<Term>,
    pre_vote_granted: HashSet<NodeId>,
    /// Last response observed from each voter while this node is leader.
    /// Used by check-quorum to step down after a lost majority.
    last_quorum_contact: HashMap<NodeId, u64>,
    pending_reads: HashMap<u64, PendingReadState>,
    next_read_id: u64,
    leader_transfer_target: Option<NodeId>,
    incoming_snapshot: Option<(u64, PartialSnapshot)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingWrite {
    pub index: LogIndex,
    pub term: Term,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingRead {
    pub id: u64,
    pub term: Term,
    pub barrier_index: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotInstallResult {
    Installed,
    Stale,
}

impl Node<MemoryStateMachine> {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self::new_with_state_machine(id, peers, MemoryStateMachine::new())
    }

    pub fn from_parts(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
    ) -> Self {
        Self::from_parts_with_state_machine(
            id,
            peers,
            current_term,
            voted_for,
            log,
            commit_index,
            MemoryStateMachine::new(),
        )
    }
}

impl<S: StateMachine> Node<S> {
    pub fn new_with_state_machine(id: NodeId, peers: Vec<NodeId>, state_machine: S) -> Self {
        let mut node = Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            log_base_index: 0,
            snapshot: None,
            commit_index: 0,
            last_applied: state_machine.last_applied(),
            state_machine,
            leader_id: None,
            election_deadline: 0,
            last_heartbeat_at: 0,
            last_leader_contact: 0,
            rng_state: (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            votes_granted: 0,
            granted_voters: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            term_start_index: 0,
            pre_vote_term: None,
            pre_vote_granted: HashSet::new(),
            last_quorum_contact: HashMap::new(),
            pending_reads: HashMap::new(),
            next_read_id: 0,
            leader_transfer_target: None,
            incoming_snapshot: None,
        };
        node.reset_election_timer(0);
        node
    }

    pub fn from_parts_with_state_machine(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
        state_machine: S,
    ) -> Self {
        let mut node = Self::new_with_state_machine(id, peers, state_machine);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.commit_index = commit_index.min(node.log.len());
        let _ = node.apply_committed();
        node
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the persisted node fields are kept explicit for the versioned storage boundary"
    )]
    pub fn from_persisted_parts(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
        snapshot: Option<Snapshot>,
        state_machine: S,
    ) -> std::io::Result<Self> {
        let mut node = Self::new_with_state_machine(id, peers, state_machine);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.snapshot = snapshot;
        if let Some(snapshot) = &node.snapshot {
            snapshot.validate()?;
            node.log_base_index = snapshot.last_included_index;
            if node.log_base_index == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "zero-index snapshot is not a valid log boundary",
                ));
            }
            if node
                .log
                .first()
                .is_none_or(|entry| entry.term != snapshot.last_included_term)
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "persisted log does not match snapshot boundary",
                ));
            }
            if node.state_machine.last_applied() < snapshot.last_included_index {
                node.state_machine.restore_snapshot(&snapshot.state)?;
            }
            if node.state_machine.last_applied() < snapshot.last_included_index {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "restored state machine index is behind snapshot",
                ));
            }
        }
        node.commit_index = commit_index.min(node.last_log_index());
        node.commit_index = node.commit_index.max(node.log_base_index);
        node.last_applied = node.state_machine.last_applied();
        node.apply_committed()?;
        Ok(node)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn role_label(&self) -> &'static str {
        match self.role {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    pub fn snapshot_index(&self) -> LogIndex {
        self.log_base_index
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn can_serve_reads(&self) -> bool {
        self.commit_index >= self.term_start_index
    }

    /// Starts a ReadIndex barrier. The caller must wait for
    /// `read_committed_and_applied` before observing the state machine. This
    /// keeps the public read path linearizable even when a leader has not yet
    /// heard from a quorum in its current term.
    pub fn start_client_read(&mut self) -> Result<(PendingRead, Vec<Message>), ClientReply> {
        if self.role != Role::Leader {
            return Err(ClientReply {
                success: false,
                leader_id: self.leader_id,
                response: None,
            });
        }
        let id = self.next_read_id;
        self.next_read_id = self.next_read_id.wrapping_add(1);
        let barrier_index = self.commit_index.max(self.term_start_index);
        let mut acknowledgements = HashSet::new();
        acknowledgements.insert(self.id);
        self.pending_reads.insert(
            id,
            PendingReadState {
                term: self.current_term,
                barrier_index,
                acknowledgements,
            },
        );
        let request = ReadIndex {
            term: self.current_term,
            leader_id: self.id,
            request_id: id,
            commit_index: barrier_index,
        };
        let messages = self
            .peers
            .iter()
            .map(|&peer| Message {
                from: self.id,
                to: peer,
                rpc: Rpc::ReadIndex(request.clone()),
            })
            .collect();
        Ok((
            PendingRead {
                id,
                term: self.current_term,
                barrier_index,
            },
            messages,
        ))
    }

    pub fn read_committed_and_applied(&self, read: PendingRead) -> bool {
        self.role == Role::Leader
            && self.current_term == read.term
            && self.commit_index >= read.barrier_index
            && self.last_applied >= read.barrier_index
            && self.pending_reads.get(&read.id).is_some_and(|state| {
                state.term == read.term
                    && state.barrier_index == read.barrier_index
                    && state.acknowledgements.len() >= self.majority()
            })
    }

    pub fn read_value(&self, key: &str) -> std::io::Result<Option<String>> {
        self.state_machine.get(key)
    }

    pub fn finish_read(&mut self, read: PendingRead) {
        self.pending_reads.remove(&read.id);
    }

    /// Ask a caught-up follower to campaign immediately. If it is behind, the
    /// leader records the transfer and first returns the missing append batch;
    /// the timeout-now message is emitted after the follower acknowledges it.
    pub fn transfer_leadership(
        &mut self,
        target: NodeId,
        now_ms: u64,
    ) -> std::io::Result<Vec<Message>> {
        if self.role != Role::Leader {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "only the leader can transfer leadership",
            ));
        }
        if !self.peers.contains(&target) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "leadership transfer target is not a peer",
            ));
        }
        self.leader_transfer_target = Some(target);
        self.last_quorum_contact.entry(target).or_insert(now_ms);
        let matched = self.match_index.get(&target).copied().unwrap_or(0);
        if matched < self.last_log_index() {
            return Ok(vec![self.append_entries_for(target)]);
        }
        Ok(vec![self.timeout_now_for(target)])
    }

    /// Explicit alias for callers that prefer the Raft terminology.
    pub fn initiate_leader_transfer(
        &mut self,
        target: NodeId,
        now_ms: u64,
    ) -> std::io::Result<Vec<Message>> {
        self.transfer_leadership(target, now_ms)
    }

    pub fn request_leadership_transfer(
        &mut self,
        target: NodeId,
        now_ms: u64,
    ) -> std::io::Result<Vec<Message>> {
        self.transfer_leadership(target, now_ms)
    }

    /// Encodes the current snapshot into ordered, replayable chunks for
    /// transports that cannot safely buffer one large frame. The regular
    /// Raft path still uses the single-frame request for small local tests.
    pub fn snapshot_transfer_chunks(&self, peer: NodeId) -> Vec<Message> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Vec::new();
        };
        let Ok(bytes) = bincode::serialize(snapshot) else {
            return Vec::new();
        };
        let snapshot_id = snapshot_id(snapshot);
        let total_size = bytes.len();
        let mut messages = Vec::new();
        if bytes.is_empty() {
            messages.push(Message {
                from: self.id,
                to: peer,
                rpc: Rpc::InstallSnapshotChunk(InstallSnapshotChunk {
                    term: self.current_term,
                    leader_id: self.id,
                    snapshot_id,
                    offset: 0,
                    total_size: 0,
                    data: Vec::new(),
                    done: true,
                }),
            });
            return messages;
        }
        for (offset, chunk) in bytes.chunks(SNAPSHOT_CHUNK_BYTES).enumerate() {
            let offset = offset * SNAPSHOT_CHUNK_BYTES;
            messages.push(Message {
                from: self.id,
                to: peer,
                rpc: Rpc::InstallSnapshotChunk(InstallSnapshotChunk {
                    term: self.current_term,
                    leader_id: self.id,
                    snapshot_id,
                    offset,
                    total_size,
                    data: chunk.to_vec(),
                    done: offset + chunk.len() == total_size,
                }),
            });
        }
        messages
    }

    fn snapshot_chunk_for(
        &self,
        peer: NodeId,
        snapshot: Snapshot,
        offset: usize,
        bytes: Vec<u8>,
    ) -> Message {
        let end = (offset + SNAPSHOT_CHUNK_BYTES).min(bytes.len());
        Message {
            from: self.id,
            to: peer,
            rpc: Rpc::InstallSnapshotChunk(InstallSnapshotChunk {
                term: self.current_term,
                leader_id: self.id,
                snapshot_id: snapshot_id(&snapshot),
                offset,
                total_size: bytes.len(),
                data: bytes[offset..end].to_vec(),
                done: end == bytes.len(),
            }),
        }
    }
    pub fn get(&self, key: &str) -> Option<String> {
        self.state_machine.get(key).ok().flatten()
    }

    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    pub fn state_machine(&self) -> &S {
        &self.state_machine
    }

    pub fn replication_lag_by_peer(&self) -> Vec<(NodeId, LogIndex)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        self.peers
            .iter()
            .map(|&peer| {
                let matched = self.match_index.get(&peer).copied().unwrap_or(0);
                (peer, self.commit_index.saturating_sub(matched))
            })
            .collect()
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<Message> {
        match self.role {
            Role::Leader if now_ms.saturating_sub(self.last_heartbeat_at) >= HEARTBEAT_MS => {
                if !self.has_quorum_contact(now_ms) {
                    tracing::warn!(
                        node = self.id,
                        term = self.current_term,
                        "check-quorum lost majority; stepping down"
                    );
                    self.step_down(self.current_term, now_ms);
                    return Vec::new();
                }
                self.last_heartbeat_at = now_ms;
                self.peers
                    .iter()
                    .map(|&peer| self.append_entries_for(peer))
                    .collect()
            }
            Role::Follower | Role::Candidate if now_ms >= self.election_deadline => {
                if self.role == Role::Candidate {
                    self.start_election(now_ms)
                } else {
                    self.start_pre_vote(now_ms)
                }
            }
            _ => Vec::new(),
        }
    }

    pub fn handle_message(&mut self, from: NodeId, rpc: Rpc, now_ms: u64) -> Vec<Message> {
        match rpc {
            Rpc::RequestVote(v) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::RequestVoteReply(self.handle_request_vote(v, now_ms)),
            }],
            Rpc::RequestVoteReply(r) => self.handle_request_vote_reply(from, r, now_ms),
            Rpc::AppendEntries(a) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::AppendEntriesReply(self.handle_append_entries(a, now_ms)),
            }],
            Rpc::AppendEntriesReply(r) => self.handle_append_entries_reply(from, r, now_ms),
            Rpc::InstallSnapshot(snapshot) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::InstallSnapshotReply(
                    self.handle_install_snapshot(from, snapshot, now_ms),
                ),
            }],
            Rpc::InstallSnapshotReply(reply) => {
                self.handle_install_snapshot_reply(from, reply, now_ms)
            }
            Rpc::PreVote(v) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::PreVoteReply(self.handle_pre_vote(v, now_ms)),
            }],
            Rpc::PreVoteReply(r) => self.handle_pre_vote_reply(from, r, now_ms),
            Rpc::InstallSnapshotRequest(request) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::InstallSnapshotReply(
                    self.handle_install_snapshot_request(from, request, now_ms),
                ),
            }],
            Rpc::ReadIndex(request) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::ReadIndexReply(self.handle_read_index(from, request, now_ms)),
            }],
            Rpc::ReadIndexReply(reply) => self.handle_read_index_reply(from, reply, now_ms),
            Rpc::TimeoutNow(request) => self.handle_timeout_now(from, request, now_ms),
            Rpc::InstallSnapshotChunk(chunk) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::InstallSnapshotChunkReply(
                    self.handle_install_snapshot_chunk(from, chunk, now_ms),
                ),
            }],
            Rpc::InstallSnapshotChunkReply(reply) => {
                self.handle_install_snapshot_chunk_reply(from, reply, now_ms)
            }
        }
    }

    pub fn handle_client_request(&mut self, request: ClientRequest) -> (ClientReply, Vec<Message>) {
        if let ClientRequest::LocalGet { key } = &request {
            return (
                ClientReply {
                    success: true,
                    leader_id: self.leader_id,
                    response: match self.state_machine.get(key) {
                        Ok(value) => value,
                        Err(_) => {
                            return (
                                ClientReply {
                                    success: false,
                                    leader_id: self.leader_id,
                                    response: None,
                                },
                                Vec::new(),
                            );
                        }
                    },
                },
                Vec::new(),
            );
        }
        if self.role != Role::Leader {
            return (
                ClientReply {
                    success: false,
                    leader_id: self.leader_id,
                    response: None,
                },
                Vec::new(),
            );
        }
        if matches!(
            request,
            ClientRequest::Set { .. } | ClientRequest::Delete { .. }
        ) {
            return (
                ClientReply {
                    success: false,
                    leader_id: Some(self.id),
                    response: Some("pending write requires proposal lifecycle".to_string()),
                },
                Vec::new(),
            );
        }
        if matches!(request, ClientRequest::Get { .. }) {
            return (
                ClientReply {
                    success: false,
                    leader_id: Some(self.id),
                    response: Some("read requires a ReadIndex lifecycle".to_string()),
                },
                Vec::new(),
            );
        }
        unreachable!("all client request variants handled")
    }

    pub fn start_client_write(
        &mut self,
        request: ClientRequest,
    ) -> Result<(PendingWrite, Vec<Message>), ClientReply> {
        if self.role != Role::Leader {
            return Err(ClientReply {
                success: false,
                leader_id: self.leader_id,
                response: None,
            });
        }
        let command = match request {
            ClientRequest::Set { key, value } => Command::Set { key, value },
            ClientRequest::Delete { key } => Command::Delete { key },
            ClientRequest::Get { .. } | ClientRequest::LocalGet { .. } => {
                return Err(ClientReply {
                    success: false,
                    leader_id: Some(self.id),
                    response: None,
                });
            }
        };
        let term = self.current_term;
        self.log.push(LogEntry { term, command });
        let last = self.last_log_index();
        self.match_index.insert(self.id, last);
        let messages = self
            .peers
            .iter()
            .map(|&peer| self.append_entries_for(peer))
            .collect();
        Ok((PendingWrite { index: last, term }, messages))
    }

    pub fn write_committed_and_applied(&self, write: PendingWrite) -> bool {
        self.term_at(write.index) == Some(write.term)
            && self.commit_index >= write.index
            && self.last_applied >= write.index
    }

    /// Builds a snapshot at the last applied index. Snapshot creation never
    /// includes uncommitted log entries.
    pub fn create_snapshot(&self) -> std::io::Result<Snapshot> {
        let state = self.state_machine.snapshot()?;
        let index = self.last_applied;
        if state.last_applied != index {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "state machine snapshot index is stale",
            ));
        }
        let snapshot = Snapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            last_included_index: index,
            last_included_term: self.term_at(index).unwrap_or(0),
            state,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Replaces the log prefix with a durable snapshot boundary. Compaction
    /// is intentionally limited to the applied point so the state machine and
    /// remaining log cannot describe different histories.
    pub fn compact_to(&mut self, index: LogIndex) -> std::io::Result<Snapshot> {
        if index <= self.log_base_index || index != self.last_applied || index > self.commit_index {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "log compaction requires a newer committed applied index",
            ));
        }
        let snapshot = self.create_snapshot()?;
        let suffix = self.entries_after(index).to_vec();
        self.log = std::iter::once(LogEntry {
            term: snapshot.last_included_term,
            command: Command::Noop,
        })
        .chain(suffix)
        .collect();
        self.log_base_index = index;
        self.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub fn install_snapshot(
        &mut self,
        snapshot: Snapshot,
    ) -> std::io::Result<SnapshotInstallResult> {
        snapshot.validate()?;
        if snapshot.last_included_index < self.log_base_index
            || (snapshot.last_included_index == self.log_base_index
                && self.snapshot.as_ref().is_some_and(|current| {
                    current.last_included_term == snapshot.last_included_term
                }))
        {
            return Ok(SnapshotInstallResult::Stale);
        }
        if snapshot.last_included_index < self.commit_index {
            return Ok(SnapshotInstallResult::Stale);
        }

        // Validate and restore the state machine before changing the log
        // boundary. A malformed or stale snapshot therefore leaves the node
        // untouched.
        self.state_machine.restore_snapshot(&snapshot.state)?;
        let suffix =
            if self.term_at(snapshot.last_included_index) == Some(snapshot.last_included_term) {
                self.entries_after(snapshot.last_included_index).to_vec()
            } else {
                Vec::new()
            };
        self.log = std::iter::once(LogEntry {
            term: snapshot.last_included_term,
            command: Command::Noop,
        })
        .chain(suffix)
        .collect();
        self.log_base_index = snapshot.last_included_index;
        self.snapshot = Some(snapshot.clone());
        self.commit_index = self.commit_index.max(snapshot.last_included_index);
        self.last_applied = snapshot.last_included_index;
        self.leader_id = None;
        Ok(SnapshotInstallResult::Installed)
    }

    fn start_pre_vote(&mut self, now_ms: u64) -> Vec<Message> {
        let prospective_term = self.current_term.saturating_add(1);
        self.pre_vote_term = Some(prospective_term);
        self.pre_vote_granted.clear();
        self.pre_vote_granted.insert(self.id);
        self.reset_election_timer(now_ms);
        let request = PreVote {
            term: prospective_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        if self.pre_vote_granted.len() >= self.majority() {
            return self.start_election(now_ms);
        }
        self.peers
            .iter()
            .map(|&peer| Message {
                from: self.id,
                to: peer,
                rpc: Rpc::PreVote(request.clone()),
            })
            .collect()
    }

    fn handle_pre_vote(&mut self, request: PreVote, now_ms: u64) -> PreVoteReply {
        let up_to_date = request.last_log_term > self.last_log_term()
            || (request.last_log_term == self.last_log_term()
                && request.last_log_index >= self.last_log_index());
        // A live leader's heartbeat has reset this timer. Refusing a pre-vote
        // during that lease prevents an isolated node from needlessly
        // advancing the cluster's term.
        let election_allowed = self.leader_id.is_none()
            || now_ms.saturating_sub(self.last_leader_contact) >= ELECTION_MIN_MS * 2;
        PreVoteReply {
            term: self.current_term,
            vote_granted: request.term >= self.current_term && up_to_date && election_allowed,
        }
    }

    fn handle_pre_vote_reply(
        &mut self,
        from: NodeId,
        reply: PreVoteReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        let Some(term) = self.pre_vote_term else {
            return Vec::new();
        };
        if term != self.current_term.saturating_add(1) || !reply.vote_granted {
            return Vec::new();
        }
        if !self.pre_vote_granted.insert(from) {
            return Vec::new();
        }
        if self.pre_vote_granted.len() >= self.majority() {
            self.start_election(now_ms)
        } else {
            Vec::new()
        }
    }

    fn start_election(&mut self, now_ms: u64) -> Vec<Message> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.pre_vote_term = None;
        self.pre_vote_granted.clear();
        tracing::info!(node = self.id, term = self.current_term, "election started");
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_granted = 1;
        self.granted_voters.clear();
        self.granted_voters.insert(self.id);
        self.reset_election_timer(now_ms);
        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        self.peers
            .iter()
            .map(|&peer| Message {
                from: self.id,
                to: peer,
                rpc: Rpc::RequestVote(request.clone()),
            })
            .collect()
    }

    fn handle_request_vote(&mut self, request: RequestVote, now_ms: u64) -> RequestVoteReply {
        if request.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if request.term == self.current_term
            && self.role == Role::Leader
            && request.candidate_id != self.id
        {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if request.term > self.current_term {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = request.term,
                "term advanced by vote request"
            );
            self.step_down(request.term, now_ms);
        }
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(request.candidate_id);
        let up_to_date = request.last_log_term > self.last_log_term()
            || (request.last_log_term == self.last_log_term()
                && request.last_log_index >= self.last_log_index());
        let vote_granted = can_vote && up_to_date;
        if vote_granted {
            self.voted_for = Some(request.candidate_id);
            self.pre_vote_term = None;
            self.pre_vote_granted.clear();
            self.reset_election_timer(now_ms);
        } else {
            tracing::info!(
                node = self.id,
                term = self.current_term,
                candidate = request.candidate_id,
                "vote rejected"
            );
        }
        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    fn handle_request_vote_reply(
        &mut self,
        from: NodeId,
        reply: RequestVoteReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = reply.term,
                "term advanced by vote reply"
            );
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Candidate || reply.term != self.current_term || !reply.vote_granted {
            return Vec::new();
        }
        if !self.granted_voters.insert(from) {
            return Vec::new();
        }
        self.votes_granted = self.granted_voters.len();
        if self.votes_granted >= self.majority() {
            tracing::info!(
                node = self.id,
                term = self.current_term,
                votes = self.votes_granted,
                "election won"
            );
            self.become_leader(now_ms)
        } else {
            Vec::new()
        }
    }

    fn become_leader(&mut self, now_ms: u64) -> Vec<Message> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.pre_vote_term = None;
        self.pre_vote_granted.clear();
        self.pending_reads.clear();
        self.leader_transfer_target = None;
        self.last_quorum_contact.clear();
        self.last_quorum_contact.insert(self.id, now_ms);
        tracing::info!(node = self.id, term = self.current_term, "became leader");
        self.last_heartbeat_at = now_ms;
        self.log.push(LogEntry {
            term: self.current_term,
            command: Command::Noop,
        });
        self.term_start_index = self.last_log_index();
        let next = self.last_log_index() + 1;
        self.next_index = self.peers.iter().map(|&peer| (peer, next)).collect();
        self.match_index = self.peers.iter().map(|&peer| (peer, 0)).collect();
        self.match_index.insert(self.id, self.last_log_index());
        self.peers
            .iter()
            .map(|&peer| self.append_entries_for(peer))
            .collect()
    }

    fn handle_append_entries(&mut self, request: AppendEntries, now_ms: u64) -> AppendEntriesReply {
        if request.term < self.current_term {
            tracing::info!(
                node = self.id,
                term = self.current_term,
                leader_term = request.term,
                "append entries rejected: stale term"
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        if request.term > self.current_term || self.role != Role::Follower {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = request.term,
                "step down for append entries"
            );
            self.step_down(request.term, now_ms);
        }
        self.leader_id = Some(request.leader_id);
        self.last_leader_contact = now_ms;
        self.reset_election_timer(now_ms);
        if request.prev_log_index < self.log_base_index {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            tracing::info!(
                node = self.id,
                term = self.current_term,
                prev_log_index = request.prev_log_index,
                "append entries rejected: log mismatch"
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        let mut index = request.prev_log_index + 1;
        for entry in request.entries {
            if self.term_at(index).is_some_and(|term| term != entry.term) {
                self.truncate_from(index);
            }
            if self.term_at(index).is_none() {
                self.log.push(entry);
            }
            index += 1;
        }
        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.last_log_index());
        }
        let _ = self.apply_committed();
        tracing::info!(
            node = self.id,
            term = self.current_term,
            match_index = index - 1,
            "append entries accepted"
        );
        AppendEntriesReply {
            term: self.current_term,
            success: true,
            match_index: index - 1,
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        from: NodeId,
        reply: AppendEntriesReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = reply.term,
                "leader step down for append reply"
            );
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Leader
            || reply.term != self.current_term
            || !self.peers.contains(&from)
        {
            return Vec::new();
        }
        self.last_quorum_contact.insert(from, now_ms);
        if reply.success {
            // Replies may be duplicated or reordered by the transport; never
            // let a stale ack regress a peer's match point.
            let matched = self
                .match_index
                .get(&from)
                .copied()
                .unwrap_or(0)
                .max(reply.match_index);
            self.match_index.insert(from, matched);
            self.next_index.insert(from, matched + 1);
            self.advance_commit_index();
            if self.leader_transfer_target == Some(from) && matched >= self.last_log_index() {
                self.leader_transfer_target = None;
                vec![self.timeout_now_for(from)]
            } else {
                Vec::new()
            }
        } else {
            tracing::info!(
                node = self.id,
                peer = from,
                term = self.current_term,
                "append entries rejected by follower"
            );
            let next = self
                .next_index
                .get(&from)
                .copied()
                .unwrap_or(self.last_log_index() + 1)
                .saturating_sub(1)
                .max(1);
            self.next_index.insert(from, next);
            vec![self.append_entries_for(from)]
        }
    }

    fn append_entries_for(&self, peer: NodeId) -> Message {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.last_log_index() + 1);
        if next <= self.log_base_index
            && let Some(snapshot) = self.snapshot.clone()
        {
            if let Ok(bytes) = bincode::serialize(&snapshot)
                && bytes.len() > SNAPSHOT_CHUNK_BYTES
            {
                return self.snapshot_chunk_for(peer, snapshot, 0, bytes);
            }
            return Message {
                from: self.id,
                to: peer,
                rpc: Rpc::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: self.current_term,
                    leader_id: self.id,
                    snapshot,
                }),
            };
        }
        let prev_log_index = next.saturating_sub(1);
        let entries = self
            .entries_from(next)
            .iter()
            .take(MAX_APPEND_ENTRIES)
            .cloned()
            .collect();
        Message {
            from: self.id,
            to: peer,
            rpc: Rpc::AppendEntries(AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term: self.term_at(prev_log_index).unwrap_or(0),
                entries,
                leader_commit: self.commit_index,
            }),
        }
    }

    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        snapshot: Snapshot,
        now_ms: u64,
    ) -> InstallSnapshotReply {
        let index = snapshot.last_included_index;
        let already_have_boundary = self.log_base_index == index
            && self.term_at(index) == Some(snapshot.last_included_term);
        let result = self.install_snapshot(snapshot);
        if matches!(result, Ok(SnapshotInstallResult::Installed)) {
            self.leader_id = Some(from);
            self.last_leader_contact = now_ms;
            self.reset_election_timer(now_ms);
        }
        InstallSnapshotReply {
            term: self.current_term,
            accepted: matches!(result, Ok(SnapshotInstallResult::Installed))
                || (already_have_boundary && matches!(result, Ok(SnapshotInstallResult::Stale))),
            last_included_index: index,
        }
    }

    fn handle_install_snapshot_request(
        &mut self,
        from: NodeId,
        request: InstallSnapshotRequest,
        now_ms: u64,
    ) -> InstallSnapshotReply {
        if request.term < self.current_term {
            return InstallSnapshotReply {
                term: self.current_term,
                accepted: false,
                last_included_index: request.snapshot.last_included_index,
            };
        }
        if request.term > self.current_term || self.role != Role::Follower {
            self.step_down(request.term, now_ms);
        }
        if request.leader_id != from {
            return InstallSnapshotReply {
                term: self.current_term,
                accepted: false,
                last_included_index: request.snapshot.last_included_index,
            };
        }
        self.handle_install_snapshot(from, request.snapshot, now_ms)
    }

    fn handle_install_snapshot_reply(
        &mut self,
        from: NodeId,
        reply: InstallSnapshotReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Leader || reply.term != self.current_term || !reply.accepted {
            return Vec::new();
        }
        self.last_quorum_contact.insert(from, now_ms);
        let matched = self
            .match_index
            .get(&from)
            .copied()
            .unwrap_or(0)
            .max(reply.last_included_index);
        self.match_index.insert(from, matched);
        self.next_index.insert(from, matched + 1);
        vec![self.append_entries_for(from)]
    }

    fn handle_read_index(
        &mut self,
        from: NodeId,
        request: ReadIndex,
        now_ms: u64,
    ) -> ReadIndexReply {
        if request.term >= self.current_term && request.leader_id == from {
            if request.term > self.current_term || self.role != Role::Follower {
                self.step_down(request.term, now_ms);
            }
            self.leader_id = Some(request.leader_id);
            self.last_leader_contact = now_ms;
            self.reset_election_timer(now_ms);
        }
        ReadIndexReply {
            term: self.current_term,
            request_id: request.request_id,
            applied_index: self.last_applied,
        }
    }

    fn handle_read_index_reply(
        &mut self,
        from: NodeId,
        reply: ReadIndexReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Leader
            || reply.term != self.current_term
            || !self.peers.contains(&from)
        {
            return Vec::new();
        }
        self.last_quorum_contact.insert(from, now_ms);
        if let Some(state) = self.pending_reads.get_mut(&reply.request_id) {
            // The reply is a quorum acknowledgement of the leader's current
            // term. The leader itself waits for its local apply index to reach
            // the captured barrier; a follower need not have applied that
            // index to prove it heard this leader.
            if state.term == self.current_term {
                state.acknowledgements.insert(from);
            }
        }
        Vec::new()
    }

    fn handle_timeout_now(
        &mut self,
        from: NodeId,
        request: TimeoutNow,
        now_ms: u64,
    ) -> Vec<Message> {
        if request.term < self.current_term {
            return Vec::new();
        }
        if request.leader_id != from {
            return Vec::new();
        }
        if request.term > self.current_term || self.role != Role::Follower {
            self.step_down(request.term, now_ms);
        }
        self.leader_id = None;
        self.start_election(now_ms)
    }

    fn handle_install_snapshot_chunk(
        &mut self,
        from: NodeId,
        chunk: InstallSnapshotChunk,
        now_ms: u64,
    ) -> InstallSnapshotChunkReply {
        let mut reply = InstallSnapshotChunkReply {
            term: self.current_term,
            snapshot_id: chunk.snapshot_id,
            accepted: false,
            next_offset: 0,
        };
        if chunk.term < self.current_term
            || chunk.leader_id != from
            || chunk.total_size > 16 * 1024 * 1024
        {
            return reply;
        }
        if chunk.term > self.current_term || self.role != Role::Follower {
            self.step_down(chunk.term, now_ms);
        }
        let transfer = if chunk.offset == 0 {
            let transfer = PartialSnapshot {
                term: chunk.term,
                leader_id: chunk.leader_id,
                total_size: chunk.total_size,
                bytes: Vec::with_capacity(chunk.total_size),
            };
            self.incoming_snapshot = Some((chunk.snapshot_id, transfer));
            self.incoming_snapshot.as_mut()
        } else {
            self.incoming_snapshot.as_mut()
        };
        let Some((snapshot_id, transfer)) = transfer else {
            return reply;
        };
        if *snapshot_id != chunk.snapshot_id
            || transfer.term != chunk.term
            || transfer.leader_id != chunk.leader_id
            || transfer.total_size != chunk.total_size
            || transfer.bytes.len() != chunk.offset
            || chunk.data.len() > chunk.total_size.saturating_sub(chunk.offset)
        {
            reply.next_offset = transfer.bytes.len();
            return reply;
        }
        transfer.bytes.extend_from_slice(&chunk.data);
        reply.next_offset = transfer.bytes.len();
        reply.accepted = true;
        if chunk.done {
            if transfer.bytes.len() != transfer.total_size {
                reply.accepted = false;
                return reply;
            }
            let bytes = std::mem::take(&mut transfer.bytes);
            let result = bincode::deserialize::<Snapshot>(&bytes)
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
                .and_then(|snapshot| {
                    let result = self.install_snapshot(snapshot)?;
                    if matches!(result, SnapshotInstallResult::Installed) {
                        self.leader_id = Some(from);
                        self.last_leader_contact = now_ms;
                        self.reset_election_timer(now_ms);
                    }
                    Ok(result)
                });
            reply.accepted = result.is_ok();
            self.incoming_snapshot = None;
        } else {
            self.reset_election_timer(now_ms);
        }
        reply.term = self.current_term;
        reply
    }

    fn handle_install_snapshot_chunk_reply(
        &mut self,
        from: NodeId,
        reply: InstallSnapshotChunkReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Leader
            || reply.term != self.current_term
            || !self.peers.contains(&from)
        {
            return Vec::new();
        }
        let Some(snapshot) = self.snapshot.clone() else {
            return Vec::new();
        };
        let Ok(bytes) = bincode::serialize(&snapshot) else {
            return Vec::new();
        };
        if snapshot_id(&snapshot) != reply.snapshot_id {
            return Vec::new();
        }
        self.last_quorum_contact.insert(from, now_ms);
        if !reply.accepted {
            return vec![self.snapshot_chunk_for(from, snapshot, reply.next_offset, bytes)];
        }
        if reply.next_offset < bytes.len() {
            return vec![self.snapshot_chunk_for(from, snapshot, reply.next_offset, bytes)];
        }
        self.match_index.insert(
            from,
            self.log_base_index
                .max(self.match_index.get(&from).copied().unwrap_or(0)),
        );
        self.next_index.insert(from, self.log_base_index + 1);
        vec![self.append_entries_for(from)]
    }

    fn timeout_now_for(&self, target: NodeId) -> Message {
        Message {
            from: self.id,
            to: target,
            rpc: Rpc::TimeoutNow(TimeoutNow {
                term: self.current_term,
                leader_id: self.id,
            }),
        }
    }

    fn has_quorum_contact(&self, now_ms: u64) -> bool {
        let active = self
            .last_quorum_contact
            .values()
            .filter(|&&contact| now_ms.saturating_sub(contact) <= ELECTION_MIN_MS * 2)
            .count();
        active >= self.majority()
    }

    fn advance_commit_index(&mut self) {
        let previous = self.commit_index;
        for index in (self.commit_index + 1)..=self.last_log_index() {
            if self.term_at(index) == Some(self.current_term) {
                let replicated = self
                    .match_index
                    .values()
                    .filter(|&&matched| matched >= index)
                    .count();
                if replicated >= self.majority() {
                    self.commit_index = index;
                }
            }
        }
        if self.commit_index != previous {
            tracing::info!(
                node = self.id,
                term = self.current_term,
                commit_index = self.commit_index,
                "commit index advanced"
            );
        }
        let _ = self.apply_committed();
    }

    fn apply_committed(&mut self) -> std::io::Result<()> {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            let command = self
                .entry_at(next)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "commit index is outside the retained log",
                    )
                })?
                .command
                .clone();
            self.state_machine.apply(next, &command)?;
            self.last_applied = next;
        }
        Ok(())
    }

    fn step_down(&mut self, term: Term, now_ms: u64) {
        if self.role == Role::Leader {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = term,
                "leader stepping down"
            );
        }
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.votes_granted = 0;
        self.granted_voters.clear();
        self.pre_vote_term = None;
        self.pre_vote_granted.clear();
        self.pending_reads.clear();
        self.leader_transfer_target = None;
        self.last_quorum_contact.clear();
        // A stepped-down leader no longer knows the current leader and must
        // not redirect clients to itself.
        self.leader_id = None;
        self.reset_election_timer(now_ms);
    }

    fn reset_election_timer(&mut self, now_ms: u64) {
        self.election_deadline = now_ms + self.next_election_timeout();
    }
    fn next_election_timeout(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        ELECTION_MIN_MS + (self.rng_state % ELECTION_SPAN_MS)
    }
    fn majority(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }
    pub fn last_log_index(&self) -> LogIndex {
        if self.log_base_index == 0 {
            self.log.len()
        } else {
            self.log_base_index + self.log.len().saturating_sub(1)
        }
    }
    fn last_log_term(&self) -> Term {
        self.term_at(self.last_log_index()).unwrap_or(0)
    }
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            Some(0)
        } else if self.log_base_index == 0 {
            self.log.get(index - 1).map(|entry| entry.term)
        } else if index == self.log_base_index {
            self.log.first().map(|entry| entry.term)
        } else if index > self.log_base_index {
            self.log
                .get(index - self.log_base_index)
                .map(|entry| entry.term)
        } else {
            None
        }
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            None
        } else if self.log_base_index == 0 {
            self.log.get(index - 1)
        } else if index > self.log_base_index {
            self.log.get(index - self.log_base_index)
        } else {
            None
        }
    }

    fn entries_from(&self, index: LogIndex) -> &[LogEntry] {
        if self.log_base_index == 0 {
            self.log.get(index.saturating_sub(1)..).unwrap_or(&[])
        } else if index <= self.log_base_index {
            &[]
        } else {
            self.log.get(index - self.log_base_index..).unwrap_or(&[])
        }
    }

    fn entries_after(&self, index: LogIndex) -> &[LogEntry] {
        self.entries_from(index.saturating_add(1))
    }

    fn truncate_from(&mut self, index: LogIndex) {
        if self.log_base_index == 0 {
            self.log.truncate(index.saturating_sub(1));
        } else if index <= self.log_base_index {
            self.log.truncate(1);
        } else {
            self.log.truncate(index - self.log_base_index);
        }
    }
}

fn snapshot_id(snapshot: &Snapshot) -> u64 {
    let bytes = bincode::serialize(snapshot).unwrap_or_default();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn elect(node: &mut Node, now_ms: u64) {
        let _ = node.tick(now_ms);
        assert_eq!(node.role(), Role::Follower);
        for peer in node.peers.clone() {
            let _ = node.handle_message(
                peer,
                Rpc::PreVoteReply(PreVoteReply {
                    term: node.current_term,
                    vote_granted: true,
                }),
                now_ms,
            );
        }
        assert_eq!(node.role(), Role::Candidate);
        for peer in node.peers.clone() {
            let _ = node.handle_message(
                peer,
                Rpc::RequestVoteReply(RequestVoteReply {
                    term: node.current_term,
                    vote_granted: true,
                }),
                now_ms,
            );
        }
        assert_eq!(node.role(), Role::Leader);
    }

    #[test]
    fn duplicate_vote_reply_is_counted_once() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        let _ = node.tick(300);
        for peer in node.peers.clone() {
            let _ = node.handle_message(
                peer,
                Rpc::PreVoteReply(PreVoteReply {
                    term: node.current_term,
                    vote_granted: true,
                }),
                300,
            );
            if node.role() == Role::Candidate {
                break;
            }
        }
        assert_eq!(node.role(), Role::Candidate);

        let _ = node.handle_message(
            1,
            Rpc::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
            300,
        );
        let _ = node.handle_message(
            1,
            Rpc::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
            300,
        );
        assert_eq!(
            node.role(),
            Role::Candidate,
            "a duplicated grant from one peer must not win the election"
        );

        let _ = node.handle_message(
            2,
            Rpc::RequestVoteReply(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
            300,
        );
        assert_eq!(node.role(), Role::Leader);
    }

    #[test]
    fn step_down_resets_election_timer_from_now() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        elect(&mut node, 300);

        let _ = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: 5,
                success: false,
                match_index: 0,
            }),
            1000,
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 5);

        let messages = node.tick(1001);
        assert!(
            messages.is_empty(),
            "a node that just stepped down must wait out the election timeout, not campaign immediately"
        );
    }

    #[test]
    fn step_down_clears_stale_self_leader_hint() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        elect(&mut node, 300);
        assert_eq!(node.leader_id(), Some(0));

        let _ = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: 9,
                success: false,
                match_index: 0,
            }),
            1000,
        );
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(
            node.leader_id(),
            None,
            "a stepped-down leader must not redirect clients to itself"
        );
    }

    #[test]
    fn stale_success_reply_does_not_regress_next_index() {
        let mut node = Node::new(0, vec![1]);
        elect(&mut node, 300);
        let _ = node.start_client_write(ClientRequest::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        });
        let _ = node.start_client_write(ClientRequest::Set {
            key: "k2".to_string(),
            value: "v2".to_string(),
        });
        assert_eq!(node.last_log_index(), 3);

        let _ = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: 1,
                success: true,
                match_index: 3,
            }),
            300,
        );
        let _ = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: 1,
                success: true,
                match_index: 2,
            }),
            300,
        );
        let messages = node.tick(360);
        let heartbeat = messages
            .iter()
            .find_map(|message| match &message.rpc {
                Rpc::AppendEntries(entries) if message.to == 1 => Some(entries),
                _ => None,
            })
            .expect("leader sends a heartbeat to the peer");
        assert_eq!(
            heartbeat.prev_log_index, 3,
            "a delayed duplicate success reply must not regress next_index and re-replicate"
        );
    }

    #[test]
    fn snapshot_install_records_the_sender_as_leader() {
        let mut source = Node::from_parts(
            0,
            vec![1],
            1,
            None,
            vec![LogEntry {
                term: 1,
                command: Command::Set {
                    key: "k".to_string(),
                    value: "v".to_string(),
                },
            }],
            1,
        );
        let snapshot = source.compact_to(1).unwrap();
        let mut node = Node::new(1, vec![0]);
        assert_eq!(node.leader_id(), None);

        let messages = node.handle_message(0, Rpc::InstallSnapshot(snapshot), 500);
        assert_eq!(node.get("k"), Some("v".to_string()));
        assert_eq!(
            node.leader_id(),
            Some(0),
            "a follower that accepted a snapshot from its leader must remember the leader"
        );
        assert!(matches!(messages[0].rpc, Rpc::InstallSnapshotReply(_)));
    }

    #[test]
    fn pre_vote_does_not_advance_term_before_a_majority() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        let messages = node.tick(300);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 0);
        assert!(
            messages
                .iter()
                .all(|message| matches!(message.rpc, Rpc::PreVote(_)))
        );
        let _ = node.handle_message(
            1,
            Rpc::PreVoteReply(PreVoteReply {
                term: 0,
                vote_granted: true,
            }),
            300,
        );
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn read_index_waits_for_quorum_and_current_term_apply() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        elect(&mut node, 300);
        let (read, messages) = node.start_client_read().unwrap();
        assert_eq!(messages.len(), 4);
        assert!(!node.read_committed_and_applied(read));
        for peer in [1, 2] {
            let _ = node.handle_message(
                peer,
                Rpc::ReadIndexReply(ReadIndexReply {
                    term: read.term,
                    request_id: read.id,
                    applied_index: 0,
                }),
                301,
            );
        }
        assert!(!node.read_committed_and_applied(read));
        for peer in [1, 2] {
            let _ = node.handle_message(
                peer,
                Rpc::AppendEntriesReply(AppendEntriesReply {
                    term: read.term,
                    success: true,
                    match_index: 1,
                }),
                302,
            );
        }
        assert!(node.read_committed_and_applied(read));
        node.finish_read(read);
    }

    #[test]
    fn check_quorum_steps_down_after_lost_majority() {
        let mut node = Node::new(0, vec![1, 2, 3, 4]);
        elect(&mut node, 300);
        assert_eq!(node.role(), Role::Leader);
        let _ = node.tick(601);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.leader_id(), None);
    }

    #[test]
    fn leader_transfer_waits_for_target_catch_up_then_sends_timeout_now() {
        let mut node = Node::new(0, vec![1]);
        elect(&mut node, 300);
        let first = node.transfer_leadership(1, 301).unwrap();
        assert!(matches!(first[0].rpc, Rpc::AppendEntries(_)));
        let second = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: node.current_term(),
                success: true,
                match_index: node.last_log_index(),
            }),
            302,
        );
        assert!(matches!(second[0].rpc, Rpc::TimeoutNow(_)));
    }

    #[test]
    fn append_entries_are_chunked_for_large_backlogs() {
        let mut node = Node::new(0, vec![1]);
        elect(&mut node, 300);
        for index in 0..(MAX_APPEND_ENTRIES + 1) {
            let _ = node.start_client_write(ClientRequest::Set {
                key: format!("k{index}"),
                value: "v".to_string(),
            });
        }
        let _ = node.handle_message(
            1,
            Rpc::AppendEntriesReply(AppendEntriesReply {
                term: node.current_term(),
                success: true,
                match_index: 1,
            }),
            301,
        );
        let messages = node.tick(351);
        let entries = messages
            .iter()
            .find_map(|message| match &message.rpc {
                Rpc::AppendEntries(request) => Some(request.entries.len()),
                _ => None,
            })
            .unwrap();
        assert!(entries <= MAX_APPEND_ENTRIES);
    }

    #[test]
    fn interrupted_snapshot_chunks_leave_follower_unchanged_until_final_chunk() {
        let value = "x".repeat(SNAPSHOT_CHUNK_BYTES);
        let mut source = Node::from_parts(
            0,
            vec![1],
            1,
            None,
            vec![
                LogEntry {
                    term: 1,
                    command: Command::Set {
                        key: "large".to_string(),
                        value: value.clone(),
                    },
                },
                LogEntry {
                    term: 1,
                    command: Command::Set {
                        key: "marker".to_string(),
                        value: "done".to_string(),
                    },
                },
            ],
            2,
        );
        source.compact_to(2).unwrap();
        let chunks = source.snapshot_transfer_chunks(1);
        assert!(chunks.len() > 1);
        let mut follower = Node::new(1, vec![0]);
        let first_reply = follower.handle_message(0, chunks[0].rpc.clone(), 10);
        assert_eq!(follower.get("large"), None);
        assert!(matches!(
            first_reply[0].rpc,
            Rpc::InstallSnapshotChunkReply(_)
        ));
        for message in chunks.into_iter().skip(1) {
            let _ = follower.handle_message(0, message.rpc, 10);
        }
        assert_eq!(follower.get("large"), Some(value));
        assert_eq!(follower.get("marker"), Some("done".to_string()));
    }
}
