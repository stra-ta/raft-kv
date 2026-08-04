use crate::state_machine::{MemoryStateMachine, StateMachine};
use crate::types::*;
use std::collections::HashMap;

const HEARTBEAT_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_SPAN_MS: u64 = 151;

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
    rng_state: u64,
    votes_granted: usize,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    /// Index of the first log entry in the current term (noop). When commit_index
    /// reaches this, the leader has proved it is still the leader and can serve reads.
    term_start_index: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingWrite {
    pub index: LogIndex,
    pub term: Term,
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
            rng_state: (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            votes_granted: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            term_start_index: 0,
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
            node.state_machine.restore_snapshot(&snapshot.state)?;
            if node.state_machine.last_applied() != snapshot.last_included_index {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "restored state machine index does not match snapshot",
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
                self.last_heartbeat_at = now_ms;
                self.peers
                    .iter()
                    .map(|&peer| self.append_entries_for(peer))
                    .collect()
            }
            Role::Follower | Role::Candidate if now_ms >= self.election_deadline => {
                self.start_election(now_ms)
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
            Rpc::AppendEntriesReply(r) => self.handle_append_entries_reply(from, r),
            Rpc::InstallSnapshot(snapshot) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::InstallSnapshotReply(self.handle_install_snapshot(snapshot, now_ms)),
            }],
            Rpc::InstallSnapshotReply(reply) => self.handle_install_snapshot_reply(from, reply),
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
        if let ClientRequest::Get { key } = &request {
            if !self.can_serve_reads() {
                return (
                    ClientReply {
                        success: false,
                        leader_id: Some(self.id),
                        response: None,
                    },
                    Vec::new(),
                );
            }
            return (
                ClientReply {
                    success: true,
                    leader_id: Some(self.id),
                    response: match self.state_machine.get(key) {
                        Ok(value) => value,
                        Err(_) => {
                            return (
                                ClientReply {
                                    success: false,
                                    leader_id: Some(self.id),
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

    fn start_election(&mut self, now_ms: u64) -> Vec<Message> {
        self.role = Role::Candidate;
        self.current_term += 1;
        tracing::info!(node = self.id, term = self.current_term, "election started");
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_granted = 1;
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
        _from: NodeId,
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
        self.votes_granted += 1;
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
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            tracing::info!(
                node = self.id,
                from_term = self.current_term,
                to_term = reply.term,
                "leader step down for append reply"
            );
            self.step_down(reply.term, 0);
            return Vec::new();
        }
        if self.role != Role::Leader || reply.term != self.current_term {
            return Vec::new();
        }
        if reply.success {
            self.match_index.insert(from, reply.match_index);
            self.next_index.insert(from, reply.match_index + 1);
            self.advance_commit_index();
            Vec::new()
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
            return Message {
                from: self.id,
                to: peer,
                rpc: Rpc::InstallSnapshot(snapshot),
            };
        }
        let prev_log_index = next.saturating_sub(1);
        let entries = self.entries_from(next).to_vec();
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

    fn handle_install_snapshot(&mut self, snapshot: Snapshot, now_ms: u64) -> InstallSnapshotReply {
        let index = snapshot.last_included_index;
        let already_have_boundary = self.log_base_index == index
            && self.term_at(index) == Some(snapshot.last_included_term);
        let result = self.install_snapshot(snapshot);
        if result.is_ok() {
            self.reset_election_timer(now_ms);
        }
        InstallSnapshotReply {
            term: self.current_term,
            accepted: matches!(result, Ok(SnapshotInstallResult::Installed))
                || (already_have_boundary && matches!(result, Ok(SnapshotInstallResult::Stale))),
            last_included_index: index,
        }
    }

    fn handle_install_snapshot_reply(
        &mut self,
        from: NodeId,
        reply: InstallSnapshotReply,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, 0);
            return Vec::new();
        }
        if self.role != Role::Leader || reply.term != self.current_term || !reply.accepted {
            return Vec::new();
        }
        self.match_index.insert(from, reply.last_included_index);
        self.next_index.insert(from, reply.last_included_index + 1);
        vec![self.append_entries_for(from)]
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
