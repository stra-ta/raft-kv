use crate::history::OperationHistory;
use crate::node::Node;
use crate::types::*;
use std::collections::{BTreeMap, HashSet};

const CLIENT_WRITE_TIMEOUT_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleAction {
    Stop,
    Restart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledFault {
    pub at_ms: u64,
    pub node: NodeId,
    pub action: LifecycleAction,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    pub seed: u64,
    pub max_delay_ms: u64,
    pub drop_rate_per_mille: u16,
    pub duplicate_rate_per_mille: u16,
    pub reorder_window: usize,
    pub lifecycle: Vec<ScheduledFault>,
}

impl FaultPlan {
    pub fn seeded(seed: u64) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }

    pub fn with_max_delay_ms(mut self, max_delay_ms: u64) -> Self {
        self.max_delay_ms = max_delay_ms;
        self
    }

    pub fn with_drop_rate_per_mille(mut self, rate: u16) -> Self {
        self.drop_rate_per_mille = rate.min(1_000);
        self
    }

    pub fn with_duplicate_rate_per_mille(mut self, rate: u16) -> Self {
        self.duplicate_rate_per_mille = rate.min(1_000);
        self
    }

    pub fn with_reorder_window(mut self, window: usize) -> Self {
        self.reorder_window = window;
        self
    }

    pub fn stop_at(mut self, at_ms: u64, node: NodeId) -> Self {
        self.lifecycle.push(ScheduledFault {
            at_ms,
            node,
            action: LifecycleAction::Stop,
        });
        self.lifecycle.sort_by_key(|fault| fault.at_ms);
        self
    }

    pub fn restart_at(mut self, at_ms: u64, node: NodeId) -> Self {
        self.lifecycle.push(ScheduledFault {
            at_ms,
            node,
            action: LifecycleAction::Restart,
        });
        self.lifecycle.sort_by_key(|fault| fault.at_ms);
        self
    }
}

#[derive(Clone, Debug)]
struct QueuedMessage {
    deliver_at: u64,
    sequence: u64,
    message: Message,
}

#[derive(Debug)]
pub struct Cluster {
    // Node iteration is part of the simulator's event order. Keep it sorted
    // so the same fault-plan seed produces the same run across processes.
    nodes: BTreeMap<NodeId, Node>,
    messages: Vec<QueuedMessage>,
    now_ms: u64,
    stopped: BTreeMap<NodeId, bool>,
    blocked: HashSet<(NodeId, NodeId)>,
    fault_plan: FaultPlan,
    rng_state: u64,
    next_fault: usize,
    next_message_sequence: u64,
    history: OperationHistory,
    next_operation_id: u64,
}

impl Cluster {
    pub fn new(size: usize) -> Self {
        Self::with_fault_plan(size, FaultPlan::default())
    }

    pub fn with_fault_plan(size: usize, fault_plan: FaultPlan) -> Self {
        let ids: Vec<_> = (0..size).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&peer| peer != id).collect();
                (id, Node::new(id, peers))
            })
            .collect();
        Self {
            nodes,
            messages: Vec::new(),
            now_ms: 0,
            stopped: BTreeMap::new(),
            blocked: HashSet::new(),
            rng_state: fault_plan.seed,
            fault_plan,
            next_fault: 0,
            next_message_sequence: 0,
            history: OperationHistory::new(),
            next_operation_id: 0,
        }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[&id]
    }

    #[cfg(test)]
    pub(crate) fn node_mut_for_test(&mut self, id: NodeId) -> &mut Node {
        self.nodes.get_mut(&id).unwrap()
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.nodes.iter().map(|(&id, node)| (id, node))
    }

    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> {
        self.nodes.keys().copied()
    }

    pub fn stop(&mut self, id: NodeId) {
        self.stopped.insert(id, true);
    }

    pub fn restart(&mut self, id: NodeId) {
        self.stopped.insert(id, false);
    }

    pub fn is_stopped(&self, id: NodeId) -> bool {
        self.is_stopped_internal(id)
    }

    pub fn compact_node(&mut self, id: NodeId, index: LogIndex) -> std::io::Result<Snapshot> {
        self.nodes
            .get_mut(&id)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "unknown cluster node")
            })?
            .compact_to(index)
    }

    pub fn fault_plan(&self) -> &FaultPlan {
        &self.fault_plan
    }

    pub fn queued_message_count(&self) -> usize {
        self.messages.len()
    }

    pub fn history(&self) -> &OperationHistory {
        &self.history
    }

    pub fn take_history(&mut self) -> OperationHistory {
        std::mem::take(&mut self.history)
    }

    pub fn clear_history(&mut self) {
        self.history.clear();
    }

    pub fn partition(&mut self, groups: &[Vec<NodeId>]) {
        self.blocked.clear();
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for &from in &ids {
            for &to in &ids {
                if from == to {
                    continue;
                }
                let connected = groups
                    .iter()
                    .any(|group| group.contains(&from) && group.contains(&to));
                if !connected {
                    self.blocked.insert((from, to));
                }
            }
        }
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    pub fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<_> = self
            .nodes
            .values()
            .filter(|node| !self.is_stopped_internal(node.id()) && node.role() == Role::Leader)
            .map(|node| node.id())
            .collect();
        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    pub fn propose(&mut self, leader: NodeId, request: ClientRequest) -> ClientReply {
        self.propose_as(0, leader, request)
    }

    pub fn propose_as(
        &mut self,
        client_id: u64,
        leader: NodeId,
        request: ClientRequest,
    ) -> ClientReply {
        let operation_id = self.next_operation_id;
        self.next_operation_id += 1;
        let invoked_at = self.now_ms;
        let reply = self.propose_unrecorded(leader, request.clone());
        self.history.record(
            operation_id,
            client_id,
            invoked_at,
            self.now_ms,
            request,
            reply.clone(),
        );
        reply
    }

    fn propose_unrecorded(&mut self, leader: NodeId, request: ClientRequest) -> ClientReply {
        if !self.nodes.contains_key(&leader) {
            return ClientReply {
                success: false,
                leader_id: None,
                response: None,
            };
        }
        if self.is_stopped_internal(leader) {
            return ClientReply {
                success: false,
                leader_id: None,
                response: None,
            };
        }
        if matches!(
            request,
            ClientRequest::Set { .. } | ClientRequest::Delete { .. }
        ) {
            return self.propose_write(leader, request);
        }
        let (reply, messages) = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .handle_client_request(request);
        self.enqueue(messages);
        reply
    }

    fn propose_write(&mut self, leader: NodeId, request: ClientRequest) -> ClientReply {
        let (write, messages) = match self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .start_client_write(request)
        {
            Ok(write) => write,
            Err(reply) => return reply,
        };
        self.enqueue(messages);
        let deadline = self.now_ms + CLIENT_WRITE_TIMEOUT_MS;
        while self.now_ms <= deadline {
            let node = self.node(leader);
            if node.write_committed_and_applied(write) {
                return ClientReply {
                    success: true,
                    leader_id: Some(leader),
                    response: Some("committed".to_string()),
                };
            }
            if node.role() != Role::Leader
                || node.current_term() != write.term
                || self.is_stopped_internal(leader)
            {
                return ClientReply {
                    success: false,
                    leader_id: self.leader(),
                    response: None,
                };
            }
            self.step();
        }
        ClientReply {
            success: false,
            leader_id: self.node(leader).leader_id(),
            response: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn start_write_for_test(
        &mut self,
        leader: NodeId,
        request: ClientRequest,
    ) -> Result<crate::PendingWrite, ClientReply> {
        let (write, messages) = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .start_client_write(request)?;
        self.enqueue(messages);
        Ok(write)
    }

    pub fn run_until<F>(&mut self, deadline_ms: u64, mut predicate: F) -> bool
    where
        F: FnMut(&Self) -> bool,
    {
        while self.now_ms <= deadline_ms {
            if predicate(self) {
                return true;
            }
            self.step();
        }
        predicate(self)
    }

    pub fn run_for(&mut self, duration_ms: u64) {
        let deadline = self.now_ms + duration_ms;
        while self.now_ms <= deadline {
            self.step();
        }
    }

    pub fn now(&self) -> u64 {
        self.now_ms
    }

    fn step(&mut self) {
        self.apply_scheduled_faults();
        if let Some(index) = self.next_ready_message_index() {
            let message = self.messages.remove(index).message;
            if self.is_stopped_internal(message.from)
                || self.is_stopped_internal(message.to)
                || self.is_blocked(message.from, message.to)
            {
                return;
            }
            let replies = self.nodes.get_mut(&message.to).unwrap().handle_message(
                message.from,
                message.rpc,
                self.now_ms,
            );
            self.enqueue(replies);
            return;
        }
        self.now_ms += 1;
        self.apply_scheduled_faults();
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.is_stopped_internal(id) {
                continue;
            }
            let messages = self.nodes.get_mut(&id).unwrap().tick(self.now_ms);
            self.enqueue(messages);
        }
    }

    fn enqueue(&mut self, messages: Vec<Message>) {
        for message in messages {
            if !self.is_stopped_internal(message.from)
                && !self.is_stopped_internal(message.to)
                && !self.is_blocked(message.from, message.to)
            {
                if self.chance(self.fault_plan.drop_rate_per_mille) {
                    continue;
                }
                let delay = self.random_delay();
                self.push_message(message.clone(), delay);
                if self.chance(self.fault_plan.duplicate_rate_per_mille) {
                    let duplicate_delay = delay.saturating_add(self.random_delay());
                    self.push_message(message, duplicate_delay);
                }
            }
        }
    }

    fn push_message(&mut self, message: Message, delay: u64) {
        let sequence = self.next_message_sequence;
        self.next_message_sequence += 1;
        self.messages.push(QueuedMessage {
            deliver_at: self.now_ms.saturating_add(delay),
            sequence,
            message,
        });
    }

    fn next_ready_message_index(&mut self) -> Option<usize> {
        let mut ready: Vec<_> = self
            .messages
            .iter()
            .enumerate()
            .filter_map(|(index, queued)| (queued.deliver_at <= self.now_ms).then_some(index))
            .collect();
        if ready.is_empty() {
            return None;
        }
        ready.sort_by_key(|&index| {
            let queued = &self.messages[index];
            (queued.deliver_at, queued.sequence)
        });
        let window = self.fault_plan.reorder_window.max(1).min(ready.len());
        let offset = if window == 1 {
            0
        } else {
            (self.next_random() as usize) % window
        };
        Some(ready[offset])
    }

    fn apply_scheduled_faults(&mut self) {
        while let Some(fault) = self.fault_plan.lifecycle.get(self.next_fault).copied() {
            if fault.at_ms > self.now_ms {
                break;
            }
            match fault.action {
                LifecycleAction::Stop => self.stop(fault.node),
                LifecycleAction::Restart => self.restart(fault.node),
            }
            self.next_fault += 1;
        }
    }

    fn random_delay(&mut self) -> u64 {
        if self.fault_plan.max_delay_ms == 0 {
            0
        } else {
            self.next_random() % (self.fault_plan.max_delay_ms + 1)
        }
    }

    fn chance(&mut self, rate_per_mille: u16) -> bool {
        rate_per_mille != 0 && (self.next_random() % 1_000) < u64::from(rate_per_mille)
    }

    fn next_random(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        self.rng_state
    }

    fn is_stopped_internal(&self, id: NodeId) -> bool {
        self.stopped.get(&id).copied().unwrap_or(false)
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to))
    }
}
