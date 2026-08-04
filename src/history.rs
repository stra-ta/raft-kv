use crate::{ClientReply, ClientRequest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const MAX_CHECKED_OPERATIONS: usize = 64;
const MAX_SEARCH_STATES: usize = 100_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistoryOperation {
    pub id: u64,
    pub client_id: u64,
    pub invoked_at: u64,
    pub completed_at: u64,
    pub request: ClientRequest,
    pub response: ClientReply,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationHistory {
    operations: Vec<HistoryOperation>,
}

impl OperationHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &mut self,
        id: u64,
        client_id: u64,
        invoked_at: u64,
        completed_at: u64,
        request: ClientRequest,
        response: ClientReply,
    ) {
        self.operations.push(HistoryOperation {
            id,
            client_id,
            invoked_at,
            completed_at,
            request,
            response,
        });
    }

    pub fn push(&mut self, operation: HistoryOperation) {
        self.operations.push(operation);
    }

    pub fn operations(&self) -> &[HistoryOperation] {
        &self.operations
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn clear(&mut self) {
        self.operations.clear();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinearizabilityViolation {
    /// A greedily minimized set of successful operations that still fails the
    /// checker. Operations keep their original IDs and timestamps so the
    /// counterexample can be replayed.
    pub counterexample: Vec<HistoryOperation>,
    pub reason: String,
}

impl fmt::Display for LinearizabilityViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({} operations)",
            self.reason,
            self.counterexample.len()
        )
    }
}

impl std::error::Error for LinearizabilityViolation {}

/// Checks successful leader-backed operations for a legal sequential history.
///
/// `LocalGet` is intentionally excluded: it is a local, potentially stale
/// observation and therefore does not have the strong read contract that a
/// leader-backed `Get` has. It remains in the recorded history for diagnostics.
///
/// The history's insertion order breaks ties at equal simulator timestamps.
/// This preserves the real-time order of synchronous calls without adding a
/// field to the public operation format. Histories and search states are
/// bounded; exceeding either limit returns an error rather than claiming that
/// an unchecked history is linearizable.
pub fn check_linearizable(history: &OperationHistory) -> Result<(), LinearizabilityViolation> {
    let mut operations = Vec::new();
    for (sequence, operation) in history.operations.iter().enumerate() {
        if !operation.response.success
            || matches!(operation.request, ClientRequest::LocalGet { .. })
        {
            continue;
        }
        if operations.len() == MAX_CHECKED_OPERATIONS {
            return Err(LinearizabilityViolation {
                counterexample: Vec::new(),
                reason: format!(
                    "history exceeds the deterministic checker limit of {MAX_CHECKED_OPERATIONS} successful operations"
                ),
            });
        }
        operations.push(SequencedOperation {
            sequence,
            operation: operation.clone(),
        });
    }
    operations.sort_by_key(|entry| {
        (
            entry.operation.invoked_at,
            entry.operation.completed_at,
            entry.sequence,
        )
    });
    match search_history(&operations) {
        SearchResult::Linearizable => return Ok(()),
        SearchResult::LimitExceeded => return Err(search_limit_error(&operations)),
        SearchResult::NotLinearizable => {}
    }

    // Delta-debug the failing history one operation at a time. This is small,
    // deterministic, and produces a useful replay even when a large stress
    // run discovers the issue.
    let mut minimized = operations;
    let mut index = 0;
    while index < minimized.len() {
        let mut candidate = minimized.clone();
        candidate.remove(index);
        match search_history(&candidate) {
            SearchResult::NotLinearizable => minimized = candidate,
            SearchResult::Linearizable | SearchResult::LimitExceeded => index += 1,
        }
    }
    Err(LinearizabilityViolation {
        counterexample: minimized.into_iter().map(|entry| entry.operation).collect(),
        reason: "no legal sequential order matches the successful operations".to_string(),
    })
}

#[derive(Clone, Debug)]
struct SequencedOperation {
    sequence: usize,
    operation: HistoryOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SearchResult {
    Linearizable,
    NotLinearizable,
    LimitExceeded,
}

fn search_history(operations: &[SequencedOperation]) -> SearchResult {
    let remaining: Vec<_> = (0..operations.len()).collect();
    let mut rejected_states = BTreeSet::new();
    let mut explored_states = 0;
    search(
        operations,
        &remaining,
        &BTreeMap::new(),
        &mut rejected_states,
        &mut explored_states,
    )
}

fn search(
    operations: &[SequencedOperation],
    remaining: &[usize],
    state: &BTreeMap<String, String>,
    rejected_states: &mut BTreeSet<(Vec<usize>, BTreeMap<String, String>)>,
    explored_states: &mut usize,
) -> SearchResult {
    if remaining.is_empty() {
        return SearchResult::Linearizable;
    }
    let search_state = (remaining.to_vec(), state.clone());
    if rejected_states.contains(&search_state) {
        return SearchResult::NotLinearizable;
    }
    if *explored_states == MAX_SEARCH_STATES {
        return SearchResult::LimitExceeded;
    }
    *explored_states += 1;

    for (position, &index) in remaining.iter().enumerate() {
        let operation = &operations[index];
        let has_predecessor = remaining.iter().any(|&other_index| {
            other_index != index && must_precede(&operations[other_index], operation)
        });
        if has_predecessor {
            continue;
        }
        let Some(next_state) = apply(&operation.operation, state) else {
            continue;
        };
        let mut next_remaining = remaining.to_vec();
        next_remaining.remove(position);
        match search(
            operations,
            &next_remaining,
            &next_state,
            rejected_states,
            explored_states,
        ) {
            SearchResult::Linearizable => return SearchResult::Linearizable,
            SearchResult::LimitExceeded => return SearchResult::LimitExceeded,
            SearchResult::NotLinearizable => {}
        }
    }
    rejected_states.insert(search_state);
    SearchResult::NotLinearizable
}

fn must_precede(left: &SequencedOperation, right: &SequencedOperation) -> bool {
    left.operation.completed_at < right.operation.invoked_at
        || (left.operation.completed_at == right.operation.invoked_at
            && left.sequence < right.sequence)
}

fn search_limit_error(operations: &[SequencedOperation]) -> LinearizabilityViolation {
    LinearizabilityViolation {
        counterexample: operations
            .iter()
            .map(|entry| entry.operation.clone())
            .collect(),
        reason: format!(
            "linearizability search exceeded the deterministic limit of {MAX_SEARCH_STATES} states"
        ),
    }
}

fn apply(
    operation: &HistoryOperation,
    state: &BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    let mut next = state.clone();
    match &operation.request {
        ClientRequest::Get { key } | ClientRequest::LocalGet { key } => {
            let expected = next.get(key).cloned();
            if operation.response.response == expected {
                Some(next)
            } else {
                None
            }
        }
        ClientRequest::Set { key, value } => {
            next.insert(key.clone(), value.clone());
            Some(next)
        }
        ClientRequest::Delete { key } => {
            next.remove(key);
            Some(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(response: Option<&str>) -> ClientReply {
        ClientReply {
            success: true,
            leader_id: Some(0),
            response: response.map(str::to_string),
        }
    }

    fn set(id: u64, value: &str, start: u64, end: u64) -> HistoryOperation {
        HistoryOperation {
            id,
            client_id: id,
            invoked_at: start,
            completed_at: end,
            request: ClientRequest::Set {
                key: "key".to_string(),
                value: value.to_string(),
            },
            response: reply(Some("committed")),
        }
    }

    #[test]
    fn accepts_overlapping_operations_in_a_legal_order() {
        let mut history = OperationHistory::new();
        history.push(set(1, "one", 0, 10));
        history.push(set(2, "two", 1, 5));
        history.push(HistoryOperation {
            id: 3,
            client_id: 3,
            invoked_at: 6,
            completed_at: 8,
            request: ClientRequest::Get {
                key: "key".to_string(),
            },
            response: reply(Some("one")),
        });
        assert!(check_linearizable(&history).is_ok());
    }

    #[test]
    fn returns_a_minimized_counterexample_for_a_stale_read() {
        let mut history = OperationHistory::new();
        history.push(set(1, "one", 0, 2));
        history.push(HistoryOperation {
            id: 2,
            client_id: 2,
            invoked_at: 3,
            completed_at: 4,
            request: ClientRequest::Get {
                key: "key".to_string(),
            },
            response: reply(None),
        });
        history.push(set(3, "noise", 0, 100));

        let violation = check_linearizable(&history).expect_err("history should fail");
        assert_eq!(violation.counterexample.len(), 2);
        assert_eq!(violation.counterexample[0].id, 1);
        assert_eq!(violation.counterexample[1].id, 2);
    }

    #[test]
    fn accepts_instantaneous_operations_at_the_same_timestamp() {
        let mut history = OperationHistory::new();
        history.push(set(1, "one", 10, 10));
        history.push(set(2, "two", 10, 10));
        history.push(HistoryOperation {
            id: 3,
            client_id: 3,
            invoked_at: 10,
            completed_at: 10,
            request: ClientRequest::Get {
                key: "key".to_string(),
            },
            response: reply(Some("two")),
        });

        assert!(check_linearizable(&history).is_ok());
    }

    #[test]
    fn rejects_stale_read_after_equal_timestamp_write_by_recording_order() {
        let mut history = OperationHistory::new();
        history.push(set(1, "one", 10, 10));
        history.push(HistoryOperation {
            id: 2,
            client_id: 2,
            invoked_at: 10,
            completed_at: 10,
            request: ClientRequest::Get {
                key: "key".to_string(),
            },
            response: reply(None),
        });

        assert!(check_linearizable(&history).is_err());
    }

    #[test]
    fn ignores_stale_local_gets_in_the_strong_checker() {
        let mut history = OperationHistory::new();
        history.push(set(1, "one", 0, 1));
        history.push(HistoryOperation {
            id: 2,
            client_id: 2,
            invoked_at: 2,
            completed_at: 3,
            request: ClientRequest::LocalGet {
                key: "key".to_string(),
            },
            response: reply(None),
        });

        assert!(check_linearizable(&history).is_ok());
    }
}
