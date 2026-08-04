use crate::{Command, LogIndex, SNAPSHOT_FORMAT_VERSION, StateSnapshot};
use std::collections::HashMap;
use std::io;

pub trait StateMachine: std::fmt::Debug {
    fn apply(&mut self, index: LogIndex, command: &Command) -> io::Result<()>;
    fn get(&self, key: &str) -> io::Result<Option<String>>;
    fn last_applied(&self) -> LogIndex;

    /// Returns a deterministic, self-contained copy of the applied state.
    /// Implementations that cannot provide atomic state transfer may keep the
    /// default error; the simulator's memory state machine supports it.
    fn snapshot(&self) -> io::Result<StateSnapshot> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "state machine does not support snapshots",
        ))
    }

    /// Replaces the applied state with a validated snapshot. Implementations
    /// must validate before mutating so a rejected snapshot cannot roll back
    /// an already-applied state.
    fn restore_snapshot(&mut self, _snapshot: &StateSnapshot) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "state machine does not support snapshots",
        ))
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStateMachine {
    data: HashMap<String, String>,
    last_applied: LogIndex,
}

impl MemoryStateMachine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateMachine for MemoryStateMachine {
    fn apply(&mut self, index: LogIndex, command: &Command) -> io::Result<()> {
        if index <= self.last_applied {
            return Ok(());
        }
        if index != self.last_applied + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "state machine apply index gap",
            ));
        }
        match command {
            Command::Noop => {}
            Command::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.data.remove(key);
            }
        }
        self.last_applied = index;
        Ok(())
    }

    fn get(&self, key: &str) -> io::Result<Option<String>> {
        Ok(self.data.get(key).cloned())
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    fn snapshot(&self) -> io::Result<StateSnapshot> {
        let mut data: Vec<_> = self
            .data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        data.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(StateSnapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            last_applied: self.last_applied,
            data,
        })
    }

    fn restore_snapshot(&mut self, snapshot: &StateSnapshot) -> io::Result<()> {
        if snapshot.version != SNAPSHOT_FORMAT_VERSION
            || snapshot.data.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid memory state snapshot",
            ));
        }
        let data = snapshot.data.iter().cloned().collect();
        self.data = data;
        self.last_applied = snapshot.last_applied;
        Ok(())
    }
}
