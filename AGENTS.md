# raft-kv engineering guide

## Purpose

raft-kv is the distributed-system target in the stra-ta lab.
It separates Raft safety and liveness rules, durable process state, the in-memory state machine recovered from the durable log, framed transport, and external history checking.

## Invariants

- A write is acknowledged only after majority commit and local state-machine apply.
- A read succeeds only after its ReadIndex quorum barrier is acknowledged and applied in the same leader term.
- Term, vote, log, snapshot boundary, and state-machine recovery survive process restart.
- Stale, partial, corrupt, or unknown-version snapshots fail closed without mutating live state.
- Peer queues are bounded and per-peer message order is FIFO while a connection is healthy.
- Simulator schedules and external histories are evidence with different boundaries.
- Minimized failing schedules become permanent deterministic regressions.

## Verification

Use `./scripts/verify` for the functional and process-level suite.
Use `./scripts/confidence` for formatting, Clippy, all-target tests, and a release build.

For a faster local test loop, `cargo nextest run` runs the same test targets in parallel with per-test process isolation.
Nextest does not run doctests, so keep `cargo test --doc` alongside it.
The scripts above remain the authority for verification and evidence.

## Lab-wide contracts

- See https://github.com/stra-ta/.github/blob/main/LAB_RULES.md and https://github.com/stra-ta/.github/blob/main/EVIDENCE.md and https://github.com/stra-ta/.github/blob/main/COMPATIBILITY.md for lab-wide naming, evidence, and schema contracts.
- Per https://github.com/stra-ta/.github/blob/main/CONTRIBUTING.md, contributions require the target repo's AGENTS.md, README, and relevant design note, preserve repo boundaries, add the narrowest regression test, run one-command verification, and keep performance claims tied to committed manifests.
