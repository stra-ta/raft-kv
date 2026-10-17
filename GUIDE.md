# raft-kv

[![CI](https://github.com/stra-ta/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/stra-ta/raft-kv/actions/workflows/ci.yml)

A learning implementation of the Raft consensus algorithm in Rust, with a deterministic simulator and a real process runner over raw TCP. Not a production database.

## Requirements

Stable Rust with Rust 2024 edition support.

## Quickstart

Start a three-node cluster in separate terminals:

```bash
cargo run --bin raft-node -- 0 ./data/node0.bin \
  0=127.0.0.1:5000 1=127.0.0.1:5001 2=127.0.0.1:5002

cargo run --bin raft-node -- 1 ./data/node1.bin \
  0=127.0.0.1:5000 1=127.0.0.1:5001 2=127.0.0.1:5002

cargo run --bin raft-node -- 2 ./data/node2.bin \
  0=127.0.0.1:5000 1=127.0.0.1:5001 2=127.0.0.1:5002
```

Write and read a key:

```bash
cargo run --bin raft-client -- 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 set foo bar
cargo run --bin raft-client -- 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 get foo
```

## What this is

A from-scratch Raft core that understands leader election, log replication, majority commit, and crash recovery. It can run in a deterministic discrete-event simulator (fake time, in-memory messages, injectable partitions and stops) or as real TCP processes with bincode framing and atomic disk persistence.

## Features

- 3–5 node clusters
- leader election with randomized 150–300 ms election timeouts
- 50 ms heartbeats
- client retries supplied peer addresses until one accepts the request
- replicated, committed writes through the leader
- committed-state reads through the leader (reads blocked until leader proves current term)
- log replication with majority commit
- leader backtracking via `next_index`
- deterministic partition/failover tests (simulator)
- seeded simulator fault plans with delay, drop, duplicate, reorder, and lifecycle faults
- operation histories with a deterministic linearizability checker and minimized counterexamples
- length-prefixed `bincode` frames over raw TCP
- batched outbound messages per peer over one persistent TCP connection per destination while that peer is healthy, reconnecting with backoff after failures
- atomic persistence: term, vote, log, and commit index via temp-file fsync + rename
- committed commands apply into an in-memory state machine recovered from the durable log and validated snapshots
- opt-in versioned snapshots, install-snapshot transfer, and log-prefix compaction
- process-level kill/restart and full-cluster restart integration tests

## Current limitations

- snapshots and log compaction are explicit in the simulator (`compact_node`) but automatic on real nodes: `persist_if_changed` compacts to the last applied index once the applied-since-snapshot gap reaches `RAFT_KV_SNAPSHOT_THRESHOLD` (default 64; see dissertation Section 6.4)
- simulator `stop`/`restart` faults pause and resume the same in-memory node; durable crash recovery is covered separately by the process-level TCP tests
- no dynamic membership changes
- no TLS or authentication
- reads run a ReadIndex quorum barrier: the leader waits for a majority to acknowledge its term and for the barrier index to commit and apply in that term before serving the read (see dissertation Section 6.4)
- peer connections persist while healthy; bounded per-peer queues defer overflow to the next heartbeat instead of blocking the Raft thread
- point-read/write key-value API only; no range scans or column families

## Shape of the system

![Raft request and replication flow](docs/system-shape.svg)

## Failover story

The simulator records the path from a steady leader to crash, election, and recovery.

The generated timeline ([`docs/failover.svg`](docs/failover.svg)) and the explanatory story show different simulator traces, not one canonical node-by-node sequence.

![Failover story](docs/failover-story.svg)

![Election timeline](docs/election.svg)

## Replicated log

![Replicated log ledger](docs/log-ledger.svg)

## Replication snapshot

After `set foo bar` commits:

<!-- replication:start -->
| node | role | term | commit | applied | log | kv |
|---:|---|---:|---:|---:|---|---|
| 0 | Follower | 1 | 2 | 2 | [noop, set foo=bar] | foo=bar |
| 1 | Follower | 1 | 2 | 2 | [noop, set foo=bar] | foo=bar |
| 2 | Follower | 1 | 2 | 2 | [noop, set foo=bar] | foo=bar |
| 3 | Follower | 1 | 2 | 2 | [noop, set foo=bar] | foo=bar |
| 4 | Leader | 1 | 2 | 2 | [noop, set foo=bar] | foo=bar |
<!-- replication:end -->

## Metrics

Each `raft-node` process optionally exposes a small unauthenticated Prometheus endpoint at `/metrics` with `RAFT_KV_METRICS_ADDR`.

The endpoint exports Raft term, role, commit/apply indexes, elections, leader changes, client writes whose handlers observed leader-side commit/apply, and leader-side replication lag. `raft_write_latency_seconds` measures handler-observed write latency, not every Raft commit. Metrics use the `prometheus` crate directly because the project only needs one local registry per node and Prometheus text output.

Structured logs use `tracing`: human-readable by default, or JSON with `RAFT_KV_LOG=json`.

## Measured simulator behavior

All timings are simulator time (not real wall-clock). These are not real distributed-system performance numbers.

<!-- metrics:start -->
| metric | value |
|---|---:|
| cluster size tested | 5 nodes |
| election timeout | 150–300 ms |
| heartbeat interval | 50 ms |
| first leader elected | 176 ms simulated |
| failover after leader kill | 244 ms simulated |
| write visible on all nodes | 52 ms simulated |
| fault tolerance | 2 failed nodes in a 5-node cluster |
| process-level TCP tests | 3 integration tests |
<!-- metrics:end -->

## Run it

Start three nodes in separate terminals as in the quickstart, then write and read (the client retries the supplied peer addresses until one accepts the request):

```bash
cargo run --bin raft-client -- 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 set foo bar
cargo run --bin raft-client -- 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 get foo
cargo run --bin raft-client -- 127.0.0.1:5000 127.0.0.1:5001 127.0.0.1:5002 delete foo
```

Kill a node and restart it into the same state file to watch recovery; the process-level integration tests do the same thing programmatically, including a full-cluster crash and restart.

## State machine

The replicated state machine sits behind a `StateMachine` trait. The default state machine applies committed `set`/`delete` commands to an in-memory map. Durability comes from the Raft log itself: a restarted node loads its persisted term, vote, log, and commit index, then re-applies committed entries through the same path, and restores from a validated snapshot when one exists. Stale, partial, corrupt, or unknown-version snapshots fail closed without mutating live state.

## Test it

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

The simulator includes Rust equivalents of the 6.824 tests: `TestInitialElection`, `TestReElection`, `TestBasicAgree`, `TestFailAgree`, `TestFailNoAgree`, `TestConcurrentStarts`, and `TestRejoin`.

The suite also covers persistence, frame encoding, metrics, and real TCP process integration tests including leader kill/restart and full-cluster crash recovery.

## Documentation

- [`docs/replication.md`](docs/replication.md): replicated-log and snapshot details.
- [`docs/election.svg`](docs/election.svg): election timeline.
- [`docs/failover.svg`](docs/failover.svg): deterministic failover flow.
- [`docs/failover-story.svg`](docs/failover-story.svg): failover trace illustration.
- [`docs/log-ledger.svg`](docs/log-ledger.svg): replicated log view.
