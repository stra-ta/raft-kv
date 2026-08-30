# raft-kv

A from-scratch Raft implementation with a deterministic simulator and real Rust processes over raw TCP.

![A five-node cluster dashboard](docs/cluster-dashboard.svg)

The simulator controls time, messages, partitions, duplication, reordering, stops, and restarts.
The process runner adds framed TCP, atomic Raft persistence, and an LSM-backed state machine.

![Client routing, leader replication, majority commit, and apply](docs/system-shape.svg)

<table>
  <tr>
    <td><img src="docs/failover-story.svg" alt="A leader failure and deterministic recovery"></td>
    <td><img src="docs/lsm-storage.svg" alt="The LSM storage path"></td>
  </tr>
</table>

## Current boundary

- Three to five nodes
- Majority-committed writes and current-term read barriers
- Durable term, vote, log, snapshot boundary, and state-machine recovery
- Deterministic histories with linearizability checks
- No membership changes, TLS, authentication, or production claim

[Start a cluster, inspect metrics, test failures, and read the limits](GUIDE.md).

[Open the interactive trace explorer](docs/raft-explorer.html).
