# raft-core

A Go implementation of the Raft consensus algorithm, designed as a core library for building distributed systems. This project focuses on providing a clear, understandable, and extensible implementation of the Raft protocol.

## Features

- **Leader Election & Log Replication**: Implements Raft's core mechanics for leader election, log replication, and ensuring log consistency across peers.
- **Persistence & Safety**: Nodes persist their state (term, vote, log) to stable storage.
  - Leaders step down if they fail to persist a new log entry.
  - Followers employ a "fail-stop" mechanism, shutting down upon persistence failure to prevent safety violations (e.g., double-voting or log inconsistency).
- **Log Compaction with Snapshots**: Supports log compaction via snapshots to manage log growth. Leaders can send snapshots to bring slow followers up-to-date.
- **Pluggable Architecture**: Key components are defined by interfaces, allowing users to provide custom implementations.
  - `FSM`: The application state machine.
  - `Transport`: The network layer for peer-to-peer communication (a default gRPC implementation is provided).
  - `Persister`: The storage layer for durable state (a default atomic, filesystem-based implementation is provided).
- **Client-Facing Coordinator**: Includes a `Coordinator` client that handles leader discovery and request retries automatically, simplifying client-side logic.
- **HTTP Monitoring**: Each node can expose a `/status` endpoint for basic monitoring.

## Project Status & Future Work

This implementation provides the fundamental features of the Raft algorithm but is not yet feature-complete for production use. The most significant missing piece is dynamic cluster membership.

### TODO: Cluster Membership Changes

The current implementation operates on a fixed set of peers defined at startup. The next major goal is to implement dynamic membership changes, which will allow nodes to be safely added to or removed from a running cluster. This will involve:

1.  Implementing special configuration log entries that trigger membership changes when committed.
2.  Adopting the "single-server change" approach to avoid split-brain scenarios during transitions.
3.  Creating an administrative API or tool to initiate requests for adding/removing nodes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
