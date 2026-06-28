# Architecture & Design

This document explains how the Raft consensus implementation is structured, the design decisions behind it, and how the packages relate to each other.

## Table of Contents

- [1. High-Level Architecture](#1-high-level-architecture)
- [2. Package Layout](#2-package-layout)
- [3. Core Components](#3-core-components)
- [4. Threading Model](#4-threading-model)
- [5. Synchronization Strategy](#5-synchronization-strategy)
- [6. Design Decisions](#6-design-decisions)
- [7. Data Flow](#7-data-flow)
- [8. Persistence Architecture](#8-persistence-architecture)
- [9. Testing Strategy](#9-testing-strategy)

---

## 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Spring Boot                              │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────────────┐  │
│  │ RaftController│  │RaftHealthIndicator│  │  RaftLifecycle   │  │
│  │  (REST API)  │  │  (Actuator)       │  │ (start/stop)     │  │
│  └──────┬───────┘  └────────┬──────────┘  └────────┬─────────┘  │
│         │                   │                      │            │
│         └───────────────────┼──────────────────────┘            │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                       RaftNode                            │   │
│  │              (facade + event loop)                        │   │
│  │  ┌────────────┐  ┌─────────────────┐  ┌──────────────┐  │   │
│  │  │ RaftServer  │  │ RaftGrpcClient  │  │RaftGrpcService│  │   │
│  │  │ (consensus) │  │ (outbound RPCs) │  │(inbound RPCs)│   │   │
│  │  └──────┬──────┘  └────────────────┘  └──────────────┘   │   │
│  │         │                                                 │   │
│  │  ┌──────┴──────────────────────────────────────────┐     │   │
│  │  │  ElectionHandler │ ReplicationHandler │ StateTransitions│   │
│  │  └─────────────────────────────────────────────────┘     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                             │                                   │
│              ┌──────────────┼──────────────┐                    │
│              ▼              ▼              ▼                    │
│       StateMachine   PersistenceLayer  ClusterConfig            │
│     (KeyValueSM)    (RiptideKV)       (membership)             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Package Layout

| Package | Responsibility | Visibility |
|---------|---------------|------------|
| `learning` | Application entry point | — |
| `learning.model` | Domain value types: `ServerID`, `ServerRole`, `LogEntry`, `SnapshotMetadata`, `ClusterConfig` | Public records/enums |
| `learning.rpc` | RPC request/response records (domain layer, not protobuf) | Public records |
| `learning.server` | **Core Raft algorithm** — transport-agnostic. `RaftServer`, `RaftState`, `ElectionHandler`, `ReplicationHandler`, `StateTransitions` | Fields package-private; public API on `RaftServer` |
| `learning.node` | `RaftNode` — wires together `RaftServer` + gRPC + event loop. Facade for external callers. | Public |
| `learning.grpc` | gRPC transport: `RaftGrpcService` (server), `RaftGrpcClient` (client) | Public |
| `learning.discovery` | `DnsPeerDiscovery` — DNS-based peer discovery for Docker | Public |
| `learning.statemachine` | `StateMachine` interface + `KeyValueStateMachine` implementation | Public |
| `learning.persistence` | `PersistenceLayer` interface + `RiptideKVPersistence` implementation | Public |
| `learning.spring` | Spring Boot configuration, REST controller, health, lifecycle | Public |

### Dependency Direction

```
spring → node → server
               → grpc
               → statemachine
               → persistence
         discovery → grpc, server
model, rpc ← (used by all)
```

The `server` package has **zero dependency on transport** (gRPC, Spring). It operates purely on domain types.

---

## 3. Core Components

### RaftServer

The heart of the implementation. Contains all Raft protocol logic but no networking code.

- **State**: delegates to `RaftState` (persistent + volatile state container)
- **Elections**: delegates to `ElectionHandler`
- **Replication**: delegates to `ReplicationHandler`
- **Transitions**: delegates to `StateTransitions`
- **Public getters**: expose read-only state for external packages
- **Field visibility**: package-private — only classes in `learning.server` can access fields directly

### RaftNode

The facade that assembles everything:

1. Creates `RaftServer`, `RaftGrpcClient`, `RaftGrpcService`
2. Runs the **event loop** (tick → elect / heartbeat)
3. Exposes **facade methods** (`submitCommand`, `query`, `getStatus`, `addServer`, `removeServer`, `triggerSnapshot`)
4. Handles **synchronization** — all `RaftServer` access is wrapped in `synchronized(raftServer)`

### RaftGrpcService

Converts inbound gRPC protobuf messages to domain RPC types, delegates to `RaftServer` under `synchronized(server)`, and converts responses back.

### RaftGrpcClient

Converts outbound domain RPC types to protobuf messages, sends them over managed gRPC channels with deadlines.

---

## 4. Threading Model

| Thread | Purpose |
|--------|---------|
| `raft-event-loop-S{id}` | Main Raft loop: tick → election / heartbeat. One per node. |
| `pre-vote-S{id}` | Virtual thread per peer for pre-vote RPCs. |
| `vote-req-S{id}` | Virtual thread per peer for RequestVote RPCs. |
| `ae-S{id}` | Virtual thread per peer for AppendEntries RPCs. |
| `snap-S{id}` | Virtual thread per peer for InstallSnapshot RPCs. |
| `peer-discovery` | DNS discovery background thread. |
| gRPC server threads | Netty threads handling inbound RPCs. |
| Spring Boot threads | Tomcat threads handling REST requests. |

**RPC sends are non-blocking with respect to the event loop.** The event loop builds requests under the `RaftServer` lock, then dispatches virtual threads that send RPCs without holding the lock. Responses are applied back under the lock.

---

## 5. Synchronization Strategy

All mutable `RaftServer` state is protected by a single monitor: `synchronized(raftServer)`.

### Why a single lock?

Raft state is deeply interconnected — term, role, log, commitIndex, votedFor all interact. A fine-grained locking scheme would be error-prone and premature for a learning implementation.

### Lock granularity

| Operation | Lock held? |
|-----------|-----------|
| Build RPC request | Yes |
| Send RPC over network | **No** — released before I/O |
| Process RPC response | Yes |
| Inbound RPC handling | Yes |
| REST API facade methods | Yes (via RaftNode) |
| DNS discovery registration | Yes |

This design avoids holding the lock during network I/O, which would serialize all RPCs.

---

## 6. Design Decisions

### 6.1 Facade Pattern (RaftNode)

External classes (`RaftController`, `RaftHealthIndicator`) never access `RaftServer` directly. `RaftNode` provides high-level methods that handle synchronization internally. This:
- Prevents callers from forgetting to synchronize
- Hides internal implementation details
- Makes the public API explicit

### 6.2 Dependency Injection of StateMachine

`RaftNode` accepts a `StateMachine` parameter instead of hardcoding `KeyValueStateMachine`. This allows:
- Swapping implementations (e.g., a SQL state machine) without modifying core code
- Easier testing with mock state machines

### 6.3 Encapsulated Peer Registration

`RaftServer.registerPeer()` encapsulates the mutation of `peers`, `config`, `nextIndex`, and `matchIndex`. Previously, `DnsPeerDiscovery` reached into `RaftServer` internals directly.

### 6.4 Package-Private Fields

`RaftState` and `RaftServer` fields are package-private (not `public`). Within `learning.server`, handler classes access them directly for performance and readability. External packages use public getters on `RaftServer`.

### 6.5 Domain RPC Types

The `learning.rpc` package defines RPC types as Java records (e.g., `AppendEntriesRequest`), separate from the protobuf-generated types. This keeps the core algorithm independent of the transport format.

### 6.6 Pre-Vote with Exponential Backoff

Failed pre-votes trigger exponential backoff (300ms → 600ms → ... → 5s) to avoid election storms when a minority partition cannot win elections.

---

## 7. Data Flow

### Write Path

```
1. Client → POST /api/raft/command → RaftController
2. RaftController → RaftNode.submitCommand()
3. RaftNode → synchronized(raftServer) { raftServer.handleClientRequest() }
4. RaftServer → append to log, return CompletableFuture
5. Event loop → sendHeartbeats() → AppendEntries RPC to peers
6. Peers → handleAppendEntries() → append, advance commitIndex
7. Leader → handleAppendEntriesResponse() → advanceCommitIndex()
8. Leader → applyCommittedEntries() → StateMachine.apply()
9. CompletableFuture completes → response returned to client
```

### Read Path (ReadIndex)

```
1. Client → GET /api/raft/query?q=GET key → RaftController
2. RaftController → RaftNode.query()
3. RaftNode → synchronized(raftServer) { raftServer.handleReadOnlyQuery() }
4. RaftServer → records readIndex = commitIndex, returns PENDING
5. RaftNode → polls for readIndex confirmation (heartbeat acks)
6. Heartbeats → peers ack → confirmReadIndex()
7. Majority confirmed → executeReadOnlyQuery() → StateMachine.apply()
8. Response returned to client
```

---

## 8. Persistence Architecture

```
RaftServer
    │
    ├── persistState()  →  PersistenceLayer.saveState(currentTerm, votedFor)
    └── persistLog()    →  PersistenceLayer.saveLog(log)

On startup:
    state.currentTerm = persistence.loadCurrentTerm()
    state.votedFor    = persistence.loadVotedFor()
    state.log         = persistence.loadLog()
```

### RiptideKV Implementation

- **RiptideKV** is an embedded key-value store with a Redis-compatible API (Jedis client).
- Each server gets its own RiptideKV instance at `{dataDir}/s{nodeId}/`.
- Keys: `currentTerm`, `votedFor`, `log:{index}` for each log entry.
- The server process manages the RiptideKV lifecycle (start/stop).

### Persistence is Optional

If `raft.data-dir` is empty, `persistence` is `null` and the server runs in-memory only. All persistence calls are null-guarded.

---

## 9. Testing Strategy

### Test Categories

| Category | Location | What it tests |
|----------|----------|---------------|
| **Unit tests** | `learning.server.*Test` | Core Raft logic in isolation (no network) |
| **Persistence tests** | `learning.persistence.*Test` | RiptideKV save/load correctness |
| **Stress tests** | `learning.persistence.*StressTest` | RiptideKV under heavy workloads |
| **Integration tests** | `learning.grpc.RaftGrpcIntegrationTest` | 3-node cluster over real gRPC |

### Unit Test Design

The `learning.server` package tests (election, replication, membership, client interaction, persistence recovery) create `RaftServer` instances directly without networking. They call RPC handlers and verify state transitions. This provides fast, deterministic tests of the core algorithm.

### Integration Tests

`RaftGrpcIntegrationTest` boots 3 real `RaftNode` instances on `localhost` with different ports. It verifies:
- Leader election and no-op commit
- Client write replication to majority
- Log consistency across all nodes
- Term convergence

### Test Count

**191 tests** covering:
- Election edge cases (split vote, stale terms, log restriction)
- Replication (heartbeats, consistency check, conflict resolution)
- Membership changes (add, remove, safety, leader self-removal)
- Client interaction (dedup, redirect, commit notification)
- Persistence & recovery (crash recovery, state reload)
- Stress scenarios (bulk writes, log churn, rapid overwrites)
