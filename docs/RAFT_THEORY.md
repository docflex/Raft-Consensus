# Raft Consensus Algorithm — Theory

> Based on ["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)
> by Diego Ongaro and John Ousterhout (2014), and the
> [extended dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf) (2014).

## Table of Contents

- [1. What Problem Does Raft Solve?](#1-what-problem-does-raft-solve)
- [2. Core Concepts](#2-core-concepts)
- [3. Leader Election](#3-leader-election)
- [4. Log Replication](#4-log-replication)
- [5. Safety Properties](#5-safety-properties)
- [6. Cluster Membership Changes](#6-cluster-membership-changes)
- [7. Log Compaction (Snapshots)](#7-log-compaction-snapshots)
- [8. Linearizable Reads](#8-linearizable-reads)
- [9. Client Interaction](#9-client-interaction)
- [10. Pre-Vote Extension](#10-pre-vote-extension)

---

## 1. What Problem Does Raft Solve?

Distributed systems need **consensus** — a way for multiple servers to agree on a single sequence of values, even when some servers crash or messages are delayed.

Raft provides **replicated state machine** consensus:

```
Client → Leader → Log → Replicate to Majority → Commit → Apply to State Machine
```

Every server maintains the same log. Commands are applied in the same order to every state machine, so all servers converge to the same state.

**Key guarantee:** as long as a **majority** (⌊n/2⌋ + 1) of servers are up, the cluster can accept new commands and serve reads.

---

## 2. Core Concepts

### 2.1 Server Roles

Every server is in one of three roles at any given time:

| Role | Responsibility |
|------|---------------|
| **Follower** | Passive — responds to RPCs from leaders and candidates. |
| **Candidate** | Actively seeking votes to become leader. |
| **Leader** | Handles all client requests. Replicates log entries to followers. |

```
              timeout              wins election
Follower ──────────────► Candidate ──────────────► Leader
    ▲                        │                        │
    │          discovers     │    discovers            │
    │        higher term     │  higher term            │
    └────────────────────────┘◄───────────────────────┘
```

### 2.2 Terms

Time is divided into **terms** — monotonically increasing integers. Each term begins with an election. At most one leader can be elected per term.

Terms act as a **logical clock**: if a server receives a message with a higher term, it immediately updates its own term and steps down to follower.

### 2.3 The Replicated Log

The log is an ordered sequence of **entries**. Each entry contains:
- **Index** — 1-based position in the log
- **Term** — the leader's term when the entry was created
- **Command** — the state machine command (e.g., `SET key value`)

The log is the central data structure. The leader's job is to get every follower's log to match its own.

### 2.4 Commit

An entry is **committed** once the leader knows it has been replicated to a majority of servers. Committed entries are guaranteed to be durable — they will survive any future leader election.

---

## 3. Leader Election

### 3.1 Election Timeout

Each follower starts a randomized **election timer** (e.g., 300–500ms). If the timer expires without hearing from a leader, the follower becomes a candidate.

Randomization is critical — it makes split votes unlikely, ensuring elections resolve quickly.

### 3.2 Voting Protocol

1. **Candidate** increments its term, votes for itself, and sends `RequestVote` RPCs to all peers.
2. **Each server** votes for at most one candidate per term (first-come-first-served).
3. **Voting constraint:** a server only votes for a candidate whose log is **at least as up-to-date** as its own.
   - Compare last log entry: higher term wins; if same term, longer log wins.
4. If a candidate receives votes from a **majority**, it becomes leader.

### 3.3 Log Up-to-Date Check (Election Restriction)

This is the key safety mechanism. By requiring voters to check the candidate's log, Raft guarantees:

> **A leader's log contains all committed entries from all previous terms.**

Without this check, a candidate with a stale log could win and overwrite committed data.

### 3.4 What Happens After Election

The new leader immediately appends a **no-op entry** (a blank entry with the current term) and replicates it. This ensures:
- Entries from previous terms get committed under the new leader's term.
- The leader can confirm its leadership quickly.

---

## 4. Log Replication

### 4.1 AppendEntries RPC

The leader sends `AppendEntries` RPCs to each follower:

| Field | Purpose |
|-------|---------|
| `term` | Leader's current term |
| `leaderId` | So followers can redirect clients |
| `prevLogIndex` | Index of the entry immediately preceding the new ones |
| `prevLogTerm` | Term of the `prevLogIndex` entry |
| `entries[]` | New log entries to append (empty for heartbeats) |
| `leaderCommit` | Leader's commit index |

### 4.2 Consistency Check

Before appending, the follower verifies that its log matches at `prevLogIndex`/`prevLogTerm`. If there's a mismatch:

1. Follower rejects the RPC.
2. Leader decrements `nextIndex` for that follower and retries.
3. Eventually the leader finds the point where logs agree, and the follower's conflicting suffix is replaced.

This ensures the **Log Matching Property**: if two logs contain an entry with the same index and term, then the logs are identical in all entries up to and including that index.

### 4.3 Heartbeats

Empty `AppendEntries` RPCs serve as heartbeats. The leader sends them periodically (e.g., every 150ms) to:
- Prevent followers from starting elections.
- Carry updated `leaderCommit` to advance followers' commit indices.

### 4.4 Commit Index Advancement

The leader advances its `commitIndex` to the highest index N where:
- N > current `commitIndex`
- A majority of servers have `matchIndex >= N`
- `log[N].term == currentTerm` (safety: only commit entries from the current term)

---

## 5. Safety Properties

Raft guarantees these properties (from the paper):

| Property | Guarantee |
|----------|-----------|
| **Election Safety** | At most one leader per term. |
| **Leader Append-Only** | A leader never overwrites or deletes log entries. |
| **Log Matching** | If two logs have an entry with the same index and term, they are identical in all preceding entries. |
| **Leader Completeness** | If an entry is committed in a given term, it will be present in the logs of all leaders for higher terms. |
| **State Machine Safety** | If a server has applied entry at index i, no other server will apply a different entry at index i. |

---

## 6. Cluster Membership Changes

### 6.1 The Problem

Changing cluster membership (adding/removing servers) is dangerous because different servers might see different configurations at the same time, creating two independent majorities.

### 6.2 Single-Server Changes (This Implementation)

Raft's simplest safe approach: change membership **one server at a time**.

The leader appends a special **configuration change entry** to the log (e.g., `CONFIG_ADD 4`). The new configuration takes effect as soon as the entry is appended — not when it's committed.

Safety property: the old and new configurations **always overlap in at least one server**, so two independent majorities cannot form.

**Constraint:** only one uncommitted configuration change at a time.

### 6.3 Adding a Server

1. Client requests `addServer(S4)` to the leader.
2. Leader appends `CONFIG_ADD 4` to its log.
3. Leader initializes `nextIndex[S4]` and starts replicating to S4.
4. Once committed, S4 is a full cluster member.

### 6.4 Removing a Server

1. Client requests `removeServer(S2)` to the leader.
2. Leader appends `CONFIG_REMOVE 2` to its log.
3. Once committed and applied, S2 is removed.
4. If the leader removes itself, it steps down after applying the entry.

---

## 7. Log Compaction (Snapshots)

### 7.1 The Problem

Logs grow without bound. A server restarting would need to replay the entire log.

### 7.2 Solution: Snapshots

Each server independently takes a snapshot of its state machine at a given log index. The snapshot replaces all log entries up to that index.

A snapshot contains:
- **Last included index & term** — the last entry covered by the snapshot.
- **State machine data** — serialized state (e.g., the key-value store contents).

### 7.3 InstallSnapshot RPC

If a follower has fallen so far behind that the leader has already compacted the entries it needs, the leader sends an `InstallSnapshot` RPC instead of `AppendEntries`.

The follower:
1. Loads the snapshot into its state machine.
2. Discards its entire log up to the snapshot point.
3. Resumes normal replication from there.

---

## 8. Linearizable Reads

### 8.1 The Problem

A naive read from the leader's state machine might return stale data if the leader has been deposed but doesn't know it yet.

### 8.2 ReadIndex Protocol (§6.4)

1. Leader records `readIndex = commitIndex`.
2. Leader sends heartbeats to all peers.
3. Once a **majority** responds, the leader confirms it's still the legitimate leader.
4. Leader waits until `lastApplied >= readIndex`.
5. Leader executes the query against the state machine.

This provides **linearizable reads** without writing to the log, avoiding log overhead for read-heavy workloads.

---

## 9. Client Interaction

### 9.1 Request Routing

- Clients send requests to any server.
- Non-leaders respond with a **leader hint** so the client can redirect.

### 9.2 Deduplication

Clients tag each request with a `(clientId, serialNumber)` pair. The state machine tracks the latest serial number and result per client. If a duplicate request arrives (e.g., due to retransmission), the cached result is returned without re-applying.

### 9.3 Write Flow

```
Client                   Leader                 Followers
  │                        │                        │
  │── POST /command ──────►│                        │
  │                        │── AppendEntries ──────►│
  │                        │◄── success ────────────│
  │                        │  (majority reached)    │
  │                        │── advance commitIndex  │
  │                        │── apply to state machine│
  │◄── {success, result} ──│                        │
```

### 9.4 Read Flow (ReadIndex)

```
Client                   Leader                 Followers
  │                        │                        │
  │── GET /query ─────────►│                        │
  │                        │── heartbeat ──────────►│
  │                        │◄── ack ────────────────│
  │                        │  (majority confirmed)  │
  │                        │── read from state machine
  │◄── {success, result} ──│                        │
```

---

## 10. Pre-Vote Extension

### 10.1 The Problem

A server partitioned from the cluster increments its term repeatedly (failed elections). When it rejoins, its inflated term forces the current leader to step down — disrupting a healthy cluster.

### 10.2 Solution: Pre-Vote (Dissertation §9.6)

Before starting a real election, a candidate runs a **pre-vote** phase:

1. Candidate asks peers: "Would you vote for me at term T+1?"
2. Peers respond based on the same criteria as real votes, but **do not change any state**.
3. Only if a majority grants the pre-vote does the candidate proceed to a real election.

This prevents disruption: a partitioned node's pre-votes will be rejected (its log is stale), so it never increments its term.

### 10.3 Additional Check: Leader Lease

Pre-vote responses also check whether the voter has recently heard from a leader. If a valid leader is active, there's no reason to grant a pre-vote — the election would be disruptive.

---

## Further Reading

- [Raft Paper (PDF)](https://raft.github.io/raft.pdf)
- [Raft Dissertation (PDF)](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [Raft Visualization](https://raft.github.io/)
- [The Secret Lives of Data — Raft](http://thesecretlivesofdata.com/raft/)
