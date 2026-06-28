# Raft Consensus

A production-style implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Java 21, built with Spring Boot and gRPC.

Durable state is persisted through **[RiptideKV](https://github.com/docflex/RiptideKV)** — our own self-developed embedded key-value storage engine with a Redis-compatible wire protocol, WAL-based durability, and SSTable compaction. Every Raft node spins up its own RiptideKV instance to persist `currentTerm`, `votedFor`, and the replicated log, ensuring crash recovery without any external infrastructure.

## Features

| Feature | Raft Paper Reference |
|---|---|
| Leader election with pre-vote | §3.4, §9.6 |
| Log replication & consistency checking | §3.5 |
| Linearizable reads (readIndex protocol) | §6.4 |
| Dynamic membership changes (single-server) | §4 |
| Log compaction via snapshots | §7 |
| InstallSnapshot RPC for lagging followers | §7 |
| Client command deduplication | §6.3 |
| Durable persistence (RiptideKV) | §3.1 |
| DNS-based peer discovery (Docker) | — |

## Tech Stack

- **Java 21** — virtual threads, records, sealed types
- **Spring Boot 3.2** — REST API, Actuator health checks, configuration
- **gRPC 1.62** — inter-node Raft RPCs (RequestVote, AppendEntries, InstallSnapshot)
- **Protocol Buffers 3.25** — wire format for gRPC
- **[RiptideKV](https://github.com/docflex/RiptideKV)** — our self-developed embedded key-value storage engine (WAL + SSTables, Redis-compatible API via Jedis)
- **Docker Compose** — multi-node cluster deployment
- **JUnit 5** — 191 tests covering unit, integration, and stress scenarios

## Quick Start

### Prerequisites

- Java 21 (Temurin recommended)
- Maven 3.8+
- Docker & Docker Compose (for cluster mode)

### Build & Test

```bash
# Compile
mvn compile

# Run all tests
mvn test

# Package JAR
mvn package -DskipTests
```

### Run a 3-Node Cluster (Docker)

```bash
# Build and start 3 replicas
docker compose up --build -d

# Check logs
docker compose logs -f

# Stop
docker compose down
```

### Run Locally (Single Node)

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--raft.node-id=1 --raft.grpc-port=9001"
```

### Run Locally (3 Nodes)

```bash
# Terminal 1 (Node 1 — REST :8081, gRPC :9001)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=1 --raft.grpc-port=9001 --server.port=8081 \
  --raft.data-dir=/tmp/raft \
  --raft.peers[0].id=2 --raft.peers[0].host=localhost --raft.peers[0].port=9002 \
  --raft.peers[1].id=3 --raft.peers[1].host=localhost --raft.peers[1].port=9003"

# Terminal 2 (Node 2 — REST :8082, gRPC :9002)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=2 --raft.grpc-port=9002 --server.port=8082 \
  --raft.data-dir=/tmp/raft \
  --raft.peers[0].id=1 --raft.peers[0].host=localhost --raft.peers[0].port=9001 \
  --raft.peers[1].id=3 --raft.peers[1].host=localhost --raft.peers[1].port=9003"

# Terminal 3 (Node 3 — REST :8083, gRPC :9003)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=3 --raft.grpc-port=9003 --server.port=8083 \
  --raft.data-dir=/tmp/raft \
  --raft.peers[0].id=1 --raft.peers[0].host=localhost --raft.peers[0].port=9001 \
  --raft.peers[1].id=2 --raft.peers[1].host=localhost --raft.peers[1].port=9002"
```

> **Note:** `--raft.data-dir=/tmp/raft` enables RiptideKV persistence. Each node stores
> its WAL and SSTables under `/tmp/raft/s{nodeId}/`. Omit this flag to run in-memory only.

---

## Walkthrough: Leader Failover, Persistence & Log Replication

This end-to-end walkthrough demonstrates the full Raft lifecycle — write data, kill the
leader, watch a new leader get elected, verify data survives via RiptideKV persistence,
and bring the old leader back to see it catch up through log replication.

### 1. Start the 3-node cluster

Start all 3 nodes using the commands above (with `--raft.data-dir=/tmp/raft`).
Wait ~2 seconds for leader election.

### 2. Find the leader

```bash
for port in 8081 8082 8083; do
  echo "Node on :$port →  $(curl -s http://localhost:$port/api/raft/status \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'role={d[\"role\"]}, term={d[\"term\"]}')") "
done
```

Example output:
```
Node on :8081 →  role=LEADER, term=1
Node on :8082 →  role=FOLLOWER, term=1
Node on :8083 →  role=FOLLOWER, term=1
```

### 3. Write data to the leader

```bash
LEADER=8081  # replace with your leader's port

curl -s -X POST http://localhost:$LEADER/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":1, "command":"SET user alice"}'

curl -s -X POST http://localhost:$LEADER/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":2, "command":"SET role admin"}'

curl -s -X POST http://localhost:$LEADER/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":3, "command":"SET status active"}'
```

All three return `{"success":true,"result":"OK"}`.

### 4. Verify replication across all nodes

```bash
for port in 8081 8082 8083; do
  echo "Node on :$port → commitIndex=$(curl -s http://localhost:$port/api/raft/status \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['commitIndex'])")"
done
```

All nodes should report the same `commitIndex` (e.g., `4` — 1 no-op + 3 writes).

### 5. Kill the leader

```bash
# Ctrl+C the leader's terminal, or:
kill $(lsof -ti:$LEADER)
```

### 6. Wait for a new leader (~2 seconds)

```bash
sleep 3

for port in 8081 8082 8083; do
  RESULT=$(curl -s --max-time 1 http://localhost:$port/api/raft/status 2>/dev/null)
  if [ -n "$RESULT" ]; then
    echo "Node on :$port → $(echo $RESULT | python3 -c \
      "import json,sys; d=json.load(sys.stdin); print(f'role={d[\"role\"]}, term={d[\"term\"]}')")"
  else
    echo "Node on :$port → UNREACHABLE (killed)"
  fi
done
```

Expected: one surviving node became `LEADER` at a higher term.

### 7. Verify data survived (persisted by RiptideKV)

```bash
NEW_LEADER=8082  # replace with the new leader's port

curl -s "http://localhost:$NEW_LEADER/api/raft/query?q=GET%20user"
# → {"success":true,"result":"alice"}

curl -s "http://localhost:$NEW_LEADER/api/raft/query?q=GET%20status"
# → {"success":true,"result":"active"}
```

The data survives because RiptideKV persisted every log entry to its WAL before
acknowledging. On restart, `currentTerm`, `votedFor`, and the full log are
recovered from RiptideKV automatically.

### 8. Write new data to the new leader

```bash
curl -s -X POST http://localhost:$NEW_LEADER/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":4, "command":"SET survived true"}'
```

### 9. Bring the old leader back up

Restart the killed node using the same command from Step 1. Wait a few seconds.

```bash
sleep 3

# Check the restarted node
curl -s http://localhost:$LEADER/api/raft/status | python3 -m json.tool
```

Expected:
- **role = FOLLOWER** — it stepped down because a higher term exists.
- **term** matches the current leader's term.
- **commitIndex** caught up — the new leader replicated all entries (including `SET survived true`) to it.

### 10. Verify the restarted node has all data

```bash
curl -s "http://localhost:$LEADER/api/raft/query?q=GET%20user"
# Will redirect to leader (it's a follower now), or query the leader directly:

curl -s "http://localhost:$NEW_LEADER/api/raft/query?q=GET%20survived"
# → {"success":true,"result":"true"}
```

**What happened under the hood:**
1. The old leader restarted and **recovered its persisted state from RiptideKV** (`currentTerm`, `votedFor`, log).
2. It discovered a higher term from the new leader's heartbeats and **stepped down to follower**.
3. The new leader's `AppendEntries` RPCs replicated any **missing log entries** to the restarted node.
4. The restarted node's `commitIndex` advanced and entries were **applied to its state machine**.

---

## Persistence Engine: RiptideKV

This project uses **[RiptideKV](https://github.com/docflex/RiptideKV)** — our own self-developed
embedded key-value storage engine — as the persistence backend for Raft durable state.

### Why RiptideKV?

| Aspect | Detail |
|--------|--------|
| **Self-developed** | Built from scratch — not an off-the-shelf dependency |
| **Embedded** | Runs in-process; no external database to install or manage |
| **WAL + SSTables** | Write-Ahead Log for durability, SSTables for efficient reads |
| **Redis-compatible** | Speaks the Redis wire protocol — uses the standard Jedis client |
| **Per-node isolation** | Each Raft node gets its own RiptideKV instance at `{dataDir}/s{nodeId}/` |

### What Gets Persisted

| Raft State | RiptideKV Key | Format |
|------------|---------------|--------|
| Current term | `raft:currentTerm` | Long as string |
| Voted for | `raft:votedFor` | Server ID as string (empty = null) |
| Log size | `raft:logSize` | Integer as string |
| Log entry _i_ | `raft:log:{i}` | `index\nterm\ncommand` |

### Persistence Flow

```
RaftServer.persistState()  →  RiptideKV.MSET(currentTerm, votedFor)
RaftServer.persistLog()    →  RiptideKV.MSET(logSize, log:0, log:1, ...)
                              RiptideKV.DEL(stale keys beyond new size)

On startup:
  RiptideKV.GET(currentTerm)  →  state.currentTerm
  RiptideKV.GET(votedFor)     →  state.votedFor
  RiptideKV.MGET(log:0..N)    →  state.log
```

Persistence is **optional** — set `raft.data-dir` to enable it, or leave it empty for in-memory mode.

---

## REST API (Quick Reference)

All endpoints are under `/api/raft`. See [docs/API_SPEC.md](docs/API_SPEC.md) for full details.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/raft/command` | Submit a write command |
| `GET`  | `/api/raft/query?q=GET key` | Linearizable read |
| `GET`  | `/api/raft/status` | Node status (role, term, etc.) |
| `POST` | `/api/raft/cluster/add/{nodeId}` | Add a server to the cluster |
| `POST` | `/api/raft/cluster/remove/{nodeId}` | Remove a server |
| `POST` | `/api/raft/snapshot` | Trigger log compaction |
| `GET`  | `/actuator/health` | Health check with Raft details |

### Example: Write and Read

```bash
# Write a key
curl -X POST http://localhost:8081/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId": 1, "serialNumber": 1, "command": "SET greeting hello"}'

# Read it back (linearizable)
curl "http://localhost:8081/api/raft/query?q=GET%20greeting"
```

## Configuration

All properties are under the `raft` prefix. See `application.yml` for defaults.

| Property | Default | Description |
|----------|---------|-------------|
| `raft.node-id` | `0` (auto) | Node ID. `0` = auto-detect from hostname |
| `raft.grpc-port` | `9001` | gRPC port for inter-node RPCs |
| `raft.discovery-dns` | _(empty)_ | DNS name for Docker peer discovery |
| `raft.data-dir` | _(empty)_ | RiptideKV persistence directory. Empty = in-memory |
| `raft.peers` | `[]` | Static peer list (id, host, port) |

## Documentation

- **[Raft Theory](docs/RAFT_THEORY.md)** — deep dive into the Raft consensus algorithm
- **[Architecture](docs/ARCHITECTURE.md)** — project structure, design decisions, package layout
- **[API Spec](docs/API_SPEC.md)** — full API reference, E2E workflows, failover verification

## Project Structure

```
src/main/java/learning/
├── RaftApplication.java            # Spring Boot entry point
├── discovery/
│   └── DnsPeerDiscovery.java       # DNS-based peer discovery
├── grpc/
│   ├── RaftGrpcClient.java         # gRPC client (outbound RPCs)
│   └── RaftGrpcService.java        # gRPC server (inbound RPCs)
├── model/
│   ├── ClusterConfig.java          # Cluster membership tracking
│   ├── LogEntry.java               # Raft log entry
│   ├── ServerID.java               # Server identifier
│   ├── ServerRole.java             # FOLLOWER / CANDIDATE / LEADER
│   └── SnapshotMetadata.java       # Snapshot metadata
├── node/
│   └── RaftNode.java               # Networked Raft node (facade + event loop)
├── persistence/
│   ├── PersistenceLayer.java       # Persistence interface
│   └── RiptideKVPersistence.java   # RiptideKV-backed implementation
├── rpc/
│   ├── *Request.java / *Response.java  # Domain RPC types
├── server/
│   ├── RaftServer.java             # Core consensus logic
│   ├── RaftState.java              # Protocol state container
│   ├── ElectionHandler.java        # Election logic
│   ├── ReplicationHandler.java     # Log replication logic
│   └── StateTransitions.java       # Role transition logic
├── spring/
│   ├── RaftConfig.java             # Spring bean configuration
│   ├── RaftController.java         # REST API
│   ├── RaftHealthIndicator.java    # Actuator health
│   ├── RaftLifecycle.java          # Start/stop lifecycle
│   ├── RaftProperties.java         # Configuration properties
│   └── GlobalExceptionHandler.java # Error handling
└── statemachine/
    ├── StateMachine.java           # State machine interface
    └── KeyValueStateMachine.java   # Key-value store implementation
```

## License

This project is for educational and demonstration purposes — a from-scratch implementation
of the Raft consensus algorithm with a from-scratch persistence engine.
