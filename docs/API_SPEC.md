# API Specification & End-to-End Workflows

Complete reference for the REST API, gRPC RPCs, and step-by-step workflows for verifying Raft behavior — including leadership failover, log replication, and cluster membership changes.

## Table of Contents

- [1. REST API Reference](#1-rest-api-reference)
- [2. gRPC RPC Reference](#2-grpc-rpc-reference)
- [3. End-to-End Workflows](#3-end-to-end-workflows)
- [4. Verifying Leadership Election](#4-verifying-leadership-election)
- [5. Verifying Log Replication](#5-verifying-log-replication)
- [6. Simulating Leader Failure & Failover](#6-simulating-leader-failure--failover)
- [7. Dynamic Membership Changes](#7-dynamic-membership-changes)
- [8. Snapshot & Log Compaction](#8-snapshot--log-compaction)
- [9. Linearizable Reads](#9-linearizable-reads)
- [10. Health & Monitoring](#10-health--monitoring)

---

## 1. REST API Reference

Base URL: `http://localhost:{port}/api/raft`

### POST `/command` — Submit a Write Command

Appends a command to the Raft log. Blocks until committed and applied, or times out (5s).

**Request:**
```json
{
  "clientId": 1,
  "serialNumber": 1,
  "command": "SET mykey myvalue"
}
```

**Response (success):**
```json
{
  "success": true,
  "result": "OK"
}
```

**Response (not leader):**
```json
{
  "success": false,
  "leaderHint": 2
}
```

**Response (timeout):**
```json
{
  "success": false,
  "result": "TIMEOUT"
}
```

**Supported commands:**

| Command | Example | Result |
|---------|---------|--------|
| `SET key value` | `SET name Alice` | `OK` |
| `GET key` | `GET name` | `Alice` or `(nil)` |
| `DEL key` | `DEL name` | `1` (deleted) or `0` (not found) |

---

### GET `/query?q={command}` — Linearizable Read

Executes a read-only query using the readIndex protocol. Does **not** write to the log.

**Request:**
```
GET /api/raft/query?q=GET%20mykey
```

**Response:**
```json
{
  "success": true,
  "result": "myvalue"
}
```

---

### GET `/status` — Node Status

Returns the current state of this Raft node.

**Response:**
```json
{
  "nodeId": 1,
  "role": "LEADER",
  "term": 3,
  "commitIndex": 7,
  "lastApplied": 7,
  "lastLogIndex": 7,
  "logSize": 5,
  "clusterSize": 3,
  "votedFor": 1,
  "leaderHint": 1
}
```

If a snapshot exists, additional fields:
```json
{
  "snapshotLastIndex": 4,
  "snapshotLastTerm": 2
}
```

---

### POST `/cluster/add/{nodeId}` — Add Server

Adds a new server to the cluster. Must be sent to the leader.

```
POST /api/raft/cluster/add/4
```

**Response:**
```json
{
  "accepted": true
}
```

---

### POST `/cluster/remove/{nodeId}` — Remove Server

Removes a server from the cluster. If the leader removes itself, it steps down after the change is committed.

```
POST /api/raft/cluster/remove/2
```

**Response:**
```json
{
  "accepted": true
}
```

---

### POST `/snapshot` — Trigger Snapshot

Manually triggers log compaction. The state machine is snapshotted and log entries up to `lastApplied` are discarded.

```
POST /api/raft/snapshot
```

**Response:**
```json
{
  "status": "snapshot_triggered"
}
```

---

### GET `/actuator/health` — Health Check

Spring Boot Actuator endpoint with Raft-specific details.

**Response:**
```json
{
  "status": "UP",
  "components": {
    "raft": {
      "status": "UP",
      "details": {
        "nodeId": 1,
        "role": "LEADER",
        "term": 3,
        "commitIndex": 7,
        "clusterSize": 3
      }
    }
  }
}
```

---

## 2. gRPC RPC Reference

Port: configured via `raft.grpc-port` (default: `9001`).

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `PreVote` | Candidate → Peer | Pre-vote check before election |
| `RequestVote` | Candidate → Peer | Request vote for election |
| `AppendEntries` | Leader → Follower | Log replication + heartbeat |
| `InstallSnapshot` | Leader → Follower | Send snapshot to lagging follower |
| `ClientCommand` | Client → Leader | Submit write command via gRPC |
| `ReadOnlyQuery` | Client → Leader | Linearizable read via gRPC |
| `Identify` | Discovery → Peer | Identify a peer's node ID |

---

## 3. End-to-End Workflows

### Prerequisites

Start a 3-node cluster locally:

```bash
# Terminal 1 (Node 1, REST on 8081, gRPC on 9001)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=1 --raft.grpc-port=9001 --server.port=8081 \
  --raft.peers[0].id=2 --raft.peers[0].host=localhost --raft.peers[0].port=9002 \
  --raft.peers[1].id=3 --raft.peers[1].host=localhost --raft.peers[1].port=9003"

# Terminal 2 (Node 2, REST on 8082, gRPC on 9002)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=2 --raft.grpc-port=9002 --server.port=8082 \
  --raft.peers[0].id=1 --raft.peers[0].host=localhost --raft.peers[0].port=9001 \
  --raft.peers[1].id=3 --raft.peers[1].host=localhost --raft.peers[1].port=9003"

# Terminal 3 (Node 3, REST on 8083, gRPC on 9003)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=3 --raft.grpc-port=9003 --server.port=8083 \
  --raft.peers[0].id=1 --raft.peers[0].host=localhost --raft.peers[0].port=9001 \
  --raft.peers[1].id=2 --raft.peers[1].host=localhost --raft.peers[1].port=9002"
```

Wait ~2 seconds for leader election to complete.

---

## 4. Verifying Leadership Election

### Step 1: Check which node is the leader

```bash
# Check all 3 nodes
for port in 8081 8082 8083; do
  echo "--- Node on port $port ---"
  curl -s http://localhost:$port/api/raft/status | python3 -m json.tool
done
```

**Expected:** Exactly one node has `"role": "LEADER"`. The other two are `"FOLLOWER"` with matching `leaderHint`.

### Step 2: Verify all nodes agree on the term

```bash
for port in 8081 8082 8083; do
  curl -s http://localhost:$port/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'Node {d[\"nodeId\"]}: role={d[\"role\"]}, term={d[\"term\"]}')"
done
```

**Expected:** All nodes report the same term.

### Step 3: Verify no-op was committed

```bash
LEADER_PORT=8081  # replace with actual leader port
curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'commitIndex={d[\"commitIndex\"]}, lastApplied={d[\"lastApplied\"]}')
assert d['commitIndex'] >= 1, 'No-op should be committed'
print('OK: No-op committed')"
```

---

## 5. Verifying Log Replication

### Step 1: Write data via the leader

```bash
LEADER_PORT=8081  # replace with actual leader port

# Write 3 keys
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":1, "command":"SET name Alice"}'

curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":2, "command":"SET city Berlin"}'

curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":3, "command":"SET count 42"}'
```

### Step 2: Verify commit indices converged

```bash
sleep 1  # allow replication

for port in 8081 8082 8083; do
  curl -s http://localhost:$port/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'Node {d[\"nodeId\"]}: commitIndex={d[\"commitIndex\"]}, logSize={d[\"logSize\"]}')"
done
```

**Expected:** All nodes have the same `commitIndex` (leader's commitIndex).

### Step 3: Read data from a follower (via leader redirect)

```bash
# Query via the leader
curl -s "http://localhost:$LEADER_PORT/api/raft/query?q=GET%20name"
# Expected: {"success":true,"result":"Alice"}

curl -s "http://localhost:$LEADER_PORT/api/raft/query?q=GET%20city"
# Expected: {"success":true,"result":"Berlin"}
```

### Step 4: Verify deduplication

```bash
# Resend the same request (same clientId + serialNumber)
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":1, "serialNumber":1, "command":"SET name Alice"}'
# Expected: {"success":true,"result":"OK"} — cached, not re-applied
```

---

## 6. Simulating Leader Failure & Failover

### Step 1: Identify the current leader

```bash
for port in 8081 8082 8083; do
  ROLE=$(curl -s http://localhost:$port/api/raft/status | python3 -c "
import json, sys; print(json.load(sys.stdin)['role'])")
  if [ "$ROLE" = "LEADER" ]; then
    echo "Leader is on port $port"
    LEADER_PORT=$port
  fi
done
```

### Step 2: Write data before killing the leader

```bash
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":2, "serialNumber":1, "command":"SET pre_crash alive"}'
```

### Step 3: Kill the leader

```bash
# Find the leader's PID (if running locally via Maven)
# Option A: Ctrl+C in the leader's terminal
# Option B: Kill the process
kill $(lsof -ti:$LEADER_PORT)
```

### Step 4: Wait for new election (~1-2 seconds)

```bash
sleep 3
```

### Step 5: Verify a new leader was elected

```bash
for port in 8081 8082 8083; do
  RESULT=$(curl -s --max-time 2 http://localhost:$port/api/raft/status 2>/dev/null)
  if [ -n "$RESULT" ]; then
    echo "Node on port $port:"
    echo "$RESULT" | python3 -m json.tool
  else
    echo "Node on port $port: UNREACHABLE (killed leader)"
  fi
done
```

**Expected:**
- One of the surviving nodes is now `LEADER` with a higher `term`.
- The killed node is unreachable.

### Step 6: Verify data survived the failover

```bash
NEW_LEADER_PORT=8082  # replace with actual new leader port

curl -s "http://localhost:$NEW_LEADER_PORT/api/raft/query?q=GET%20pre_crash"
# Expected: {"success":true,"result":"alive"}
```

### Step 7: Write new data to the new leader

```bash
curl -s -X POST http://localhost:$NEW_LEADER_PORT/api/raft/command \
  -H 'Content-Type: application/json' \
  -d '{"clientId":2, "serialNumber":2, "command":"SET post_crash survived"}'

curl -s "http://localhost:$NEW_LEADER_PORT/api/raft/query?q=GET%20post_crash"
# Expected: {"success":true,"result":"survived"}
```

### Step 8: Restart the old leader and verify it catches up

```bash
# Restart the killed node (same terminal command as before)
# Wait a few seconds for it to rejoin as a follower

sleep 3

curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'Restarted node: role={d[\"role\"]}, term={d[\"term\"]}, commitIndex={d[\"commitIndex\"]}')"
# Expected: role=FOLLOWER, same term as the new leader, commitIndex caught up
```

---

## 7. Dynamic Membership Changes

### Add a Server

```bash
# Start a 4th node
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --raft.node-id=4 --raft.grpc-port=9004 --server.port=8084 \
  --raft.peers[0].id=1 --raft.peers[0].host=localhost --raft.peers[0].port=9001 \
  --raft.peers[1].id=2 --raft.peers[1].host=localhost --raft.peers[1].port=9002 \
  --raft.peers[2].id=3 --raft.peers[2].host=localhost --raft.peers[2].port=9003"

# Tell the leader to add node 4
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/cluster/add/4
# Expected: {"accepted":true}

# Verify cluster size increased
sleep 2
curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys; print(f'clusterSize={json.load(sys.stdin)[\"clusterSize\"]}')"
# Expected: clusterSize=4
```

### Remove a Server

```bash
# Remove node 4
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/cluster/remove/4
# Expected: {"accepted":true}

sleep 2
curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys; print(f'clusterSize={json.load(sys.stdin)[\"clusterSize\"]}')"
# Expected: clusterSize=3
```

---

## 8. Snapshot & Log Compaction

### Step 1: Write enough data to make compaction worthwhile

```bash
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:$LEADER_PORT/api/raft/command \
    -H 'Content-Type: application/json' \
    -d "{\"clientId\":3, \"serialNumber\":$i, \"command\":\"SET key$i value$i\"}" > /dev/null
done

echo "Before snapshot:"
curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  logSize={d[\"logSize\"]}, lastLogIndex={d[\"lastLogIndex\"]}')"
```

### Step 2: Trigger snapshot

```bash
curl -s -X POST http://localhost:$LEADER_PORT/api/raft/snapshot
# Expected: {"status":"snapshot_triggered"}
```

### Step 3: Verify log was compacted

```bash
echo "After snapshot:"
curl -s http://localhost:$LEADER_PORT/api/raft/status | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  logSize={d[\"logSize\"]}')
print(f'  snapshotLastIndex={d.get(\"snapshotLastIndex\", \"none\")}')
print(f'  snapshotLastTerm={d.get(\"snapshotLastTerm\", \"none\")}')"
```

**Expected:** `logSize` decreased significantly. `snapshotLastIndex` is set.

### Step 4: Verify data is still accessible

```bash
curl -s "http://localhost:$LEADER_PORT/api/raft/query?q=GET%20key10"
# Expected: {"success":true,"result":"value10"}
```

---

## 9. Linearizable Reads

### Basic Read

```bash
curl -s "http://localhost:$LEADER_PORT/api/raft/query?q=GET%20name"
# Expected: {"success":true,"result":"Alice"}
```

### Read non-existent key

```bash
curl -s "http://localhost:$LEADER_PORT/api/raft/query?q=GET%20nonexistent"
# Expected: {"success":true,"result":"(nil)"}
```

### Read from non-leader

```bash
FOLLOWER_PORT=8082  # replace with a follower port
curl -s "http://localhost:$FOLLOWER_PORT/api/raft/query?q=GET%20name"
# Expected: {"success":false,"leaderHint":1}
# The client should redirect to the leader
```

---

## 10. Health & Monitoring

### Actuator Health

```bash
curl -s http://localhost:8081/actuator/health | python3 -m json.tool
```

### Monitoring Script

A quick script to continuously monitor all nodes:

```bash
while true; do
  clear
  echo "=== Raft Cluster Status ==="
  for port in 8081 8082 8083; do
    RESULT=$(curl -s --max-time 1 http://localhost:$port/api/raft/status 2>/dev/null)
    if [ -n "$RESULT" ]; then
      echo "$RESULT" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'  S{d[\"nodeId\"]}: {d[\"role\"]:9s} term={d[\"term\"]} commit={d[\"commitIndex\"]} log={d[\"logSize\"]}')"
    else
      echo "  Port $port: UNREACHABLE"
    fi
  done
  sleep 1
done
```

**Example output:**
```
=== Raft Cluster Status ===
  S1: LEADER    term=3 commit=12 log=10
  S2: FOLLOWER  term=3 commit=12 log=10
  S3: FOLLOWER  term=3 commit=12 log=10
```

---

## Docker Compose Workflows

### Start Cluster

```bash
docker compose up --build -d
docker compose logs -f
```

### Check Cluster Status

```bash
# Get container IPs
docker compose ps

# Query each node (ports are not exposed by default — use docker exec)
for i in 1 2 3; do
  docker compose exec --index=$i raft curl -s http://localhost:8080/api/raft/status
done
```

### Kill a Node

```bash
# Stop one replica to test failover
docker compose stop raft --index=2

# Verify new leader elected
sleep 3
docker compose exec --index=1 raft curl -s http://localhost:8080/api/raft/status

# Restart the stopped node
docker compose start raft --index=2
```

### Tear Down

```bash
docker compose down -v  # -v removes volumes (clears persisted data)
```
