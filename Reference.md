# Introduction

Raft breaks down the responsibilities associated with consensus into 3 parts:

(Decomposition)

- Leader Election
- Log Replication
- Safety

State Space reduction is also done in Raft.

## Similarity to other consensus protocols

- Strong Leader
- Leader Election
- Membership Changes


    Raft’s mechanism for changing the set of servers in the cluster 
    uses a new joint consensus approach 
    where 
    the majorities of two different configurations overlap during transitions. 
    This allows the cluster to continue operating normally during configuration change

## What is a Replicated State Machine:

| A Replicated State Machine is a distributed system architecture where multiple replicas of a state machine execute the same sequence of operations, ensuring that all replicas remain in sync. The core components of an RSM include:

- State:
  The state is a snapshot of information maintained by the machine. It evolves as operations are executed.

- Operations:
  Operations are the commands that change the state. Each operation must be applied in the same order across all replicas to maintain consistency.

- Replication:
  Each replica processes the same sequence of operations, allowing them to remain consistent even in the presence of failures.


## Raft: Understandability as a Consensus Algorithm

When venturing out to actually create a practical and coherent version of Paxos (Leslie Lamport), the researchers of Raft came across a terrible realisation. Understandability of a consensus algorithm is almost as important as the Correctness of it. This can be understood by seeing that the Paxos Algorithm and its implementations vary by quite a bit and mostly it lacks when it comes to practicality in implementations.

> Raft is an algorithm for managing a replicated log of the form described in Section 2. (RSMs)

Raft implements consensus by first electing a distinguished leader, then giving the leader complete responsibility for managing the replicated log.

The leader accepts log entries from clients, replicates them on other servers, and tells servers when it is safe to apply log entries to their state machines.

| Having a leader simplifies the management of the replicated log. This greatly simplifies that Flow of Data in the Application.

A leader can fail or become disconnected from the other servers, in which case a new leader is elected.

### Decomposing the process into sub processes:

1. Leader Election: a new leader must be chosen when an existing leader fails.
2. Log Replication: leader must accept log entries from clients and replicate them across the cluster, forcing the other logs to agree with its own
3. Safety: he key safety property for Raft is the State Machine Safety Property. If any server has applied a particular log entry to its state machine, then no other server may apply a different command for the same log index.

### Raft Basics

Raft system may contain multiple servers (typically 5). And tolerates the failure of upto 2 of them.

Each of these Servers are always in one of these 3 states:

- Leader
- Follower
- Candidate

During normals operations there will be exactly 1 Leader in the entire cluster and all the others are followers.

Followers don't generate any request by themselves, they merely acknowledge and act on the messages that they receive from Leader and Candidates.

Leader alone handles all client requests. If a Follower were to receive a client request, it will merely forward it to the Leader.

Candidates are merely used to elect new leaders.

### Terms: Raft's Logical Clock

Raft divides time into **terms** of arbitrary length. Terms are numbered with consecutive integers (1, 2, 3, ...). Each term begins with an **election**. If a candidate wins, it serves as leader for the rest of that term. If the election results in a split vote, the term ends with no leader and a new term begins immediately.

| Terms act as a logical clock in Raft. They allow servers to detect obsolete information such as stale leaders. Each server stores a `currentTerm` number which increases monotonically over time.

**Rules around terms:**

- Current terms are exchanged whenever servers communicate.
- If one server's `currentTerm` is smaller than the other's, it updates its `currentTerm` to the larger value.
- If a candidate or leader discovers that its term is out of date, it **immediately reverts to follower** state.
- If a server receives a request with a stale term number, it **rejects** the request.

> Terms are the mechanism by which Raft detects stale leaders. Think of it like version numbers — if your version is old, you're not in charge anymore.

#### Worked Example: Terms in Action

```
Cluster: S1, S2, S3, S4, S5

Time ──────────────────────────────────────────────────────────────►

Term 1:  [  Election  |    S1 is Leader (normal operations)          ]
Term 2:  [  Election (S1 crashed)  | S3 is Leader                   ]
Term 3:  [  Election (split vote, no leader) ]
Term 4:  [  Election  |  S2 is Leader (normal operations)           ]
```

- Term 1: S1 wins election. All 5 servers set `currentTerm = 1`. S1 serves client requests.
- Term 2: S1 crashes. S3's election timeout fires first. S3 increments `currentTerm` to 2, votes for itself, sends `RequestVote` to S2, S4, S5. Gets majority (S3, S4, S5 = 3 votes). S3 becomes leader for Term 2.
- Term 3: S3 crashes. S2 and S4 both timeout at nearly the same time. Both increment to term 3 and send `RequestVote`. Votes split: S2 gets vote from S5, S4 gets vote from S1 (now recovered). Neither gets majority. Term 3 ends with no leader.
- Term 4: After randomized timeouts, S2 alone starts election for Term 4. Gets votes from S1, S3, S5. S2 becomes leader.

**Key Insight:** Different servers may observe transitions between terms at different times. S1 in the example above might not even know Term 2 or Term 3 happened until it receives a message with a higher term number.

### RPCs: The Communication Backbone

Raft servers communicate using **Remote Procedure Calls (RPCs)**. The basic consensus algorithm requires only **two types of RPCs**:

1. **RequestVote RPC** — Initiated by candidates during elections.
2. **AppendEntries RPC** — Initiated by leaders to:
    - Replicate log entries to followers
    - Serve as heartbeats (when sent with no log entries)

A third RPC is added later for log compaction:

3. **InstallSnapshot RPC** — Used by leader to send snapshot chunks to followers that have fallen too far behind.

**RPC Behaviour:**
- Servers retry RPCs if they do not receive a response in a timely manner.
- RPCs are issued in parallel for best performance.
- All Raft RPCs are **idempotent** — receiving the same RPC twice causes no harm.

---

## Leader Election (Detailed)

Raft uses a **heartbeat mechanism** to trigger leader election.

**The lifecycle:**

1. All servers start as **followers**.
2. A server remains a follower as long as it receives valid RPCs from a leader or candidate.
3. Leaders send periodic **heartbeats** (empty `AppendEntries` RPCs) to all followers to maintain authority.
4. If a follower receives no communication over a period called the **election timeout**, it assumes there is no viable leader and begins an election.

**Starting an Election:**

1. Follower increments its `currentTerm`.
2. Transitions to **candidate** state.
3. Votes for itself.
4. Issues `RequestVote` RPCs **in parallel** to all other servers in the cluster.

**Three possible outcomes for a candidate:**

- **(a) It wins the election** — receives votes from a majority of servers for the same term. Each server votes for **at most one** candidate in a given term (first-come-first-served). Once it wins, it becomes leader and sends heartbeats to all servers.
- **(b) Another server establishes itself as leader** — While waiting for votes, candidate receives an `AppendEntries` RPC from another server claiming to be leader. If that leader's term >= candidate's `currentTerm`, the candidate recognizes the leader and steps down to follower. If the term is smaller, the candidate rejects the RPC and continues.
- **(c) A period of time goes by with no winner (split vote)** — Multiple followers became candidates simultaneously, votes were split, no one got majority. Each candidate times out and starts a new election (increments term, sends new `RequestVote` RPCs).

**Preventing Repeated Split Votes:**

Raft uses **randomized election timeouts** chosen from a fixed interval (e.g., 150–300ms). This spreads out the servers so that in most cases only a single server will time out first, win the election, and send heartbeats before any other servers time out.

#### Worked Example: Successful Election

```
Cluster: S1, S2, S3, S4, S5
All start as followers in Term 0 (initial state).
Election timeouts: S1=280ms, S2=200ms, S3=250ms, S4=170ms, S5=300ms
```

**Step 1:** S4 has the shortest timeout (170ms). It fires first.
- S4 increments `currentTerm` to 1.
- S4 transitions to candidate.
- S4 votes for itself. (votedFor = S4)
- S4 sends `RequestVote(term=1, candidateId=S4, lastLogIndex=0, lastLogTerm=0)` to S1, S2, S3, S5.

**Step 2:** S1, S2, S3, S5 receive the `RequestVote`.
- None of them have voted yet in Term 1.
- They each update `currentTerm = 1` (if it was lower).
- They each grant their vote to S4 and set `votedFor = S4`.
- They reply `{term: 1, voteGranted: true}`.

**Step 3:** S4 receives 4 yes votes + its own = 5 votes. Majority is 3. Election won!
- S4 transitions to **leader**.
- S4 immediately sends empty `AppendEntries` (heartbeat) to S1, S2, S3, S5.
- All other servers reset their election timers upon receiving heartbeats.

**Result:** S4 is leader for Term 1. Normal operations begin.

#### Worked Example: Split Vote and Re-election

```
Cluster: S1, S2, S3, S4, S5
All are followers. Current leader crashed.
Election timeouts: S1=155ms, S2=300ms, S3=158ms, S4=250ms, S5=290ms
```

**Step 1:** S1 and S3 timeout almost simultaneously (155ms and 158ms).
- S1 increments to Term 2, votes for itself, sends `RequestVote` to all.
- S3 increments to Term 2 (a few ms later), votes for itself, sends `RequestVote` to all.

**Step 2:** Votes arrive and get split:
- S2 receives S1's `RequestVote` first → votes for S1. (votedFor = S1 for Term 2)
- S4 receives S3's `RequestVote` first → votes for S3. (votedFor = S3 for Term 2)
- S5 receives S1's `RequestVote` first → votes for S1.
- S1 has: S1 (self) + S2 + S5 = 3 votes. **Majority!** S1 wins.
- S3 has: S3 (self) + S4 = 2 votes. Not enough.

*Wait — in this case S1 actually gets majority.* Let's adjust for a true split:

```
Election timeouts: S1=155ms, S2=300ms, S3=156ms, S4=250ms, S5=290ms
```

**Step 1:** S1 and S3 timeout almost simultaneously.
- Both increment to Term 2 and vote for themselves.

**Step 2:** Votes split:
- S2 receives S3's `RequestVote` first → votes for S3.
- S4 receives S1's `RequestVote` first → votes for S1.
- S5 receives S3's `RequestVote` first → votes for S3.
- S1 has: S1 + S4 = 2 votes. Not majority.
- S3 has: S3 + S2 + S5 = 3 votes. **S3 wins!**

*For a true split with 5 nodes, one side always gets 3. Let's show a true split scenario that requires re-election:*

```
Cluster: S1, S2, S3, S4 (4-node cluster, majority = 3)
S1 and S2 both timeout simultaneously, both become candidates for Term 2.
```

- S1 votes for itself, S2 votes for itself.
- S3 receives S1's request first → votes for S1.
- S4 receives S2's request first → votes for S2.
- S1 has 2 votes (S1, S3). S2 has 2 votes (S2, S4). **Neither has majority (3).**
- Both candidates' election timers expire.
- S1 picks new timeout = 220ms, S2 picks new timeout = 180ms.
- S2 fires first for Term 3, sends `RequestVote(term=3)` to all.
- S1, S3, S4 haven't started Term 3 yet, so they update their terms and vote for S2.
- S2 gets 4 votes. **S2 is leader for Term 3.**

> The randomized timeout is the key: after a split, the next election is unlikely to split again because one candidate will almost certainly start before the other.

#### Worked Example: Stale Leader Discovery

```
Cluster: S1 (leader, term=3), S2, S3, S4, S5 (all followers, term=3)
```

S1 gets network-partitioned from S2-S5. S1 can't send heartbeats.

- S2-S5 don't receive heartbeats. S3's timeout fires first.
- S3 becomes candidate for Term 4. Gets votes from S2, S4, S5. S3 becomes leader (Term 4).
- Meanwhile, S1 is still operating as leader for Term 3 (but can't reach anyone).
- Network heals. S1 sends an `AppendEntries(term=3)` to S2.
- S2 responds with `{term: 4, success: false}`.
- S1 sees `term 4 > currentTerm 3`. **S1 immediately steps down to follower and updates `currentTerm = 4`.**

| A stale leader will always discover it's been replaced the moment it communicates with any server that has moved on to a higher term.

---

## Log Replication (Detailed)

Once a leader is elected, it begins servicing client requests. Each client request contains a command to be executed by the replicated state machines.

**The Flow:**

1. Leader receives a command from a client.
2. Leader appends the command to its own log as a new entry (with the current term number).
3. Leader issues `AppendEntries` RPCs **in parallel** to all other servers to replicate the entry.
4. Once the entry has been **safely replicated** (on a majority of servers), the leader **commits** the entry.
5. Leader applies the committed entry to its own state machine and returns the result to the client.
6. Followers learn about committed entries via the `leaderCommit` field in subsequent `AppendEntries` RPCs and apply them to their own state machines.

**What does a log entry look like?**

Each log entry contains:
- The **command** for the state machine (e.g., `SET x = 5`)
- The **term number** when the entry was received by the leader
- A **log index** (integer position in the log, starting from 1)

```
Log Entry:  { index: 3, term: 2, command: "SET x = 5" }
```

### The Commitment Rule

| A log entry is **committed** once the leader that created it has replicated it on a **majority** of the servers. Committing an entry also commits all preceding entries in the leader's log, including entries created by previous leaders.

The leader keeps track of the highest index it knows to be committed (`commitIndex`), and includes it in future `AppendEntries` RPCs so that followers eventually find out and apply entries up to that index.

### The Log Matching Property

Raft maintains two crucial properties that together form the **Log Matching Property**:

1. **If two entries in different logs have the same index and term, they store the same command.**
    - This follows from the fact that a leader creates at most one entry with a given log index in a given term, and log entries never change their position in the log.

2. **If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.**
    - Guaranteed by a consistency check in `AppendEntries`. When sending an `AppendEntries` RPC, the leader includes the `prevLogIndex` and `prevLogTerm` (the index and term of the entry immediately preceding the new entries). If the follower does not find an entry matching those, it **refuses** the new entries.
    - This acts as an **induction step**: the initial empty logs satisfy the property, and the consistency check preserves it whenever logs are extended.

#### Worked Example: Normal Log Replication

```
Cluster: S1 (leader, term=2), S2, S3, S4, S5 (followers)
All logs currently have 3 committed entries:

S1: [ (1,1,"SET x=1"), (2,1,"SET y=2"), (3,2,"SET x=3") ]  commitIndex=3
S2: [ (1,1,"SET x=1"), (2,1,"SET y=2"), (3,2,"SET x=3") ]  commitIndex=3
S3: [ (1,1,"SET x=1"), (2,1,"SET y=2"), (3,2,"SET x=3") ]  commitIndex=3
S4: [ (1,1,"SET x=1"), (2,1,"SET y=2"), (3,2,"SET x=3") ]  commitIndex=3
S5: [ (1,1,"SET x=1"), (2,1,"SET y=2"), (3,2,"SET x=3") ]  commitIndex=3

Format: (index, term, command)
```

**Client sends `SET z=7` to S1.**

**Step 1:** S1 appends to its own log:
```
S1: [ (1,1), (2,1), (3,2), (4,2,"SET z=7") ]  commitIndex=3
```

**Step 2:** S1 sends `AppendEntries` to all followers:
```
AppendEntries(
  term=2, leaderId=S1,
  prevLogIndex=3, prevLogTerm=2,    ← "your last entry should be index 3, term 2"
  entries=[(4,2,"SET z=7")],
  leaderCommit=3
)
```

**Step 3:** Each follower checks: "Do I have an entry at index 3 with term 2?" YES.
- They append the new entry and reply `{term: 2, success: true}`.

**Step 4:** S1 receives success from S2, S3, S4 (3 followers + itself = 4 servers have entry 4). Majority is 3. Entry 4 is **committed**.
- S1 updates `commitIndex = 4`.
- S1 applies `SET z=7` to its state machine, returns result to client.

**Step 5:** Next heartbeat (or next `AppendEntries`) carries `leaderCommit=4`. Followers see `leaderCommit > commitIndex`, so they update their `commitIndex` to 4 and apply `SET z=7` to their state machines.

### Handling Log Inconsistencies

During normal operations, logs stay consistent. But **leader crashes can leave logs inconsistent**. A follower might:
- Be **missing entries** the leader has (it was offline during some rounds)
- Have **extra uncommitted entries** the leader doesn't have (it was a leader in a past term that didn't finish replicating)
- **Both** — missing some entries AND having extraneous entries

| The leader handles all inconsistencies by **forcing the followers' logs to duplicate its own**. Conflicting entries in follower logs will be overwritten with entries from the leader's log.

**The Mechanism (nextIndex):**

For each follower, the leader maintains a `nextIndex` — the index of the next log entry to send to that follower. When a leader first comes to power, it initializes all `nextIndex` values to the index just after the last one in its log.

If a follower's log is inconsistent with the leader's, the `AppendEntries` consistency check will fail on the next `AppendEntries` RPC. After a rejection, the leader **decrements `nextIndex`** for that follower and retries. Eventually `nextIndex` reaches a point where the leader and follower logs agree, the `AppendEntries` succeeds, and any conflicting entries in the follower's log are removed and replaced with the leader's entries.

#### Worked Example: Repairing an Inconsistent Follower

```
S1 (new leader, Term 4) has log:
  Index:  1    2    3    4    5    6
  Term:   1    1    2    3    3    4
  Cmd:    a    b    c    d    e    f

S3 (follower, was offline for a while) has log:
  Index:  1    2    3
  Term:   1    1    2
  Cmd:    a    b    c

S1's nextIndex[S3] is initialized to 7 (leader's last index + 1).
```

**Round 1:** S1 sends `AppendEntries(prevLogIndex=6, prevLogTerm=4, entries=[])` to S3.
- S3 checks: "Do I have entry at index 6?" NO (my log only goes to index 3).
- S3 replies `{success: false}`.
- S1 decrements `nextIndex[S3]` to 6.

**Round 2:** S1 sends `AppendEntries(prevLogIndex=5, prevLogTerm=3, entries=[(6,4,f)])` to S3.
- S3 checks: "Do I have entry at index 5?" NO.
- S3 replies `{success: false}`.
- S1 decrements `nextIndex[S3]` to 5.

**Round 3:** S1 sends `AppendEntries(prevLogIndex=4, prevLogTerm=3, entries=[(5,3,e),(6,4,f)])` to S3.
- S3 checks: "Do I have entry at index 4?" NO.
- S3 replies `{success: false}`.
- S1 decrements `nextIndex[S3]` to 4.

**Round 4:** S1 sends `AppendEntries(prevLogIndex=3, prevLogTerm=2, entries=[(4,3,d),(5,3,e),(6,4,f)])` to S3.
- S3 checks: "Do I have entry at index 3 with term 2?" YES!
- S3 appends entries 4, 5, 6.
- S3 replies `{success: true}`.
- S1 updates `nextIndex[S3] = 7`, `matchIndex[S3] = 6`.

```
S3 now has log:
  Index:  1    2    3    4    5    6
  Term:   1    1    2    3    3    4
  Cmd:    a    b    c    d    e    f
```

**S3 is now in sync with S1.**

#### Worked Example: Follower With Conflicting (Extraneous) Entries

```
S1 (new leader, Term 3) has log:
  Index:  1    2    3    4
  Term:   1    1    2    3
  Cmd:    a    b    c    f

S4 (follower, was an old leader in term 2) has log:
  Index:  1    2    3    4    5
  Term:   1    1    2    2    2
  Cmd:    a    b    c    d    e
  (entries 4 and 5 were from S4's stint as leader in term 2, never committed)

S1's nextIndex[S4] = 5.
```

**Round 1:** S1 sends `AppendEntries(prevLogIndex=4, prevLogTerm=3, entries=[])` to S4.
- S4 checks: "Entry at index 4 — I have term 2, but leader says term 3." MISMATCH.
- S4 replies `{success: false}`.
- S1 decrements `nextIndex[S4]` to 4.

**Round 2:** S1 sends `AppendEntries(prevLogIndex=3, prevLogTerm=2, entries=[(4,3,f)])` to S4.
- S4 checks: "Entry at index 3 with term 2?" YES.
- S4 sees new entry at index 4 has term 3, but its existing entry at index 4 has term 2. **Conflict!**
- S4 **deletes** entries at index 4 and 5 (and everything after the conflict).
- S4 appends the new entry `(4,3,f)`.
- S4 replies `{success: true}`.

```
S4 now has log:
  Index:  1    2    3    4
  Term:   1    1    2    3
  Cmd:    a    b    c    f
```

**S4's extraneous uncommitted entries have been replaced.** The leader's log always wins.

> This is safe because entries 4 and 5 on S4 were **never committed** — they were never replicated on a majority. If they had been committed, the election restriction (covered in Safety) guarantees S1 would have had them too.

### The AppendEntries RPC — Full Specification

```
Arguments:
  term            — leader's term
  leaderId        — so follower can redirect clients
  prevLogIndex    — index of log entry immediately preceding new ones
  prevLogTerm     — term of prevLogIndex entry
  entries[]       — log entries to store (empty for heartbeat)
  leaderCommit    — leader's commitIndex

Results:
  term            — currentTerm, for leader to update itself
  success         — true if follower contained entry matching prevLogIndex and prevLogTerm

Receiver Implementation:
  1. Reply false if term < currentTerm
  2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
  3. If an existing entry conflicts with a new one (same index but different terms),
     delete the existing entry and all that follow it
  4. Append any new entries not already in the log
  5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
```

### The RequestVote RPC — Full Specification

```
Arguments:
  term            — candidate's term
  candidateId     — candidate requesting vote
  lastLogIndex    — index of candidate's last log entry
  lastLogTerm     — term of candidate's last log entry

Results:
  term            — currentTerm, for candidate to update itself
  voteGranted     — true means candidate received vote

Receiver Implementation:
  1. Reply false if term < currentTerm
  2. If votedFor is null or candidateId, and candidate's log is at
     least as up-to-date as receiver's log, grant vote
```

### Persistent State vs Volatile State

**Persistent State** (saved to stable storage BEFORE responding to any RPC):
- `currentTerm` — latest term server has seen
- `votedFor` — candidateId that received vote in current term (or null)
- `log[]` — log entries

**Volatile State** (on all servers):
- `commitIndex` — index of highest log entry known to be committed (init: 0)
- `lastApplied` — index of highest log entry applied to state machine (init: 0)

**Volatile State** (on leaders only, re-initialized after election):
- `nextIndex[]` — for each server, index of next log entry to send (init: leader's last log index + 1)
- `matchIndex[]` — for each server, index of highest log entry known to be replicated (init: 0)

> `currentTerm`, `votedFor`, and `log[]` MUST be persisted because they are needed for correctness after a crash. If a server crashes and restarts, it must remember what term it was in and who it voted for, otherwise it could vote twice in the same term.

### Rules for All Servers

- If `commitIndex > lastApplied`: increment `lastApplied`, apply `log[lastApplied]` to state machine.
- If RPC request or response contains term `T > currentTerm`: set `currentTerm = T`, convert to follower.

---

## Safety

The leader election and log replication mechanisms described so far are **not sufficient** to guarantee that each state machine executes exactly the same commands in the same order. Consider this danger:

> A follower might be unavailable while the leader commits several log entries, then it could be elected leader and overwrite these entries with new ones. Different state machines would then execute different command sequences. **This would break consensus.**

Raft adds a critical restriction on **which servers may be elected leader** to prevent this. This restriction ensures that the leader for any given term contains all entries committed in previous terms — the **Leader Completeness Property**.

### The Five Safety Guarantees

These properties hold at all times in a correct Raft implementation:

1. **Election Safety:** At most one leader can be elected in a given term.
2. **Leader Append-Only:** A leader never overwrites or deletes entries in its log; it only appends new entries.
3. **Log Matching:** If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
4. **Leader Completeness:** If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
5. **State Machine Safety:** If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

### 5.4.1 Election Restriction

| Raft uses the voting process to prevent a candidate from winning an election unless its log contains all committed entries.

**How it works:**

A candidate must contact a majority of the cluster to be elected. Every committed entry must be present on at least one server in that majority. So if the candidate's log is **at least as up-to-date** as any other log in that majority, it will hold all committed entries.

**"At least as up-to-date" is defined precisely:**

Raft compares the **index and term of the last entries** in the logs of the candidate and the voter:
1. If the logs have last entries with **different terms**, the log with the **later term** is more up-to-date.
2. If the logs end with the **same term**, whichever log is **longer** is more up-to-date.

The `RequestVote` RPC includes the candidate's `lastLogIndex` and `lastLogTerm`. The voter denies its vote if its own log is more up-to-date than the candidate's.

#### Worked Example: Election Restriction Prevents Unsafe Leader

```
Cluster: S1, S2, S3, S4, S5

S1's log (was leader in term 2, replicated entry to majority):
  Index:  1    2    3    4
  Term:   1    1    2    2
  
S2's log:
  Index:  1    2    3    4
  Term:   1    1    2    2

S3's log:
  Index:  1    2    3    4
  Term:   1    1    2    2

S4's log (was offline, missed entries):
  Index:  1    2
  Term:   1    1

S5's log (was offline, missed entries):
  Index:  1    2
  Term:   1    1

Entry at index 3 and 4 (term 2) are committed (on S1, S2, S3 = majority).
S1 (leader) crashes.
```

**S4 tries to become leader for Term 3:**
- S4 sends `RequestVote(term=3, lastLogIndex=2, lastLogTerm=1)` to all.
- S2 checks: "My last entry is (index=4, term=2). S4's last entry is (index=2, term=1). My term 2 > S4's term 1. My log is more up-to-date." → **Vote denied.**
- S3 does the same check → **Vote denied.**
- S5 checks: "My last entry is (index=2, term=1). Same term, same index." → **Vote granted.**
- S4 has: S4 + S5 = 2 votes. **Not majority. S4 cannot become leader.**

**S2 tries to become leader for Term 3:**
- S2 sends `RequestVote(term=3, lastLogIndex=4, lastLogTerm=2)` to all.
- S3: "My last entry is (index=4, term=2). Same." → **Vote granted.**
- S4: "My last entry is (index=2, term=1). S2's term 2 > my term 1. S2 is more up-to-date." → **Vote granted.**
- S5: Same as S4 → **Vote granted.**
- S2 has: S2 + S3 + S4 + S5 = 4 votes. **S2 becomes leader.**

**Result:** S2 has all committed entries. The committed entries at index 3 and 4 are safe. S4 (which was missing committed entries) was correctly prevented from becoming leader.

> The election restriction ensures that any server elected as leader must have a log that is at least as complete as any majority of servers. Since committed entries exist on a majority, the new leader is guaranteed to have them.

### 5.4.2 Committing Entries From Previous Terms

This is one of the **subtlest and most critical** parts of the Raft algorithm.

| A leader knows that an entry from its **current term** is committed once it is stored on a majority of servers. However, a leader **CANNOT** immediately conclude that an entry from a **previous term** is committed just because it is stored on a majority of servers.

**Why?** Because it could be overwritten. Let's walk through the exact scenario from the paper (Figure 8):

#### Worked Example: The Danger of Committing Old-Term Entries (Figure 8)

```
Cluster: S1, S2, S3, S4, S5

Timeline:

(a) Term 2: S1 is leader. S1 creates entry at index 2 with term 2.
    S1 replicates it to S2 only (partial replication), then crashes.

    S1: [(1,1), (2,2)]
    S2: [(1,1), (2,2)]
    S3: [(1,1)]
    S4: [(1,1)]
    S5: [(1,1)]

(b) Term 3: S5 is elected leader (gets votes from S3, S4, and itself).
    S5 can win because:
    - S5's last entry: (index=1, term=1)
    - S3's last entry: (index=1, term=1) — same, so vote granted
    - S4's last entry: (index=1, term=1) — same, so vote granted
    S5 receives a client request and creates entry at index 2 with term 3.
    S5 crashes before replicating to anyone.

    S1: [(1,1), (2,2)]          (crashed, still has old entry)
    S2: [(1,1), (2,2)]
    S3: [(1,1)]
    S4: [(1,1)]
    S5: [(1,1), (2,3)]          (new entry from term 3)

(c) Term 4: S1 restarts, gets elected leader for term 4.
    S1 continues replicating its old entry (2,2) to S3.
    
    S1: [(1,1), (2,2), (3,4)]   (also appends new entry from term 4)
    S2: [(1,1), (2,2)]
    S3: [(1,1), (2,2)]           (received old entry from S1)
    S4: [(1,1)]
    S5: [(1,1), (2,3)]

    At this point, entry (2,2) is on S1, S2, S3 = MAJORITY.
    BUT IS IT SAFE TO CALL IT COMMITTED?

(d) DANGER PATH: If S1 crashes NOW and S5 gets elected for term 5:
    S5's last entry: (index=2, term=3)
    S5 asks S2 for vote: S2's last entry is (index=2, term=2). term 3 > term 2. Vote granted.
    S5 asks S3: same logic. Vote granted.
    S5 asks S4: S4's last entry is (index=1, term=1). Vote granted.
    S5 gets majority (S3, S4, S5 + itself or S2, S4, S5).
    S5 becomes leader and **overwrites** entry at index 2 on all servers with (2,3).
    
    THE ENTRY (2,2) THAT WAS ON A MAJORITY IS NOW GONE.

(e) SAFE PATH: If instead S1 had replicated a NEW entry from its current term (term 4) 
    to a majority BEFORE crashing:
    
    S1: [(1,1), (2,2), (3,4)]
    S2: [(1,1), (2,2), (3,4)]    (received entry 3 from term 4)
    S3: [(1,1), (2,2), (3,4)]    (received entry 3 from term 4)
    S4: [(1,1)]
    S5: [(1,1), (2,3)]

    Now S5 tries to get elected:
    S5's last entry: (index=2, term=3)
    S2's last entry: (index=3, term=4). term 4 > term 3. S2 denies vote.
    S3's last entry: (index=3, term=4). term 4 > term 3. S3 denies vote.
    S5 can only get votes from S4 and itself = 2. NOT majority.
    S5 CANNOT become leader. Entry (2,2) is NOW safe = committed.
```

**The Rule (critical for implementation):**

| A leader can only commit entries from its **current term** by counting replicas. Once a current-term entry is committed, all preceding entries are **indirectly committed** as well (by the Log Matching Property). The leader NEVER commits an old-term entry by counting replicas alone.

In code terms, the leader's commit rule is:

```
if there exists N such that:
    N > commitIndex AND
    a majority of matchIndex[i] >= N AND
    log[N].term == currentTerm       ← THIS CHECK IS CRUCIAL
then:
    set commitIndex = N
```

The `log[N].term == currentTerm` condition is what prevents the scenario in (d) above.

### 5.4.3 Safety Argument (Leader Completeness Proof Sketch)

The paper proves the **Leader Completeness Property** by contradiction. Here's the intuition:

**Claim:** If a log entry is committed in term T, then all leaders in terms > T will contain that entry.

**Proof Sketch:**

1. Assume for contradiction that leader U (term U > T) does NOT contain the committed entry from term T.
2. The entry was committed by leader T on a majority. Leader U received votes from a majority. These two majorities **must overlap** (pigeonhole principle). Call the overlapping server "the voter."
3. The voter accepted the entry from leader T before voting for leader U (otherwise it would have rejected T's `AppendEntries` due to having a higher term).
4. The voter still had the entry when it voted for U (leaders don't remove entries; followers only remove entries that conflict with the leader; intervening leaders contained the entry by assumption).
5. The voter voted for U, so U's log must have been at least as up-to-date as the voter's. But the voter had the committed entry and U didn't. **Contradiction.**

| This proves that once an entry is committed, every future leader will have it. Combined with the fact that servers apply entries in log-index order, this guarantees the **State Machine Safety Property**: no two servers will ever apply different commands at the same log index.

#### Worked Example: Why the Proof Works in Practice

```
Term 2: S1 is leader. Commits entry E at index 5 on S1, S2, S3 (majority).
Term 3: Election. Any candidate must get 3 votes (majority of 5).

Possible voters: S1, S2, S3, S4, S5

S4 and S5 do NOT have entry E. 
So S4/S5 together = 2 votes. Not enough to elect anyone alone.
Any winning candidate MUST get at least one vote from {S1, S2, S3}.
Those servers have entry E at index 5, term 2.

If a candidate doesn't have entry E:
  - Its last log entry would be earlier/smaller than index 5 with term 2.
  - S1/S2/S3 would deny the vote (their log is more up-to-date).
  
So the candidate must have entry E (or a more recent entry at that index) 
to get any vote from the majority that has E.

Therefore: the new leader for term 3 MUST have entry E. QED.
```

---

## Follower and Candidate Crashes

Follower and candidate crashes are much simpler than leader crashes. They are both handled the same way:

- If a follower or candidate crashes, future `RequestVote` and `AppendEntries` RPCs sent to it will **fail**.
- Raft handles these failures by **retrying indefinitely**. When the crashed server restarts, the RPC will complete successfully.
- If a server crashes **after completing an RPC but before responding**, it will receive the same RPC again after restart. This is fine because **Raft RPCs are idempotent**.

| If a follower receives an `AppendEntries` request that includes log entries already present in its log, it simply ignores those entries in the new request. No harm done.

#### Worked Example: Follower Crash and Recovery

```
Cluster: S1 (leader, term=3), S2, S3, S4, S5

S1 sends AppendEntries with entry (5, 3, "SET x=10") to all followers.

Step 1: S2, S3, S4 respond successfully. S5 crashes mid-RPC.
Step 2: Entry 5 is committed (S1 + S2 + S3 + S4 = 4 servers, majority).
Step 3: S1 sends next heartbeat with leaderCommit=5 to all. S5 is still down — RPC fails.
Step 4: S1 keeps retrying AppendEntries to S5 on each heartbeat interval.
Step 5: S5 restarts. S1's next AppendEntries reaches S5.
         - S5 sees prevLogIndex=4 with the right term. Match!
         - S5 appends entry 5, updates commitIndex to 5.
         - S5 applies entry to state machine.
Step 6: S5 is now fully caught up.
```

> The beauty of this design: the leader doesn't need to do anything special. It just keeps retrying as part of normal operation. Recovery is automatic.

#### Worked Example: Candidate Crash During Election

```
Cluster: S1, S2, S3, S4, S5 (no leader, term=4)

Step 1: S3 times out, becomes candidate for Term 5.
Step 2: S3 sends RequestVote to all. S1 and S2 vote for S3.
Step 3: S3 has 3 votes (S3 + S1 + S2) = majority.
Step 4: S3 crashes BEFORE sending heartbeats to establish leadership.

What happens now?
- S1 and S2 voted for S3 in Term 5, so they can't vote for anyone else in Term 5.
- S4 and S5 never heard from S3.
- Everyone waits. No heartbeats arrive. Election timeouts fire again.
- S4 times out first, becomes candidate for Term 6.
- S4 sends RequestVote(term=6) to all.
- S1, S2, S3 (restarted), S5 all see term 6 > their currentTerm 5.
- They update to term 6, and their votedFor resets for the new term.
- S1, S2, S5 vote for S4. S4 has majority. S4 becomes leader for Term 6.
```

| `votedFor` is scoped to a term. When a server moves to a new term, its `votedFor` resets to null. This is why a candidate crash doesn't permanently deadlock the cluster.

---

## Timing and Availability

| Safety in Raft must **not depend on timing**. The system must never produce incorrect results just because some event happens more quickly or slowly than expected. However, **availability** (the ability to respond to clients) inevitably depends on timing.

If message exchanges take longer than the typical time between server crashes, candidates won't stay up long enough to win an election, and without a steady leader, Raft cannot make progress.

### The Timing Requirement

```
broadcastTime  ≪  electionTimeout  ≪  MTBF
```

- **broadcastTime:** Average time to send RPCs in parallel to every server and receive responses. Typically 0.5ms to 20ms (depends on storage technology since RPCs require persisting to stable storage).
- **electionTimeout:** The election timeout from Section 5.2. Typically 10ms to 500ms.
- **MTBF:** Mean Time Between Failures for a single server. Typically several months or more.

**Why each inequality matters:**

- `broadcastTime ≪ electionTimeout`: Leaders must reliably send heartbeats before followers start elections. Also makes split votes unlikely (randomized timeouts spread across the interval).
- `electionTimeout ≪ MTBF`: The system is unavailable for roughly one `electionTimeout` when a leader crashes. This should be a tiny fraction of overall time.

#### Worked Example: Good vs Bad Timing

```
GOOD:  broadcastTime = 5ms, electionTimeout = 150-300ms, MTBF = 6 months
  - Heartbeats arrive every ~50ms (well within 150ms timeout).
  - After leader crash, new leader elected within ~300ms.
  - System is down for 300ms out of every 6 months = negligible.

BAD:   broadcastTime = 100ms, electionTimeout = 110ms, MTBF = 6 months
  - Heartbeats barely arrive before timeout! Followers keep starting spurious elections.
  - Leader can barely maintain authority.
  - System is constantly disrupted by unnecessary elections.

WORSE: broadcastTime = 5ms, electionTimeout = 150ms, MTBF = 200ms
  - Servers crash every 200ms.
  - System spends 150ms electing a leader who only lives for 50ms before crashing.
  - Almost no useful work gets done.
```

---

## Cluster Membership Changes

In practice, the set of servers in a Raft cluster needs to change — replacing failed servers, changing the degree of replication, etc. Taking the entire cluster offline, updating config files, and restarting is unsafe and leaves the cluster unavailable. Raft automates configuration changes as part of the consensus algorithm.

### The Fundamental Problem

| It is **impossible** to atomically switch all servers from the old configuration to the new configuration at once. During the transition, the cluster could split into two independent majorities — each electing a different leader for the same term.

#### Worked Example: Why Direct Switching is Unsafe

```
Old config (Cold): S1, S2, S3          (majority = 2)
New config (Cnew): S1, S2, S3, S4, S5  (majority = 3)

Servers switch at different times:
  - S1, S2 switch to Cnew early.
  - S3 still uses Cold.

Under Cold: S3 + (any one of S1,S2 who hasn't switched yet) = 2 = majority of 3.
  → S3 could elect itself leader using Cold rules.

Under Cnew: S1 + S2 + S4 = 3 = majority of 5.
  → S1 could elect itself leader using Cnew rules.

TWO LEADERS in the same term. Consensus violated.
```

### The Solution: Joint Consensus

Raft uses a **two-phase approach** with an intermediate configuration called **joint consensus** (`C_old,new`).

**Phase 1: Transition to joint consensus**

The leader creates a special log entry `C_old,new` that combines both the old and new configurations. Once a server adds this entry to its log, it uses `C_old,new` for all future decisions (regardless of whether the entry is committed).

During joint consensus:
- Log entries are replicated to **all** servers in both configurations.
- **Any** server from either configuration may serve as leader.
- Agreement (for elections and entry commitment) requires **separate majorities from BOTH** the old and new configurations.

**Phase 2: Transition to new configuration**

Once `C_old,new` is committed, the leader creates a new log entry `C_new`. Once `C_new` is committed (requiring majority of `C_new`), the old configuration is irrelevant and servers not in the new configuration can be shut down.

| At no point in time can `C_old` and `C_new` both make unilateral decisions independently. This guarantees safety throughout the transition.

#### Worked Example: Safe Cluster Expansion (3 → 5 nodes)

```
Starting state: Cold = {S1, S2, S3}, S1 is leader, term=5
Goal: Add S4, S5 to make Cnew = {S1, S2, S3, S4, S5}

Step 1: Admin sends reconfiguration request to leader S1.

Step 2: S1 creates log entry C_old,new = {Cold={S1,S2,S3}, Cnew={S1,S2,S3,S4,S5}}.
  S1 appends it to its own log.
  S1 immediately starts using C_old,new rules:
    - Must get majority of Cold (≥2 of {S1,S2,S3}) AND
    - Must get majority of Cnew (≥3 of {S1,S2,S3,S4,S5})
  S1 replicates C_old,new to S2, S3, S4, S5.

Step 3: C_old,new is committed when:
  - Majority of Cold has it: S1 + S2 = 2 of 3 ✓
  - Majority of Cnew has it: S1 + S2 + S3 = 3 of 5 ✓
  (Both conditions satisfied.)

Step 4: S1 now creates log entry Cnew = {S1, S2, S3, S4, S5}.
  S1 replicates Cnew to all.

Step 5: Cnew is committed when majority of Cnew has it:
  S1 + S2 + S4 = 3 of 5 ✓

Step 6: Configuration change complete.
  - Cold is irrelevant.
  - Any server not in Cnew can be shut down (none in this example).
  - Future elections and commits use Cnew rules (majority of 5 = 3).
```

### Three Additional Issues for Reconfiguration

**Issue 1: New servers have no log entries initially.**

If new servers are added to the cluster immediately, they need time to catch up. During this time, it might be impossible to commit new entries (the new servers can't form a majority until caught up).

**Solution:** New servers join the cluster as **non-voting members** first. The leader replicates log entries to them, but they don't count toward majorities. Once caught up, the actual configuration change begins.

**Issue 2: The leader might not be in the new configuration.**

The leader might be removing itself from the cluster. In this case, it steps down (transitions to follower) once `C_new` is committed. Up until that point, it manages the cluster including replicating to servers not in `C_new` but in `C_old`.

| There is a period where the leader is managing a configuration that doesn't include itself. It replicates entries but doesn't count itself toward `C_new` majorities.

**Issue 3: Removed servers can disrupt the cluster.**

Servers not in `C_new` won't receive heartbeats, so they'll time out and start elections with incremented terms. These could cause the current leader to step down.

**Solution:** Servers don't start an election (and don't vote for a candidate either) if they believe a current leader exists — specifically, if they received a heartbeat within the minimum election timeout. This prevents disruption without affecting normal elections.

#### Worked Example: Leader Stepping Down After Reconfiguration

```
Cold = {S1, S2, S3, S4, S5}, S1 is leader
Cnew = {S2, S3, S4, S5, S6}  (S1 removed, S6 added)

Step 1: S1 creates C_old,new entry. Replicates to all servers (including S6, a new node catching up).

Step 2: C_old,new committed (majority of Cold AND majority of Cnew agree).

Step 3: S1 creates Cnew entry. Replicates to S2, S3, S4, S5, S6.
  S1 does NOT count itself toward Cnew majority.
  Cnew needs majority of 5 in {S2,S3,S4,S5,S6}: 
  S2 + S3 + S4 = 3 ✓ — committed.

Step 4: Cnew committed. S1 steps down to follower.
  S1 is no longer part of the cluster.
  One of {S2, S3, S4, S5, S6} will time out and become leader.
```

---

## Log Compaction (Snapshotting)

As the system runs, the log grows without bound. This creates problems:
- Takes up more disk space.
- Takes longer to replay on server restart.
- Eventually exhausts storage capacity.

Raft uses **snapshotting** as its compaction mechanism: the entire current state is written to a snapshot on stable storage, and all log entries up to that point are discarded.

### How Snapshots Work

Each server takes snapshots **independently** (not coordinated by the leader), covering just the **committed entries** in its log.

**A snapshot contains:**
- The **state machine's current state** (the bulk of the snapshot — e.g., the current values of all key-value pairs).
- **Metadata:**
    - `lastIncludedIndex` — the index of the last entry in the log that the snapshot replaces.
    - `lastIncludedTerm` — the term of that entry.
    - The **latest cluster configuration** as of `lastIncludedIndex`.

These metadata are needed to support the `AppendEntries` consistency check for the first log entry after the snapshot.

Once a snapshot is complete, the server deletes all log entries up through `lastIncludedIndex` and any prior snapshot.

#### Worked Example: Taking a Snapshot

```
Server S2 has been running for a while. Its log:

  Index:  1    2    3    4    5    6    7    8    9    10
  Term:   1    1    1    2    2    2    3    3    3    3
  Cmd:    SET  SET  SET  SET  DEL  SET  SET  SET  SET  SET
          x=1  y=2  z=3  x=5  y    z=7  a=1  b=2  x=9  c=4

commitIndex = 10, lastApplied = 10
State Machine state: { x=9, z=7, a=1, b=2, c=4 }
(y was deleted at index 5)

S2 decides to snapshot (log has grown past threshold).

Snapshot:
  state: { x=9, z=7, a=1, b=2, c=4 }
  lastIncludedIndex: 10
  lastIncludedTerm: 3
  config: {S1, S2, S3, S4, S5}

After snapshot:
  Log: [ ]  (empty — all entries were covered by the snapshot)
  Next entry will be at index 11.

Disk savings: 10 log entries → 1 compact state snapshot.
```

### The InstallSnapshot RPC

Sometimes a leader needs to send a snapshot to a follower that has fallen so far behind that the leader has already discarded the log entries the follower needs. This happens when:

- A follower was offline for a long time.
- A new server joins and is very behind.
- The leader's log has been compacted past the follower's position.

The leader sends the snapshot using the **InstallSnapshot RPC**, which sends the snapshot in chunks.

```
InstallSnapshot RPC:

Arguments:
  term                — leader's term
  leaderId            — so follower can redirect clients
  lastIncludedIndex   — the snapshot replaces all entries up through this index
  lastIncludedTerm    — term of lastIncludedIndex
  offset              — byte offset where chunk is positioned in the snapshot file
  data[]              — raw bytes of the snapshot chunk
  done                — true if this is the last chunk

Results:
  term                — currentTerm, for leader to update itself

Receiver Implementation:
  1. Reply immediately if term < currentTerm
  2. Create new snapshot file if first chunk (offset is 0)
  3. Write data at given offset into snapshot file
  4. Reply and wait for more chunks if done is false
  5. Save snapshot file, discard any existing snapshot with a smaller index
  6. If existing log entry has same index and term as snapshot's last included entry,
     retain log entries following it and reply
  7. Discard the entire log
  8. Reset state machine using snapshot contents
```

#### Worked Example: Leader Sends Snapshot to Lagging Follower

```
S1 (leader, term=5) has taken a snapshot:
  Snapshot: lastIncludedIndex=100, lastIncludedTerm=4
  Log: [(101,5,cmd), (102,5,cmd), (103,5,cmd)]
  
S3 (follower) has been offline, just came back:
  Log: [(1,1), (2,1), ..., (50,2)]   (only has entries up to index 50)

S1's nextIndex[S3] = 104.
S1 sends AppendEntries(prevLogIndex=103, ...) → S3 says no.
S1 decrements nextIndex... but eventually needs entries before index 101.
S1 no longer HAS those entries (compacted away).

S1 sends InstallSnapshot to S3:
  term=5, lastIncludedIndex=100, lastIncludedTerm=4
  data = [snapshot bytes in chunks]

S3 receives the snapshot:
  - Discards its entire log (entries 1-50 are all before the snapshot's index 100).
  - Loads the snapshot's state into its state machine.
  - Sets lastIncludedIndex=100, lastIncludedTerm=4.
  
S1 now sends AppendEntries(prevLogIndex=100, prevLogTerm=4, entries=[(101,5),(102,5),(103,5)]).
S3 checks: "Do I have entry at index 100 with term 4?" Yes (from snapshot metadata).
S3 appends entries 101, 102, 103. Now fully caught up.
```

### Snapshot Policies

- **When to snapshot:** A simple strategy is to snapshot when the log reaches a fixed size in bytes. If this size is significantly larger than the expected snapshot size, the disk bandwidth overhead is small.
- **Performance:** Writing a snapshot can be slow. Use **copy-on-write** techniques (e.g., `fork()` on Linux) so new updates can be accepted while the snapshot is being written.

> Each server snapshots independently. This is better than having the leader send snapshots to everyone because: (1) it saves network bandwidth, and (2) it keeps the leader's implementation simpler.

---

## Client Interaction

### Finding the Leader

Clients send all requests to the leader. When a client first starts up, it connects to a randomly-chosen server:
- If that server **is** the leader, it handles the request.
- If that server **is not** the leader, it rejects the request and supplies the network address of the most recent leader it knows about (from `AppendEntries` requests, which include the leader's address).
- If the leader crashes, client requests time out. The client then tries again with randomly-chosen servers.

### Linearizable Semantics

| Linearizability means each operation appears to execute **instantaneously, exactly once**, at some point between its invocation and its response. This is the strongest consistency guarantee.

**Problem: Duplicate Execution**

If the leader crashes **after committing** a log entry but **before responding** to the client, the client will retry the command with the new leader. The command gets executed a second time.

**Solution: Client-Assigned Serial Numbers**

- Each client assigns a **unique serial number** to every command.
- The state machine tracks the **latest serial number processed** for each client, along with the associated response.
- If the state machine receives a command whose serial number has already been executed, it responds immediately **without re-executing** the request.

#### Worked Example: Preventing Duplicate Execution

```
Client C1 sends: { serialNumber: 42, command: "TRANSFER $100 from A to B" }

Step 1: Leader S1 appends to log, replicates to majority, commits.
Step 2: S1 applies command to state machine: A -= 100, B += 100.
Step 3: S1 CRASHES before sending response to C1.

Client C1 times out. Connects to S2 (new leader).
C1 retries: { serialNumber: 42, command: "TRANSFER $100 from A to B" }

Step 4: S2 appends to log, replicates, commits.
Step 5: S2's state machine checks: "serialNumber 42 for client C1 — already processed!"
Step 6: S2 returns the CACHED response from the first execution.
         A and B balances remain correct. No double-transfer.
```

### Read-Only Operations (Stale Read Prevention)

Read-only operations can be handled **without writing anything to the log**. But without precautions, a leader might return stale data — it might have been deposed by a newer leader it doesn't know about yet.

**Two precautions for linearizable reads:**

1. **No-op on term start:** A leader must know which entries are committed when it takes office. The Leader Completeness Property guarantees it has all committed entries, but it may not know which are committed. So the leader commits a **blank no-op entry** at the start of its term to establish its commit point.

2. **Heartbeat check before reads:** Before responding to a read-only request, the leader exchanges heartbeat messages with a majority of the cluster. If a majority responds, the leader knows it hasn't been deposed.

| Without these precautions, a partitioned leader could serve stale reads indefinitely, violating linearizability.

#### Worked Example: Stale Read Prevention

```
S1 was leader (term=5). Network partition isolates S1 from S2-S5.
S3 becomes new leader for term 6. Client writes "SET x=99" through S3. Committed.

Meanwhile, S1 still thinks it's leader. Client C2 connects to S1, asks "GET x".
S1's state machine has x=50 (old value).

WITHOUT precaution: S1 returns x=50. WRONG. Stale data.

WITH heartbeat check:
  S1 tries to send heartbeats to majority before responding to C2.
  S1 can't reach S2-S5 (partitioned). Heartbeat check fails.
  S1 does NOT respond to C2. C2 times out, tries another server.
  C2 reaches S3 (actual leader). S3 returns x=99. CORRECT.
```

---

## Summary of All RPC Types

| RPC | Initiated By | Purpose |
|-----|-------------|---------|
| `RequestVote` | Candidates | Gather votes during election |
| `AppendEntries` | Leaders | Replicate log entries + heartbeats |
| `InstallSnapshot` | Leaders | Send snapshot to lagging followers |

---

## Summary of Server State Transitions

```
                    times out,              receives votes from
                    starts election         majority of servers
  ┌──────────┐    ┌──────────────┐        ┌──────────┐
  │ Follower │───►│  Candidate   │───────►│  Leader  │
  └──────────┘    └──────────────┘        └──────────┘
       ▲               │    │                   │
       │               │    │                   │
       │   discovers   │    │   discovers       │
       │   leader or   │    │   higher          │
       │   higher term │    │   term            │
       │◄──────────────┘    │                   │
       │◄───────────────────┴───────────────────┘
       │         (higher term discovered)
```

---
---

# End-to-End Implementation Specification

This specification is designed to be followed top-to-bottom when building a Raft implementation. Each section maps directly to a module/component in the final system.

## Spec 1: Data Structures

### 1.1 Log Entry

```
struct LogEntry {
    index: u64          // 1-indexed position in the log
    term: u64           // term when entry was received by leader
    command: bytes      // serialized command for the state machine
}
```

### 1.2 Server Persistent State

These MUST be flushed to durable storage BEFORE responding to any RPC.

```
struct PersistentState {
    currentTerm: u64    // latest term server has seen (init: 0)
    votedFor: Option<ServerId>  // candidateId voted for in current term (init: null)
    log: Vec<LogEntry>  // log entries (first index is 1)
}
```

### 1.3 Server Volatile State

```
struct VolatileState {
    commitIndex: u64    // index of highest log entry known committed (init: 0)
    lastApplied: u64    // index of highest log entry applied to state machine (init: 0)
    role: Role          // Follower | Candidate | Leader
    leaderId: Option<ServerId>  // who the current leader is (for client redirects)
}
```

### 1.4 Leader Volatile State (re-initialized after each election)

```
struct LeaderState {
    nextIndex: Map<ServerId, u64>   // for each peer: next log index to send (init: last log index + 1)
    matchIndex: Map<ServerId, u64>  // for each peer: highest replicated index (init: 0)
}
```

### 1.5 Snapshot Metadata

```
struct SnapshotMetadata {
    lastIncludedIndex: u64
    lastIncludedTerm: u64
    config: ClusterConfig
}
```

### 1.6 Cluster Configuration

```
struct ClusterConfig {
    servers: Set<ServerId>          // current member set
    joint: Option<Set<ServerId>>    // if non-null, we are in joint consensus (this is C_new)
}
```

---

## Spec 2: RPC Definitions

### 2.1 RequestVote

```
RequestVoteRequest {
    term: u64               // candidate's term
    candidateId: ServerId   // candidate requesting vote
    lastLogIndex: u64       // index of candidate's last log entry
    lastLogTerm: u64        // term of candidate's last log entry
}

RequestVoteResponse {
    term: u64               // currentTerm, for candidate to update itself
    voteGranted: bool       // true = vote granted
}
```

### 2.2 AppendEntries

```
AppendEntriesRequest {
    term: u64               // leader's term
    leaderId: ServerId      // so follower can redirect clients
    prevLogIndex: u64       // index of log entry immediately preceding new ones
    prevLogTerm: u64        // term of prevLogIndex entry
    entries: Vec<LogEntry>  // entries to store (empty for heartbeat)
    leaderCommit: u64       // leader's commitIndex
}

AppendEntriesResponse {
    term: u64               // currentTerm, for leader to update itself
    success: bool           // true if follower contained matching prevLogIndex/prevLogTerm

    // OPTIMIZATION: fast log backtracking (optional but recommended)
    conflictTerm: Option<u64>   // term of the conflicting entry (if any)
    conflictIndex: Option<u64>  // first index of conflictTerm in follower's log
}
```

### 2.3 InstallSnapshot

```
InstallSnapshotRequest {
    term: u64
    leaderId: ServerId
    lastIncludedIndex: u64
    lastIncludedTerm: u64
    offset: u64             // byte offset into snapshot file
    data: bytes             // raw snapshot chunk
    done: bool              // true if last chunk
}

InstallSnapshotResponse {
    term: u64
}
```

### 2.4 Client Request / Response

```
ClientRequest {
    clientId: ClientId
    serialNumber: u64       // unique per-client monotonic sequence number
    command: bytes           // command for the state machine
}

ClientResponse {
    success: bool
    result: bytes           // result from state machine (if success)
    leaderHint: Option<ServerId>  // redirect hint if this server is not the leader
}
```

---

## Spec 3: Core Algorithm — Event Loop

The server runs a single-threaded event loop (or equivalent async model) that reacts to:

1. **Timers:** Election timeout, heartbeat interval
2. **Incoming RPCs:** RequestVote, AppendEntries, InstallSnapshot
3. **Client requests:** (only processed if leader)
4. **Commit advancement:** apply committed-but-not-applied entries

### 3.1 All Servers — Global Rules (run continuously)

```
ON every tick:
    if commitIndex > lastApplied:
        lastApplied += 1
        apply(log[lastApplied]) to state machine
        if this entry was a client command and we are leader:
            send response to client

ON any RPC request or response with term T:
    if T > currentTerm:
        currentTerm = T
        votedFor = null
        convert to Follower
        persist(currentTerm, votedFor)
```

### 3.2 Follower Behaviour

```
ON receiving AppendEntries RPC:
    → run AppendEntries handler (Spec 4.2)
    → reset election timer

ON receiving RequestVote RPC:
    → run RequestVote handler (Spec 4.1)

ON election timeout elapsed (no heartbeat or vote grant received):
    → convert to Candidate (Spec 3.3)

ON receiving client request:
    → reject, return leaderHint
```

### 3.3 Candidate Behaviour

```
ON conversion to Candidate:
    currentTerm += 1
    votedFor = self
    persist(currentTerm, votedFor)
    reset election timer (randomized: 150-300ms)
    votesReceived = {self}
    send RequestVote RPC to all other servers

ON receiving RequestVoteResponse:
    if response.term > currentTerm:
        → step down to Follower (handled by global rule)
    if response.voteGranted:
        votesReceived.add(sender)
        if votesReceived.size() > cluster.size() / 2:
            → convert to Leader (Spec 3.4)

ON receiving AppendEntries RPC with term >= currentTerm:
    → recognize sender as leader
    → convert to Follower
    → process the AppendEntries

ON election timeout elapsed:
    → restart election (re-run "ON conversion to Candidate")
```

### 3.4 Leader Behaviour

```
ON conversion to Leader:
    for each peer:
        nextIndex[peer] = last log index + 1
        matchIndex[peer] = 0
    
    append no-op entry {term: currentTerm, command: NO_OP} to log
    persist(log)
    
    send AppendEntries (heartbeat) to all peers immediately
    start heartbeat timer (e.g., every 50ms)

ON heartbeat timer:
    for each peer:
        send AppendEntries RPC to peer (Spec 5)

ON client request received:
    append new LogEntry {term: currentTerm, command: request.command} to log
    persist(log)
    → entries will be sent to peers on next heartbeat (or immediately)

ON receiving AppendEntriesResponse from peer:
    if response.term > currentTerm:
        → step down to Follower
    if response.success:
        nextIndex[peer] = max(nextIndex[peer], matchIndex update)
        matchIndex[peer] = prevLogIndex + entries.length
        nextIndex[peer] = matchIndex[peer] + 1
        → check if commitIndex can advance (Spec 6)
    else:
        → decrement nextIndex[peer] (use optimization if available)
        → retry AppendEntries to this peer

ON receiving RequestVote RPC:
    → if request.term > currentTerm: step down to Follower, then process
    → otherwise: deny vote (we are the leader)
```

---

## Spec 4: RPC Handlers

### 4.1 RequestVote Handler

```
function handleRequestVote(request) -> RequestVoteResponse:
    // Step 1: reject if stale term
    if request.term < currentTerm:
        return {term: currentTerm, voteGranted: false}
    
    // Step 2: update term if newer
    if request.term > currentTerm:
        currentTerm = request.term
        votedFor = null
        convert to Follower
        persist(currentTerm, votedFor)
    
    // Step 3: check if we can vote for this candidate
    canVote = (votedFor == null OR votedFor == request.candidateId)
    
    // Step 4: check if candidate's log is at least as up-to-date
    myLastLogIndex = log.lastIndex()
    myLastLogTerm = log.lastTerm()
    
    logIsUpToDate = (request.lastLogTerm > myLastLogTerm) OR
                    (request.lastLogTerm == myLastLogTerm AND 
                     request.lastLogIndex >= myLastLogIndex)
    
    if canVote AND logIsUpToDate:
        votedFor = request.candidateId
        persist(votedFor)
        reset election timer    // IMPORTANT: reset timer when granting vote
        return {term: currentTerm, voteGranted: true}
    else:
        return {term: currentTerm, voteGranted: false}
```

### 4.2 AppendEntries Handler

```
function handleAppendEntries(request) -> AppendEntriesResponse:
    // Step 1: reject if stale term
    if request.term < currentTerm:
        return {term: currentTerm, success: false}
    
    // Step 2: update term if needed, recognize leader
    if request.term >= currentTerm:
        if request.term > currentTerm:
            currentTerm = request.term
            votedFor = null
            persist(currentTerm, votedFor)
        convert to Follower
        leaderId = request.leaderId
        reset election timer
    
    // Step 3: consistency check
    if request.prevLogIndex > 0:
        if log.lastIndex() < request.prevLogIndex:
            return {term: currentTerm, success: false, 
                    conflictIndex: log.lastIndex() + 1, conflictTerm: null}
        
        if log[request.prevLogIndex].term != request.prevLogTerm:
            conflictTerm = log[request.prevLogIndex].term
            // find first index of conflictTerm
            conflictIndex = request.prevLogIndex
            while conflictIndex > 1 AND log[conflictIndex - 1].term == conflictTerm:
                conflictIndex -= 1
            // delete conflicting entry and all that follow
            log.truncateFrom(request.prevLogIndex)
            persist(log)
            return {term: currentTerm, success: false,
                    conflictIndex: conflictIndex, conflictTerm: conflictTerm}
    
    // Step 4: append new entries (skip entries already in our log)
    for i, entry in request.entries:
        logIndex = request.prevLogIndex + 1 + i
        if logIndex <= log.lastIndex():
            if log[logIndex].term != entry.term:
                log.truncateFrom(logIndex)
                log.append(entry)
            // else: already have this entry, skip
        else:
            log.append(entry)
    persist(log)
    
    // Step 5: advance commitIndex
    if request.leaderCommit > commitIndex:
        commitIndex = min(request.leaderCommit, log.lastIndex())
    
    return {term: currentTerm, success: true}
```

### 4.3 InstallSnapshot Handler

```
function handleInstallSnapshot(request) -> InstallSnapshotResponse:
    if request.term < currentTerm:
        return {term: currentTerm}
    
    // update term, recognize leader
    if request.term > currentTerm:
        currentTerm = request.term
        votedFor = null
        persist(currentTerm, votedFor)
    convert to Follower
    reset election timer
    
    // handle chunked transfer
    if request.offset == 0:
        create new snapshot file
    write request.data at request.offset in snapshot file
    
    if NOT request.done:
        return {term: currentTerm}  // wait for more chunks
    
    // snapshot complete — apply it
    save snapshot file
    
    if log contains entry at request.lastIncludedIndex 
       with term == request.lastIncludedTerm:
        // retain entries after the snapshot
        log.discardBefore(request.lastIncludedIndex + 1)
    else:
        // discard entire log
        log.clear()
    
    // reset state machine from snapshot
    stateMachine.loadFromSnapshot(snapshotFile)
    lastApplied = request.lastIncludedIndex
    commitIndex = request.lastIncludedIndex
    
    persist(log)
    return {term: currentTerm}
```

---

## Spec 5: Leader — Sending AppendEntries to a Peer

```
function sendAppendEntries(peerId):
    nextIdx = nextIndex[peerId]
    
    // Check if we need to send a snapshot instead
    if nextIdx <= snapshot.lastIncludedIndex:
        → send InstallSnapshot RPC to peer
        return
    
    prevLogIndex = nextIdx - 1
    prevLogTerm = 0
    if prevLogIndex > 0:
        if prevLogIndex == snapshot.lastIncludedIndex:
            prevLogTerm = snapshot.lastIncludedTerm
        else:
            prevLogTerm = log[prevLogIndex].term
    
    entries = log[nextIdx .. log.lastIndex()]   // all entries from nextIdx onwards
    
    send AppendEntriesRequest {
        term: currentTerm,
        leaderId: self,
        prevLogIndex: prevLogIndex,
        prevLogTerm: prevLogTerm,
        entries: entries,
        leaderCommit: commitIndex
    } to peerId
```

### Handling AppendEntries Response

```
function handleAppendEntriesResponse(peerId, request, response):
    if response.term > currentTerm:
        → step down to Follower (global rule)
        return
    
    if response.success:
        matchIndex[peerId] = request.prevLogIndex + request.entries.length
        nextIndex[peerId] = matchIndex[peerId] + 1
        → advanceCommitIndex()
    else:
        // BASIC backtracking:
        nextIndex[peerId] = max(1, nextIndex[peerId] - 1)
        
        // OPTIMIZED backtracking (using conflictTerm/conflictIndex):
        if response.conflictTerm != null:
            // search our log for the last entry of conflictTerm
            lastIndexOfTerm = findLastIndexOfTerm(response.conflictTerm)
            if lastIndexOfTerm != null:
                nextIndex[peerId] = lastIndexOfTerm + 1
            else:
                nextIndex[peerId] = response.conflictIndex
        else:
            nextIndex[peerId] = response.conflictIndex
        
        → retry sendAppendEntries(peerId)
```

---

## Spec 6: Commit Index Advancement (Leader Only)

```
function advanceCommitIndex():
    // find the highest N such that:
    //   N > commitIndex AND
    //   log[N].term == currentTerm AND
    //   a majority of matchIndex[i] >= N (including self)
    
    for N = log.lastIndex() down to commitIndex + 1:
        if log[N].term != currentTerm:
            continue    // CRITICAL: only commit entries from current term
        
        replicaCount = 1    // count self
        for each peer:
            if matchIndex[peer] >= N:
                replicaCount += 1
        
        if replicaCount > clusterSize / 2:
            commitIndex = N
            break
```

---

## Spec 7: Election Timer

```
ELECTION_TIMEOUT_MIN = 150   // milliseconds
ELECTION_TIMEOUT_MAX = 300   // milliseconds
HEARTBEAT_INTERVAL   = 50    // milliseconds (must be << ELECTION_TIMEOUT_MIN)

function resetElectionTimer():
    electionTimeout = random(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    electionTimerStart = now()

function isElectionTimedOut():
    return (now() - electionTimerStart) >= electionTimeout
```

> The heartbeat interval must be at least an order of magnitude smaller than the election timeout to prevent spurious elections.

---

## Spec 8: Persistence Layer

```
interface PersistenceLayer {
    // Called before responding to ANY RPC
    function save(currentTerm: u64, votedFor: Option<ServerId>, log: Vec<LogEntry>)
    
    // Called on server startup
    function load() -> (currentTerm, votedFor, log)
    
    // Snapshot persistence
    function saveSnapshot(state: bytes, metadata: SnapshotMetadata)
    function loadSnapshot() -> Option<(bytes, SnapshotMetadata)>
}
```

**Implementation notes:**
- Use write-ahead logging or equivalent to ensure atomicity.
- `fsync()` after each write to guarantee durability.
- Consider batching writes for performance (but never respond to an RPC before the data is persisted).

---

## Spec 9: State Machine Interface

```
interface StateMachine {
    // Apply a committed command
    function apply(command: bytes) -> bytes   // returns result
    
    // Take a snapshot of current state
    function takeSnapshot() -> bytes
    
    // Restore state from a snapshot
    function loadFromSnapshot(data: bytes)
    
    // For linearizable reads: check if command was already executed
    function isDuplicate(clientId: ClientId, serialNumber: u64) -> Option<bytes>
    
    // Register executed command (for deduplication)
    function recordExecution(clientId: ClientId, serialNumber: u64, result: bytes)
}
```

---

## Spec 10: Client-Side Logic

```
function clientSendCommand(command, clusterServers):
    serialNumber = nextSerialNumber++
    targetServer = lastKnownLeader OR random(clusterServers)
    
    loop:
        response = sendRPC(targetServer, ClientRequest {
            clientId: self.id,
            serialNumber: serialNumber,
            command: command
        })
        
        if response == TIMEOUT:
            targetServer = random(clusterServers)
            continue
        
        if response.success:
            lastKnownLeader = targetServer
            return response.result
        
        if response.leaderHint != null:
            targetServer = response.leaderHint
            continue
        
        // no leader known, try random
        targetServer = random(clusterServers)
```

---

## Spec 11: Snapshot Trigger

```
SNAPSHOT_THRESHOLD = 10_000  // bytes (or number of entries)

ON after applying entry to state machine:
    if log.sizeInBytes() > SNAPSHOT_THRESHOLD:
        triggerSnapshot()

function triggerSnapshot():
    snapshotData = stateMachine.takeSnapshot()
    lastIncludedIndex = lastApplied
    lastIncludedTerm = log[lastApplied].term
    config = currentConfig
    
    persistence.saveSnapshot(snapshotData, SnapshotMetadata {
        lastIncludedIndex, lastIncludedTerm, config
    })
    
    log.discardUpTo(lastIncludedIndex)  // delete entries covered by snapshot
```

---

## Spec 12: Cluster Membership Changes (Joint Consensus)

```
function handleConfigChange(newServers: Set<ServerId>):
    // Phase 1: Create joint consensus entry
    jointConfig = ClusterConfig {
        servers: currentConfig.servers,   // C_old
        joint: newServers                 // C_new
    }
    append LogEntry { term: currentTerm, command: CONFIG_CHANGE(jointConfig) }
    replicate to all servers in BOTH C_old and C_new
    
    // Wait for C_old,new to be committed 
    // (requires majority of C_old AND majority of C_new)
    wait until jointConfig entry is committed

    // Phase 2: Create new config entry
    newConfig = ClusterConfig {
        servers: newServers,
        joint: null
    }
    append LogEntry { term: currentTerm, command: CONFIG_CHANGE(newConfig) }
    replicate to all servers in C_new
    
    // Wait for C_new to be committed (majority of C_new)
    wait until newConfig entry is committed
    
    // If leader is not in C_new, step down
    if self NOT in newServers:
        convert to Follower

// Majority calculation during joint consensus:
function isMajority(replicaSet):
    if currentConfig.joint == null:
        // normal mode
        return replicaSet.countIn(currentConfig.servers) > currentConfig.servers.size() / 2
    else:
        // joint consensus mode: need majority in BOTH
        oldMajority = replicaSet.countIn(currentConfig.servers) > currentConfig.servers.size() / 2
        newMajority = replicaSet.countIn(currentConfig.joint) > currentConfig.joint.size() / 2
        return oldMajority AND newMajority
```

---

## Spec 13: Implementation Milestones (Suggested Build Order)

Building Raft incrementally in this order lets you test each piece in isolation before moving on:

### Milestone 1: Leader Election
- Implement server state (Follower/Candidate/Leader), persistent state, election timer.
- Implement `RequestVote` RPC (send + handle).
- Implement term management and state transitions.
- Test: cluster of 5 can elect a leader from cold start; re-elects when leader dies.

### Milestone 2: Log Replication
- Implement `AppendEntries` RPC (send + handle), including consistency check.
- Implement `nextIndex` / `matchIndex` tracking and decrement-on-failure.
- Implement `commitIndex` advancement with the `log[N].term == currentTerm` guard.
- Test: leader can replicate entries to followers; lagging followers catch up.

### Milestone 3: Persistence & Safety
- Implement durable storage for `currentTerm`, `votedFor`, `log`.
- Verify election restriction (vote denial for stale logs).
- Test: crash a server, restart it, verify it recovers correctly and doesn't double-vote.

### Milestone 4: Client Interaction
- Implement client request handling (append, replicate, commit, respond).
- Implement client-side serial numbers and server-side deduplication.
- Implement leader redirect.
- Test: client can send commands, get responses; commands survive leader crashes.

### Milestone 5: Log Compaction
- Implement snapshotting (take + restore).
- Implement `InstallSnapshot` RPC.
- Test: snapshot a server, delete old log entries, verify new entries still work; verify lagging follower receives snapshot.

### Milestone 6: Cluster Membership Changes
- Implement joint consensus configuration entries.
- Implement dual-majority logic.
- Implement non-voting member catch-up phase.
- Test: add/remove servers while cluster is running; verify safety throughout.

### Milestone 7: Linearizable Reads
- Implement no-op entry on leader election.
- Implement heartbeat-check before read-only requests.
- Test: partitioned old leader does not serve stale reads.

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                     RAFT QUICK REFERENCE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PERSISTENT STATE (persist before responding to RPCs):          │
│    currentTerm, votedFor, log[]                                 │
│                                                                 │
│  VOLATILE STATE:                                                │
│    commitIndex, lastApplied                                     │
│    nextIndex[] (leader only), matchIndex[] (leader only)        │
│                                                                 │
│  RPCS:                                                          │
│    RequestVote      — candidates → all servers                  │
│    AppendEntries    — leader → all followers                    │
│    InstallSnapshot  — leader → lagging followers                │
│                                                                 │
│  ELECTION:                                                      │
│    timeout in [150, 300] ms (randomized)                        │
│    need majority to win                                         │
│    one vote per term per server                                 │
│    candidate's log must be >= voter's log (up-to-date check)    │
│                                                                 │
│  COMMIT RULE:                                                   │
│    entry N committed when:                                      │
│      majority has it AND log[N].term == currentTerm             │
│    preceding entries committed indirectly                       │
│                                                                 │
│  LOG REPAIR:                                                    │
│    leader decrements nextIndex on failure                       │
│    follower deletes conflicting entries                         │
│    leader's log always wins                                     │
│                                                                 │
│  SAFETY INVARIANTS:                                             │
│    Election Safety, Leader Append-Only, Log Matching,           │
│    Leader Completeness, State Machine Safety                    │
│                                                                 │
│  TIMING:                                                        │
│    broadcastTime ≪ electionTimeout ≪ MTBF                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
``` 