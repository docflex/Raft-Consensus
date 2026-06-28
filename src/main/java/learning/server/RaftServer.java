package learning.server;

import learning.model.ClusterConfig;
import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.model.SnapshotMetadata;
import learning.persistence.PersistenceLayer;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.ClientRequest;
import learning.rpc.ClientResponse;
import learning.rpc.InstallSnapshotRequest;
import learning.rpc.InstallSnapshotResponse;
import learning.rpc.PreVoteRequest;
import learning.rpc.PreVoteResponse;
import learning.rpc.RequestVoteRequest;
import learning.rpc.RequestVoteResponse;
import learning.statemachine.StateMachine;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Core Raft consensus implementation (transport-agnostic).
 *
 * <p>Implements the Raft algorithm as described in
 * <a href="https://raft.github.io/raft.pdf">"In Search of an Understandable Consensus Algorithm"</a>
 * by Ongaro &amp; Ousterhout, including:
 * <ul>
 *   <li>Leader election with pre-vote protocol (§3.4, §9.6)</li>
 *   <li>Log replication and consistency checking (§3.5)</li>
 *   <li>Linearizable reads via the readIndex protocol (§6.4)</li>
 *   <li>Dynamic membership changes — single-server add/remove (§4)</li>
 *   <li>Log compaction via snapshots and InstallSnapshot RPC (§7)</li>
 *   <li>Optional durable persistence via {@link PersistenceLayer}</li>
 *   <li>Client command deduplication via {@link StateMachine}</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> all public methods must be called while holding
 * {@code synchronized(this)}. The {@link learning.node.RaftNode} orchestrator
 * enforces this invariant.
 *
 * @see learning.node.RaftNode
 * @see StateMachine
 * @see PersistenceLayer
 */
@Slf4j
public class RaftServer {
    RaftState state;               // State of the Current Server in the Cluster
    ServerID selfID;               // Identifier of Current Server
    List<ServerID> peers;          // All other Servers in the Cluster (derived from config)
    ClusterConfig config;          // Current cluster membership

    long electionTimeoutMs;     // Randomized value between 150-300
    long electionTimerStart;    // System.currentTimeMillis() when last reset
    long lastLeaderContactTime; // Last time we heard from a valid leader (0 = never)

    Set<ServerID> votesReceived;

    Map<ServerID, Long> nextIndex;    // for each peer: next log index to send (init: last log index + 1)
    Map<ServerID, Long> matchIndex;   // for each peer: highest replicated index (init: 0)

    StateTransitions transitions;
    ElectionHandler election;
    ReplicationHandler replication;
    PersistenceLayer persistence;
    StateMachine stateMachine;

    // Pending client requests: logIndex -> original ClientRequest (awaiting commit)
    final Map<Long, ClientRequest> pendingRequests = new HashMap<>();
    // Futures completed when a pending entry is committed and applied
    final Map<Long, CompletableFuture<ClientResponse>> pendingFutures = new HashMap<>();

    // Linearizable reads: readIndex protocol state
    long readIndex = 0;                              // commitIndex at time of read request
    final Set<ServerID> readIndexAcks = new HashSet<>();  // heartbeat acks confirming leadership
    boolean readIndexConfirmed = false;              // true once majority confirmed

    private static final long ELECTION_TIMEOUT_MIN_MILLI_SECONDS = 300;
    private static final long ELECTION_TIMEOUT_MAX_MILLI_SECONDS = 500;

    /**
     * Creates a Raft server with in-memory state only (no persistence, no state machine).
     *
     * @param selfID this server's unique identifier
     * @param peers  initial set of peer server IDs
     */
    public RaftServer(ServerID selfID, List<ServerID> peers) {
        this(selfID, peers, null, null);
    }

    /**
     * Creates a Raft server with optional persistence (no state machine).
     *
     * @param selfID      this server's unique identifier
     * @param peers       initial set of peer server IDs
     * @param persistence persistence layer for durable state, or {@code null} for in-memory
     */
    public RaftServer(ServerID selfID, List<ServerID> peers, PersistenceLayer persistence) {
        this(selfID, peers, persistence, null);
    }

    /**
     * Creates a Raft server with optional persistence and state machine.
     *
     * <p>If {@code persistence} is non-null, persisted state ({@code currentTerm},
     * {@code votedFor}, and the log) is recovered at construction time.
     *
     * @param selfID       this server's unique identifier
     * @param peers        initial set of peer server IDs
     * @param persistence  persistence layer for durable state, or {@code null}
     * @param stateMachine replicated state machine for applying committed commands, or {@code null}
     */
    public RaftServer(ServerID selfID, List<ServerID> peers, PersistenceLayer persistence, StateMachine stateMachine) {
        this.selfID = selfID;
        this.peers  = peers;
        this.config = new ClusterConfig(selfID, peers);
        this.state = new RaftState();
        this.votesReceived = new HashSet<>();
        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.persistence = persistence;
        this.stateMachine = stateMachine;

        this.transitions = new StateTransitions(this);
        this.election = new ElectionHandler(this, transitions);
        this.replication = new ReplicationHandler(this, transitions);

        // Recover persisted state if persistence layer is available
        if (persistence != null) {
            state.currentTerm = persistence.loadCurrentTerm();
            state.votedFor = persistence.loadVotedFor();
            state.log = new java.util.ArrayList<>(persistence.loadLog());
        }

        resetElectionTimer();
    }

    // --------------------------------------------- PEER REGISTRATION -----------------------------------------

    /**
     * Registers a dynamically discovered peer with the cluster.
     *
     * <p>Adds the peer to the cluster configuration, the peer list, and
     * initializes its {@code nextIndex} and {@code matchIndex} for replication.
     *
     * @param peerId the peer to register
     * @return {@code true} if the peer was newly registered, {@code false} if already known
     */
    public boolean registerPeer(ServerID peerId) {
        if (config.contains(peerId)) return false;
        peers.add(peerId);
        config = config.withAdded(peerId);
        nextIndex.put(peerId, state.lastLogIndex() + 1);
        matchIndex.put(peerId, 0L);
        log.info("[S{}] Registered peer S{}", selfID.id(), peerId.id());
        return true;
    }

    // --------------------------------------------- PUBLIC GETTERS -----------------------------------------

    public ServerID getSelfID()                  { return selfID; }
    public ServerRole getRole()                  { return state.role; }
    public long getCurrentTerm()                 { return state.currentTerm; }
    public long getCommitIndex()                 { return state.commitIndex; }
    public long getLastApplied()                 { return state.lastApplied; }
    public long getLastLogIndex()                { return state.lastLogIndex(); }
    public long getLastLogTerm()                 { return state.lastLogTerm(); }
    public int getLogSize()                      { return state.log.size(); }
    public ServerID getVotedFor()                { return state.votedFor; }
    public ServerID getLeaderHint()              { return state.leaderID; }
    public int getClusterSize()                  { return config.size(); }
    public int getMajority()                     { return config.majority(); }
    public List<ServerID> getPeers()             { return List.copyOf(peers); }
    public boolean hasPeers()                    { return !peers.isEmpty(); }
    public SnapshotMetadata getSnapshotMetadata() { return state.snapshotMetadata; }

    // --------------------------------------------- TIMERS ---------------------------------------------

    public void resetElectionTimer() {
        electionTimeoutMs = ThreadLocalRandom.current()
                .nextLong(ELECTION_TIMEOUT_MIN_MILLI_SECONDS, ELECTION_TIMEOUT_MAX_MILLI_SECONDS);
        electionTimerStart = System.currentTimeMillis();
    }

    public void resetElectionTimer(long minMs) {
        long lo = Math.max(minMs, ELECTION_TIMEOUT_MIN_MILLI_SECONDS);
        long hi = lo + (ELECTION_TIMEOUT_MAX_MILLI_SECONDS - ELECTION_TIMEOUT_MIN_MILLI_SECONDS);
        electionTimeoutMs = ThreadLocalRandom.current().nextLong(lo, hi);
        electionTimerStart = System.currentTimeMillis();
    }

    public boolean isElectionTimedOut() {
        return (System.currentTimeMillis() - electionTimerStart) >= electionTimeoutMs;
    }

    public void recordLeaderContact() {
        lastLeaderContactTime = System.currentTimeMillis();
    }

    public boolean isLeaderContactTimedOut() {
        return (System.currentTimeMillis() - lastLeaderContactTime) >= electionTimeoutMs;
    }

    // --------------------------------------------- PERSISTENCE HELPERS ---------------------------------------------

    void persistState() {
        if (persistence != null) persistence.saveState(state.currentTerm, state.votedFor);
    }

    void persistLog() {
        if (persistence != null) persistence.saveLog(state.log);
    }

    // --------------------------------------------- DELEGATION ---------------------------------------------

    public void startElection() {
        election.startElection();
    }

    public PreVoteResponse handlePreVote(PreVoteRequest preVoteRequest) {
        return election.handlePreVote(preVoteRequest);
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest voteRequest) {
        return election.handleRequestVote(voteRequest);
    }

    public void handleRequestVoteResponse(ServerID sender, RequestVoteResponse voteResponse) {
        election.handleRequestVoteResponse(sender, voteResponse);
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        return replication.handleAppendEntries(request);
    }

    public AppendEntriesRequest sendAppendEntriesRequest(ServerID peer) {
        return replication.sendAppendEntriesRequest(peer);
    }

    public void handleAppendEntriesResponse(ServerID peer, AppendEntriesResponse response, int entriesSent) {
        replication.handleAppendEntriesResponse(peer, response, entriesSent);
    }

    void advanceCommitIndex() {
        replication.advanceCommitIndex();
    }

    public boolean peerNeedsSnapshot(ServerID peer) {
        return replication.peerNeedsSnapshot(peer);
    }

    public InstallSnapshotRequest buildInstallSnapshotRequest(ServerID peer) {
        return replication.buildInstallSnapshotRequest(peer);
    }

    public void handleInstallSnapshotResponse(ServerID peer, InstallSnapshotResponse response) {
        replication.handleInstallSnapshotResponse(peer, response);
    }

    // --------------------------------------------- CLIENT INTERACTION ---------------------------------------------

    /**
     * Handles an incoming client write request.
     *
     * <p>If this server is the leader, the command is appended to the log and a
     * {@link CompletableFuture} is returned that will complete once the entry is
     * committed and applied to the state machine. If not the leader, a redirect
     * response with a leader hint is returned immediately.
     *
     * <p>Duplicate requests (same {@code clientId} + {@code serialNumber}) are
     * detected and return the cached result without re-appending.
     *
     * @param request the client command request
     * @return a future that completes with the command result after commit
     */
    public CompletableFuture<ClientResponse> handleClientRequest(ClientRequest request) {
        // Not the leader → redirect
        if (state.role != ServerRole.LEADER) {
            log.info("[S{}] Rejecting client request (not leader), redirecting to S{}",
                    selfID.id(), state.leaderID == null ? "?" : state.leaderID.id());
            return CompletableFuture.completedFuture(ClientResponse.builder()
                    .success(false)
                    .leaderHint(state.leaderID)
                    .build());
        }

        // Deduplication check
        if (stateMachine != null) {
            String cached = stateMachine.isDuplicate(request.clientId(), request.serialNumber());
            if (cached != null) {
                log.info("[S{}] Duplicate client request (client={}, serial={}), returning cached result",
                        selfID.id(), request.clientId(), request.serialNumber());
                return CompletableFuture.completedFuture(ClientResponse.builder()
                        .success(true)
                        .result(cached)
                        .build());
            }
        }

        // Append command to log
        long newIndex = state.lastLogIndex() + 1;
        LogEntry entry = LogEntry.builder()
                .index(newIndex)
                .term(state.currentTerm)
                .command(request.command())
                .build();
        state.log.add(entry);
        persistLog();

        // Track the pending request so we can respond after commit
        pendingRequests.put(newIndex, request);
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        pendingFutures.put(newIndex, future);

        log.info("[S{}] Appended client command at index {} (term={}, command='{}')",
                selfID.id(), newIndex, state.currentTerm, request.command());

        return future;
    }

    // --------------------------------------------- COMMIT APPLICATION ---------------------------------------------

    void applyCommittedEntries() {
        while (state.lastApplied < state.commitIndex) {
            state.lastApplied++;
            LogEntry entry = state.getEntry(state.lastApplied);

            if (entry == null) {
                // Entry was compacted away (covered by snapshot); skip
                continue;
            }

            // Config change entries are applied to the cluster config, not the state machine
            if (isConfigChangeCommand(entry.command())) {
                applyConfigChange(entry.command());
                continue;
            }

            String result = null;
            if (stateMachine != null) {
                result = stateMachine.apply(entry.command());
                log.info("[S{}] Applied log[{}] to state machine: '{}' -> '{}'",
                        selfID.id(), state.lastApplied, entry.command(), result);

                // Record execution for deduplication if this was a client command
                ClientRequest pending = pendingRequests.remove(state.lastApplied);
                if (pending != null) {
                    stateMachine.recordExecution(pending.clientId(), pending.serialNumber(), result);
                }
                // Complete the waiting future if present
                CompletableFuture<ClientResponse> future = pendingFutures.remove(state.lastApplied);
                if (future != null) {
                    future.complete(ClientResponse.builder()
                            .success(true)
                            .result(result)
                            .build());
                }
            }
        }
    }

    // --------------------------------------------- MEMBERSHIP CHANGES ---------------------------------------------

    private static final String CONFIG_ADD_PREFIX = "CONFIG_ADD ";
    private static final String CONFIG_REMOVE_PREFIX = "CONFIG_REMOVE ";

    /**
     * Initiates a cluster membership change to add a new server.
     *
     * <p>Only the leader can accept membership changes. A configuration change
     * entry ({@code CONFIG_ADD <id>}) is appended to the log and will take
     * effect once committed. Only one uncommitted config change is allowed
     * at a time (Raft single-server change safety).
     *
     * @param newServer the server to add
     * @return {@code true} if the config change was accepted, {@code false} otherwise
     */
    public boolean addServer(ServerID newServer) {
        if (state.role != ServerRole.LEADER) {
            log.info("[S{}] Rejected addServer: not leader", selfID.id());
            return false;
        }
        if (config.contains(newServer)) {
            log.info("[S{}] Rejected addServer: S{} already a member", selfID.id(), newServer.id());
            return false;
        }
        if (hasUncommittedConfigChange()) {
            log.info("[S{}] Rejected addServer: config change already in progress", selfID.id());
            return false;
        }

        // Append config change entry
        long newIndex = state.lastLogIndex() + 1;
        LogEntry entry = LogEntry.builder()
                .index(newIndex)
                .term(state.currentTerm)
                .command(CONFIG_ADD_PREFIX + newServer.id())
                .build();
        state.log.add(entry);
        persistLog();

        // Initialize replication state for the new peer
        nextIndex.put(newServer, newIndex + 1);
        matchIndex.put(newServer, 0L);

        log.info("[S{}] Config change queued: ADD S{} at index {}", selfID.id(), newServer.id(), newIndex);
        return true;
    }

    /**
     * Initiates a cluster membership change to remove a server.
     *
     * <p>If the leader removes itself, it steps down after the config change
     * is committed and applied.
     *
     * @param target the server to remove
     * @return {@code true} if the config change was accepted, {@code false} otherwise
     * @see #addServer(ServerID)
     */
    public boolean removeServer(ServerID target) {
        if (state.role != ServerRole.LEADER) {
            log.info("[S{}] Rejected removeServer: not leader", selfID.id());
            return false;
        }
        if (!config.contains(target)) {
            log.info("[S{}] Rejected removeServer: S{} not a member", selfID.id(), target.id());
            return false;
        }
        if (hasUncommittedConfigChange()) {
            log.info("[S{}] Rejected removeServer: config change already in progress", selfID.id());
            return false;
        }

        // Append config change entry
        long newIndex = state.lastLogIndex() + 1;
        LogEntry entry = LogEntry.builder()
                .index(newIndex)
                .term(state.currentTerm)
                .command(CONFIG_REMOVE_PREFIX + target.id())
                .build();
        state.log.add(entry);
        persistLog();

        log.info("[S{}] Config change queued: REMOVE S{} at index {}", selfID.id(), target.id(), newIndex);
        return true;
    }

    /**
     * Check if there's an uncommitted config change in the log.
     * Only one config change at a time is allowed.
     */
    boolean hasUncommittedConfigChange() {
        for (int i = state.log.size() - 1; i >= 0; i--) {
            LogEntry entry = state.log.get(i);
            if (entry.index() <= state.commitIndex) break;
            if (isConfigChangeCommand(entry.command())) return true;
        }
        return false;
    }

    /**
     * Apply a committed config change entry to the cluster configuration.
     * Called from applyCommittedEntries when a CONFIG_ command is encountered.
     */
    void applyConfigChange(String command) {
        if (command.startsWith(CONFIG_ADD_PREFIX)) {
            long id = Long.parseLong(command.substring(CONFIG_ADD_PREFIX.length()));
            ServerID newServer = new ServerID(id);
            config = config.withAdded(newServer);
            peers = new java.util.ArrayList<>(config.getPeers(selfID));
            log.info("[S{}] Config applied: ADDED S{} — cluster now {} members: {}",
                    selfID.id(), id, config.size(), config.getMembers());
        } else if (command.startsWith(CONFIG_REMOVE_PREFIX)) {
            long id = Long.parseLong(command.substring(CONFIG_REMOVE_PREFIX.length()));
            ServerID removed = new ServerID(id);
            config = config.withRemoved(removed);
            peers = new java.util.ArrayList<>(config.getPeers(selfID));
            nextIndex.remove(removed);
            matchIndex.remove(removed);
            log.info("[S{}] Config applied: REMOVED S{} — cluster now {} members: {}",
                    selfID.id(), id, config.size(), config.getMembers());

            // Leader steps down if it removed itself
            if (removed.equals(selfID) && state.role == ServerRole.LEADER) {
                log.info("[S{}] Leader removed itself from cluster — stepping down", selfID.id());
                state.role = ServerRole.FOLLOWER;
                state.leaderID = null;
                pendingRequests.clear();
                // Fail any waiting futures since we're no longer leader
                pendingFutures.values().forEach(f -> f.complete(ClientResponse.builder()
                        .success(false).result("LEADERSHIP_LOST").build()));
                pendingFutures.clear();
            }
        }
    }

    static boolean isConfigChangeCommand(String command) {
        return command != null &&
                (command.startsWith(CONFIG_ADD_PREFIX) || command.startsWith(CONFIG_REMOVE_PREFIX));
    }

    // --------------------------------------------- SNAPSHOT / LOG COMPACTION ---------------------------------------------

    /**
     * Triggers a snapshot of the current state machine state (log compaction).
     *
     * <p>All log entries up to {@code lastApplied} are discarded and replaced
     * by a compact snapshot. Reduces memory usage and speeds up catch-up for
     * lagging followers via {@code InstallSnapshot} RPC.
     */
    public void triggerSnapshot() {
        if (stateMachine == null) return;
        if (state.lastApplied == 0) return;

        long snapIndex = state.lastApplied;
        long snapTerm = state.termAt(snapIndex);
        byte[] data = stateMachine.takeSnapshot();

        // Discard log entries BEFORE setting snapshot metadata (toListPos depends on old offset)
        state.discardLogUpTo(snapIndex);

        state.snapshotMetadata = SnapshotMetadata.builder()
                .lastIncludedIndex(snapIndex)
                .lastIncludedTerm(snapTerm)
                .build();
        state.snapshotData = data;

        persistLog();

        log.info("[S{}] Snapshot taken at index={}, term={}, dataSize={}, remainingLog={}",
                selfID.id(), snapIndex, snapTerm, data.length, state.log.size());
    }

    /**
     * Handles an incoming {@code InstallSnapshot} RPC from the leader.
     *
     * <p>Replaces the local log and state machine with the leader's snapshot
     * when this follower has fallen too far behind for normal log replication.
     *
     * @param request the snapshot RPC from the leader
     * @return response containing this server's current term
     */
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        log.info("[S{}] Received InstallSnapshot from S{} (term={}, lastIncluded=[{},{}], dataSize={})",
                selfID.id(), request.leaderId().id(), request.term(),
                request.lastIncludedIndex(), request.lastIncludedTerm(), request.data().length);

        // Step 1: reject if stale term
        if (request.term() < state.currentTerm) {
            return InstallSnapshotResponse.builder().term(state.currentTerm).build();
        }

        // Step 2: recognize sender as leader
        if (request.term() > state.currentTerm) {
            transitions.stepDownToFollower(request.term());
        } else {
            state.role = ServerRole.FOLLOWER;
            resetElectionTimer();
        }
        state.leaderID = request.leaderId();
        recordLeaderContact();

        // Step 3: apply the snapshot
        // Discard log entries BEFORE setting snapshot metadata (toListPos depends on old offset)
        state.discardLogUpTo(request.lastIncludedIndex());

        state.snapshotMetadata = SnapshotMetadata.builder()
                .lastIncludedIndex(request.lastIncludedIndex())
                .lastIncludedTerm(request.lastIncludedTerm())
                .build();
        state.snapshotData = request.data();

        // Load snapshot into state machine
        if (stateMachine != null) {
            stateMachine.loadFromSnapshot(request.data());
        }

        // Update commit/applied indices
        state.commitIndex = Math.max(state.commitIndex, request.lastIncludedIndex());
        state.lastApplied = Math.max(state.lastApplied, request.lastIncludedIndex());

        persistLog();

        log.info("[S{}] Installed snapshot (lastIncluded=[{},{}], commitIndex={}, lastApplied={})",
                selfID.id(), request.lastIncludedIndex(), request.lastIncludedTerm(),
                state.commitIndex, state.lastApplied);

        return InstallSnapshotResponse.builder().term(state.currentTerm).build();
    }

    // --------------------------------------------- LINEARIZABLE READS ---------------------------------------------

    /**
     * Initiate a linearizable read-only query (readIndex protocol, Raft §6.4).
     * Does NOT append to the log. Instead:
     *   1. Records readIndex = current commitIndex
     *   2. Returns PENDING — caller must then send heartbeats and call confirmReadIndex()
     *   3. Once a majority confirms, call executeReadOnlyQuery() to get the result
     */
    public ClientResponse handleReadOnlyQuery(String query) {
        if (state.role != ServerRole.LEADER) {
            return ClientResponse.builder()
                    .success(false)
                    .leaderHint(state.leaderID)
                    .build();
        }

        // Step 1: record readIndex
        readIndex = state.commitIndex;
        readIndexAcks.clear();
        readIndexAcks.add(selfID); // leader counts as one ack
        readIndexConfirmed = false;

        log.info("[S{}] ReadIndex request: readIndex={}, query='{}'",
                selfID.id(), readIndex, query);

        // For a single-node cluster, leadership is immediately confirmed
        if (readIndexAcks.size() >= config.majority()) {
            readIndexConfirmed = true;
            return executeReadOnlyQuery(query);
        }

        return ClientResponse.builder()
                .success(false)
                .result("PENDING_LEADER_CONFIRM")
                .build();
    }

    /**
     * Record a heartbeat acknowledgment from a peer for readIndex confirmation.
     * Called when a peer responds successfully to a heartbeat sent after handleReadOnlyQuery.
     */
    public void confirmReadIndex(ServerID peer) {
        if (readIndexConfirmed) return;

        readIndexAcks.add(peer);
        if (readIndexAcks.size() >= config.majority()) {
            readIndexConfirmed = true;
            log.info("[S{}] ReadIndex confirmed: {} acks from majority",
                    selfID.id(), readIndexAcks.size());
        }
    }

    /**
     * Execute a read-only query against the state machine.
     * Only valid after readIndex has been confirmed (leadership verified).
     */
    public ClientResponse executeReadOnlyQuery(String query) {
        if (!readIndexConfirmed) {
            return ClientResponse.builder()
                    .success(false)
                    .result("LEADER_NOT_CONFIRMED")
                    .build();
        }

        // Wait until state machine has applied up to readIndex
        applyCommittedEntries();
        if (state.lastApplied < readIndex) {
            return ClientResponse.builder()
                    .success(false)
                    .result("NOT_YET_APPLIED")
                    .build();
        }

        // Execute read against state machine
        String result = null;
        if (stateMachine != null) {
            result = stateMachine.apply(query);
            log.info("[S{}] ReadIndex query executed: '{}' -> '{}'",
                    selfID.id(), query, result);
        }

        return ClientResponse.builder()
                .success(true)
                .result(result)
                .build();
    }

    /**
     * Check if readIndex has been confirmed by a majority.
     */
    public boolean isReadIndexConfirmed() {
        return readIndexConfirmed;
    }

    /**
     * Retrieve the committed result for a client request at the given log index.
     * Called after replication + commit to get the final response.
     */
    ClientResponse getCommittedResult(long logIndex, ClientRequest originalRequest) {
        if (logIndex > state.commitIndex) {
            return ClientResponse.builder()
                    .success(false)
                    .result("NOT_COMMITTED")
                    .build();
        }

        // Apply any unapplied entries up to this point
        applyCommittedEntries();

        // Use the cached result from dedup (recorded during applyCommittedEntries)
        String result = null;
        if (stateMachine != null && originalRequest != null) {
            result = stateMachine.isDuplicate(originalRequest.clientId(), originalRequest.serialNumber());
        }

        return ClientResponse.builder()
                .success(true)
                .result(result)
                .build();
    }
}
