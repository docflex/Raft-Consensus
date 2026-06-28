package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.persistence.RiptideKVPersistence;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.RequestVoteRequest;
import learning.rpc.RequestVoteResponse;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RaftServer Persistence & Recovery")
class RaftServerPersistenceTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);
    static final List<ServerID> PEERS_OF_S1 = List.of(S2, S3, S4, S5);

    static Path baseDir;
    static RiptideKVPersistence persistence;

    @BeforeAll
    static void startPersistence() throws IOException {
        baseDir = Files.createTempDirectory("raft-server-persistence-test");
        persistence = RiptideKVPersistence.forServer(S1, baseDir);
    }

    @AfterAll
    static void stopPersistence() {
        if (persistence != null) persistence.close();
    }

    @BeforeEach
    void clearPersistedState() {
        persistence.saveState(0, null);
        persistence.saveLog(List.of());
    }

    // Helper: create a fresh RaftServer wired to the shared persistence
    private RaftServer createServer() {
        return new RaftServer(S1, PEERS_OF_S1, persistence);
    }

    // Helper: simulate crash + restart — new RaftServer instance, same persistence
    private RaftServer restartServer() {
        return new RaftServer(S1, PEERS_OF_S1, persistence);
    }

    // ========================== State Persistence on Mutations ==========================

    @Nested
    @DisplayName("Persist on State Changes")
    class PersistOnStateChanges {

        @Test
        @DisplayName("persists currentTerm and votedFor after voting")
        void persistsAfterVoting() {
            RaftServer server = createServer();

            RequestVoteRequest request = RequestVoteRequest.builder()
                    .term(3)
                    .candidateID(S2)
                    .lastLogIndex(0)
                    .lastLogTerm(0)
                    .build();

            RequestVoteResponse response = server.handleRequestVote(request);
            assertTrue(response.voteGranted());

            // Verify persistence layer has the state
            assertEquals(3, persistence.loadCurrentTerm());
            assertEquals(S2, persistence.loadVotedFor());
        }

        @Test
        @DisplayName("persists currentTerm after stepping down to higher term")
        void persistsAfterStepDown() {
            RaftServer server = createServer();
            server.state.currentTerm = 2;
            server.state.role = ServerRole.CANDIDATE;

            // Receive AppendEntries from a leader with higher term
            AppendEntriesRequest request = AppendEntriesRequest.builder()
                    .term(5)
                    .leaderId(S3)
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(List.of())
                    .leaderCommit(0)
                    .build();

            server.handleAppendEntries(request);

            assertEquals(5, persistence.loadCurrentTerm());
            assertNull(persistence.loadVotedFor());
        }

        @Test
        @DisplayName("persists log after AppendEntries adds entries")
        void persistsLogAfterAppend() {
            RaftServer server = createServer();

            LogEntry entry1 = new LogEntry(1, 1, "SET x=1");
            LogEntry entry2 = new LogEntry(2, 1, "SET y=2");

            AppendEntriesRequest request = AppendEntriesRequest.builder()
                    .term(1)
                    .leaderId(S2)
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(List.of(entry1, entry2))
                    .leaderCommit(0)
                    .build();

            server.handleAppendEntries(request);

            List<LogEntry> persistedLog = persistence.loadLog();
            assertEquals(2, persistedLog.size());
            assertEquals("SET x=1", persistedLog.get(0).command());
            assertEquals("SET y=2", persistedLog.get(1).command());
        }

        @Test
        @DisplayName("persists term and votedFor when starting election")
        void persistsOnStartElection() {
            RaftServer server = createServer();

            server.startElection();

            assertEquals(1, persistence.loadCurrentTerm());
            assertEquals(S1, persistence.loadVotedFor());
        }

        @Test
        @DisplayName("persists log when leader appends NO OP on election win")
        void persistsNoOpOnLeaderTransition() {
            RaftServer server = createServer();

            // Win an election
            server.startElection();
            // Get votes from S2 and S3 (majority with self)
            server.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            server.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());

            assertEquals(ServerRole.LEADER, server.state.role);

            List<LogEntry> persistedLog = persistence.loadLog();
            assertEquals(1, persistedLog.size());
            assertEquals("NO OP", persistedLog.get(0).command());
        }
    }

    // ========================== Recovery After Crash ==========================

    @Nested
    @DisplayName("Recovery After Crash")
    class RecoveryAfterCrash {

        @Test
        @DisplayName("recovers currentTerm from persistence")
        void recoversCurrentTerm() {
            RaftServer original = createServer();
            original.startElection(); // term goes to 1
            // Simulate receiving a higher term
            original.handleRequestVote(RequestVoteRequest.builder()
                    .term(5).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build());

            // "Crash" and restart
            RaftServer recovered = restartServer();

            assertEquals(5, recovered.state.currentTerm);
        }

        @Test
        @DisplayName("recovers votedFor from persistence")
        void recoversVotedFor() {
            RaftServer original = createServer();
            original.handleRequestVote(RequestVoteRequest.builder()
                    .term(3).candidateID(S4).lastLogIndex(0).lastLogTerm(0).build());

            RaftServer recovered = restartServer();

            assertEquals(3, recovered.state.currentTerm);
            assertEquals(S4, recovered.state.votedFor);
        }

        @Test
        @DisplayName("recovers log from persistence")
        void recoversLog() {
            RaftServer original = createServer();
            original.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(
                            new LogEntry(1, 1, "SET x=1"),
                            new LogEntry(2, 1, "SET y=2")))
                    .leaderCommit(0).build());

            RaftServer recovered = restartServer();

            assertEquals(2, recovered.state.log.size());
            assertEquals("SET x=1", recovered.state.log.get(0).command());
            assertEquals("SET y=2", recovered.state.log.get(1).command());
        }

        @Test
        @DisplayName("recovers as FOLLOWER regardless of previous role")
        void recoversAsFollower() {
            RaftServer original = createServer();
            original.startElection(); // becomes CANDIDATE

            RaftServer recovered = restartServer();

            assertEquals(ServerRole.FOLLOWER, recovered.state.role);
        }

        @Test
        @DisplayName("volatile state resets on recovery (commitIndex, lastApplied)")
        void volatileStateResetsOnRecovery() {
            RaftServer original = createServer();
            original.state.commitIndex = 5;
            original.state.lastApplied = 3;

            RaftServer recovered = restartServer();

            assertEquals(0, recovered.state.commitIndex);
            assertEquals(0, recovered.state.lastApplied);
        }
    }

    // ========================== Safety After Recovery ==========================

    @Nested
    @DisplayName("Safety After Recovery")
    class SafetyAfterRecovery {

        @Test
        @DisplayName("does not double-vote in same term after crash")
        void noDoubleVoteAfterCrash() {
            RaftServer original = createServer();

            // Vote for S2 in term 3
            original.handleRequestVote(RequestVoteRequest.builder()
                    .term(3).candidateID(S2).lastLogIndex(0).lastLogTerm(0).build());

            // "Crash" and restart
            RaftServer recovered = restartServer();

            // S3 asks for vote in same term 3
            RequestVoteResponse response = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(3).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build());

            assertFalse(response.voteGranted(), "Must not grant vote to S3 — already voted for S2 in term 3");
        }

        @Test
        @DisplayName("can vote for same candidate again after crash (idempotent)")
        void canRevoteForSameCandidateAfterCrash() {
            RaftServer original = createServer();

            original.handleRequestVote(RequestVoteRequest.builder()
                    .term(3).candidateID(S2).lastLogIndex(0).lastLogTerm(0).build());

            RaftServer recovered = restartServer();

            // S2 retries the same RequestVote (idempotent)
            RequestVoteResponse response = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(3).candidateID(S2).lastLogIndex(0).lastLogTerm(0).build());

            assertTrue(response.voteGranted(), "Should grant vote to same candidate in same term");
        }

        @Test
        @DisplayName("can vote in a new higher term after crash")
        void canVoteInNewTermAfterCrash() {
            RaftServer original = createServer();

            original.handleRequestVote(RequestVoteRequest.builder()
                    .term(3).candidateID(S2).lastLogIndex(0).lastLogTerm(0).build());

            RaftServer recovered = restartServer();

            // S3 asks for vote in term 4 (new term — should be allowed)
            RequestVoteResponse response = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(4).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build());

            assertTrue(response.voteGranted(), "Should grant vote in new term");
        }

        @Test
        @DisplayName("recovered server has correct log for election restriction")
        void electionRestrictionAfterRecovery() {
            RaftServer original = createServer();

            // Receive entries at term 2
            original.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(2).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(
                            new LogEntry(1, 1, "a"),
                            new LogEntry(2, 2, "b")))
                    .leaderCommit(0).build());

            RaftServer recovered = restartServer();

            // S4 has stale log (term 1 only) — should be denied
            RequestVoteResponse denied = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(3).candidateID(S4).lastLogIndex(1).lastLogTerm(1).build());

            assertFalse(denied.voteGranted(), "Deny vote to candidate with stale log");

            // S5 has up-to-date log — should be granted
            RequestVoteResponse granted = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(3).candidateID(S5).lastLogIndex(2).lastLogTerm(2).build());

            assertTrue(granted.voteGranted(), "Grant vote to candidate with up-to-date log");
        }
    }

    // ========================== Persist on Additional Mutation Paths ==========================

    @Nested
    @DisplayName("Persist on Additional Mutation Paths")
    class PersistOnAdditionalPaths {

        @Test
        @DisplayName("persists truncated log after conflict in consistency check")
        void persistsLogAfterConflictTruncation() {
            RaftServer server = createServer();

            // Give the follower entries [1:t1, 2:t1, 3:t1]
            server.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(
                            new LogEntry(1, 1, "a"),
                            new LogEntry(2, 1, "b"),
                            new LogEntry(3, 1, "c")))
                    .leaderCommit(0).build());
            assertEquals(3, server.state.log.size());

            // Leader sends AppendEntries with prevLogIndex=2, prevLogTerm=2 (conflict at index 2)
            AppendEntriesResponse response = server.handleAppendEntries(
                    AppendEntriesRequest.builder()
                            .term(2).leaderId(S2).prevLogIndex(2).prevLogTerm(2)
                            .entries(List.of())
                            .leaderCommit(0).build());

            assertFalse(response.success());

            // Crash and recover — truncated log should survive
            RaftServer recovered = restartServer();
            assertEquals(1, recovered.state.log.size(), "Truncated log should persist: only index 1 remains");
            assertEquals("a", recovered.state.log.get(0).command());
        }

        @Test
        @DisplayName("persists state on step-down via higher-term RequestVote")
        void persistsOnStepDownViaRequestVote() {
            RaftServer server = createServer();
            server.state.currentTerm = 3;
            server.state.role = ServerRole.LEADER;
            server.persistState();

            // Receive RequestVote with higher term → triggers stepDownToFollower
            server.handleRequestVote(RequestVoteRequest.builder()
                    .term(7).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build());

            assertEquals(7, persistence.loadCurrentTerm());
            // votedFor should be S3 (stepped down, then granted vote)
            assertEquals(S3, persistence.loadVotedFor());
        }

        @Test
        @DisplayName("persists state on step-down via higher-term AppendEntriesResponse")
        void persistsOnStepDownViaAppendEntriesResponse() {
            RaftServer server = createServer();

            // Make S1 a leader in term 1
            server.startElection();
            server.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            server.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            assertEquals(ServerRole.LEADER, server.state.role);

            // Leader receives response with higher term → step down
            server.handleAppendEntriesResponse(S2,
                    AppendEntriesResponse.builder().term(5).success(false).build(), 0);

            assertEquals(5, persistence.loadCurrentTerm());
            assertNull(persistence.loadVotedFor());
            assertEquals(ServerRole.FOLLOWER, server.state.role);
        }

        @Test
        @DisplayName("heartbeat (empty AppendEntries) does not corrupt persisted log")
        void heartbeatDoesNotCorruptLog() {
            RaftServer server = createServer();

            // Add some entries
            server.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(
                            new LogEntry(1, 1, "a"),
                            new LogEntry(2, 1, "b")))
                    .leaderCommit(0).build());

            // Send heartbeat (empty entries)
            server.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(2).prevLogTerm(1)
                    .entries(List.of())
                    .leaderCommit(1).build());

            // Log should still be intact
            List<LogEntry> persisted = persistence.loadLog();
            assertEquals(2, persisted.size());
            assertEquals("a", persisted.get(0).command());
            assertEquals("b", persisted.get(1).command());
        }

        @Test
        @DisplayName("stale RPC after recovery does not alter persisted state")
        void staleRpcAfterRecoveryDoesNotPersist() {
            RaftServer server = createServer();

            // Set server to term 5
            server.handleRequestVote(RequestVoteRequest.builder()
                    .term(5).candidateID(S2).lastLogIndex(0).lastLogTerm(0).build());

            RaftServer recovered = restartServer();

            // Stale RequestVote for term 2 — should be rejected without persisting
            RequestVoteResponse response = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(2).candidateID(S4).lastLogIndex(0).lastLogTerm(0).build());

            assertFalse(response.voteGranted());
            assertEquals(5, persistence.loadCurrentTerm(), "Term should NOT change on stale RPC");
            assertEquals(S2, persistence.loadVotedFor(), "votedFor should NOT change on stale RPC");
        }
    }

    // ========================== End-to-End with Persistence ==========================

    @Nested
    @DisplayName("End-to-End Persistence Scenarios")
    class E2EPersistence {

        @Test
        @DisplayName("full cycle: elect → replicate → crash → recover → resume")
        void fullCycleWithRecovery() {
            RaftServer leader = createServer();

            // Win election for term 1
            leader.startElection();
            leader.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            leader.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            assertEquals(ServerRole.LEADER, leader.state.role);

            // Simulate client command
            leader.state.log.add(new LogEntry(2, 1, "SET x=42"));
            persistence.saveLog(leader.state.log); // leader would persist

            // "Crash"
            RaftServer recovered = restartServer();

            // Verify recovered state
            assertEquals(1, recovered.state.currentTerm);
            assertEquals(S1, recovered.state.votedFor); // voted for self in election
            assertEquals(ServerRole.FOLLOWER, recovered.state.role); // always recover as follower
            assertEquals(2, recovered.state.log.size());
            assertEquals("NO OP", recovered.state.log.get(0).command());
            assertEquals("SET x=42", recovered.state.log.get(1).command());
        }

        @Test
        @DisplayName("follower crash during replication — catches up after recovery")
        void followerRecoveryAndCatchUp() {
            RaftServer follower = createServer();

            // Receive first batch of entries
            follower.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(
                            new LogEntry(1, 1, "a"),
                            new LogEntry(2, 1, "b")))
                    .leaderCommit(1).build());

            // "Crash"
            RaftServer recovered = restartServer();

            // Verify log survived
            assertEquals(2, recovered.state.log.size());
            assertEquals(1, recovered.state.currentTerm);

            // commitIndex resets to 0 on recovery (volatile)
            assertEquals(0, recovered.state.commitIndex);

            // Leader retries with more entries — follower can accept them
            AppendEntriesResponse response = recovered.handleAppendEntries(
                    AppendEntriesRequest.builder()
                            .term(1).leaderId(S2).prevLogIndex(2).prevLogTerm(1)
                            .entries(List.of(new LogEntry(3, 1, "c")))
                            .leaderCommit(2).build());

            assertTrue(response.success());
            assertEquals(3, recovered.state.log.size());
            assertEquals(2, recovered.state.commitIndex); // caught up via leaderCommit
        }

        @Test
        @DisplayName("leader crash → recovers as follower → new leader replicates over stale entries")
        void leaderCrashRecoveryAndNewLeaderReplicates() {
            RaftServer leader = createServer();

            // S1 becomes leader in term 1
            leader.startElection();
            leader.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            leader.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            assertEquals(ServerRole.LEADER, leader.state.role);
            assertEquals(1, leader.state.log.size()); // NO OP

            // "Crash"
            RaftServer recovered = restartServer();
            assertEquals(ServerRole.FOLLOWER, recovered.state.role);
            assertEquals(1, recovered.state.log.size()); // NO OP survived

            // S2 is new leader in term 2, sends its own NO OP that overwrites S1's
            AppendEntriesResponse response = recovered.handleAppendEntries(
                    AppendEntriesRequest.builder()
                            .term(2).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                            .entries(List.of(
                                    new LogEntry(1, 2, "NO OP"),
                                    new LogEntry(2, 2, "SET y=99")))
                            .leaderCommit(1).build());

            assertTrue(response.success());
            assertEquals(2, recovered.state.log.size());
            assertEquals(2, recovered.state.log.get(0).term()); // overwritten to term 2
            assertEquals("SET y=99", recovered.state.log.get(1).command());
        }

        @Test
        @DisplayName("candidate crash mid-election → recovers → participates in new election")
        void candidateCrashAndNewElection() {
            RaftServer server = createServer();

            // Start election (term goes to 1, votedFor = self)
            server.startElection();
            assertEquals(ServerRole.CANDIDATE, server.state.role);
            assertEquals(1, server.state.currentTerm);

            // "Crash" before winning
            RaftServer recovered = restartServer();

            assertEquals(1, recovered.state.currentTerm);
            assertEquals(S1, recovered.state.votedFor); // voted for self
            assertEquals(ServerRole.FOLLOWER, recovered.state.role); // recovers as follower

            // Can participate in a new election in a higher term
            RequestVoteResponse response = recovered.handleRequestVote(
                    RequestVoteRequest.builder()
                            .term(2).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build());

            assertTrue(response.voteGranted());
            assertEquals(2, recovered.state.currentTerm);
            assertEquals(S3, recovered.state.votedFor);
        }

        @Test
        @DisplayName("leader volatile state (nextIndex/matchIndex) is NOT persisted — reinitialized on re-election")
        void leaderVolatileStateNotPersisted() {
            RaftServer leader = createServer();

            // Win election
            leader.startElection();
            leader.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            leader.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(1).voteGranted(true).build());
            assertEquals(ServerRole.LEADER, leader.state.role);
            assertFalse(leader.nextIndex.isEmpty());
            assertFalse(leader.matchIndex.isEmpty());

            // "Crash"
            RaftServer recovered = restartServer();

            // nextIndex and matchIndex should be empty — volatile, not persisted
            assertTrue(recovered.nextIndex.isEmpty(), "nextIndex must be empty after recovery");
            assertTrue(recovered.matchIndex.isEmpty(), "matchIndex must be empty after recovery");

            // Win election again → maps reinitialized
            recovered.startElection();
            recovered.handleRequestVoteResponse(S2,
                    RequestVoteResponse.builder().term(2).voteGranted(true).build());
            recovered.handleRequestVoteResponse(S3,
                    RequestVoteResponse.builder().term(2).voteGranted(true).build());

            assertEquals(ServerRole.LEADER, recovered.state.role);
            assertFalse(recovered.nextIndex.isEmpty(), "nextIndex reinitialized on re-election");
            assertFalse(recovered.matchIndex.isEmpty(), "matchIndex reinitialized on re-election");
        }

        @Test
        @DisplayName("multiple crashes: crash → recover → operate → crash → recover")
        void multipleCrashCycles() {
            // --- Cycle 1: follower receives entries, crashes ---
            RaftServer server = createServer();
            server.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of(new LogEntry(1, 1, "a")))
                    .leaderCommit(0).build());

            // Crash 1
            RaftServer recovered1 = restartServer();
            assertEquals(1, recovered1.state.log.size());

            // --- Cycle 2: receives more entries, crashes again ---
            recovered1.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(1).leaderId(S2).prevLogIndex(1).prevLogTerm(1)
                    .entries(List.of(new LogEntry(2, 1, "b")))
                    .leaderCommit(1).build());

            // Vote in term 3
            recovered1.handleRequestVote(RequestVoteRequest.builder()
                    .term(3).candidateID(S4).lastLogIndex(2).lastLogTerm(1).build());

            // Crash 2
            RaftServer recovered2 = restartServer();

            // All state from both cycles should be recovered
            assertEquals(3, recovered2.state.currentTerm);
            assertEquals(S4, recovered2.state.votedFor);
            assertEquals(2, recovered2.state.log.size());
            assertEquals("a", recovered2.state.log.get(0).command());
            assertEquals("b", recovered2.state.log.get(1).command());
            assertEquals(ServerRole.FOLLOWER, recovered2.state.role);
            assertEquals(0, recovered2.state.commitIndex); // volatile resets
        }
    }
}
