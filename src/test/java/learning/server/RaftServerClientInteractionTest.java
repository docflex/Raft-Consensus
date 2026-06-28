package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.ClientRequest;
import learning.rpc.ClientResponse;
import learning.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Milestone 4: Client Interaction")
class RaftServerClientInteractionTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);
    static final List<ServerID> PEERS_OF_S1 = List.of(S2, S3, S4, S5);

    // Helper: create a server with a KV state machine
    private RaftServer createServer(ServerID id, List<ServerID> peers) {
        return new RaftServer(id, peers, null, new KeyValueStateMachine());
    }

    // Helper: elect S1 as leader in the given term
    private RaftServer electLeader(long term) {
        RaftServer leader = createServer(S1, PEERS_OF_S1);
        // Force to candidate, then to leader
        leader.state.currentTerm = term;
        leader.state.role = ServerRole.LEADER;
        leader.state.leaderID = S1;
        for (ServerID peer : PEERS_OF_S1) {
            leader.nextIndex.put(peer, leader.state.lastLogIndex() + 1);
            leader.matchIndex.put(peer, 0L);
        }
        // Append the no-op entry a leader would add on election
        LogEntry noOp = LogEntry.builder()
                .index(leader.state.lastLogIndex() + 1)
                .term(term)
                .command("NO OP")
                .build();
        leader.state.log.add(noOp);
        return leader;
    }

    // Helper: simulate replication + commit of the leader's last entry on a majority
    private void simulateReplicationAndCommit(RaftServer leader) {
        long lastIndex = leader.state.lastLogIndex();
        long term = leader.state.currentTerm;

        // Send AppendEntries to S2 and S3, they accept
        for (ServerID peer : List.of(S2, S3)) {
            AppendEntriesRequest req = leader.sendAppendEntriesRequest(peer);
            // Simulate follower success
            AppendEntriesResponse resp = AppendEntriesResponse.builder()
                    .term(term).success(true).build();
            leader.handleAppendEntriesResponse(peer, resp, req.entries().size());
        }
    }

    // ========================== Leader Redirect ==========================

    @Nested
    @DisplayName("Leader Redirect")
    class LeaderRedirect {

        @Test
        @DisplayName("follower rejects client request and returns leader hint")
        void followerRejectsWithLeaderHint() {
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));
            follower.state.leaderID = S1; // follower knows S1 is leader

            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();

            ClientResponse resp = follower.handleClientRequest(req).join();

            assertFalse(resp.success());
            assertEquals(S1, resp.leaderHint());
        }

        @Test
        @DisplayName("candidate rejects client request")
        void candidateRejects() {
            RaftServer candidate = createServer(S3, List.of(S1, S2, S4, S5));
            candidate.state.role = ServerRole.CANDIDATE;
            candidate.state.leaderID = null;

            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();

            ClientResponse resp = candidate.handleClientRequest(req).join();

            assertFalse(resp.success());
            assertNull(resp.leaderHint());
        }

        @Test
        @DisplayName("follower with no known leader returns null hint")
        void followerNoKnownLeader() {
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));
            // freshly started, no leader known

            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();

            ClientResponse resp = follower.handleClientRequest(req).join();

            assertFalse(resp.success());
            assertNull(resp.leaderHint());
        }
    }

    // ========================== Client Request Handling ==========================

    @Nested
    @DisplayName("Client Request Handling")
    class ClientRequestHandling {

        @Test
        @DisplayName("leader appends client command to log")
        void leaderAppendsCommand() {
            RaftServer leader = electLeader(1);
            long logSizeBefore = leader.state.lastLogIndex();

            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();

            CompletableFuture<ClientResponse> future = leader.handleClientRequest(req);

            // Entry appended but not yet committed
            assertEquals(logSizeBefore + 1, leader.state.lastLogIndex());
            LogEntry entry = leader.state.log.getLast();
            assertEquals("SET x 42", entry.command());
            assertEquals(leader.state.currentTerm, entry.term());

            // Future should not be completed yet (awaiting commit)
            assertFalse(future.isDone());
        }

        @Test
        @DisplayName("leader handles multiple client commands sequentially")
        void leaderMultipleCommands() {
            RaftServer leader = electLeader(1);
            long baseIndex = leader.state.lastLogIndex();

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 1").build());
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET y 2").build());
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(101).serialNumber(1).command("SET z 3").build());

            assertEquals(baseIndex + 3, leader.state.lastLogIndex());
            assertEquals("SET x 1", leader.state.log.get((int) baseIndex).command());
            assertEquals("SET y 2", leader.state.log.get((int) baseIndex + 1).command());
            assertEquals("SET z 3", leader.state.log.get((int) baseIndex + 2).command());
        }

        @Test
        @DisplayName("client command is committed after majority replication")
        void commandCommittedAfterMajority() {
            RaftServer leader = electLeader(1);

            // First, commit the no-op so advanceCommitIndex works
            simulateReplicationAndCommit(leader);

            // Now submit a client command
            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();
            leader.handleClientRequest(req);

            long cmdIndex = leader.state.lastLogIndex();

            // Simulate replication to S2 and S3
            simulateReplicationAndCommit(leader);

            // Entry should be committed
            assertTrue(leader.state.commitIndex >= cmdIndex,
                    "commitIndex should be >= " + cmdIndex + " but was " + leader.state.commitIndex);

            // After commit, result should be available
            ClientResponse result = leader.getCommittedResult(cmdIndex, req);
            assertTrue(result.success());
            assertEquals("OK", result.result());
        }
    }

    // ========================== State Machine Application ==========================

    @Nested
    @DisplayName("State Machine Application")
    class StateMachineApplication {

        @Test
        @DisplayName("committed SET then GET returns correct value")
        void setThenGet() {
            RaftServer leader = electLeader(1);

            // Commit the no-op first
            simulateReplicationAndCommit(leader);

            // SET x 42
            ClientRequest setReq = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();
            leader.handleClientRequest(setReq);
            simulateReplicationAndCommit(leader);

            // GET x — submit as another client command
            ClientRequest getReq = ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("GET x").build();
            leader.handleClientRequest(getReq);
            long getIndex = leader.state.lastLogIndex();
            simulateReplicationAndCommit(leader);

            ClientResponse getResult = leader.getCommittedResult(getIndex, getReq);
            assertTrue(getResult.success());
            assertEquals("42", getResult.result());
        }

        @Test
        @DisplayName("DEL removes key, GET returns nil")
        void delThenGet() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader); // commit no-op

            // SET then DEL
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET y hello").build());
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("DEL y").build());
            simulateReplicationAndCommit(leader);

            // GET y
            ClientRequest getReq = ClientRequest.builder()
                    .clientId(100).serialNumber(3).command("GET y").build();
            leader.handleClientRequest(getReq);
            long getIndex = leader.state.lastLogIndex();
            simulateReplicationAndCommit(leader);

            ClientResponse resp = leader.getCommittedResult(getIndex, getReq);
            assertTrue(resp.success());
            assertEquals("(nil)", resp.result());
        }

        @Test
        @DisplayName("follower applies committed entries via leaderCommit")
        void followerAppliesEntries() {
            RaftServer leader = electLeader(1);
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));

            // Leader has no-op at index 1 committed
            simulateReplicationAndCommit(leader);

            // Leader adds client command
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 99").build());
            simulateReplicationAndCommit(leader);

            // Send AppendEntries to follower with full log and leaderCommit
            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(leader.state.currentTerm)
                    .leaderId(S1)
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(leader.state.log)
                    .leaderCommit(leader.state.commitIndex)
                    .build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);
            assertTrue(resp.success());

            // Follower should have applied entries
            assertEquals(leader.state.commitIndex, follower.state.commitIndex);
            assertEquals(leader.state.commitIndex, follower.state.lastApplied);

            // Follower's state machine should reflect the SET
            KeyValueStateMachine fsm = (KeyValueStateMachine) follower.stateMachine;
            assertEquals("99", fsm.getStore().get("x"));
        }

        @Test
        @DisplayName("NO_OP entries are applied silently")
        void noOpApplied() {
            RaftServer leader = electLeader(1);
            // The no-op is at index 1. Commit it.
            simulateReplicationAndCommit(leader);

            assertEquals(1, leader.state.lastApplied);
            // State machine store should be empty (no-op doesn't add keys)
            KeyValueStateMachine sm = (KeyValueStateMachine) leader.stateMachine;
            assertTrue(sm.getStore().isEmpty());
        }
    }

    // ========================== Deduplication ==========================

    @Nested
    @DisplayName("Serial Number Deduplication")
    class Deduplication {

        @Test
        @DisplayName("duplicate request returns cached result without re-appending")
        void duplicateReturnsCached() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader); // commit no-op

            // First request
            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();
            leader.handleClientRequest(req);
            simulateReplicationAndCommit(leader);

            // Apply so result is recorded
            leader.applyCommittedEntries();

            long logSizeAfterFirst = leader.state.lastLogIndex();

            // Duplicate request with same clientId + serialNumber
            ClientResponse dupResp = leader.handleClientRequest(req).join();

            // Should return cached result without adding to log
            assertTrue(dupResp.success());
            assertEquals("OK", dupResp.result());
            assertEquals(logSizeAfterFirst, leader.state.lastLogIndex(),
                    "Log should NOT grow on duplicate request");
        }

        @Test
        @DisplayName("different serial number from same client is NOT a duplicate")
        void differentSerialNotDuplicate() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            ClientRequest req1 = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 1").build();
            leader.handleClientRequest(req1);
            simulateReplicationAndCommit(leader);
            leader.applyCommittedEntries();

            long logSizeAfterFirst = leader.state.lastLogIndex();

            // Different serial number
            ClientRequest req2 = ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET x 2").build();
            CompletableFuture<ClientResponse> future = leader.handleClientRequest(req2);

            assertFalse(future.isDone()); // should be pending (new command, awaiting commit)
            assertEquals(logSizeAfterFirst + 1, leader.state.lastLogIndex());
        }

        @Test
        @DisplayName("same serial from different client is NOT a duplicate")
        void differentClientNotDuplicate() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            ClientRequest req1 = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 1").build();
            leader.handleClientRequest(req1);
            simulateReplicationAndCommit(leader);
            leader.applyCommittedEntries();

            long logSizeBefore = leader.state.lastLogIndex();

            // Same serial, different client
            ClientRequest req2 = ClientRequest.builder()
                    .clientId(200).serialNumber(1).command("SET y 2").build();
            CompletableFuture<ClientResponse> future = leader.handleClientRequest(req2);

            assertFalse(future.isDone()); // pending, awaiting commit
            assertEquals(logSizeBefore + 1, leader.state.lastLogIndex());
        }
    }

    // ========================== End-to-End Scenarios ==========================

    @Nested
    @DisplayName("End-to-End Client Scenarios")
    class EndToEnd {

        @Test
        @DisplayName("full flow: client SET → replicate → commit → GET returns value")
        void fullClientFlow() {
            RaftServer leader = electLeader(1);
            RaftServer follower2 = createServer(S2, List.of(S1, S3, S4, S5));
            RaftServer follower3 = createServer(S3, List.of(S1, S2, S4, S5));

            // Commit no-op by replicating to followers
            AppendEntriesRequest noOpReq = leader.sendAppendEntriesRequest(S2);
            follower2.handleAppendEntries(noOpReq);
            follower3.handleAppendEntries(leader.sendAppendEntriesRequest(S3));
            leader.handleAppendEntriesResponse(S2,
                    AppendEntriesResponse.builder().term(1).success(true).build(),
                    noOpReq.entries().size());
            leader.handleAppendEntriesResponse(S3,
                    AppendEntriesResponse.builder().term(1).success(true).build(),
                    noOpReq.entries().size());

            // Client sends SET x 42
            ClientRequest setReq = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();
            CompletableFuture<ClientResponse> pendingFuture = leader.handleClientRequest(setReq);
            assertFalse(pendingFuture.isDone()); // not yet committed

            // Replicate to S2 and S3
            AppendEntriesRequest setReplReq = leader.sendAppendEntriesRequest(S2);
            follower2.handleAppendEntries(setReplReq);
            follower3.handleAppendEntries(leader.sendAppendEntriesRequest(S3));
            leader.handleAppendEntriesResponse(S2,
                    AppendEntriesResponse.builder().term(1).success(true).build(),
                    setReplReq.entries().size());
            leader.handleAppendEntriesResponse(S3,
                    AppendEntriesResponse.builder().term(1).success(true).build(),
                    setReplReq.entries().size());

            // Now committed — get result
            long setIndex = 2; // no-op at 1, SET at 2
            ClientResponse setResult = leader.getCommittedResult(setIndex, setReq);
            assertTrue(setResult.success());
            assertEquals("OK", setResult.result());

            // Send follow-up heartbeat to propagate updated leaderCommit to followers
            follower2.handleAppendEntries(leader.sendAppendEntriesRequest(S2));
            follower3.handleAppendEntries(leader.sendAppendEntriesRequest(S3));

            // Verify follower state machines also have the value
            KeyValueStateMachine f2sm = (KeyValueStateMachine) follower2.stateMachine;
            assertEquals("42", f2sm.getStore().get("x"));

            KeyValueStateMachine f3sm = (KeyValueStateMachine) follower3.stateMachine;
            assertEquals("42", f3sm.getStore().get("x"));
        }

        @Test
        @DisplayName("command survives leader crash — new leader has the entry")
        void commandSurvivesLeaderCrash() {
            RaftServer leader = electLeader(1);

            // Commit no-op
            simulateReplicationAndCommit(leader);

            // Client sends SET
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build());

            // Replicate to S2, S3 (committed)
            simulateReplicationAndCommit(leader);

            // Verify it's committed
            assertTrue(leader.state.commitIndex >= 2);

            // Leader crashes. S2 becomes new leader for term 2.
            // S2 should have the entry in its log (it was replicated).
            RaftServer newLeader = createServer(S2, List.of(S1, S3, S4, S5));
            // Simulate S2 having received the entries from old leader
            newLeader.state.log.add(LogEntry.builder().index(1).term(1).command("NO OP").build());
            newLeader.state.log.add(LogEntry.builder().index(2).term(1).command("SET x 42").build());
            newLeader.state.currentTerm = 2;
            newLeader.state.role = ServerRole.LEADER;
            newLeader.state.leaderID = S2;
            for (ServerID peer : List.of(S1, S3, S4, S5)) {
                newLeader.nextIndex.put(peer, 3L);
                newLeader.matchIndex.put(peer, 0L);
            }

            // New leader appends its own no-op
            LogEntry newNoOp = LogEntry.builder().index(3).term(2).command("NO OP").build();
            newLeader.state.log.add(newNoOp);

            // Replicate to majority to commit term-2 entry (which also commits prior entries)
            for (ServerID peer : List.of(S3, S4)) {
                newLeader.matchIndex.put(peer, 3L);
                newLeader.nextIndex.put(peer, 4L);
            }
            newLeader.advanceCommitIndex();

            // commitIndex should be 3 (committed term-2 no-op, which indirectly commits indices 1-2)
            assertEquals(3, newLeader.state.commitIndex);

            // Apply entries
            newLeader.applyCommittedEntries();

            // State machine should have the SET from the old leader
            KeyValueStateMachine sm = (KeyValueStateMachine) newLeader.stateMachine;
            assertEquals("42", sm.getStore().get("x"));
        }

        @Test
        @DisplayName("client request to follower redirects, then succeeds on leader")
        void clientRedirectThenSuccess() {
            RaftServer leader = electLeader(1);
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));
            follower.state.leaderID = S1; // follower knows S1 is leader

            ClientRequest req = ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build();

            // First try on follower — redirected
            ClientResponse redirect = follower.handleClientRequest(req).join();
            assertFalse(redirect.success());
            assertEquals(S1, redirect.leaderHint());

            // Client follows redirect to leader
            CompletableFuture<ClientResponse> pendingFuture = leader.handleClientRequest(req);
            assertFalse(pendingFuture.isDone()); // not yet committed

            // Commit
            simulateReplicationAndCommit(leader); // no-op
            simulateReplicationAndCommit(leader); // SET

            ClientResponse committed = leader.getCommittedResult(2, req);
            assertTrue(committed.success());
            assertEquals("OK", committed.result());
        }
    }

    // ========================== KeyValueStateMachine Unit Tests ==========================

    @Nested
    @DisplayName("KeyValueStateMachine")
    class KVStateMachineTests {

        @Test
        @DisplayName("SET and GET basic operations")
        void setAndGet() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertEquals("OK", sm.apply("SET key1 value1"));
            assertEquals("value1", sm.apply("GET key1"));
        }

        @Test
        @DisplayName("GET non-existent key returns nil")
        void getNonExistent() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertEquals("(nil)", sm.apply("GET missing"));
        }

        @Test
        @DisplayName("DEL existing key returns 1")
        void delExisting() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            sm.apply("SET a b");
            assertEquals("1", sm.apply("DEL a"));
            assertEquals("(nil)", sm.apply("GET a"));
        }

        @Test
        @DisplayName("DEL non-existent key returns 0")
        void delNonExistent() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertEquals("0", sm.apply("DEL nothing"));
        }

        @Test
        @DisplayName("SET value with spaces")
        void setValueWithSpaces() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertEquals("OK", sm.apply("SET greeting hello world"));
            assertEquals("hello world", sm.apply("GET greeting"));
        }

        @Test
        @DisplayName("NO OP returns OK")
        void noOp() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertEquals("OK", sm.apply("NO OP"));
            assertTrue(sm.getStore().isEmpty());
        }

        @Test
        @DisplayName("unknown command returns error")
        void unknownCommand() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            String result = sm.apply("FLUSHALL");
            assertTrue(result.startsWith("ERR"));
        }

        @Test
        @DisplayName("deduplication tracks per-client sessions")
        void deduplication() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            assertNull(sm.isDuplicate(1, 1));

            sm.recordExecution(1, 1, "OK");
            assertEquals("OK", sm.isDuplicate(1, 1));

            // Different serial number
            assertNull(sm.isDuplicate(1, 2));

            // Different client
            assertNull(sm.isDuplicate(2, 1));
        }
    }
}
