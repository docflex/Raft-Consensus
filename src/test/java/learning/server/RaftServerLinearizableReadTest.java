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

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Milestone 7: Linearizable Reads")
class RaftServerLinearizableReadTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);

    private RaftServer createServer(ServerID id, List<ServerID> peers) {
        return new RaftServer(id, peers, null, new KeyValueStateMachine());
    }

    private RaftServer electLeader(int term) {
        RaftServer server = createServer(S1, List.of(S2, S3, S4, S5));
        server.state.currentTerm = term;
        server.state.role = ServerRole.LEADER;
        server.state.leaderID = S1;
        for (ServerID peer : server.peers) {
            server.nextIndex.put(peer, 1L);
            server.matchIndex.put(peer, 0L);
        }
        server.state.log.add(LogEntry.builder().index(1).term(term).command("NO OP").build());
        return server;
    }

    private void simulateReplicationAndCommit(RaftServer leader) {
        for (ServerID peer : leader.peers) {
            AppendEntriesRequest req = leader.sendAppendEntriesRequest(peer);
            leader.handleAppendEntriesResponse(peer,
                    AppendEntriesResponse.builder().term(leader.state.currentTerm).success(true).build(),
                    req.entries().size());
        }
    }

    private void writeAndCommit(RaftServer leader, String command, long clientId, long serial) {
        leader.handleClientRequest(ClientRequest.builder()
                .clientId(clientId).serialNumber(serial).command(command).build());
        simulateReplicationAndCommit(leader);
    }

    // ========================== ReadIndex Protocol ==========================

    @Nested
    @DisplayName("ReadIndex Protocol")
    class ReadIndexProtocol {

        @Test
        @DisplayName("non-leader redirects read-only query")
        void nonLeaderRedirect() {
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));
            follower.state.leaderID = S1;

            ClientResponse resp = follower.handleReadOnlyQuery("GET x");

            assertFalse(resp.success());
            assertEquals(S1, resp.leaderHint());
        }

        @Test
        @DisplayName("leader initiates readIndex — returns PENDING until confirmed")
        void leaderInitiatesReadIndex() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            ClientResponse resp = leader.handleReadOnlyQuery("GET x");

            // Not yet confirmed — need majority heartbeat acks
            assertFalse(resp.success());
            assertEquals("PENDING_LEADER_CONFIRM", resp.result());
            assertFalse(leader.isReadIndexConfirmed());
        }

        @Test
        @DisplayName("readIndex confirmed after majority heartbeat acks")
        void confirmedAfterMajorityAcks() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            leader.handleReadOnlyQuery("GET x");
            assertFalse(leader.isReadIndexConfirmed());

            // 5-node cluster: need 3 acks (self + 2 peers)
            leader.confirmReadIndex(S2);
            assertFalse(leader.isReadIndexConfirmed());

            leader.confirmReadIndex(S3);
            assertTrue(leader.isReadIndexConfirmed());
        }

        @Test
        @DisplayName("duplicate ack from same peer doesn't double-count")
        void duplicateAckIgnored() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleReadOnlyQuery("GET x");

            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S2); // duplicate
            assertFalse(leader.isReadIndexConfirmed()); // still only 2 acks (self + S2)

            leader.confirmReadIndex(S3);
            assertTrue(leader.isReadIndexConfirmed()); // now 3 acks
        }

        @Test
        @DisplayName("readIndex records commitIndex at time of request")
        void readIndexCapturesCommitIndex() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            assertEquals(2, leader.state.commitIndex);

            leader.handleReadOnlyQuery("GET x");
            assertEquals(2, leader.readIndex);
        }
    }

    // ========================== Execute Read ==========================

    @Nested
    @DisplayName("Execute Read-Only Query")
    class ExecuteRead {

        @Test
        @DisplayName("execute returns correct value after confirmation")
        void executeAfterConfirmation() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);

            ClientResponse resp = leader.executeReadOnlyQuery("GET x");
            assertTrue(resp.success());
            assertEquals("42", resp.result());
        }

        @Test
        @DisplayName("execute fails if not confirmed")
        void executeBeforeConfirmation() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleReadOnlyQuery("GET x");
            // Don't confirm

            ClientResponse resp = leader.executeReadOnlyQuery("GET x");
            assertFalse(resp.success());
            assertEquals("LEADER_NOT_CONFIRMED", resp.result());
        }

        @Test
        @DisplayName("read does NOT append to log")
        void readDoesNotAppendToLog() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            int logSizeBefore = leader.state.log.size();

            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);
            leader.executeReadOnlyQuery("GET x");

            assertEquals(logSizeBefore, leader.state.log.size());
        }

        @Test
        @DisplayName("read returns (nil) for non-existent key")
        void readNonExistentKey() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleReadOnlyQuery("GET missing");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);

            ClientResponse resp = leader.executeReadOnlyQuery("GET missing");
            assertTrue(resp.success());
            assertEquals("(nil)", resp.result());
        }

        @Test
        @DisplayName("multiple reads return latest committed state")
        void multipleReads() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            writeAndCommit(leader, "SET x 1", 100, 1);
            writeAndCommit(leader, "SET x 2", 100, 2);
            writeAndCommit(leader, "SET x 3", 100, 3);

            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);

            ClientResponse resp = leader.executeReadOnlyQuery("GET x");
            assertTrue(resp.success());
            assertEquals("3", resp.result());
        }
    }

    // ========================== Single-Node Cluster ==========================

    @Nested
    @DisplayName("Single-Node Cluster")
    class SingleNode {

        @Test
        @DisplayName("single-node leader immediately executes read")
        void singleNodeImmediateRead() {
            RaftServer leader = new RaftServer(S1, List.of(), null, new KeyValueStateMachine());
            leader.state.currentTerm = 1;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.add(LogEntry.builder().index(1).term(1).command("NO OP").build());
            leader.state.commitIndex = 1;
            leader.state.lastApplied = 1;

            leader.stateMachine.apply("SET x 99");

            ClientResponse resp = leader.handleReadOnlyQuery("GET x");
            assertTrue(resp.success());
            assertEquals("99", resp.result());
            assertTrue(leader.isReadIndexConfirmed());
        }
    }

    // ========================== Safety ==========================

    @Nested
    @DisplayName("Safety Guarantees")
    class Safety {

        @Test
        @DisplayName("readIndex state cleared on step-down")
        void clearedOnStepDown() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);
            assertTrue(leader.isReadIndexConfirmed());

            // Step down due to higher term
            leader.handleAppendEntries(AppendEntriesRequest.builder()
                    .term(2)
                    .leaderId(S2)
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(List.of())
                    .leaderCommit(0)
                    .build());

            assertFalse(leader.isReadIndexConfirmed());
            assertTrue(leader.readIndexAcks.isEmpty());
            assertEquals(ServerRole.FOLLOWER, leader.state.role);
        }

        @Test
        @DisplayName("new readIndex request resets previous confirmation")
        void newReadResetsOld() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            // First read — confirmed
            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);
            assertTrue(leader.isReadIndexConfirmed());

            // Second read — resets state
            leader.handleReadOnlyQuery("GET y");
            assertFalse(leader.isReadIndexConfirmed());
            assertEquals(1, leader.readIndexAcks.size()); // only self
        }

        @Test
        @DisplayName("read sees writes committed before readIndex")
        void readAfterWriteLinearizability() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            // Write x=10, then x=20
            writeAndCommit(leader, "SET x 10", 100, 1);
            writeAndCommit(leader, "SET x 20", 100, 2);

            // ReadIndex captures commitIndex=3 (NO OP + SET x 10 + SET x 20)
            leader.handleReadOnlyQuery("GET x");
            assertEquals(3, leader.readIndex);

            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);

            ClientResponse resp = leader.executeReadOnlyQuery("GET x");
            assertTrue(resp.success());
            assertEquals("20", resp.result());
        }
    }

    // ========================== End-to-End ==========================

    @Nested
    @DisplayName("End-to-End Linearizable Read Flow")
    class EndToEnd {

        @Test
        @DisplayName("full flow: write → read with heartbeat confirmation")
        void writeReadFlow() {
            RaftServer leader = electLeader(1);
            RaftServer follower2 = createServer(S2, List.of(S1, S3, S4, S5));
            RaftServer follower3 = createServer(S3, List.of(S1, S2, S4, S5));

            // Write a value (commit via simulated peers S4, S5)
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET counter 100", 100, 1);

            // Catch up real followers so they can respond to heartbeats
            leader.nextIndex.put(S2, 1L);
            leader.nextIndex.put(S3, 1L);
            AppendEntriesResponse catchUp2 = follower2.handleAppendEntries(leader.sendAppendEntriesRequest(S2));
            assertTrue(catchUp2.success());
            AppendEntriesResponse catchUp3 = follower3.handleAppendEntries(leader.sendAppendEntriesRequest(S3));
            assertTrue(catchUp3.success());
            // Update leader's tracking
            leader.handleAppendEntriesResponse(S2, catchUp2, 2);
            leader.handleAppendEntriesResponse(S3, catchUp3, 2);

            // Initiate a read
            ClientResponse pending = leader.handleReadOnlyQuery("GET counter");
            assertFalse(pending.success());

            // Send heartbeats to followers and collect acks
            AppendEntriesRequest hb2 = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp2 = follower2.handleAppendEntries(hb2);
            assertTrue(resp2.success());
            leader.confirmReadIndex(S2);

            AppendEntriesRequest hb3 = leader.sendAppendEntriesRequest(S3);
            AppendEntriesResponse resp3 = follower3.handleAppendEntries(hb3);
            assertTrue(resp3.success());
            leader.confirmReadIndex(S3);

            // Now confirmed — execute the read
            assertTrue(leader.isReadIndexConfirmed());
            ClientResponse result = leader.executeReadOnlyQuery("GET counter");
            assertTrue(result.success());
            assertEquals("100", result.result());

            // Verify log was NOT modified by the read
            assertEquals(2, leader.state.log.size()); // NO OP + SET counter
        }

        @Test
        @DisplayName("read after snapshot — state machine has data even though log is truncated")
        void readAfterSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            writeAndCommit(leader, "SET x 42", 100, 1);

            // Snapshot and truncate
            leader.triggerSnapshot();
            assertEquals(0, leader.state.log.size());

            // Read should still work — state machine has x=42
            leader.handleReadOnlyQuery("GET x");
            leader.confirmReadIndex(S2);
            leader.confirmReadIndex(S3);

            ClientResponse resp = leader.executeReadOnlyQuery("GET x");
            assertTrue(resp.success());
            assertEquals("42", resp.result());
        }
    }
}
