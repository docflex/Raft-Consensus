package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.model.SnapshotMetadata;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.ClientRequest;
import learning.rpc.ClientResponse;
import learning.rpc.InstallSnapshotRequest;
import learning.rpc.InstallSnapshotResponse;
import learning.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Milestone 5: Log Compaction")
class RaftServerLogCompactionTest {

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
        // Append and commit a no-op entry (standard leader behavior)
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

    // ========================== KV State Machine Snapshot ==========================

    @Nested
    @DisplayName("KeyValueStateMachine Snapshot")
    class KVSnapshotTests {

        @Test
        @DisplayName("takeSnapshot and loadFromSnapshot round-trip preserves store and sessions")
        void snapshotRoundTrip() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            sm.apply("SET x 1");
            sm.apply("SET y 2");
            sm.apply("SET z 3");
            sm.recordExecution(100, 1, "OK");
            sm.recordExecution(200, 5, "val");

            byte[] snapshot = sm.takeSnapshot();

            // Load into a fresh state machine
            KeyValueStateMachine sm2 = new KeyValueStateMachine();
            sm2.loadFromSnapshot(snapshot);

            assertEquals("1", sm2.getStore().get("x"));
            assertEquals("2", sm2.getStore().get("y"));
            assertEquals("3", sm2.getStore().get("z"));
            assertEquals("OK", sm2.isDuplicate(100, 1));
            assertEquals("val", sm2.isDuplicate(200, 5));
        }

        @Test
        @DisplayName("loadFromSnapshot clears existing state before restoring")
        void snapshotClearsOldState() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            sm.apply("SET old_key old_value");
            sm.recordExecution(999, 1, "old_result");

            // Create snapshot from different state machine
            KeyValueStateMachine src = new KeyValueStateMachine();
            src.apply("SET new_key new_value");
            byte[] snapshot = src.takeSnapshot();

            sm.loadFromSnapshot(snapshot);

            assertNull(sm.getStore().get("old_key"));
            assertEquals("new_value", sm.getStore().get("new_key"));
            assertNull(sm.isDuplicate(999, 1));
        }

        @Test
        @DisplayName("empty state machine snapshot round-trips correctly")
        void emptySnapshotRoundTrip() {
            KeyValueStateMachine sm = new KeyValueStateMachine();
            byte[] snapshot = sm.takeSnapshot();

            KeyValueStateMachine sm2 = new KeyValueStateMachine();
            sm2.apply("SET leftovers should_be_cleared");
            sm2.loadFromSnapshot(snapshot);

            assertTrue(sm2.getStore().isEmpty());
        }
    }

    // ========================== Trigger Snapshot ==========================

    @Nested
    @DisplayName("Trigger Snapshot")
    class TriggerSnapshotTests {

        @Test
        @DisplayName("triggerSnapshot captures state and truncates log")
        void triggerSnapshotBasic() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            // Add some client commands
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 10").build());
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET y 20").build());
            simulateReplicationAndCommit(leader);

            // Now log has: [NO OP, SET x 10, SET y 20], all committed
            assertEquals(3, leader.state.commitIndex);
            assertEquals(3, leader.state.lastApplied);
            assertEquals(3, leader.state.log.size());

            // Take snapshot
            leader.triggerSnapshot();

            // Log should be truncated
            assertEquals(0, leader.state.log.size());
            // Snapshot metadata should be set
            assertNotNull(leader.state.snapshotMetadata);
            assertEquals(3, leader.state.snapshotLastIndex());
            assertEquals(1, leader.state.snapshotLastTerm());
            assertNotNull(leader.state.snapshotData);
            // lastLogIndex should still report 3 (from snapshot)
            assertEquals(3, leader.state.lastLogIndex());
        }

        @Test
        @DisplayName("triggerSnapshot does nothing without state machine")
        void triggerSnapshotNoStateMachine() {
            RaftServer server = new RaftServer(S1, List.of(S2, S3));
            server.triggerSnapshot();
            assertNull(server.state.snapshotMetadata);
        }

        @Test
        @DisplayName("triggerSnapshot does nothing if nothing applied")
        void triggerSnapshotNothingApplied() {
            RaftServer server = createServer(S1, List.of(S2, S3));
            server.triggerSnapshot();
            assertNull(server.state.snapshotMetadata);
        }

        @Test
        @DisplayName("new entries can be appended after snapshot")
        void appendAfterSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 10").build());
            simulateReplicationAndCommit(leader);

            leader.triggerSnapshot();
            assertEquals(0, leader.state.log.size());

            // Append new command after snapshot
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET y 20").build());
            assertEquals(1, leader.state.log.size());
            assertEquals(3, leader.state.log.getFirst().index()); // index continues from snapshot
        }

        @Test
        @DisplayName("partial snapshot — only committed entries are compacted")
        void partialSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            // Add 2 commands, only commit the first one partially
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET a 1").build());
            simulateReplicationAndCommit(leader);

            // Add another command but DON'T commit it
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET b 2").build());

            // lastApplied = 2 (NO OP + SET a 1), log has 3 entries (including uncommitted SET b 2)
            leader.triggerSnapshot();

            // Snapshot at index 2 (lastApplied), uncommitted entry at index 3 remains
            assertEquals(2, leader.state.snapshotLastIndex());
            assertEquals(1, leader.state.log.size());
            assertEquals(3, leader.state.log.getFirst().index());
            assertEquals("SET b 2", leader.state.log.getFirst().command());
        }
    }

    // ========================== Install Snapshot ==========================

    @Nested
    @DisplayName("Install Snapshot")
    class InstallSnapshotTests {

        @Test
        @DisplayName("follower installs snapshot from leader")
        void followerInstallsSnapshot() {
            // Leader has state: x=10, y=20
            KeyValueStateMachine leaderSM = new KeyValueStateMachine();
            leaderSM.apply("SET x 10");
            leaderSM.apply("SET y 20");
            byte[] snapshotData = leaderSM.takeSnapshot();

            // Follower is brand new
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));

            InstallSnapshotRequest request = InstallSnapshotRequest.builder()
                    .term(1)
                    .leaderId(S1)
                    .lastIncludedIndex(3)
                    .lastIncludedTerm(1)
                    .data(snapshotData)
                    .done(true)
                    .build();

            InstallSnapshotResponse response = follower.handleInstallSnapshot(request);

            assertEquals(1, response.term());
            assertEquals(3, follower.state.snapshotLastIndex());
            assertEquals(1, follower.state.snapshotLastTerm());
            assertEquals(3, follower.state.commitIndex);
            assertEquals(3, follower.state.lastApplied);

            // State machine should have the snapshot data
            KeyValueStateMachine followerSM = (KeyValueStateMachine) follower.stateMachine;
            assertEquals("10", followerSM.getStore().get("x"));
            assertEquals("20", followerSM.getStore().get("y"));
        }

        @Test
        @DisplayName("reject InstallSnapshot with stale term")
        void rejectStaleTerm() {
            RaftServer follower = createServer(S2, List.of(S1, S3));
            follower.state.currentTerm = 5;

            InstallSnapshotRequest request = InstallSnapshotRequest.builder()
                    .term(3)
                    .leaderId(S1)
                    .lastIncludedIndex(10)
                    .lastIncludedTerm(3)
                    .data(new byte[0])
                    .done(true)
                    .build();

            InstallSnapshotResponse response = follower.handleInstallSnapshot(request);

            assertEquals(5, response.term());
            assertNull(follower.state.snapshotMetadata);
        }

        @Test
        @DisplayName("InstallSnapshot with higher term causes step-down")
        void higherTermCausesStepDown() {
            RaftServer candidate = createServer(S2, List.of(S1, S3));
            candidate.state.currentTerm = 1;
            candidate.state.role = ServerRole.CANDIDATE;

            KeyValueStateMachine sm = new KeyValueStateMachine();
            byte[] data = sm.takeSnapshot();

            InstallSnapshotRequest request = InstallSnapshotRequest.builder()
                    .term(3)
                    .leaderId(S1)
                    .lastIncludedIndex(5)
                    .lastIncludedTerm(2)
                    .data(data)
                    .done(true)
                    .build();

            follower_installs(candidate, request);

            assertEquals(ServerRole.FOLLOWER, candidate.state.role);
            assertEquals(3, candidate.state.currentTerm);
            assertEquals(S1, candidate.state.leaderID);
        }

        private void follower_installs(RaftServer server, InstallSnapshotRequest request) {
            server.handleInstallSnapshot(request);
        }

        @Test
        @DisplayName("InstallSnapshot discards conflicting log entries")
        void discardConflictingLog() {
            RaftServer follower = createServer(S2, List.of(S1, S3));
            // Follower has entries from an old leader
            follower.state.log.add(LogEntry.builder().index(1).term(1).command("old1").build());
            follower.state.log.add(LogEntry.builder().index(2).term(1).command("old2").build());
            follower.state.log.add(LogEntry.builder().index(3).term(1).command("old3").build());

            KeyValueStateMachine sm = new KeyValueStateMachine();
            sm.apply("SET x new_value");
            byte[] data = sm.takeSnapshot();

            InstallSnapshotRequest request = InstallSnapshotRequest.builder()
                    .term(2)
                    .leaderId(S1)
                    .lastIncludedIndex(5)
                    .lastIncludedTerm(2)
                    .data(data)
                    .done(true)
                    .build();

            follower.handleInstallSnapshot(request);

            // All old entries should be gone
            assertTrue(follower.state.log.isEmpty());
            assertEquals(5, follower.state.snapshotLastIndex());
            assertEquals("new_value", ((KeyValueStateMachine) follower.stateMachine).getStore().get("x"));
        }
    }

    // ========================== Peer Needs Snapshot ==========================

    @Nested
    @DisplayName("Peer Needs Snapshot")
    class PeerNeedsSnapshotTests {

        @Test
        @DisplayName("peerNeedsSnapshot returns true when nextIndex <= snapshot.lastIncludedIndex")
        void peerBehindSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 1").build());
            simulateReplicationAndCommit(leader);

            leader.triggerSnapshot();

            // Simulate a lagging peer whose nextIndex is 1 (way behind snapshot at index 2)
            leader.nextIndex.put(S2, 1L);
            assertTrue(leader.peerNeedsSnapshot(S2));
        }

        @Test
        @DisplayName("peerNeedsSnapshot returns false when nextIndex > snapshot.lastIncludedIndex")
        void peerCaughtUp() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            leader.triggerSnapshot();

            leader.nextIndex.put(S2, 2L); // snapshot ends at 1, nextIndex is 2
            assertFalse(leader.peerNeedsSnapshot(S2));
        }

        @Test
        @DisplayName("peerNeedsSnapshot returns false when no snapshot exists")
        void noSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);
            assertFalse(leader.peerNeedsSnapshot(S2));
        }

        @Test
        @DisplayName("buildInstallSnapshotRequest creates correct request")
        void buildRequest() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build());
            simulateReplicationAndCommit(leader);

            leader.triggerSnapshot();

            InstallSnapshotRequest req = leader.buildInstallSnapshotRequest(S2);
            assertEquals(1, req.term());
            assertEquals(S1, req.leaderId());
            assertEquals(2, req.lastIncludedIndex());
            assertEquals(1, req.lastIncludedTerm());
            assertNotNull(req.data());
            assertTrue(req.done());
        }
    }

    // ========================== End-to-End Log Compaction ==========================

    @Nested
    @DisplayName("End-to-End Log Compaction")
    class EndToEnd {

        @Test
        @DisplayName("lagging follower catches up via InstallSnapshot + AppendEntries")
        void laggingFollowerCatchesUp() {
            RaftServer leader = electLeader(1);
            RaftServer follower = createServer(S2, List.of(S1, S3, S4, S5));

            // Replicate no-op and first command to all peers (but not follower S2)
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 10").build());
            simulateReplicationAndCommit(leader);

            // Take snapshot at index 2 — log truncated
            leader.triggerSnapshot();
            assertEquals(0, leader.state.log.size());

            // Add new command after snapshot
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(2).command("SET y 20").build());
            simulateReplicationAndCommit(leader);

            // Follower S2 is lagging — nextIndex=1, snapshot covers up to index 2
            leader.nextIndex.put(S2, 1L);
            assertTrue(leader.peerNeedsSnapshot(S2));

            // Step 1: Send InstallSnapshot
            InstallSnapshotRequest snapReq = leader.buildInstallSnapshotRequest(S2);
            InstallSnapshotResponse snapResp = follower.handleInstallSnapshot(snapReq);
            leader.handleInstallSnapshotResponse(S2, snapResp);

            // Follower should now have snapshot state
            assertEquals(2, follower.state.snapshotLastIndex());
            assertEquals("10", ((KeyValueStateMachine) follower.stateMachine).getStore().get("x"));
            assertEquals(3, leader.nextIndex.get(S2)); // snapshot end + 1

            // Step 2: Send remaining entries via AppendEntries
            assertFalse(leader.peerNeedsSnapshot(S2));
            AppendEntriesRequest aeReq = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse aeResp = follower.handleAppendEntries(aeReq);
            assertTrue(aeResp.success());

            // Follower should now have the new command too
            assertEquals("20", ((KeyValueStateMachine) follower.stateMachine).getStore().get("y"));
        }

        @Test
        @DisplayName("snapshot + new entries: leader continues appending after snapshot")
        void leaderContinuesAfterSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            // 3 commands, commit all
            for (int i = 1; i <= 3; i++) {
                leader.handleClientRequest(ClientRequest.builder()
                        .clientId(100).serialNumber(i).command("SET k" + i + " v" + i).build());
            }
            simulateReplicationAndCommit(leader);

            // Snapshot at index 4 (NO OP + 3 commands)
            assertEquals(4, leader.state.lastApplied);
            leader.triggerSnapshot();

            // Add 2 more commands
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(4).command("SET k4 v4").build());
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(5).command("SET k5 v5").build());

            assertEquals(2, leader.state.log.size());
            assertEquals(5, leader.state.log.getFirst().index());
            assertEquals(6, leader.state.log.getLast().index());
            assertEquals(6, leader.state.lastLogIndex());

            // Replicate and commit the new entries
            simulateReplicationAndCommit(leader);
            assertEquals(6, leader.state.commitIndex);
        }

        @Test
        @DisplayName("snapshot preserves deduplication across compaction")
        void deduplicationSurvivesSnapshot() {
            RaftServer leader = electLeader(1);
            simulateReplicationAndCommit(leader);

            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build());
            simulateReplicationAndCommit(leader);

            leader.triggerSnapshot();

            // Retry the same command — should still be deduplicated
            ClientResponse resp = leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build()).join();
            assertTrue(resp.success());
            assertEquals("OK", resp.result());
        }

        @Test
        @DisplayName("RaftState index helpers work correctly after snapshot")
        void indexHelpersAfterSnapshot() {
            RaftServer server = createServer(S1, List.of(S2));
            // Simulate having a snapshot at index 5
            server.state.snapshotMetadata = SnapshotMetadata.builder()
                    .lastIncludedIndex(5).lastIncludedTerm(2).build();
            // And 3 entries after: indices 6, 7, 8
            server.state.log.add(LogEntry.builder().index(6).term(2).command("a").build());
            server.state.log.add(LogEntry.builder().index(7).term(3).command("b").build());
            server.state.log.add(LogEntry.builder().index(8).term(3).command("c").build());

            assertEquals(8, server.state.lastLogIndex());
            assertEquals(3, server.state.lastLogTerm());
            assertEquals(6, server.state.firstLogIndex());
            assertEquals(5, server.state.snapshotLastIndex());
            assertEquals(2, server.state.snapshotLastTerm());

            // termAt
            assertEquals(2, server.state.termAt(5)); // snapshot boundary
            assertEquals(2, server.state.termAt(6));
            assertEquals(3, server.state.termAt(7));

            // getEntry
            assertNull(server.state.getEntry(4)); // before snapshot
            assertNull(server.state.getEntry(5)); // at snapshot boundary (not in log)
            assertEquals("a", server.state.getEntry(6).command());
            assertEquals("c", server.state.getEntry(8).command());
            assertNull(server.state.getEntry(9)); // beyond log

            // toListPos
            assertEquals(0, server.state.toListPos(6));
            assertEquals(2, server.state.toListPos(8));
            assertEquals(-1, server.state.toListPos(5)); // snapshot boundary → -1
        }
    }
}
