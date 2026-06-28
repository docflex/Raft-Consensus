package learning.server;

import learning.model.ClusterConfig;
import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.ClientRequest;
import learning.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Milestone 6: Cluster Membership Changes")
class RaftServerMembershipTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);
    static final ServerID S6 = new ServerID(6);

    private RaftServer createServer(ServerID id, List<ServerID> peers) {
        return new RaftServer(id, peers, null, new KeyValueStateMachine());
    }

    private RaftServer electLeader(ServerID id, List<ServerID> peers, int term) {
        RaftServer server = createServer(id, peers);
        server.state.currentTerm = term;
        server.state.role = ServerRole.LEADER;
        server.state.leaderID = id;
        for (ServerID peer : peers) {
            server.nextIndex.put(peer, 1L);
            server.matchIndex.put(peer, 0L);
        }
        server.state.log.add(LogEntry.builder().index(1).term(term).command("NO OP").build());
        return server;
    }

    private void simulateReplicationAndCommit(RaftServer leader) {
        for (ServerID peer : leader.peers) {
            if (!leader.nextIndex.containsKey(peer)) continue;
            AppendEntriesRequest req = leader.sendAppendEntriesRequest(peer);
            leader.handleAppendEntriesResponse(peer,
                    AppendEntriesResponse.builder().term(leader.state.currentTerm).success(true).build(),
                    req.entries().size());
        }
    }

    // ========================== ClusterConfig ==========================

    @Nested
    @DisplayName("ClusterConfig")
    class ClusterConfigTests {

        @Test
        @DisplayName("majority calculation for various cluster sizes")
        void majorityCalculation() {
            // 1 node → majority 1
            assertEquals(1, new ClusterConfig(Set.of(S1)).majority());
            // 2 nodes → majority 2
            assertEquals(2, new ClusterConfig(Set.of(S1, S2)).majority());
            // 3 nodes → majority 2
            assertEquals(2, new ClusterConfig(Set.of(S1, S2, S3)).majority());
            // 4 nodes → majority 3
            assertEquals(3, new ClusterConfig(Set.of(S1, S2, S3, S4)).majority());
            // 5 nodes → majority 3
            assertEquals(3, new ClusterConfig(Set.of(S1, S2, S3, S4, S5)).majority());
        }

        @Test
        @DisplayName("withAdded creates new config with additional member")
        void withAdded() {
            ClusterConfig config = new ClusterConfig(Set.of(S1, S2, S3));
            ClusterConfig newConfig = config.withAdded(S4);
            assertEquals(3, config.size());
            assertEquals(4, newConfig.size());
            assertTrue(newConfig.contains(S4));
        }

        @Test
        @DisplayName("withRemoved creates new config without the member")
        void withRemoved() {
            ClusterConfig config = new ClusterConfig(Set.of(S1, S2, S3));
            ClusterConfig newConfig = config.withRemoved(S3);
            assertEquals(3, config.size());
            assertEquals(2, newConfig.size());
            assertFalse(newConfig.contains(S3));
        }

        @Test
        @DisplayName("getPeers returns all members except self")
        void getPeers() {
            ClusterConfig config = new ClusterConfig(Set.of(S1, S2, S3));
            Set<ServerID> peers = config.getPeers(S1);
            assertEquals(2, peers.size());
            assertFalse(peers.contains(S1));
            assertTrue(peers.contains(S2));
            assertTrue(peers.contains(S3));
        }
    }

    // ========================== Add Server ==========================

    @Nested
    @DisplayName("Add Server")
    class AddServerTests {

        @Test
        @DisplayName("leader accepts addServer and appends config entry")
        void leaderAcceptsAdd() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            assertTrue(leader.addServer(S4));

            // Config entry appended to log
            LogEntry last = leader.state.log.getLast();
            assertEquals("CONFIG_ADD 4", last.command());

            // nextIndex/matchIndex initialized for new peer
            assertNotNull(leader.nextIndex.get(S4));
            assertNotNull(leader.matchIndex.get(S4));
        }

        @Test
        @DisplayName("non-leader rejects addServer")
        void followerRejectsAdd() {
            RaftServer follower = createServer(S1, List.of(S2, S3));
            assertFalse(follower.addServer(S4));
        }

        @Test
        @DisplayName("reject addServer for already-member")
        void rejectDuplicateAdd() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);
            assertFalse(leader.addServer(S2));
        }

        @Test
        @DisplayName("reject addServer while another config change is pending")
        void rejectConcurrentConfigChange() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);
            assertTrue(leader.addServer(S4));  // First one accepted
            assertFalse(leader.addServer(S5)); // Second one rejected
        }

        @Test
        @DisplayName("config updates when addServer entry is committed")
        void configAppliedOnCommit() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            leader.addServer(S4);

            // Before commit: config still has 3 members
            assertEquals(3, leader.config.size());

            // Commit the config entry
            simulateReplicationAndCommit(leader);

            // After commit: config has 4 members
            assertEquals(4, leader.config.size());
            assertTrue(leader.config.contains(S4));
            assertTrue(leader.peers.contains(S4));
        }
    }

    // ========================== Remove Server ==========================

    @Nested
    @DisplayName("Remove Server")
    class RemoveServerTests {

        @Test
        @DisplayName("leader accepts removeServer and appends config entry")
        void leaderAcceptsRemove() {
            RaftServer leader = electLeader(S1, List.of(S2, S3, S4), 1);
            simulateReplicationAndCommit(leader);

            assertTrue(leader.removeServer(S4));

            LogEntry last = leader.state.log.getLast();
            assertEquals("CONFIG_REMOVE 4", last.command());
        }

        @Test
        @DisplayName("non-leader rejects removeServer")
        void followerRejectsRemove() {
            RaftServer follower = createServer(S1, List.of(S2, S3));
            assertFalse(follower.removeServer(S2));
        }

        @Test
        @DisplayName("reject removeServer for non-member")
        void rejectNonMemberRemove() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);
            assertFalse(leader.removeServer(S5));
        }

        @Test
        @DisplayName("config updates when removeServer entry is committed")
        void configAppliedOnCommit() {
            RaftServer leader = electLeader(S1, List.of(S2, S3, S4), 1);
            simulateReplicationAndCommit(leader);

            leader.removeServer(S4);
            assertEquals(4, leader.config.size());

            simulateReplicationAndCommit(leader);

            assertEquals(3, leader.config.size());
            assertFalse(leader.config.contains(S4));
            assertFalse(leader.peers.contains(S4));
            assertFalse(leader.nextIndex.containsKey(S4));
            assertFalse(leader.matchIndex.containsKey(S4));
        }

        @Test
        @DisplayName("leader steps down when it removes itself")
        void leaderSelfRemoval() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            assertTrue(leader.removeServer(S1));
            simulateReplicationAndCommit(leader);

            assertEquals(ServerRole.FOLLOWER, leader.state.role);
            assertNull(leader.state.leaderID);
            assertFalse(leader.config.contains(S1));
        }
    }

    // ========================== Safety Guarantees ==========================

    @Nested
    @DisplayName("Safety Guarantees")
    class SafetyTests {

        @Test
        @DisplayName("only one config change at a time")
        void singleConfigChangeAtATime() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            assertTrue(leader.addServer(S4));
            assertTrue(leader.hasUncommittedConfigChange());
            assertFalse(leader.addServer(S5));
            assertFalse(leader.removeServer(S2));

            // Commit the first one
            simulateReplicationAndCommit(leader);
            assertFalse(leader.hasUncommittedConfigChange());

            // Now second one is allowed
            assertTrue(leader.addServer(S5));
        }

        @Test
        @DisplayName("majority changes correctly after add — 3→4 nodes")
        void majorityAfterAdd() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);
            assertEquals(2, leader.config.majority()); // 3 nodes → majority 2

            leader.addServer(S4);
            simulateReplicationAndCommit(leader);
            assertEquals(3, leader.config.majority()); // 4 nodes → majority 3
        }

        @Test
        @DisplayName("majority changes correctly after remove — 5→4 nodes")
        void majorityAfterRemove() {
            RaftServer leader = electLeader(S1, List.of(S2, S3, S4, S5), 1);
            simulateReplicationAndCommit(leader);
            assertEquals(3, leader.config.majority()); // 5 nodes → majority 3

            leader.removeServer(S5);
            simulateReplicationAndCommit(leader);
            assertEquals(3, leader.config.majority()); // 4 nodes → majority 3 (still)
        }

        @Test
        @DisplayName("config change is replicated to followers")
        void configReplicatedToFollower() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            RaftServer follower = createServer(S2, List.of(S1, S3));

            // Commit no-op on leader side (using simulated S3 response only)
            leader.handleAppendEntriesResponse(S3,
                    AppendEntriesResponse.builder().term(1).success(true).build(), 1);

            // Replicate no-op to real follower
            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp = follower.handleAppendEntries(req);
            assertTrue(resp.success());
            leader.handleAppendEntriesResponse(S2, resp, req.entries().size());
            assertEquals(1, follower.state.log.size());

            // Leader adds S4
            leader.addServer(S4);

            // Commit the config entry (S3 replicates)
            leader.handleAppendEntriesResponse(S3,
                    AppendEntriesResponse.builder().term(1).success(true).build(), 1);

            // Replicate config change + updated leaderCommit to real follower
            req = leader.sendAppendEntriesRequest(S2);
            resp = follower.handleAppendEntries(req);
            assertTrue(resp.success());
            leader.handleAppendEntriesResponse(S2, resp, req.entries().size());

            // Follower should have the config entry in its log
            assertEquals(2, follower.state.log.size());
            LogEntry configEntry = follower.state.log.getLast();
            assertEquals("CONFIG_ADD 4", configEntry.command());

            // Follower's config should be updated (after commit application)
            assertEquals(4, follower.config.size());
            assertTrue(follower.config.contains(S4));
        }
    }

    // ========================== End-to-End ==========================

    @Nested
    @DisplayName("End-to-End Membership Changes")
    class EndToEnd {

        @Test
        @DisplayName("add server → replicate → client command works with new majority")
        void addServerThenClientCommand() {
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            // Add S4
            leader.addServer(S4);
            simulateReplicationAndCommit(leader);
            assertEquals(4, leader.config.size());

            // Client command — must now satisfy majority of 3 (out of 4)
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET x 42").build());

            // Simulate only S2 and S3 replicate (2 out of 3 peers), plus leader = 3 >= majority(3)
            for (ServerID peer : List.of(S2, S3)) {
                AppendEntriesRequest req = leader.sendAppendEntriesRequest(peer);
                leader.handleAppendEntriesResponse(peer,
                        AppendEntriesResponse.builder().term(1).success(true).build(),
                        req.entries().size());
            }

            assertEquals(3, leader.state.commitIndex);
        }

        @Test
        @DisplayName("remove server → client command works with smaller majority")
        void removeServerThenClientCommand() {
            RaftServer leader = electLeader(S1, List.of(S2, S3, S4, S5), 1);
            simulateReplicationAndCommit(leader);

            // Remove S5 and S4
            leader.removeServer(S5);
            simulateReplicationAndCommit(leader);

            leader.removeServer(S4);
            simulateReplicationAndCommit(leader);

            assertEquals(3, leader.config.size()); // S1, S2, S3

            // Client command — majority is now 2 (out of 3)
            leader.handleClientRequest(ClientRequest.builder()
                    .clientId(100).serialNumber(1).command("SET y 99").build());

            // Only S2 replicates: leader(1) + S2(1) = 2 >= majority(2) ← should commit
            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);
            leader.handleAppendEntriesResponse(S2,
                    AppendEntriesResponse.builder().term(1).success(true).build(),
                    req.entries().size());

            assertEquals(4, leader.state.commitIndex);
        }

        @Test
        @DisplayName("sequential add + remove produces correct final config")
        void sequentialAddRemove() {
            // Start with {S1, S2, S3}
            RaftServer leader = electLeader(S1, List.of(S2, S3), 1);
            simulateReplicationAndCommit(leader);

            // Add S4 → {S1, S2, S3, S4}
            leader.addServer(S4);
            simulateReplicationAndCommit(leader);
            assertEquals(4, leader.config.size());

            // Add S5 → {S1, S2, S3, S4, S5}
            leader.addServer(S5);
            simulateReplicationAndCommit(leader);
            assertEquals(5, leader.config.size());

            // Remove S2 → {S1, S3, S4, S5}
            leader.removeServer(S2);
            simulateReplicationAndCommit(leader);
            assertEquals(4, leader.config.size());
            assertFalse(leader.config.contains(S2));

            // Final check
            assertTrue(leader.config.contains(S1));
            assertTrue(leader.config.contains(S3));
            assertTrue(leader.config.contains(S4));
            assertTrue(leader.config.contains(S5));
        }
    }
}
