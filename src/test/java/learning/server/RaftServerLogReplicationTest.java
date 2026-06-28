package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftServerLogReplicationTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);

    // ========================== handleAppendEntries() ==========================

    @Nested
    @DisplayName("handleAppendEntries()")
    class HandleAppendEntries {

        RaftServer follower;

        @BeforeEach
        void setup() {
            follower = new RaftServer(S2, List.of(S1, S3, S4, S5));
        }

        @Test
        @DisplayName("rejects AppendEntries with stale term")
        void rejectStaleTerm() {
            follower.state.currentTerm = 3;

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(2).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertFalse(resp.success());
            assertEquals(3, resp.term());
            // Should NOT step down or change role
            assertEquals(3, follower.state.currentTerm);
        }

        @Test
        @DisplayName("accepts heartbeat (empty entries) with current term")
        void acceptHeartbeat() {
            follower.state.currentTerm = 1;

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(1, resp.term());
            assertEquals(ServerRole.FOLLOWER, follower.state.role);
            assertEquals(S1, follower.state.leaderID);
        }

        @Test
        @DisplayName("steps down and accepts when receiving higher term")
        void stepDownOnHigherTerm() {
            follower.state.currentTerm = 1;
            follower.state.role = ServerRole.CANDIDATE;

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(3).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(3, follower.state.currentTerm);
            assertEquals(ServerRole.FOLLOWER, follower.state.role);
            assertNull(follower.state.votedFor);
            assertEquals(S1, follower.state.leaderID);
        }

        @Test
        @DisplayName("consistency check fails: log too short")
        void consistencyFailLogTooShort() {
            follower.state.currentTerm = 1;
            // Follower has empty log, but prevLogIndex=2 requires entry at index 2

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(2).prevLogTerm(1)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertFalse(resp.success());
            assertEquals(1, resp.term());
        }

        @Test
        @DisplayName("consistency check fails: term mismatch at prevLogIndex")
        void consistencyFailTermMismatch() {
            follower.state.currentTerm = 2;
            // Follower has entry at index 1 with term 1
            follower.state.log.add(new LogEntry(1, 1, "a"));

            // Leader says prevLogIndex=1 should have term=2 (conflict!)
            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(2).leaderId(S1).prevLogIndex(1).prevLogTerm(2)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertFalse(resp.success());
            // Conflicting entry should be deleted
            assertEquals(0, follower.state.log.size());
        }

        @Test
        @DisplayName("appends new entries to empty log")
        void appendToEmptyLog() {
            follower.state.currentTerm = 1;

            List<LogEntry> entries = List.of(
                    new LogEntry(1, 1, "x"),
                    new LogEntry(2, 1, "y"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(entries).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(2, follower.state.log.size());
            assertEquals("x", follower.state.log.get(0).command());
            assertEquals("y", follower.state.log.get(1).command());
        }

        @Test
        @DisplayName("appends entries after existing log")
        void appendAfterExistingLog() {
            follower.state.currentTerm = 1;
            follower.state.log.add(new LogEntry(1, 1, "a"));
            follower.state.log.add(new LogEntry(2, 1, "b"));

            List<LogEntry> newEntries = List.of(
                    new LogEntry(3, 1, "c"),
                    new LogEntry(4, 1, "d"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(2).prevLogTerm(1)
                    .entries(newEntries).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(4, follower.state.log.size());
            assertEquals("c", follower.state.log.get(2).command());
            assertEquals("d", follower.state.log.get(3).command());
        }

        @Test
        @DisplayName("skips already-present entries (idempotent)")
        void skipExistingEntries() {
            follower.state.currentTerm = 1;
            follower.state.log.add(new LogEntry(1, 1, "a"));
            follower.state.log.add(new LogEntry(2, 1, "b"));

            // Resend same entries — should be idempotent
            List<LogEntry> entries = List.of(
                    new LogEntry(1, 1, "a"),
                    new LogEntry(2, 1, "b"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(entries).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(2, follower.state.log.size()); // no duplicates
        }

        @Test
        @DisplayName("truncates conflicting entries and appends new ones")
        void truncateConflictAndAppend() {
            follower.state.currentTerm = 2;
            // Follower has: [1,t1,"a"], [2,t1,"b"], [3,t1,"old"]
            follower.state.log.add(new LogEntry(1, 1, "a"));
            follower.state.log.add(new LogEntry(2, 1, "b"));
            follower.state.log.add(new LogEntry(3, 1, "old"));

            // Leader sends entry at index 3 with term 2 (conflicts with follower's term 1)
            List<LogEntry> newEntries = List.of(
                    new LogEntry(3, 2, "new"),
                    new LogEntry(4, 2, "extra"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(2).leaderId(S1).prevLogIndex(2).prevLogTerm(1)
                    .entries(newEntries).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(4, follower.state.log.size());
            assertEquals("a", follower.state.log.get(0).command());
            assertEquals("b", follower.state.log.get(1).command());
            assertEquals("new", follower.state.log.get(2).command());
            assertEquals("extra", follower.state.log.get(3).command());
            assertEquals(2, follower.state.log.get(2).term());
        }

        @Test
        @DisplayName("advances commitIndex to min(leaderCommit, lastNewIndex)")
        void advanceCommitIndex() {
            follower.state.currentTerm = 1;

            List<LogEntry> entries = List.of(
                    new LogEntry(1, 1, "x"),
                    new LogEntry(2, 1, "y"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(entries).leaderCommit(1).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(1, follower.state.commitIndex);
        }

        @Test
        @DisplayName("commitIndex capped at last log index even if leaderCommit is higher")
        void commitIndexCappedAtLastLogIndex() {
            follower.state.currentTerm = 1;

            List<LogEntry> entries = List.of(new LogEntry(1, 1, "x"));

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(1).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(entries).leaderCommit(100).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(1, follower.state.commitIndex); // capped at lastLogIndex=1
        }

        @Test
        @DisplayName("candidate steps down when receiving AppendEntries from current term leader")
        void candidateStepsDownOnAppendEntries() {
            follower.state.currentTerm = 2;
            follower.state.role = ServerRole.CANDIDATE;

            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(2).leaderId(S1).prevLogIndex(0).prevLogTerm(0)
                    .entries(List.of()).leaderCommit(0).build();

            AppendEntriesResponse resp = follower.handleAppendEntries(req);

            assertTrue(resp.success());
            assertEquals(ServerRole.FOLLOWER, follower.state.role);
            assertEquals(S1, follower.state.leaderID);
        }
    }

    // ========================== sendAppendEntriesRequest() ==========================

    @Nested
    @DisplayName("sendAppendEntriesRequest()")
    class SendAppendEntries {

        RaftServer leader;

        @BeforeEach
        void setup() {
            leader = new RaftServer(S1, List.of(S2, S3, S4, S5));
            leader.state.currentTerm = 1;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
        }

        @Test
        @DisplayName("sends heartbeat when follower is up to date")
        void heartbeatWhenUpToDate() {
            // Leader has entry at index 1
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.nextIndex.put(S2, 2L);  // peer is up to date
            leader.matchIndex.put(S2, 1L);

            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);

            assertEquals(1, req.term());
            assertEquals(S1, req.leaderId());
            assertEquals(1, req.prevLogIndex());
            assertEquals(1, req.prevLogTerm());
            assertEquals(0, req.entries().size()); // heartbeat, no new entries
        }

        @Test
        @DisplayName("sends entries from nextIndex onwards")
        void sendsEntriesFromNextIndex() {
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.state.log.add(new LogEntry(2, 1, "b"));
            leader.state.log.add(new LogEntry(3, 1, "c"));
            leader.nextIndex.put(S2, 2L);  // peer has only entry 1
            leader.matchIndex.put(S2, 1L);

            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);

            assertEquals(1, req.prevLogIndex());
            assertEquals(1, req.prevLogTerm());
            assertEquals(2, req.entries().size());
            assertEquals("b", req.entries().get(0).command());
            assertEquals("c", req.entries().get(1).command());
        }

        @Test
        @DisplayName("sends all entries when peer has empty log")
        void sendsAllEntriesToEmptyPeer() {
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.state.log.add(new LogEntry(2, 1, "b"));
            leader.nextIndex.put(S2, 1L);  // peer has nothing
            leader.matchIndex.put(S2, 0L);

            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);

            assertEquals(0, req.prevLogIndex());
            assertEquals(0, req.prevLogTerm());
            assertEquals(2, req.entries().size());
        }

        @Test
        @DisplayName("includes correct leaderCommit")
        void includesLeaderCommit() {
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.state.commitIndex = 1;
            leader.nextIndex.put(S2, 2L);
            leader.matchIndex.put(S2, 1L);

            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);

            assertEquals(1, req.leaderCommit());
        }

        @Test
        @DisplayName("prevLogTerm correct with multi-term log")
        void prevLogTermWithMultiTermLog() {
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.state.log.add(new LogEntry(2, 2, "b"));
            leader.state.log.add(new LogEntry(3, 2, "c"));
            leader.nextIndex.put(S2, 3L); // peer has entries 1-2
            leader.matchIndex.put(S2, 2L);

            AppendEntriesRequest req = leader.sendAppendEntriesRequest(S2);

            assertEquals(2, req.prevLogIndex());
            assertEquals(2, req.prevLogTerm()); // entry 2 has term 2
            assertEquals(1, req.entries().size());
            assertEquals("c", req.entries().get(0).command());
        }
    }

    // ========================== handleAppendEntriesResponse() ==========================

    @Nested
    @DisplayName("handleAppendEntriesResponse()")
    class HandleAppendEntriesResponse {

        RaftServer leader;

        @BeforeEach
        void setup() {
            leader = new RaftServer(S1, List.of(S2, S3, S4, S5));
            leader.state.currentTerm = 1;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.add(new LogEntry(1, 1, "a"));
            leader.state.log.add(new LogEntry(2, 1, "b"));
            for (ServerID peer : List.of(S2, S3, S4, S5)) {
                leader.nextIndex.put(peer, 1L);
                leader.matchIndex.put(peer, 0L);
            }
        }

        @Test
        @DisplayName("success: updates nextIndex and matchIndex")
        void successUpdatesIndices() {
            AppendEntriesResponse resp = new AppendEntriesResponse(1, true);

            leader.handleAppendEntriesResponse(S2, resp, 2); // sent 2 entries

            assertEquals(3, leader.nextIndex.get(S2));  // nextIndex = matchIndex + 1
            assertEquals(2, leader.matchIndex.get(S2));  // replicated up to index 2
        }

        @Test
        @DisplayName("failure: decrements nextIndex")
        void failureDecrementsNextIndex() {
            leader.nextIndex.put(S2, 3L);

            AppendEntriesResponse resp = new AppendEntriesResponse(1, false);

            leader.handleAppendEntriesResponse(S2, resp, 0);

            assertEquals(2, leader.nextIndex.get(S2)); // decremented from 3 to 2
        }

        @Test
        @DisplayName("failure: nextIndex does not go below 1")
        void nextIndexFloorAtOne() {
            leader.nextIndex.put(S2, 1L);

            AppendEntriesResponse resp = new AppendEntriesResponse(1, false);

            leader.handleAppendEntriesResponse(S2, resp, 0);

            assertEquals(1, leader.nextIndex.get(S2)); // stays at 1
        }

        @Test
        @DisplayName("steps down on higher term response")
        void stepsDownOnHigherTerm() {
            AppendEntriesResponse resp = new AppendEntriesResponse(5, false);

            leader.handleAppendEntriesResponse(S2, resp, 0);

            assertEquals(ServerRole.FOLLOWER, leader.state.role);
            assertEquals(5, leader.state.currentTerm);
            assertNull(leader.state.votedFor);
        }

        @Test
        @DisplayName("ignores response if no longer leader")
        void ignoresIfNotLeader() {
            leader.state.role = ServerRole.FOLLOWER;

            AppendEntriesResponse resp = new AppendEntriesResponse(1, true);

            leader.handleAppendEntriesResponse(S2, resp, 2);

            // matchIndex should not change since we're not leader
            assertEquals(0, leader.matchIndex.get(S2));
        }
    }

    // ========================== advanceCommitIndex() ==========================

    @Nested
    @DisplayName("advanceCommitIndex()")
    class AdvanceCommitIndex {

        RaftServer leader;

        @BeforeEach
        void setup() {
            leader = new RaftServer(S1, List.of(S2, S3, S4, S5));
            leader.state.currentTerm = 2;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            // Log: [1,t1], [2,t1], [3,t2], [4,t2]
            leader.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"),
                    new LogEntry(2, 1, "b"),
                    new LogEntry(3, 2, "c"),
                    new LogEntry(4, 2, "d")));
            for (ServerID peer : List.of(S2, S3, S4, S5)) {
                leader.nextIndex.put(peer, 5L);
                leader.matchIndex.put(peer, 0L);
            }
        }

        @Test
        @DisplayName("commits when majority have replicated current-term entry")
        void commitsWithMajority() {
            // Majority: self + S2 + S3 = 3/5
            leader.matchIndex.put(S2, 4L);
            leader.matchIndex.put(S3, 4L);

            leader.advanceCommitIndex();

            assertEquals(4, leader.state.commitIndex);
        }

        @Test
        @DisplayName("does not commit without majority")
        void doesNotCommitWithoutMajority() {
            // Only self + S2 = 2/5, not majority
            leader.matchIndex.put(S2, 4L);

            leader.advanceCommitIndex();

            assertEquals(0, leader.state.commitIndex);
        }

        @Test
        @DisplayName("Figure 8 safety: does not commit entries from previous term")
        void doesNotCommitPreviousTermEntries() {
            // All peers have replicated up to index 2 (term 1), but none have index 3+ (term 2)
            leader.matchIndex.put(S2, 2L);
            leader.matchIndex.put(S3, 2L);
            leader.matchIndex.put(S4, 2L);

            leader.advanceCommitIndex();

            // Should NOT commit index 2, because log[2].term == 1 != currentTerm == 2
            assertEquals(0, leader.state.commitIndex);
        }

        @Test
        @DisplayName("commits highest N where majority replicated")
        void commitsHighestN() {
            // S2 replicated up to 4, S3 up to 3, S4 up to 3
            leader.matchIndex.put(S2, 4L);
            leader.matchIndex.put(S3, 3L);
            leader.matchIndex.put(S4, 3L);

            leader.advanceCommitIndex();

            // For N=4: self + S2 = 2 (not majority)
            // For N=3: self + S2 + S3 + S4 = 4 (majority), and log[3].term == 2 == currentTerm
            assertEquals(3, leader.state.commitIndex);
        }

        @Test
        @DisplayName("does not regress commitIndex")
        void doesNotRegressCommitIndex() {
            leader.state.commitIndex = 3;
            // Even if no peers have caught up further, commitIndex stays at 3
            leader.advanceCommitIndex();

            assertEquals(3, leader.state.commitIndex);
        }

        @Test
        @DisplayName("previous-term entries become committed indirectly when current-term entry commits")
        void indirectCommitOfPreviousTermEntries() {
            // All peers replicated through index 4 (term 2)
            leader.matchIndex.put(S2, 4L);
            leader.matchIndex.put(S3, 4L);
            leader.matchIndex.put(S4, 4L);

            leader.advanceCommitIndex();

            // commitIndex jumps to 4 — entries 1,2 (term 1) are committed indirectly
            assertEquals(4, leader.state.commitIndex);
        }
    }

    // ========================== Full E2E Log Replication Scenarios ==========================

    @Nested
    @DisplayName("Full Log Replication Scenarios")
    class FullLogReplicationScenarios {

        @Test
        @DisplayName("Scenario: Leader replicates entries to all followers and commits")
        void leaderReplicatesToAllFollowers() {
            // Setup: S1 is leader at term 1 with 3 entries
            RaftServer leader = new RaftServer(S1, List.of(S2, S3, S4, S5));
            leader.state.currentTerm = 1;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.addAll(List.of(
                    new LogEntry(1, 1, "x"),
                    new LogEntry(2, 1, "y"),
                    new LogEntry(3, 1, "z")));
            for (ServerID peer : List.of(S2, S3, S4, S5)) {
                leader.nextIndex.put(peer, 1L);
                leader.matchIndex.put(peer, 0L);
            }

            // Create followers with empty logs
            RaftServer f2 = new RaftServer(S2, List.of(S1, S3, S4, S5));
            RaftServer f3 = new RaftServer(S3, List.of(S1, S2, S4, S5));
            RaftServer f4 = new RaftServer(S4, List.of(S1, S2, S3, S5));
            RaftServer f5 = new RaftServer(S5, List.of(S1, S2, S3, S4));

            // Leader sends to each follower, follower responds
            for (var entry : List.of(
                    new Object[]{S2, f2},
                    new Object[]{S3, f3},
                    new Object[]{S4, f4},
                    new Object[]{S5, f5})) {
                ServerID peerId = (ServerID) entry[0];
                RaftServer follower = (RaftServer) entry[1];

                AppendEntriesRequest req = leader.sendAppendEntriesRequest(peerId);
                assertEquals(3, req.entries().size());

                AppendEntriesResponse resp = follower.handleAppendEntries(req);
                assertTrue(resp.success());

                leader.handleAppendEntriesResponse(peerId, resp, req.entries().size());
            }

            // All followers have 3 entries
            assertEquals(3, f2.state.log.size());
            assertEquals(3, f3.state.log.size());
            assertEquals(3, f4.state.log.size());
            assertEquals(3, f5.state.log.size());

            // Leader should have committed
            assertEquals(3, leader.state.commitIndex);

            // All matchIndex/nextIndex updated
            for (ServerID peer : List.of(S2, S3, S4, S5)) {
                assertEquals(3, leader.matchIndex.get(peer));
                assertEquals(4, leader.nextIndex.get(peer));
            }
        }

        @Test
        @DisplayName("Scenario: Lagging follower catches up after retry")
        void laggingFollowerCatchesUpAfterRetry() {
            // Leader at term 2 with 3 entries
            RaftServer leader = new RaftServer(S1, List.of(S2, S3));
            leader.state.currentTerm = 2;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"),
                    new LogEntry(2, 1, "b"),
                    new LogEntry(3, 2, "c")));

            // Follower S2 has only entry 1
            RaftServer follower = new RaftServer(S2, List.of(S1, S3));
            follower.state.currentTerm = 1;
            follower.state.log.add(new LogEntry(1, 1, "a"));

            // Leader thinks S2 is up to date (nextIndex=4), which is wrong
            leader.nextIndex.put(S2, 4L);
            leader.matchIndex.put(S2, 0L);
            leader.nextIndex.put(S3, 4L);
            leader.matchIndex.put(S3, 3L);

            // Attempt 1: prevLogIndex=3, but follower only has 1 entry → fail
            AppendEntriesRequest req1 = leader.sendAppendEntriesRequest(S2);
            assertEquals(3, req1.prevLogIndex());
            AppendEntriesResponse resp1 = follower.handleAppendEntries(req1);
            assertFalse(resp1.success());
            leader.handleAppendEntriesResponse(S2, resp1, 0);
            assertEquals(3, leader.nextIndex.get(S2)); // decremented 4→3

            // Attempt 2: prevLogIndex=2, follower still only has 1 → fail
            AppendEntriesRequest req2 = leader.sendAppendEntriesRequest(S2);
            assertEquals(2, req2.prevLogIndex());
            AppendEntriesResponse resp2 = follower.handleAppendEntries(req2);
            assertFalse(resp2.success());
            leader.handleAppendEntriesResponse(S2, resp2, 0);
            assertEquals(2, leader.nextIndex.get(S2)); // decremented 3→2

            // Attempt 3: prevLogIndex=1, follower has entry 1 with matching term → success!
            AppendEntriesRequest req3 = leader.sendAppendEntriesRequest(S2);
            assertEquals(1, req3.prevLogIndex());
            assertEquals(1, req3.prevLogTerm());
            assertEquals(2, req3.entries().size()); // entries 2 and 3
            AppendEntriesResponse resp3 = follower.handleAppendEntries(req3);
            assertTrue(resp3.success());
            leader.handleAppendEntriesResponse(S2, resp3, req3.entries().size());

            // Follower is now caught up
            assertEquals(3, follower.state.log.size());
            assertEquals("a", follower.state.log.get(0).command());
            assertEquals("b", follower.state.log.get(1).command());
            assertEquals("c", follower.state.log.get(2).command());

            // Leader state updated
            assertEquals(3, leader.matchIndex.get(S2));
            assertEquals(4, leader.nextIndex.get(S2));
        }

        @Test
        @DisplayName("Scenario: Follower with conflicting entries gets corrected")
        void followerWithConflictingEntriesGetsCorrected() {
            // Leader at term 3 with log: [1,t1,"a"], [2,t2,"b"], [3,t3,"c"]
            RaftServer leader = new RaftServer(S1, List.of(S2, S3));
            leader.state.currentTerm = 3;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"),
                    new LogEntry(2, 2, "b"),
                    new LogEntry(3, 3, "c")));

            // Follower has conflicting log: [1,t1,"a"], [2,t1,"WRONG"], [3,t1,"ALSO_WRONG"]
            // (it was a leader in term 1 that appended entries before being deposed)
            RaftServer follower = new RaftServer(S2, List.of(S1, S3));
            follower.state.currentTerm = 1;
            follower.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"),
                    new LogEntry(2, 1, "WRONG"),
                    new LogEntry(3, 1, "ALSO_WRONG")));

            leader.nextIndex.put(S2, 4L);
            leader.matchIndex.put(S2, 0L);
            leader.nextIndex.put(S3, 4L);
            leader.matchIndex.put(S3, 3L);

            // Attempt 1: prevLogIndex=3, prevLogTerm=3, but follower has term=1 at index 3 → conflict
            AppendEntriesRequest req1 = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp1 = follower.handleAppendEntries(req1);
            assertFalse(resp1.success());
            // Follower truncated from index 3 (and the consistency check deleted the entry)
            assertEquals(2, follower.state.log.size());
            leader.handleAppendEntriesResponse(S2, resp1, 0);

            // Attempt 2: prevLogIndex=2, prevLogTerm=2, but follower has term=1 at index 2 → conflict
            AppendEntriesRequest req2 = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp2 = follower.handleAppendEntries(req2);
            assertFalse(resp2.success());
            assertEquals(1, follower.state.log.size()); // truncated index 2
            leader.handleAppendEntriesResponse(S2, resp2, 0);

            // Attempt 3: prevLogIndex=1, prevLogTerm=1, follower has term=1 at index 1 → match!
            AppendEntriesRequest req3 = leader.sendAppendEntriesRequest(S2);
            assertEquals(1, req3.prevLogIndex());
            assertEquals(1, req3.prevLogTerm());
            AppendEntriesResponse resp3 = follower.handleAppendEntries(req3);
            assertTrue(resp3.success());
            leader.handleAppendEntriesResponse(S2, resp3, req3.entries().size());

            // Follower now matches leader's log exactly
            assertEquals(3, follower.state.log.size());
            assertEquals("a", follower.state.log.get(0).command());
            assertEquals("b", follower.state.log.get(1).command());
            assertEquals("c", follower.state.log.get(2).command());
            assertEquals(2, follower.state.log.get(1).term());
            assertEquals(3, follower.state.log.get(2).term());
        }

        @Test
        @DisplayName("Scenario: Election + Log Replication end-to-end")
        void fullElectionAndReplication() {
            // 3-node cluster: S1 wins election, replicates a client command, commits
            RaftServer s1 = new RaftServer(S1, List.of(S2, S3));
            RaftServer s2 = new RaftServer(S2, List.of(S1, S3));
            RaftServer s3 = new RaftServer(S3, List.of(S1, S2));

            // S1 starts election
            s1.startElection();
            assertEquals(1, s1.state.currentTerm);

            // S2 and S3 grant votes
            var voteReq = learning.rpc.RequestVoteRequest.builder()
                    .term(1).candidateID(S1).lastLogIndex(0).lastLogTerm(0).build();
            var vr2 = s2.handleRequestVote(voteReq);
            var vr3 = s3.handleRequestVote(voteReq);
            assertTrue(vr2.voteGranted());
            assertTrue(vr3.voteGranted());

            // S1 processes votes and becomes leader
            s1.handleRequestVoteResponse(S2, vr2);
            assertEquals(ServerRole.LEADER, s1.state.role);
            // transitionToLeader() appended a no-op at index 1
            assertEquals(1, s1.state.log.size());
            assertEquals("NO OP", s1.state.log.get(0).command());

            // Client sends a command — leader appends it
            LogEntry clientEntry = new LogEntry(2, 1, "SET x=42");
            s1.state.log.add(clientEntry);

            // Leader replicates to S2
            AppendEntriesRequest req2 = s1.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp2 = s2.handleAppendEntries(req2);
            assertTrue(resp2.success());
            s1.handleAppendEntriesResponse(S2, resp2, req2.entries().size());

            // After S2 success: self + S2 = 2/3 = majority → committed
            assertEquals(2, s1.state.commitIndex);

            // Replicate to S3 as well
            AppendEntriesRequest req3 = s1.sendAppendEntriesRequest(S3);
            AppendEntriesResponse resp3 = s3.handleAppendEntries(req3);
            assertTrue(resp3.success());
            s1.handleAppendEntriesResponse(S3, resp3, req3.entries().size());

            // Verify all nodes have the same log
            assertEquals(2, s1.state.log.size());
            assertEquals(2, s2.state.log.size());
            assertEquals(2, s3.state.log.size());
            assertEquals("SET x=42", s2.state.log.get(1).command());
            assertEquals("SET x=42", s3.state.log.get(1).command());
        }

        @Test
        @DisplayName("Scenario: Leader commits only after majority (not before)")
        void leaderCommitsOnlyAfterMajority() {
            // 5-node cluster
            RaftServer leader = new RaftServer(S1, List.of(S2, S3, S4, S5));
            leader.state.currentTerm = 1;
            leader.state.role = ServerRole.LEADER;
            leader.state.leaderID = S1;
            leader.state.log.add(new LogEntry(1, 1, "cmd"));
            for (ServerID peer : List.of(S2, S3, S4, S5)) {
                leader.nextIndex.put(peer, 1L);
                leader.matchIndex.put(peer, 0L);
            }

            RaftServer f2 = new RaftServer(S2, List.of(S1, S3, S4, S5));
            RaftServer f3 = new RaftServer(S3, List.of(S1, S2, S4, S5));

            // Replicate to S2 only — 2/5, not majority
            AppendEntriesRequest req2 = leader.sendAppendEntriesRequest(S2);
            AppendEntriesResponse resp2 = f2.handleAppendEntries(req2);
            assertTrue(resp2.success());
            leader.handleAppendEntriesResponse(S2, resp2, req2.entries().size());
            assertEquals(0, leader.state.commitIndex); // NOT committed yet

            // Replicate to S3 — now 3/5 = majority
            AppendEntriesRequest req3 = leader.sendAppendEntriesRequest(S3);
            AppendEntriesResponse resp3 = f3.handleAppendEntries(req3);
            assertTrue(resp3.success());
            leader.handleAppendEntriesResponse(S3, resp3, req3.entries().size());
            assertEquals(1, leader.state.commitIndex); // NOW committed
        }
    }
}
