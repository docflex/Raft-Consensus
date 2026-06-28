package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.RequestVoteRequest;
import learning.rpc.RequestVoteResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftServerElectionTest {

    // Standard 5-node cluster: S1, S2, S3, S4, S5
    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);
    static final ServerID S4 = new ServerID(4);
    static final ServerID S5 = new ServerID(5);

    // ========================== startElection() ==========================

    @Nested
    @DisplayName("startElection()")
    class StartElection {

        @Test
        @DisplayName("increments term, becomes candidate, votes for self")
        void basicElectionStart() {
            RaftServer server = new RaftServer(S1, List.of(S2, S3, S4, S5));
            assertEquals(ServerRole.FOLLOWER, server.state.role);
            assertEquals(0, server.state.currentTerm);

            server.startElection();

            assertEquals(1, server.state.currentTerm);
            assertEquals(ServerRole.CANDIDATE, server.state.role);
            assertEquals(S1, server.state.votedFor);
            assertTrue(server.votesReceived.contains(S1));
            assertEquals(1, server.votesReceived.size());
        }

        @Test
        @DisplayName("calling startElection twice increments term each time (split vote scenario)")
        void repeatedElection() {
            RaftServer server = new RaftServer(S1, List.of(S2, S3, S4, S5));
            server.startElection();
            assertEquals(1, server.state.currentTerm);

            server.startElection(); // simulates timeout → re-election
            assertEquals(2, server.state.currentTerm);
            assertEquals(ServerRole.CANDIDATE, server.state.role);
            assertEquals(1, server.votesReceived.size()); // cleared and re-added self
        }

        @Test
        @DisplayName("works with empty log")
        void emptyLogDoesNotCrash() {
            RaftServer server = new RaftServer(S1, List.of(S2, S3));
            assertDoesNotThrow(server::startElection);
        }
    }

    // ========================== handleRequestVote() ==========================

    @Nested
    @DisplayName("handleRequestVote()")
    class HandleRequestVote {

        RaftServer voter;

        @BeforeEach
        void setup() {
            voter = new RaftServer(S2, List.of(S1, S3, S4, S5));
        }

        @Test
        @DisplayName("grants vote to first candidate with same term and up-to-date log")
        void grantVoteBasic() {
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(1).candidateID(S1).lastLogIndex(0).lastLogTerm(0).build();

            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertTrue(resp.voteGranted());
            assertEquals(1, resp.term());
            assertEquals(S1, voter.state.votedFor);
        }

        @Test
        @DisplayName("rejects vote if candidate's term is stale")
        void rejectStaleTerm() {
            voter.state.currentTerm = 3;

            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(2).candidateID(S1).lastLogIndex(0).lastLogTerm(0).build();

            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertFalse(resp.voteGranted());
            assertEquals(3, resp.term());
        }

        @Test
        @DisplayName("rejects second candidate in same term (already voted)")
        void rejectDoubleVote() {
            // Vote for S1 first
            RequestVoteRequest req1 = RequestVoteRequest.builder()
                    .term(1).candidateID(S1).lastLogIndex(0).lastLogTerm(0).build();
            voter.handleRequestVote(req1);
            assertEquals(S1, voter.state.votedFor);

            // S3 asks for vote in same term
            RequestVoteRequest req2 = RequestVoteRequest.builder()
                    .term(1).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build();
            RequestVoteResponse resp = voter.handleRequestVote(req2);

            assertFalse(resp.voteGranted());
            assertEquals(S1, voter.state.votedFor); // still voted for S1
        }

        @Test
        @DisplayName("allows re-vote in a new (higher) term")
        void revoteInNewTerm() {
            // Vote for S1 in term 1
            RequestVoteRequest req1 = RequestVoteRequest.builder()
                    .term(1).candidateID(S1).lastLogIndex(0).lastLogTerm(0).build();
            voter.handleRequestVote(req1);

            // S3 asks in term 2 — should be allowed
            RequestVoteRequest req2 = RequestVoteRequest.builder()
                    .term(2).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build();
            RequestVoteResponse resp = voter.handleRequestVote(req2);

            assertTrue(resp.voteGranted());
            assertEquals(S3, voter.state.votedFor);
            assertEquals(2, voter.state.currentTerm);
        }

        @Test
        @DisplayName("steps down to follower when receiving higher term")
        void stepDownOnHigherTerm() {
            voter.state.currentTerm = 1;
            voter.state.role = ServerRole.CANDIDATE;

            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(3).candidateID(S4).lastLogIndex(0).lastLogTerm(0).build();
            voter.handleRequestVote(req);

            assertEquals(ServerRole.FOLLOWER, voter.state.role);
            assertEquals(3, voter.state.currentTerm);
        }

        @Test
        @DisplayName("rejects candidate with stale log — voter has higher lastLogTerm")
        void rejectStaleLogByTerm() {
            // Voter has entries up to term 2
            voter.state.log.add(new LogEntry(1, 1, "a"));
            voter.state.log.add(new LogEntry(2, 2, "b"));

            // Candidate only has entries in term 1
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(3).candidateID(S1).lastLogIndex(2).lastLogTerm(1).build();
            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertFalse(resp.voteGranted());
        }

        @Test
        @DisplayName("rejects candidate with shorter log when same lastLogTerm")
        void rejectShorterLog() {
            // Voter has 3 entries, all term 1
            voter.state.log.add(new LogEntry(1, 1, "a"));
            voter.state.log.add(new LogEntry(2, 1, "b"));
            voter.state.log.add(new LogEntry(3, 1, "c"));

            // Candidate has only 2 entries, also term 1
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(2).candidateID(S1).lastLogIndex(2).lastLogTerm(1).build();
            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertFalse(resp.voteGranted());
        }

        @Test
        @DisplayName("grants vote to candidate with longer log")
        void grantToLongerLog() {
            // Voter has 1 entry
            voter.state.log.add(new LogEntry(1, 1, "a"));

            // Candidate has 3 entries, same term
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(2).candidateID(S1).lastLogIndex(3).lastLogTerm(1).build();
            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertTrue(resp.voteGranted());
        }

        @Test
        @DisplayName("grants vote to candidate with higher lastLogTerm even if shorter log")
        void grantToHigherTermLog() {
            // Voter: 3 entries, last term is 1
            voter.state.log.add(new LogEntry(1, 1, "a"));
            voter.state.log.add(new LogEntry(2, 1, "b"));
            voter.state.log.add(new LogEntry(3, 1, "c"));

            // Candidate: 1 entry, but term 2 (more recent)
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(3).candidateID(S1).lastLogIndex(1).lastLogTerm(2).build();
            RequestVoteResponse resp = voter.handleRequestVote(req);

            assertTrue(resp.voteGranted());
        }
    }

    // ========================== handleRequestVoteResponse() ==========================

    @Nested
    @DisplayName("handleRequestVoteResponse()")
    class HandleRequestVoteResponse {

        @Test
        @DisplayName("becomes leader after receiving majority votes (3/5)")
        void winElectionWithMajority() {
            RaftServer candidate = new RaftServer(S1, List.of(S2, S3, S4, S5));
            candidate.startElection(); // term=1, votes={S1}

            // Receive 2 more grants → 3/5 = majority
            candidate.handleRequestVoteResponse(S2, new RequestVoteResponse(1, true));
            assertEquals(ServerRole.CANDIDATE, candidate.state.role); // not yet

            candidate.handleRequestVoteResponse(S3, new RequestVoteResponse(1, true));
            assertEquals(ServerRole.LEADER, candidate.state.role); // now!
            assertEquals(S1, candidate.state.leaderID);
        }

        @Test
        @DisplayName("does not become leader without majority (2/5)")
        void noLeaderWithoutMajority() {
            RaftServer candidate = new RaftServer(S1, List.of(S2, S3, S4, S5));
            candidate.startElection();

            candidate.handleRequestVoteResponse(S2, new RequestVoteResponse(1, true));
            assertEquals(ServerRole.CANDIDATE, candidate.state.role); // only 2 votes
        }

        @Test
        @DisplayName("steps down when response has higher term")
        void stepDownOnHigherTermResponse() {
            RaftServer candidate = new RaftServer(S1, List.of(S2, S3, S4, S5));
            candidate.startElection(); // term=1

            candidate.handleRequestVoteResponse(S2, new RequestVoteResponse(5, false));

            assertEquals(ServerRole.FOLLOWER, candidate.state.role);
            assertEquals(5, candidate.state.currentTerm);
            assertNull(candidate.state.votedFor);
        }

        @Test
        @DisplayName("ignores granted vote if already stepped down")
        void ignoreVoteAfterStepDown() {
            RaftServer candidate = new RaftServer(S1, List.of(S2, S3, S4, S5));
            candidate.startElection(); // term=1

            // Step down due to higher term
            candidate.handleRequestVoteResponse(S2, new RequestVoteResponse(5, false));
            assertEquals(ServerRole.FOLLOWER, candidate.state.role);

            // Late grant arrives — should be ignored
            candidate.handleRequestVoteResponse(S3, new RequestVoteResponse(1, true));
            assertEquals(ServerRole.FOLLOWER, candidate.state.role); // still follower
        }

        @Test
        @DisplayName("denied votes don't count toward majority")
        void deniedVotesDontCount() {
            RaftServer candidate = new RaftServer(S1, List.of(S2, S3, S4, S5));
            candidate.startElection();

            candidate.handleRequestVoteResponse(S2, new RequestVoteResponse(1, false));
            candidate.handleRequestVoteResponse(S3, new RequestVoteResponse(1, false));
            candidate.handleRequestVoteResponse(S4, new RequestVoteResponse(1, false));
            candidate.handleRequestVoteResponse(S5, new RequestVoteResponse(1, false));

            assertEquals(ServerRole.CANDIDATE, candidate.state.role); // no majority
            assertEquals(1, candidate.votesReceived.size()); // only self-vote
        }
    }

    // ========================== Full Election Scenarios ==========================

    @Nested
    @DisplayName("Full Election Scenarios (from Reference.md)")
    class FullElectionScenarios {

        @Test
        @DisplayName("Scenario: Successful election from cold start (Reference: Worked Example)")
        void coldStartElection() {
            // 5 servers, S4 times out first
            RaftServer s1 = new RaftServer(S1, List.of(S2, S3, S4, S5));
            RaftServer s2 = new RaftServer(S2, List.of(S1, S3, S4, S5));
            RaftServer s3 = new RaftServer(S3, List.of(S1, S2, S4, S5));
            RaftServer s4 = new RaftServer(S4, List.of(S1, S2, S3, S5));
            RaftServer s5 = new RaftServer(S5, List.of(S1, S2, S3, S4));

            // S4 starts election
            s4.startElection();
            assertEquals(1, s4.state.currentTerm);
            assertEquals(ServerRole.CANDIDATE, s4.state.role);

            // Build the request S4 would send
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(1).candidateID(S4).lastLogIndex(0).lastLogTerm(0).build();

            // All servers receive and grant
            RequestVoteResponse r1 = s1.handleRequestVote(req);
            RequestVoteResponse r2 = s2.handleRequestVote(req);
            RequestVoteResponse r3 = s3.handleRequestVote(req);
            RequestVoteResponse r5 = s5.handleRequestVote(req);

            assertTrue(r1.voteGranted());
            assertTrue(r2.voteGranted());
            assertTrue(r3.voteGranted());
            assertTrue(r5.voteGranted());

            // S4 processes responses
            s4.handleRequestVoteResponse(S1, r1);
            s4.handleRequestVoteResponse(S2, r2);
            // After S1 + S2 + self = 3 → majority
            assertEquals(ServerRole.LEADER, s4.state.role);
        }

        @Test
        @DisplayName("Scenario: Split vote in 4-node cluster forces re-election")
        void splitVoteReElection() {
            // 4-node cluster
            ServerID A = S1, B = S2, C = S3, D = S4;
            RaftServer sA = new RaftServer(A, List.of(B, C, D));
            RaftServer sB = new RaftServer(B, List.of(A, C, D));
            RaftServer sC = new RaftServer(C, List.of(A, B, D));
            RaftServer sD = new RaftServer(D, List.of(A, B, C));

            // Both A and B start elections simultaneously for term 1
            sA.startElection();
            sB.startElection();

            // C votes for A, D votes for B
            RequestVoteRequest reqA = RequestVoteRequest.builder()
                    .term(1).candidateID(A).lastLogIndex(0).lastLogTerm(0).build();
            RequestVoteRequest reqB = RequestVoteRequest.builder()
                    .term(1).candidateID(B).lastLogIndex(0).lastLogTerm(0).build();

            RequestVoteResponse cVotesA = sC.handleRequestVote(reqA);
            RequestVoteResponse dVotesB = sD.handleRequestVote(reqB);
            assertTrue(cVotesA.voteGranted());
            assertTrue(dVotesB.voteGranted());

            // C already voted for A, so rejects B
            RequestVoteResponse cRejectsB = sC.handleRequestVote(reqB);
            assertFalse(cRejectsB.voteGranted());

            // D already voted for B, so rejects A
            RequestVoteResponse dRejectsA = sD.handleRequestVote(reqA);
            assertFalse(dRejectsA.voteGranted());

            // A has: self + C = 2. Not majority of 4 (need 3).
            sA.handleRequestVoteResponse(C, cVotesA);
            assertEquals(ServerRole.CANDIDATE, sA.state.role);

            // B has: self + D = 2. Not majority of 4 (need 3).
            sB.handleRequestVoteResponse(D, dVotesB);
            assertEquals(ServerRole.CANDIDATE, sB.state.role);

            // --- Re-election: B starts term 2 first ---
            sB.startElection();
            assertEquals(2, sB.state.currentTerm);

            RequestVoteRequest reqB2 = RequestVoteRequest.builder()
                    .term(2).candidateID(B).lastLogIndex(0).lastLogTerm(0).build();

            // All servers vote for B in term 2 (their votedFor resets for new term)
            RequestVoteResponse aVotesB = sA.handleRequestVote(reqB2);
            RequestVoteResponse cVotesB = sC.handleRequestVote(reqB2);
            RequestVoteResponse dVotesB2 = sD.handleRequestVote(reqB2);

            assertTrue(aVotesB.voteGranted());
            assertTrue(cVotesB.voteGranted());
            assertTrue(dVotesB2.voteGranted());

            sB.handleRequestVoteResponse(A, aVotesB);
            sB.handleRequestVoteResponse(C, cVotesB);
            // B now has: self + A + C = 3 → majority of 4
            assertEquals(ServerRole.LEADER, sB.state.role);
            assertEquals(2, sB.state.currentTerm);
        }

        @Test
        @DisplayName("Scenario: Election restriction prevents stale server from becoming leader")
        void electionRestrictionPreventsStaleLeader() {
            // S1, S2, S3 have committed entries up to index 4, term 2
            // S4, S5 only have entries up to index 2, term 1
            RaftServer s4 = new RaftServer(S4, List.of(S1, S2, S3, S5));
            RaftServer s2 = new RaftServer(S2, List.of(S1, S3, S4, S5));
            RaftServer s3 = new RaftServer(S3, List.of(S1, S2, S4, S5));
            RaftServer s5 = new RaftServer(S5, List.of(S1, S2, S3, S4));

            // Give S2, S3 logs up to (index=4, term=2)
            s2.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"), new LogEntry(2, 1, "b"),
                    new LogEntry(3, 2, "c"), new LogEntry(4, 2, "d")));
            s3.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"), new LogEntry(2, 1, "b"),
                    new LogEntry(3, 2, "c"), new LogEntry(4, 2, "d")));

            // S4, S5 only have (index=2, term=1)
            s4.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"), new LogEntry(2, 1, "b")));
            s5.state.log.addAll(List.of(
                    new LogEntry(1, 1, "a"), new LogEntry(2, 1, "b")));

            // S4 tries to become leader for term 3
            s4.startElection();
            RequestVoteRequest req4 = RequestVoteRequest.builder()
                    .term(s4.state.currentTerm).candidateID(S4)
                    .lastLogIndex(2).lastLogTerm(1).build();

            // S2 and S3 deny — their logs are more up-to-date
            RequestVoteResponse r2 = s2.handleRequestVote(req4);
            RequestVoteResponse r3 = s3.handleRequestVote(req4);
            assertFalse(r2.voteGranted(), "S2 should deny: its log (term=2) > candidate's (term=1)");
            assertFalse(r3.voteGranted(), "S3 should deny: its log (term=2) > candidate's (term=1)");

            // S5 grants (same log level)
            RequestVoteResponse r5 = s5.handleRequestVote(req4);
            assertTrue(r5.voteGranted());

            // S4 has: self + S5 = 2. Not majority. Cannot become leader.
            s4.handleRequestVoteResponse(S2, r2);
            s4.handleRequestVoteResponse(S3, r3);
            s4.handleRequestVoteResponse(S5, r5);
            assertEquals(ServerRole.CANDIDATE, s4.state.role, "S4 must NOT become leader");
        }

        @Test
        @DisplayName("Scenario: Stale leader discovers higher term and steps down")
        void staleLeaderStepsDown() {
            // S1 thinks it's leader at term 3
            RaftServer s1 = new RaftServer(S1, List.of(S2, S3, S4, S5));
            s1.state.currentTerm = 3;
            s1.state.role = ServerRole.LEADER;
            s1.state.leaderID = S1;

            // S1 receives a RequestVote from S3 at term 4 (S3 became candidate after S1 was partitioned)
            RequestVoteRequest req = RequestVoteRequest.builder()
                    .term(4).candidateID(S3).lastLogIndex(0).lastLogTerm(0).build();

            RequestVoteResponse resp = s1.handleRequestVote(req);

            // S1 should have stepped down
            assertEquals(ServerRole.FOLLOWER, s1.state.role);
            assertEquals(4, s1.state.currentTerm);
            assertTrue(resp.voteGranted()); // and granted the vote
        }
    }

    // ========================== Timer ==========================

    @Nested
    @DisplayName("Election Timer")
    class ElectionTimer {

        @Test
        @DisplayName("election has not timed out immediately after reset")
        void notTimedOutImmediately() {
            RaftServer server = new RaftServer(S1, List.of(S2, S3));
            server.resetElectionTimer();
            assertFalse(server.isElectionTimedOut());
        }

        @Test
        @DisplayName("election times out after sufficient time")
        void timesOutEventually() throws InterruptedException {
            RaftServer server = new RaftServer(S1, List.of(S2, S3));
            server.electionTimeoutMs = 50; // override to 50ms for fast test
            server.electionTimerStart = System.currentTimeMillis();

            Thread.sleep(60);
            assertTrue(server.isElectionTimedOut());
        }
    }
}
