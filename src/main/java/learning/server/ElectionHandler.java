package learning.server;

import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.PreVoteRequest;
import learning.rpc.PreVoteResponse;
import learning.rpc.RequestVoteRequest;
import learning.rpc.RequestVoteResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles all leader election logic for the Raft protocol.
 *
 * <p>Implements:
 * <ul>
 *   <li><b>Pre-vote</b> (Raft Dissertation &sect;9.6) &mdash; prevents disruptive elections
 *       by candidates that cannot win.</li>
 *   <li><b>RequestVote</b> (Raft &sect;3.4) &mdash; term-based voting with log up-to-date check.</li>
 *   <li><b>Vote response handling</b> &mdash; tallies votes and transitions to leader on majority.</li>
 * </ul>
 *
 * @see StateTransitions
 * @see ReplicationHandler
 */
@Slf4j
@AllArgsConstructor
class ElectionHandler {

    private final RaftServer server;
    private final StateTransitions transitions;

    /** Begins a new election: transitions to candidate and checks for immediate majority. */
    void startElection() {
        log.info("[S{}] Starting election for term {}", server.selfID.id(), server.state.currentTerm + 1);
        transitions.transitionToCandidate();

        // Self-vote may already be a majority (single-node or no peers discovered yet)
        if (server.votesReceived.size() >= server.config.majority()) {
            transitions.transitionToLeader();
        }
    }

    /** Handles an incoming pre-vote request. Does not modify any server state. */
    PreVoteResponse handlePreVote(PreVoteRequest request) {
        log.info("[S{}] Received PreVote from S{} (term={}, lastLog=[{},{}])",
                server.selfID.id(), request.candidateID().id(), request.term(),
                request.lastLogIndex(), request.lastLogTerm());

        // Pre-vote granted if:
        // 1. Candidate's proposed term > our current term
        // 2. Candidate's log is at least as up-to-date as ours
        // 3. We haven't heard from a valid leader recently (election timed out)
        boolean termOk = request.term() > server.state.currentTerm;
        boolean logIsUpToDate =
                (request.lastLogTerm() > server.state.lastLogTerm()) ||
                (request.lastLogTerm() == server.state.lastLogTerm()
                        && request.lastLogIndex() >= server.state.lastLogIndex());
        boolean leaderTimeout = server.isLeaderContactTimedOut();

        boolean granted = termOk && logIsUpToDate && leaderTimeout;

        if (granted) {
            log.info("[S{}] Granted pre-vote to S{} for term {}",
                    server.selfID.id(), request.candidateID().id(), request.term());
        } else {
            log.info("[S{}] Denied pre-vote to S{} for term {} (termOk={}, logUpToDate={}, leaderTimeout={})",
                    server.selfID.id(), request.candidateID().id(), request.term(),
                    termOk, logIsUpToDate, leaderTimeout);
        }

        // IMPORTANT: do NOT update our term, votedFor, or role — pre-vote is read-only
        return PreVoteResponse.builder()
                .term(server.state.currentTerm)
                .voteGranted(granted)
                .build();
    }

    /** Handles an incoming RequestVote RPC. May grant vote and persist state. */
    RequestVoteResponse handleRequestVote(RequestVoteRequest voteRequest) {
        log.info("[S{}] Received RequestVote from S{} (term={}, lastLog=[{},{}])",
                server.selfID.id(), voteRequest.candidateID().id(), voteRequest.term(),
                voteRequest.lastLogIndex(), voteRequest.lastLogTerm());

        // Reject Outdated Request
        if (voteRequest.term() < server.state.currentTerm) {
            log.info("[S{}] Rejected vote for S{}: stale term {} < {}",
                    server.selfID.id(), voteRequest.candidateID().id(),
                    voteRequest.term(), server.state.currentTerm);
            return RequestVoteResponse.builder()
                    .term(server.state.currentTerm)
                    .voteGranted(false)
                    .build();
        }

        // If current server is outdated then step down
        if (voteRequest.term() > server.state.currentTerm) {
            transitions.stepDownToFollower(voteRequest.term());
        }

        // Check voting eligibility & log up-to-date
        boolean canVote = server.state.votedFor == null
                || server.state.votedFor.equals(voteRequest.candidateID());

        boolean logIsUpToDate =
                (voteRequest.lastLogTerm() > server.state.lastLogTerm()) ||
                (voteRequest.lastLogTerm() == server.state.lastLogTerm()
                        && voteRequest.lastLogIndex() >= server.state.lastLogIndex());

        if (canVote && logIsUpToDate) {
            log.info("[S{}] Granted vote to S{} for term {}",
                    server.selfID.id(), voteRequest.candidateID().id(), server.state.currentTerm);
            server.state.votedFor = voteRequest.candidateID();
            server.persistState();
            server.resetElectionTimer();
        } else {
            log.info("[S{}] Denied vote to S{} for term {} (canVote={}, logUpToDate={})",
                    server.selfID.id(), voteRequest.candidateID().id(),
                    server.state.currentTerm, canVote, logIsUpToDate);
        }

        return RequestVoteResponse.builder()
                .term(server.state.currentTerm)
                .voteGranted(canVote && logIsUpToDate)
                .build();
    }

    /** Processes a RequestVote response. Tallies votes and transitions to leader on majority. */
    void handleRequestVoteResponse(ServerID sender, RequestVoteResponse voteResponse) {
        log.info("[S{}] Received vote response from S{} (term={}, granted={})",
                server.selfID.id(), sender.id(), voteResponse.term(), voteResponse.voteGranted());

        if (voteResponse.term() > server.state.currentTerm) {
            transitions.stepDownToFollower(voteResponse.term());
            return;
        }

        if (server.state.role != ServerRole.CANDIDATE) {
            log.debug("[S{}] Ignoring vote response: no longer a candidate (role={})",
                    server.selfID.id(), server.state.role);
            return;
        }

        if (voteResponse.voteGranted()) {
            server.votesReceived.add(sender);
            int majority = server.config.majority();
            log.info("[S{}] Vote tally: {}/{} (need {})",
                    server.selfID.id(), server.votesReceived.size(),
                    server.config.size(), majority);
            if (server.votesReceived.size() >= majority) {
                transitions.transitionToLeader();
            }
        }
    }
}
