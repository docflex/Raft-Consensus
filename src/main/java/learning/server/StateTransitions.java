package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages Raft role transitions: follower, candidate, and leader.
 *
 * <p>Each transition updates the relevant protocol state (term, votedFor, role),
 * persists durable state, and initializes role-specific structures (e.g.,
 * {@code nextIndex}/{@code matchIndex} for leaders, no-op entry on election win).
 *
 * @see ElectionHandler
 * @see ReplicationHandler
 */
@Slf4j
@AllArgsConstructor
class StateTransitions {

    private final RaftServer server;

    /** Steps down to follower for a higher term. Clears pending state and persists. */
    void stepDownToFollower(long newTerm) {
        log.info("[S{}] Stepping down to FOLLOWER: term {} -> {}",
                server.selfID.id(), server.state.currentTerm, newTerm);
        server.state.currentTerm = newTerm;
        server.state.votedFor = null;
        server.state.role = ServerRole.FOLLOWER;
        server.pendingRequests.clear();
        server.readIndexAcks.clear();
        server.readIndexConfirmed = false;
        server.persistState();
        server.resetElectionTimer();
    }

    /** Transitions to candidate: increments term, votes for self, resets election timer. */
    void transitionToCandidate() {
        server.state.currentTerm += 1;
        server.state.role = ServerRole.CANDIDATE;
        server.state.votedFor = server.selfID;
        server.resetElectionTimer();
        server.votesReceived.clear();
        server.votesReceived.add(server.selfID);
        server.persistState();
        log.info("[S{}] Transitioned to CANDIDATE for term {}",
                server.selfID.id(), server.state.currentTerm);
    }

    /** Transitions to leader: initializes replication state and appends a no-op entry. */
    void transitionToLeader() {
        log.info("[S{}] *** ELECTED LEADER for term {} ***",
                server.selfID.id(), server.state.currentTerm);
        server.state.role = ServerRole.LEADER;
        server.state.leaderID = server.selfID;

        // Initialize the Next Index and Match Index Maps
        for (ServerID peer : server.peers) {
            server.nextIndex.put(peer, server.state.lastLogIndex() + 1);
            server.matchIndex.put(peer, 0L);
        }

        // Append a No-OP to commit entries from previous terms
        LogEntry noOpEntry = LogEntry.builder()
                .term(server.state.currentTerm)
                .index(server.state.lastLogIndex() + 1)
                .command("NO OP")
                .build();
        server.state.log.add(noOpEntry);
        server.persistLog();
    }
}
