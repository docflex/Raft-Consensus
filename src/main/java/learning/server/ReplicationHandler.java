package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.AppendEntriesRequest;
import learning.rpc.AppendEntriesResponse;
import learning.rpc.InstallSnapshotRequest;
import learning.rpc.InstallSnapshotResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Handles log replication, consistency checking, and commit index advancement.
 *
 * <p>Implements Raft &sect;3.5 (AppendEntries RPC) and &sect;7 (InstallSnapshot RPC):
 * <ul>
 *   <li><b>Leader side</b> &mdash; builds AppendEntries/InstallSnapshot requests,
 *       processes responses, and advances {@code commitIndex} on majority replication.</li>
 *   <li><b>Follower side</b> &mdash; validates log consistency, appends entries,
 *       and applies committed entries to the state machine.</li>
 * </ul>
 *
 * @see ElectionHandler
 * @see StateTransitions
 */
@Slf4j
@AllArgsConstructor
class ReplicationHandler {

    private final RaftServer server;
    private final StateTransitions transitions;

    /** Handles an incoming AppendEntries RPC (heartbeat or log replication). */
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.entries().isEmpty()) {
            log.debug("[S{}] Received heartbeat from S{} (term={}, leaderCommit={})",
                    server.selfID.id(), request.leaderId().id(), request.term(), request.leaderCommit());
        } else {
            log.info("[S{}] Received AppendEntries from S{} (term={}, prevLog=[{},{}], entries={}, leaderCommit={})",
                    server.selfID.id(), request.leaderId().id(), request.term(),
                    request.prevLogIndex(), request.prevLogTerm(),
                    request.entries().size(), request.leaderCommit());
        }

        // Step 1: reject if stale term
        if (request.term() < server.state.currentTerm) {
            log.info("[S{}] Rejected AppendEntries: stale term {} < {}",
                    server.selfID.id(), request.term(), server.state.currentTerm);
            return AppendEntriesResponse.builder()
                    .term(server.state.currentTerm)
                    .success(false)
                    .build();
        }

        // Step 2: recognize the sender as leader
        if (request.term() > server.state.currentTerm) {
            transitions.stepDownToFollower(request.term());
        } else {
            server.state.role = ServerRole.FOLLOWER;
            server.resetElectionTimer();
        }
        server.state.leaderID = request.leaderId();
        server.recordLeaderContact();

        // Step 3: Consistency Check — verify entry at prevLogIndex has matching prevLogTerm
        if (request.prevLogIndex() > 0) {
            // prevLogIndex is covered by snapshot — snapshot term must match
            if (request.prevLogIndex() == server.state.snapshotLastIndex()) {
                if (server.state.snapshotLastTerm() != request.prevLogTerm()) {
                    log.info("[S{}] Consistency check failed: snapshot term mismatch",
                            server.selfID.id());
                    return AppendEntriesResponse.builder()
                            .term(server.state.currentTerm)
                            .success(false)
                            .build();
                }
            } else if (request.prevLogIndex() < server.state.firstLogIndex()) {
                // Log is too short — we don't have an entry at prevLogIndex
                log.info("[S{}] Consistency check failed: log too short (firstIndex={}, lastIndex={}, needed={})",
                        server.selfID.id(), server.state.firstLogIndex(),
                        server.state.lastLogIndex(), request.prevLogIndex());
                return AppendEntriesResponse.builder()
                        .term(server.state.currentTerm)
                        .success(false)
                        .build();
            } else if (server.state.lastLogIndex() < request.prevLogIndex()) {
                log.info("[S{}] Consistency check failed: log too short (lastIndex={}, needed={})",
                        server.selfID.id(), server.state.lastLogIndex(), request.prevLogIndex());
                return AppendEntriesResponse.builder()
                        .term(server.state.currentTerm)
                        .success(false)
                        .build();
            } else {
                // Entry exists but term doesn't match — conflict
                long ourTerm = server.state.termAt(request.prevLogIndex());
                if (ourTerm != request.prevLogTerm()) {
                    log.info("[S{}] Consistency check failed: term mismatch at index {} (ours={}, expected={})",
                            server.selfID.id(), request.prevLogIndex(), ourTerm, request.prevLogTerm());
                    int listPos = server.state.toListPos(request.prevLogIndex());
                    if (listPos >= 0 && listPos < server.state.log.size()) {
                        server.state.log.subList(listPos, server.state.log.size()).clear();
                    }
                    server.persistLog();
                    return AppendEntriesResponse.builder()
                            .term(server.state.currentTerm)
                            .success(false)
                            .build();
                }
            }
        }

        // Step 4: Append new entries (skip entries already in our log)
        for (int i = 0; i < request.entries().size(); i++) {
            LogEntry entry = request.entries().get(i);
            long raftIndex = request.prevLogIndex() + 1 + i;  // 1-based Raft index
            int listPos = server.state.toListPos(raftIndex);

            if (listPos >= 0 && listPos < server.state.log.size()) {
                if (server.state.log.get(listPos).term() != entry.term()) {
                    // Conflict at this position: truncate from here and append
                    server.state.log.subList(listPos, server.state.log.size()).clear();
                    server.state.log.add(entry);
                }
                // else: same term at same index — already have this entry, skip
            } else if (listPos >= server.state.log.size()) {
                server.state.log.add(entry);
            }
            // else: listPos < 0 means entry is covered by snapshot, skip
        }

        server.persistLog();

        // Step 5: Advance commitIndex if leaderCommit > commitIndex
        if (request.leaderCommit() > server.state.commitIndex) {
            long lastNewIndex = server.state.lastLogIndex();
            long oldCommitIndex = server.state.commitIndex;
            server.state.commitIndex = Math.min(request.leaderCommit(), lastNewIndex);
            log.info("[S{}] Advanced commitIndex: {} -> {}",
                    server.selfID.id(), oldCommitIndex, server.state.commitIndex);

            // Apply newly committed entries to state machine
            server.applyCommittedEntries();
        }

        if (!request.entries().isEmpty()) {
            log.info("[S{}] AppendEntries success (logSize={})", server.selfID.id(), server.state.log.size());
        }
        return AppendEntriesResponse.builder()
                .term(server.state.currentTerm)
                .success(true)
                .build();
    }

    /** Builds an AppendEntries request for the given peer based on its {@code nextIndex}. */
    AppendEntriesRequest sendAppendEntriesRequest(ServerID peer) {
        long nextIdx = server.nextIndex.get(peer);
        long prevLogIndex = nextIdx - 1;
        long prevLogTerm = server.state.termAt(prevLogIndex);

        // Entries from nextIndex[peer] onwards
        List<LogEntry> entries;
        int listPos = server.state.toListPos(nextIdx);
        if (listPos >= 0 && listPos < server.state.log.size()) {
            entries = server.state.log.subList(listPos, server.state.log.size());
        } else {
            entries = List.of();
        }

        log.debug("[S{}] Sending AppendEntries to S{} (prevLog=[{},{}], entries={}, leaderCommit={})",
                server.selfID.id(), peer.id(), prevLogIndex, prevLogTerm,
                entries.size(), server.state.commitIndex);

        return AppendEntriesRequest.builder()
                .term(server.state.currentTerm)
                .leaderId(server.selfID)
                .prevLogIndex(prevLogIndex)
                .prevLogTerm(prevLogTerm)
                .entries(entries)
                .leaderCommit(server.state.commitIndex)
                .build();
    }

    /**
     * Check if the peer needs a snapshot instead of AppendEntries.
     * Returns true if nextIndex[peer] <= snapshot.lastIncludedIndex.
     */
    boolean peerNeedsSnapshot(ServerID peer) {
        if (server.state.snapshotMetadata == null) return false;
        long nextIdx = server.nextIndex.get(peer);
        return nextIdx <= server.state.snapshotLastIndex();
    }

    InstallSnapshotRequest buildInstallSnapshotRequest(ServerID peer) {
        log.info("[S{}] Sending InstallSnapshot to S{} (lastIncluded=[{},{}], dataSize={})",
                server.selfID.id(), peer.id(),
                server.state.snapshotLastIndex(), server.state.snapshotLastTerm(),
                server.state.snapshotData.length);

        return InstallSnapshotRequest.builder()
                .term(server.state.currentTerm)
                .leaderId(server.selfID)
                .lastIncludedIndex(server.state.snapshotLastIndex())
                .lastIncludedTerm(server.state.snapshotLastTerm())
                .data(server.state.snapshotData)
                .done(true)
                .build();
    }

    void handleInstallSnapshotResponse(ServerID peer, InstallSnapshotResponse response) {
        if (response.term() > server.state.currentTerm) {
            transitions.stepDownToFollower(response.term());
            return;
        }

        // Update nextIndex and matchIndex for this peer
        long snapshotEnd = server.state.snapshotLastIndex();
        server.nextIndex.put(peer, snapshotEnd + 1);
        server.matchIndex.put(peer, snapshotEnd);
        log.info("[S{}] InstallSnapshot success for S{}: nextIndex={}, matchIndex={}",
                server.selfID.id(), peer.id(), snapshotEnd + 1, snapshotEnd);
    }

    /** Processes a peer's AppendEntries response, updating replication state. */
    void handleAppendEntriesResponse(ServerID peer, AppendEntriesResponse response, int entriesSent) {
        if (entriesSent > 0) {
            log.info("[S{}] Received AppendEntries response from S{} (term={}, success={})",
                    server.selfID.id(), peer.id(), response.term(), response.success());
        }

        // Higher term → step down
        if (response.term() > server.state.currentTerm) {
            transitions.stepDownToFollower(response.term());
            return;
        }

        if (server.state.role != ServerRole.LEADER) {
            log.debug("[S{}] Ignoring AppendEntries response: no longer leader", server.selfID.id());
            return;
        }

        if (response.success()) {
            // Update nextIndex and matchIndex for this peer
            long newMatchIndex = server.nextIndex.get(peer) - 1 + entriesSent;
            server.nextIndex.put(peer, newMatchIndex + 1);
            server.matchIndex.put(peer, newMatchIndex);
            if (entriesSent > 0) {
                log.info("[S{}] Replication success for S{}: nextIndex={}, matchIndex={}",
                        server.selfID.id(), peer.id(), newMatchIndex + 1, newMatchIndex);
            }
            advanceCommitIndex();
        } else {
            // Decrement nextIndex and retry
            long decremented = Math.max(1, server.nextIndex.get(peer) - 1);
            server.nextIndex.put(peer, decremented);
            log.info("[S{}] Replication failed for S{}: decremented nextIndex to {}",
                    server.selfID.id(), peer.id(), decremented);
        }
    }

    /** Advances {@code commitIndex} to the highest index replicated to a majority. */
    void advanceCommitIndex() {
        // Find the highest N where: N > commitIndex, majority matchIndex[i] >= N, log[N].term == currentTerm
        for (long n = server.state.lastLogIndex(); n > server.state.commitIndex; n--) {
            long termAtN = server.state.termAt(n);
            if (termAtN != server.state.currentTerm) {
                continue; // Safety: only commit entries from current term
            }

            // Count replicas: self + peers with matchIndex >= n
            long replicaCount = 1; // count self
            for (ServerID peer : server.peers) {
                if (server.matchIndex.get(peer) >= n) {
                    replicaCount++;
                }
            }

            int majority = server.config.majority();
            if (replicaCount >= majority) {
                long oldCommitIndex = server.state.commitIndex;
                server.state.commitIndex = n;
                log.info("[S{}] Advanced commitIndex: {} -> {} (replicas={})",
                        server.selfID.id(), oldCommitIndex, n, replicaCount);

                // Apply newly committed entries to state machine
                server.applyCommittedEntries();
                break; // Highest N found, no need to continue
            }
        }
    }
}
