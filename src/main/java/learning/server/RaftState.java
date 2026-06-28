package learning.server;

import learning.model.LogEntry;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.model.SnapshotMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates all Raft protocol state for a single server.
 *
 * <p>State is partitioned according to the Raft paper:
 * <ul>
 *   <li><b>Persistent state</b> &mdash; {@code currentTerm}, {@code votedFor}, {@code log}.
 *       Must be saved to stable storage before responding to RPCs.</li>
 *   <li><b>Volatile state</b> &mdash; {@code commitIndex}, {@code lastApplied}, {@code role},
 *       {@code leaderID}. Reconstructed after a crash.</li>
 *   <li><b>Snapshot state</b> &mdash; {@code snapshotMetadata}, {@code snapshotData}.
 *       Replaces prefix of the log after compaction.</li>
 * </ul>
 *
 * <p>Fields are package-private for direct access within the {@code learning.server}
 * package. External callers should use {@link RaftServer} public getters.
 */
public class RaftState {
    // Persistent State (Must Be Saved to Disk before Responding to RPCs)
    long currentTerm    = 0;
    ServerID votedFor   = null;
    List<LogEntry> log  = new ArrayList<>();

    // Snapshot State
    SnapshotMetadata snapshotMetadata = null;
    byte[] snapshotData               = null;

    // Volatile State
    long commitIndex    = 0;
    long lastApplied    = 0;
    ServerRole role     = ServerRole.FOLLOWER;
    ServerID leaderID   = null;

    long lastLogIndex() {
        return log.isEmpty() ? snapshotLastIndex() : log.getLast().index();
    }

    long lastLogTerm() {
        return log.isEmpty() ? snapshotLastTerm() : log.getLast().term();
    }

    long snapshotLastIndex() {
        return snapshotMetadata != null ? snapshotMetadata.lastIncludedIndex() : 0;
    }

    long snapshotLastTerm() {
        return snapshotMetadata != null ? snapshotMetadata.lastIncludedTerm() : 0;
    }

    /**
     * Convert a 1-based Raft log index to a 0-based list position.
     * After log compaction, the log may not start at index 1.
     * Returns -1 if the index falls within the snapshot (before the log).
     */
    int toListPos(long raftIndex) {
        long firstLogIndex = snapshotLastIndex() + 1;
        return (int) (raftIndex - firstLogIndex);
    }

    /**
     * Returns the Raft index of the first entry still in the in-memory log.
     * If the log is empty, returns snapshotLastIndex + 1 (the next expected index).
     */
    long firstLogIndex() {
        return log.isEmpty() ? snapshotLastIndex() + 1 : log.getFirst().index();
    }

    /**
     * Get the log entry at the given Raft index, or null if it's not in memory.
     */
    LogEntry getEntry(long raftIndex) {
        int pos = toListPos(raftIndex);
        if (pos < 0 || pos >= log.size()) return null;
        return log.get(pos);
    }

    /**
     * Get the term at the given Raft index.
     * If the index matches the snapshot's lastIncludedIndex, returns the snapshot term.
     */
    long termAt(long raftIndex) {
        if (raftIndex == 0) return 0;
        if (snapshotMetadata != null && raftIndex == snapshotMetadata.lastIncludedIndex()) {
            return snapshotMetadata.lastIncludedTerm();
        }
        LogEntry entry = getEntry(raftIndex);
        return entry != null ? entry.term() : 0;
    }

    /**
     * Discard all log entries up to and including the given index.
     * Entries after the index are retained.
     */
    void discardLogUpTo(long lastIncludedIndex) {
        int pos = toListPos(lastIncludedIndex);
        if (pos >= 0 && pos < log.size()) {
            log.subList(0, pos + 1).clear();
        } else if (pos >= log.size()) {
            log.clear();
        }
    }
}
