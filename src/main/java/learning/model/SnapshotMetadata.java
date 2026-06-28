package learning.model;

import lombok.Builder;

/**
 * Metadata for a Raft log snapshot (log compaction).
 *
 * @param lastIncludedIndex the index of the last log entry included in the snapshot
 * @param lastIncludedTerm  the term of that entry
 */
@Builder
public record SnapshotMetadata(long lastIncludedIndex, long lastIncludedTerm) {
}
