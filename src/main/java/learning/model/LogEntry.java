package learning.model;

import lombok.Builder;

/**
 * A single entry in the Raft replicated log.
 *
 * @param index   1-based position in the log
 * @param term    the leader's term when the entry was created
 * @param command the state machine command (e.g., {@code "SET key value"})
 */
@Builder
public record LogEntry(long index, long term, String command) {
}
