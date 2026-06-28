package learning.persistence;

import learning.model.LogEntry;
import learning.model.ServerID;

import java.util.List;

/**
 * Abstraction for durable storage of Raft persistent state.
 *
 * <p>The Raft protocol requires that {@code currentTerm}, {@code votedFor},
 * and the log are persisted to stable storage before responding to any RPC.
 * Implementations may use any backing store (e.g., RiptideKV, disk files).
 *
 * @see learning.persistence.RiptideKVPersistence
 */
public interface PersistenceLayer extends AutoCloseable {

    /** Persists the current term and the candidate this server voted for (may be {@code null}). */
    void saveState(long currentTerm, ServerID votedFor);

    /** Persists the entire log (replaces any previously saved log). */
    void saveLog(List<LogEntry> log);

    /** Loads the persisted current term (returns 0 if none saved). */
    long loadCurrentTerm();

    /** Loads the persisted voted-for candidate (returns {@code null} if none saved). */
    ServerID loadVotedFor();

    /** Loads the persisted log entries (returns an empty list if none saved). */
    List<LogEntry> loadLog();

    @Override
    void close();
}
