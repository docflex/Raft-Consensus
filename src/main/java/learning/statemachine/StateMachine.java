package learning.statemachine;

/**
 * Interface for the replicated state machine.
 * Commands are applied in log order once committed.
 */
public interface StateMachine {

    /**
     * Apply a committed command and return the result.
     * Commands are strings like "SET key value", "GET key", "DEL key".
     */
    String apply(String command);

    /**
     * Check if a command from this client with this serial number was already executed.
     * Returns the cached result if so, or null if not a duplicate.
     */
    String isDuplicate(long clientId, long serialNumber);

    /**
     * Record that a command was executed, for deduplication.
     */
    void recordExecution(long clientId, long serialNumber, String result);

    /**
     * Take a snapshot of the current state machine state.
     * Returns a serialized byte array that can be restored later.
     */
    byte[] takeSnapshot();

    /**
     * Restore state machine state from a snapshot.
     * Replaces all current state with the snapshot contents.
     */
    void loadFromSnapshot(byte[] data);
}
