package learning.rpc;

import lombok.Builder;

/**
 * A client command request to the Raft cluster.
 *
 * @param clientId     unique client identifier (used for deduplication)
 * @param serialNumber monotonically increasing serial number per client
 * @param command      the state machine command (e.g., {@code "SET key value"})
 */
@Builder
public record ClientRequest(long clientId, long serialNumber, String command) {
}
