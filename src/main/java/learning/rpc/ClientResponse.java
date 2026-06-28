package learning.rpc;

import learning.model.ServerID;
import lombok.Builder;

/**
 * Response to a client command or query.
 *
 * @param success    {@code true} if the command was applied successfully
 * @param result     the state machine result (e.g., the value for a GET)
 * @param leaderHint if not the leader, a hint to the client about who is
 */
@Builder
public record ClientResponse(boolean success, String result, ServerID leaderHint) {
}
