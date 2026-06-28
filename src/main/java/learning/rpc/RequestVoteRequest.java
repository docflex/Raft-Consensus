package learning.rpc;

import learning.model.ServerID;
import lombok.Builder;

@Builder
public record RequestVoteRequest(long term, ServerID candidateID, long lastLogIndex, long lastLogTerm) {
}
