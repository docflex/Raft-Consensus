package learning.rpc;

import learning.model.ServerID;
import lombok.Builder;

@Builder
public record PreVoteRequest(long term, ServerID candidateID, long lastLogIndex, long lastLogTerm) {
}
