package learning.rpc;

import lombok.Builder;

@Builder
public record RequestVoteResponse(long term, boolean voteGranted) {
}
