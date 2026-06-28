package learning.rpc;

import lombok.Builder;

@Builder
public record PreVoteResponse(long term, boolean voteGranted) {
}
