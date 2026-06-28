package learning.rpc;

import lombok.Builder;

@Builder
public record AppendEntriesResponse(long term, boolean success) {
}
