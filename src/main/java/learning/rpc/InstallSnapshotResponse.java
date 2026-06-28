package learning.rpc;

import lombok.Builder;

@Builder
public record InstallSnapshotResponse(long term) {
}
