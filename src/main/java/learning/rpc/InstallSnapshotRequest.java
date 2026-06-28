package learning.rpc;

import learning.model.ServerID;
import lombok.Builder;

@Builder
public record InstallSnapshotRequest(
        long term,
        ServerID leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data,
        boolean done
) {
}
