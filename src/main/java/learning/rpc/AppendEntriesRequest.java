package learning.rpc;

import learning.model.LogEntry;
import learning.model.ServerID;
import lombok.Builder;

import java.util.List;

@Builder
public record AppendEntriesRequest(long term, ServerID leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                                   long leaderCommit) {
}