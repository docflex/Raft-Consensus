package learning.grpc;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import learning.grpc.generated.*;
import learning.model.LogEntry;
import learning.model.ServerID;
import learning.rpc.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for sending RPCs to peer Raft nodes.
 * Manages one channel per peer and converts between domain objects and proto messages.
 */
@Slf4j
public class RaftGrpcClient {

    private static final long RPC_DEADLINE_MS = 500;
    private final Map<ServerID, PeerConnection> peers = new ConcurrentHashMap<>();
    private final ClientInterceptor interceptor;

    public RaftGrpcClient() {
        this(null);
    }

    public RaftGrpcClient(ClientInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    /**
     * Register a peer's address. Must be called before sending RPCs.
     */
    public void addPeer(ServerID peerId, String host, int port) {
        if (peers.containsKey(peerId)) return;
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext();
        if (interceptor != null) {
            builder.intercept(interceptor);
        }
        ManagedChannel channel = builder.build();
        peers.put(peerId, new PeerConnection(
                RaftServiceGrpc.newBlockingStub(channel),
                channel
        ));
        log.info("Added peer S{} at {}:{}", peerId.id(), host, port);
    }

    private RaftServiceGrpc.RaftServiceBlockingStub stubFor(ServerID peerId) {
        PeerConnection conn = peers.get(peerId);
        if (conn == null) throw new IllegalArgumentException("Unknown peer: S" + peerId.id());
        return conn.stub.withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
    }

    public PreVoteResponse sendPreVote(ServerID peerId, PreVoteRequest request) {
        try {
            PreVoteResponseProto resp = stubFor(peerId).preVote(
                    PreVoteRequestProto.newBuilder()
                            .setTerm(request.term())
                            .setCandidateId(request.candidateID().id())
                            .setLastLogIndex(request.lastLogIndex())
                            .setLastLogTerm(request.lastLogTerm())
                            .build()
            );
            return PreVoteResponse.builder()
                    .term(resp.getTerm())
                    .voteGranted(resp.getVoteGranted())
                    .build();
        } catch (StatusRuntimeException e) {
            log.debug("PreVote to S{} failed: {}", peerId.id(), e.getStatus().getCode());
            return null;
        }
    }

    public RequestVoteResponse sendRequestVote(ServerID peerId, RequestVoteRequest request) {
        try {
            RequestVoteResponseProto resp = stubFor(peerId).requestVote(
                    RequestVoteRequestProto.newBuilder()
                            .setTerm(request.term())
                            .setCandidateId(request.candidateID().id())
                            .setLastLogIndex(request.lastLogIndex())
                            .setLastLogTerm(request.lastLogTerm())
                            .build()
            );
            return RequestVoteResponse.builder()
                    .term(resp.getTerm())
                    .voteGranted(resp.getVoteGranted())
                    .build();
        } catch (StatusRuntimeException e) {
            log.debug("RequestVote to S{} failed: {}", peerId.id(), e.getStatus().getCode());
            return null;
        }
    }

    public AppendEntriesResponse sendAppendEntries(ServerID peerId, AppendEntriesRequest request) {
        try {
            AppendEntriesRequestProto.Builder builder = AppendEntriesRequestProto.newBuilder()
                    .setTerm(request.term())
                    .setLeaderId(request.leaderId().id())
                    .setPrevLogIndex(request.prevLogIndex())
                    .setPrevLogTerm(request.prevLogTerm())
                    .setLeaderCommit(request.leaderCommit());

            for (LogEntry entry : request.entries()) {
                builder.addEntries(LogEntryProto.newBuilder()
                        .setIndex(entry.index())
                        .setTerm(entry.term())
                        .setCommand(entry.command())
                        .build());
            }

            AppendEntriesResponseProto resp = stubFor(peerId).appendEntries(builder.build());
            return AppendEntriesResponse.builder()
                    .term(resp.getTerm())
                    .success(resp.getSuccess())
                    .build();
        } catch (StatusRuntimeException e) {
            log.debug("AppendEntries to S{} failed: {}", peerId.id(), e.getStatus().getCode());
            return null;
        }
    }

    public InstallSnapshotResponse sendInstallSnapshot(ServerID peerId, InstallSnapshotRequest request) {
        try {
            InstallSnapshotResponseProto resp = stubFor(peerId).installSnapshot(
                    InstallSnapshotRequestProto.newBuilder()
                            .setTerm(request.term())
                            .setLeaderId(request.leaderId().id())
                            .setLastIncludedIndex(request.lastIncludedIndex())
                            .setLastIncludedTerm(request.lastIncludedTerm())
                            .setData(com.google.protobuf.ByteString.copyFrom(request.data()))
                            .setDone(request.done())
                            .build()
            );
            return InstallSnapshotResponse.builder()
                    .term(resp.getTerm())
                    .build();
        } catch (StatusRuntimeException e) {
            log.debug("InstallSnapshot to S{} failed: {}", peerId.id(), e.getStatus().getCode());
            return null;
        }
    }

    public void shutdown() {
        for (Map.Entry<ServerID, PeerConnection> entry : peers.entrySet()) {
            ManagedChannel channel = entry.getValue().channel;
            channel.shutdown();
            try {
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        peers.clear();
    }

    private record PeerConnection(RaftServiceGrpc.RaftServiceBlockingStub stub, ManagedChannel channel) {}
}
