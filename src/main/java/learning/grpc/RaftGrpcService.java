package learning.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import learning.grpc.generated.*;
import learning.model.LogEntry;
import learning.model.ServerID;
import learning.rpc.*;
import learning.server.RaftServer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * gRPC service implementation that delegates all RPCs to the local RaftServer.
 */
@Slf4j
public class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {

    private final RaftServer server;
    private final int grpcPort;

    public RaftGrpcService(RaftServer server, int grpcPort) {
        this.server = server;
        this.grpcPort = grpcPort;
    }

    @Override
    public void preVote(PreVoteRequestProto request, StreamObserver<PreVoteResponseProto> responseObserver) {
        learning.rpc.PreVoteRequest req = learning.rpc.PreVoteRequest.builder()
                .term(request.getTerm())
                .candidateID(new ServerID(request.getCandidateId()))
                .lastLogIndex(request.getLastLogIndex())
                .lastLogTerm(request.getLastLogTerm())
                .build();

        learning.rpc.PreVoteResponse resp;
        synchronized (server) {
            resp = server.handlePreVote(req);
        }

        responseObserver.onNext(PreVoteResponseProto.newBuilder()
                .setTerm(resp.term())
                .setVoteGranted(resp.voteGranted())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(RequestVoteRequestProto request, StreamObserver<RequestVoteResponseProto> responseObserver) {
        RequestVoteRequest req = RequestVoteRequest.builder()
                .term(request.getTerm())
                .candidateID(new ServerID(request.getCandidateId()))
                .lastLogIndex(request.getLastLogIndex())
                .lastLogTerm(request.getLastLogTerm())
                .build();

        RequestVoteResponse resp;
        synchronized (server) {
            resp = server.handleRequestVote(req);
        }

        responseObserver.onNext(RequestVoteResponseProto.newBuilder()
                .setTerm(resp.term())
                .setVoteGranted(resp.voteGranted())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequestProto request, StreamObserver<AppendEntriesResponseProto> responseObserver) {
        List<LogEntry> entries = request.getEntriesList().stream()
                .map(e -> LogEntry.builder()
                        .index(e.getIndex())
                        .term(e.getTerm())
                        .command(e.getCommand())
                        .build())
                .toList();

        AppendEntriesRequest req = AppendEntriesRequest.builder()
                .term(request.getTerm())
                .leaderId(new ServerID(request.getLeaderId()))
                .prevLogIndex(request.getPrevLogIndex())
                .prevLogTerm(request.getPrevLogTerm())
                .entries(entries)
                .leaderCommit(request.getLeaderCommit())
                .build();

        AppendEntriesResponse resp;
        synchronized (server) {
            resp = server.handleAppendEntries(req);
        }

        responseObserver.onNext(AppendEntriesResponseProto.newBuilder()
                .setTerm(resp.term())
                .setSuccess(resp.success())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void installSnapshot(InstallSnapshotRequestProto request, StreamObserver<InstallSnapshotResponseProto> responseObserver) {
        InstallSnapshotRequest req = InstallSnapshotRequest.builder()
                .term(request.getTerm())
                .leaderId(new ServerID(request.getLeaderId()))
                .lastIncludedIndex(request.getLastIncludedIndex())
                .lastIncludedTerm(request.getLastIncludedTerm())
                .data(request.getData().toByteArray())
                .done(request.getDone())
                .build();

        InstallSnapshotResponse resp;
        synchronized (server) {
            resp = server.handleInstallSnapshot(req);
        }

        responseObserver.onNext(InstallSnapshotResponseProto.newBuilder()
                .setTerm(resp.term())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void clientCommand(ClientRequestProto request, StreamObserver<ClientResponseProto> responseObserver) {
        ClientRequest req = ClientRequest.builder()
                .clientId(request.getClientId())
                .serialNumber(request.getSerialNumber())
                .command(request.getCommand())
                .build();

        java.util.concurrent.CompletableFuture<ClientResponse> future;
        synchronized (server) {
            future = server.handleClientRequest(req);
        }

        future.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
              .whenComplete((resp, ex) -> {
                  ClientResponseProto.Builder builder;
                  if (ex != null) {
                      builder = ClientResponseProto.newBuilder().setSuccess(false).setResult("TIMEOUT");
                  } else {
                      builder = ClientResponseProto.newBuilder().setSuccess(resp.success());
                      if (resp.result() != null) builder.setResult(resp.result());
                      if (resp.leaderHint() != null) builder.setLeaderHintId(resp.leaderHint().id());
                  }
                  responseObserver.onNext(builder.build());
                  responseObserver.onCompleted();
              });
    }

    @Override
    public void readOnlyQuery(ClientRequestProto request, StreamObserver<ClientResponseProto> responseObserver) {
        ClientResponse resp;
        synchronized (server) {
            resp = server.handleReadOnlyQuery(request.getCommand());
        }

        ClientResponseProto.Builder builder = ClientResponseProto.newBuilder()
                .setSuccess(resp.success());
        if (resp.result() != null) builder.setResult(resp.result());
        if (resp.leaderHint() != null) builder.setLeaderHintId(resp.leaderHint().id());

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void identify(IdentifyRequest request, StreamObserver<IdentifyResponse> responseObserver) {
        responseObserver.onNext(IdentifyResponse.newBuilder()
                .setNodeId(server.getSelfID().id())
                .setGrpcPort(grpcPort)
                .build());
        responseObserver.onCompleted();
    }
}
