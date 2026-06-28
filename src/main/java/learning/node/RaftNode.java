package learning.node;

import io.grpc.ClientInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import learning.grpc.RaftGrpcClient;
import learning.grpc.RaftGrpcService;
import learning.model.ServerID;
import learning.model.ServerRole;
import learning.rpc.*;
import learning.server.RaftServer;
import learning.statemachine.StateMachine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A fully networked Raft node.
 * Runs a gRPC server, manages peer connections, and drives the Raft event loop
 * (election timeouts, heartbeats, log replication).
 */
@Slf4j
public class RaftNode {

    private final RaftServer raftServer;
    private final RaftGrpcClient grpcClient;
    private final Server grpcServer;
    private final int port;
    private volatile boolean running = false;
    private Thread eventLoopThread;

    private static final long TICK_INTERVAL_MS = 50;
    private static final long HEARTBEAT_INTERVAL_MS = 150;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final long PRE_VOTE_TIMEOUT_MS = 600;
    private static final long MAX_BACKOFF_MS = 5000;
    private long lastHeartbeatTime = 0;
    private int preVoteFailures = 0;

    public RaftNode(ServerID selfId, List<ServerID> peerIds, int port, Map<ServerID, String> peerAddresses) {
        this(selfId, peerIds, port, peerAddresses, null, null, null, null);
    }

    public RaftNode(ServerID selfId, List<ServerID> peerIds, int port, Map<ServerID, String> peerAddresses,
                    ServerInterceptor serverInterceptor, ClientInterceptor clientInterceptor,
                    learning.persistence.PersistenceLayer persistence) {
        this(selfId, peerIds, port, peerAddresses, serverInterceptor, clientInterceptor, persistence, null);
    }

    public RaftNode(ServerID selfId, List<ServerID> peerIds, int port, Map<ServerID, String> peerAddresses,
                    ServerInterceptor serverInterceptor, ClientInterceptor clientInterceptor,
                    learning.persistence.PersistenceLayer persistence, StateMachine stateMachine) {
        this.port = port;
        this.raftServer = new RaftServer(selfId, peerIds, persistence, stateMachine);
        this.grpcClient = new RaftGrpcClient(clientInterceptor);

        // Register peer connections
        for (Map.Entry<ServerID, String> entry : peerAddresses.entrySet()) {
            String[] parts = entry.getValue().split(":");
            grpcClient.addPeer(entry.getKey(), parts[0], Integer.parseInt(parts[1]));
        }

        // Build gRPC server
        ServerBuilder<?> sb = ServerBuilder.forPort(port)
                .addService(new RaftGrpcService(raftServer, port));
        if (serverInterceptor != null) {
            sb.intercept(serverInterceptor);
        }
        this.grpcServer = sb.build();
    }

    public void start() throws IOException {
        grpcServer.start();
        running = true;
        log.info("[S{}] gRPC server started on port {}", raftServer.getSelfID().id(), port);

        eventLoopThread = new Thread(this::eventLoop, "raft-event-loop-S" + raftServer.getSelfID().id());
        eventLoopThread.setDaemon(true);
        eventLoopThread.start();
    }

    public void stop() {
        log.info("[S{}] Shutting down...", raftServer.getSelfID().id());
        running = false;

        // Wait for event loop to finish
        if (eventLoopThread != null) {
            try {
                eventLoopThread.join(SHUTDOWN_TIMEOUT_SECONDS * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Graceful gRPC shutdown: allow in-flight RPCs to complete
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException e) {
            grpcServer.shutdownNow();
            Thread.currentThread().interrupt();
        }

        grpcClient.shutdown();
        log.info("[S{}] Node stopped", raftServer.getSelfID().id());
    }

    private enum TickAction { NONE, ELECT, HEARTBEAT }

    private void eventLoop() {
        log.info("[S{}] Event loop started", raftServer.getSelfID().id());
        while (running) {
            try {
                TickAction action;
                synchronized (raftServer) {
                    action = tick();
                }
                switch (action) {
                    case ELECT -> startElection();
                    case HEARTBEAT -> sendHeartbeats();
                    default -> {}
                }
                Thread.sleep(TICK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("[S{}] Event loop error: {}", raftServer.getSelfID().id(), e.getMessage(), e);
            }
        }
    }

    /**
     * One tick of the Raft event loop. Called while holding the RaftServer lock.
     * Returns the action to perform outside the lock.
     */
    private TickAction tick() {
        ServerRole role = raftServer.getRole();

        switch (role) {
            case FOLLOWER, CANDIDATE -> {
                if (raftServer.isElectionTimedOut() && raftServer.hasPeers()) {
                    return TickAction.ELECT;
                } else if (!raftServer.hasPeers()) {
                    // No peers yet — reset timer to avoid instant election when peers appear
                    raftServer.resetElectionTimer();
                }
            }
            case LEADER -> {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatTime >= HEARTBEAT_INTERVAL_MS) {
                    lastHeartbeatTime = now;
                    return TickAction.HEARTBEAT;
                }
            }
        }
        return TickAction.NONE;
    }

    private void startElection() {
        // Phase 1: Pre-Vote — build request under lock, send RPCs without lock
        PreVoteRequest preVoteReq;
        List<ServerID> currentPeers;
        int majority;
        synchronized (raftServer) {
            long proposedTerm = raftServer.getCurrentTerm() + 1;
            preVoteReq = PreVoteRequest.builder()
                    .term(proposedTerm)
                    .candidateID(raftServer.getSelfID())
                    .lastLogIndex(raftServer.getLastLogIndex())
                    .lastLogTerm(raftServer.getLastLogTerm())
                    .build();
            currentPeers = raftServer.getPeers();
            majority = raftServer.getMajority();
            log.info("[S{}] Starting pre-vote for term {}", raftServer.getSelfID().id(), proposedTerm);
        }

        java.util.concurrent.atomic.AtomicInteger preVoteCount = new java.util.concurrent.atomic.AtomicInteger(1); // self
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(currentPeers.size());

        for (ServerID peer : currentPeers) {
            Thread.ofVirtual().name("pre-vote-S" + peer.id()).start(() -> {
                try {
                    PreVoteResponse resp = grpcClient.sendPreVote(peer, preVoteReq);
                    if (resp != null && resp.voteGranted()) {
                        preVoteCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for pre-vote responses (with timeout)
        try {
            latch.await(PRE_VOTE_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        // Phase 2: Check pre-vote result
        int preVotes = preVoteCount.get();
        if (preVotes < majority) {
            preVoteFailures++;
            long backoff = Math.min(MAX_BACKOFF_MS, 300L * (1L << Math.min(preVoteFailures - 1, 4)));
            log.info("[S{}] Pre-vote failed: {}/{} (need {}), backoff {}ms", raftServer.getSelfID().id(),
                    preVotes, raftServer.getClusterSize(), majority, backoff);
            synchronized (raftServer) {
                raftServer.resetElectionTimer(backoff);
            }
            return;
        }

        preVoteFailures = 0;
        log.info("[S{}] Pre-vote succeeded: {}/{} (need {})", raftServer.getSelfID().id(),
                preVotes, raftServer.getClusterSize(), majority);

        // Phase 3: Real election — acquire lock, verify staleness, increment term
        RequestVoteRequest voteReq;
        synchronized (raftServer) {
            // Guard: pre-vote was approved for a specific term — verify it's still valid
            if (raftServer.getCurrentTerm() + 1 != preVoteReq.term()) {
                log.info("[S{}] Skipping election: term changed since pre-vote ({} vs proposed {})",
                        raftServer.getSelfID().id(), raftServer.getCurrentTerm(), preVoteReq.term());
                return;
            }
            if (raftServer.getRole() == ServerRole.LEADER) {
                log.info("[S{}] Skipping election: already leader", raftServer.getSelfID().id());
                return;
            }
            raftServer.startElection();
            voteReq = RequestVoteRequest.builder()
                    .term(raftServer.getCurrentTerm())
                    .candidateID(raftServer.getSelfID())
                    .lastLogIndex(raftServer.getLastLogIndex())
                    .lastLogTerm(raftServer.getLastLogTerm())
                    .build();
        }

        for (ServerID peer : currentPeers) {
            Thread.ofVirtual().name("vote-req-S" + peer.id()).start(() -> {
                RequestVoteResponse resp = grpcClient.sendRequestVote(peer, voteReq);
                if (resp != null) {
                    synchronized (raftServer) {
                        raftServer.handleRequestVoteResponse(peer, resp);
                    }
                }
            });
        }
    }

    private void sendHeartbeats() {
        List<ServerID> currentPeers;
        synchronized (raftServer) {
            if (raftServer.getRole() != ServerRole.LEADER) return;
            currentPeers = raftServer.getPeers();
        }

        for (ServerID peer : currentPeers) {
            // Build request under lock, send RPC without lock
            synchronized (raftServer) {
                if (raftServer.getRole() != ServerRole.LEADER) break;

                if (raftServer.peerNeedsSnapshot(peer)) {
                    InstallSnapshotRequest snapReq = raftServer.buildInstallSnapshotRequest(peer);
                    Thread.ofVirtual().name("snap-S" + peer.id()).start(() -> {
                        InstallSnapshotResponse resp = grpcClient.sendInstallSnapshot(peer, snapReq);
                        if (resp != null) {
                            synchronized (raftServer) {
                                raftServer.handleInstallSnapshotResponse(peer, resp);
                            }
                        }
                    });
                    continue;
                }

                AppendEntriesRequest aeReq = raftServer.sendAppendEntriesRequest(peer);
                int entriesSent = aeReq.entries().size();
                Thread.ofVirtual().name("ae-S" + peer.id()).start(() -> {
                    AppendEntriesResponse resp = grpcClient.sendAppendEntries(peer, aeReq);
                    if (resp != null) {
                        synchronized (raftServer) {
                            raftServer.handleAppendEntriesResponse(peer, resp, entriesSent);

                            if (resp.success()) {
                                raftServer.confirmReadIndex(peer);
                            }
                        }
                    }
                });
            }
        }
    }

    // --------------------------------------------- FACADE METHODS -----------------------------------------

    public CompletableFuture<ClientResponse> submitCommand(ClientRequest request) {
        synchronized (raftServer) {
            return raftServer.handleClientRequest(request);
        }
    }

    public ClientResponse query(String queryCommand) {
        ClientResponse resp;
        synchronized (raftServer) {
            resp = raftServer.handleReadOnlyQuery(queryCommand);
        }

        if (!resp.success() && "PENDING_LEADER_CONFIRM".equals(resp.result())) {
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                try { Thread.sleep(50); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                synchronized (raftServer) {
                    if (raftServer.isReadIndexConfirmed()) {
                        return raftServer.executeReadOnlyQuery(queryCommand);
                    }
                }
            }
        }

        return resp;
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new java.util.LinkedHashMap<>();
        synchronized (raftServer) {
            status.put("nodeId", raftServer.getSelfID().id());
            status.put("role", raftServer.getRole().name());
            status.put("term", raftServer.getCurrentTerm());
            status.put("commitIndex", raftServer.getCommitIndex());
            status.put("lastApplied", raftServer.getLastApplied());
            status.put("lastLogIndex", raftServer.getLastLogIndex());
            status.put("logSize", raftServer.getLogSize());
            status.put("clusterSize", raftServer.getClusterSize());
            status.put("votedFor", raftServer.getVotedFor() != null ? raftServer.getVotedFor().id() : null);
            status.put("leaderHint", raftServer.getLeaderHint() != null ? raftServer.getLeaderHint().id() : null);
            if (raftServer.getSnapshotMetadata() != null) {
                status.put("snapshotLastIndex", raftServer.getSnapshotMetadata().lastIncludedIndex());
                status.put("snapshotLastTerm", raftServer.getSnapshotMetadata().lastIncludedTerm());
            }
        }
        return status;
    }

    public boolean addServer(ServerID newServer) {
        synchronized (raftServer) {
            return raftServer.addServer(newServer);
        }
    }

    public boolean removeServer(ServerID target) {
        synchronized (raftServer) {
            return raftServer.removeServer(target);
        }
    }

    public void triggerSnapshot() {
        synchronized (raftServer) {
            raftServer.triggerSnapshot();
        }
    }

    public RaftServer getRaftServer() {
        return raftServer;
    }

    public RaftGrpcClient getGrpcClient() {
        return grpcClient;
    }
}
