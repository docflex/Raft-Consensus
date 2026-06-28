package learning.discovery;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import learning.grpc.RaftGrpcClient;
import learning.grpc.generated.IdentifyRequest;
import learning.grpc.generated.IdentifyResponse;
import learning.grpc.generated.RaftServiceGrpc;
import learning.model.ServerID;
import learning.server.RaftServer;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Discovers Raft peers by resolving a DNS name (e.g. Docker Compose service name).
 * Periodically resolves the DNS, calls Identify on each discovered IP, and
 * registers new peers with the gRPC client + Raft server.
 */
@Slf4j
public class DnsPeerDiscovery {

    private final String dnsName;
    private final int grpcPort;
    private final ServerID selfId;
    private final RaftServer raftServer;
    private final RaftGrpcClient grpcClient;
    private final Set<ServerID> knownPeers = new HashSet<>();
    private volatile boolean running = false;
    private Thread discoveryThread;

    private static final long DISCOVERY_INTERVAL_MS = 2000;

    public DnsPeerDiscovery(String dnsName, int grpcPort, ServerID selfId,
                            RaftServer raftServer, RaftGrpcClient grpcClient) {
        this.dnsName = dnsName;
        this.grpcPort = grpcPort;
        this.selfId = selfId;
        this.raftServer = raftServer;
        this.grpcClient = grpcClient;
    }

    public void start() {
        if (dnsName == null || dnsName.isBlank()) {
            log.info("[S{}] DNS peer discovery disabled (no dns name configured)", selfId.id());
            return;
        }
        running = true;
        discoveryThread = new Thread(this::discoveryLoop, "peer-discovery");
        discoveryThread.setDaemon(true);
        discoveryThread.start();
        log.info("[S{}] DNS peer discovery started (dns={}, port={})", selfId.id(), dnsName, grpcPort);
    }

    public void stop() {
        running = false;
        if (discoveryThread != null) {
            discoveryThread.interrupt();
            try {
                discoveryThread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void discoveryLoop() {
        // Initial delay to let the gRPC server start
        try { Thread.sleep(1000); } catch (InterruptedException e) { return; }

        while (running) {
            try {
                discover();
                Thread.sleep(DISCOVERY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.debug("[S{}] Discovery cycle error: {}", selfId.id(), e.getMessage());
            }
        }
    }

    private void discover() {
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(dnsName);
        } catch (UnknownHostException e) {
            log.debug("[S{}] DNS resolution failed for '{}': {}", selfId.id(), dnsName, e.getMessage());
            return;
        }

        String selfHost;
        try {
            selfHost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            selfHost = "";
        }

        for (InetAddress addr : addresses) {
            String host = addr.getHostAddress();

            // Skip self
            if (host.equals(selfHost)) continue;

            // Try to identify the peer
            try {
                ServerID peerId = identifyPeer(host, grpcPort);
                if (peerId != null && !peerId.equals(selfId) && !knownPeers.contains(peerId)) {
                    grpcClient.addPeer(peerId, host, grpcPort);
                    knownPeers.add(peerId);

                    synchronized (raftServer) {
                        if (raftServer.registerPeer(peerId)) {
                            log.info("[S{}] Discovered peer S{} at {}:{}",
                                    selfId.id(), peerId.id(), host, grpcPort);
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("[S{}] Could not identify peer at {}:{}: {}",
                        selfId.id(), host, grpcPort, e.getMessage());
            }
        }
    }

    private ServerID identifyPeer(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            RaftServiceGrpc.RaftServiceBlockingStub stub =
                    RaftServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(1, TimeUnit.SECONDS);
            IdentifyResponse resp = stub.identify(IdentifyRequest.getDefaultInstance());
            return new ServerID(resp.getNodeId());
        } catch (StatusRuntimeException e) {
            return null;
        } finally {
            channel.shutdownNow();
        }
    }
}
