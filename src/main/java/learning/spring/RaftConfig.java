package learning.spring;

import learning.discovery.DnsPeerDiscovery;
import learning.node.RaftNode;
import learning.model.ServerID;
import learning.persistence.PersistenceLayer;
import learning.persistence.RiptideKVPersistence;
import learning.statemachine.KeyValueStateMachine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Configuration
@EnableConfigurationProperties(RaftProperties.class)
public class RaftConfig {

    private static final Pattern HOSTNAME_ID_PATTERN = Pattern.compile(".*[^0-9](\\d+)$");

    @Bean
    public RaftNode raftNode(RaftProperties props) {
        long nodeId = resolveNodeId(props.getNodeId());
        ServerID selfId = new ServerID(nodeId);

        List<ServerID> peerIds = new ArrayList<>();
        Map<ServerID, String> peerAddresses = new LinkedHashMap<>();

        for (RaftProperties.PeerConfig peer : props.getPeers()) {
            ServerID peerId = new ServerID(peer.getId());
            peerIds.add(peerId);
            peerAddresses.put(peerId, peer.getHost() + ":" + peer.getPort());
        }

        boolean useDns = props.getDiscoveryDns() != null && !props.getDiscoveryDns().isBlank();

        if (useDns) {
            log.info("Raft node S{} (gRPC port {}) — DNS discovery enabled: '{}'",
                    nodeId, props.getGrpcPort(), props.getDiscoveryDns());
        } else {
            log.info("Raft node S{} (gRPC port {}) — static peers: {}",
                    nodeId, props.getGrpcPort(), peerAddresses);
        }

        PersistenceLayer persistence = createPersistence(selfId, props);

        return new RaftNode(selfId, peerIds, props.getGrpcPort(), peerAddresses,
                null, null, persistence, new KeyValueStateMachine());
    }

    @Bean
    public DnsPeerDiscovery dnsPeerDiscovery(RaftProperties props, RaftNode raftNode) {
        long nodeId = resolveNodeId(props.getNodeId());
        String dns = props.getDiscoveryDns();
        return new DnsPeerDiscovery(
                dns != null ? dns : "",
                props.getGrpcPort(),
                new ServerID(nodeId),
                raftNode.getRaftServer(),
                raftNode.getGrpcClient()
        );
    }

    private PersistenceLayer createPersistence(ServerID selfId, RaftProperties props) {
        String dataDir = props.getDataDir();
        if (dataDir == null || dataDir.isBlank()) {
            log.info("[S{}] Persistence disabled (raft.data-dir is empty)", selfId.id());
            return null;
        }
        try {
            return RiptideKVPersistence.forServer(selfId, Path.of(dataDir));
        } catch (IOException e) {
            log.error("[S{}] Failed to start RiptideKV persistence: {} — falling back to in-memory",
                    selfId.id(), e.getMessage(), e);
            return null;
        }
    }

    /**
     * Resolve the node ID: if 0, auto-detect.
     * Strategy:
     *   1. Try hostname — match "service-N" pattern (e.g. "raft-3", NOT hex container IDs)
     *   2. Fall back to last octet of container IP (unique within a Docker network)
     */
    private long resolveNodeId(long configured) {
        if (configured > 0) return configured;

        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            // Only match if hostname looks like a service name with a numeric suffix,
            // not a hex container ID (which could also end in digits)
            Matcher m = HOSTNAME_ID_PATTERN.matcher(hostname);
            if (m.matches() && !hostname.matches("[0-9a-f]{8,}")) {
                long id = Long.parseLong(m.group(1));
                log.info("Auto-assigned node ID {} from hostname '{}'", id, hostname);
                return id;
            }

            // Fall back to last octet of IP — unique within a Docker bridge network
            String ip = InetAddress.getLocalHost().getHostAddress();
            String[] octets = ip.split("\\.");
            if (octets.length == 4) {
                long id = Long.parseLong(octets[3]);
                log.info("Auto-assigned node ID {} from IP address '{}'", id, ip);
                return id;
            }
        } catch (Exception e) {
            log.warn("Could not auto-detect node ID: {}", e.getMessage());
        }

        log.warn("Falling back to node ID 1 (set raft.node-id explicitly)");
        return 1;
    }
}
