package learning.spring;

import learning.discovery.DnsPeerDiscovery;
import learning.node.RaftNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;


/**
 * Manages the Raft node lifecycle within Spring Boot.
 * Starts the gRPC server + event loop + peer discovery on application startup,
 * stops them on shutdown.
 */
@Slf4j
@Component
public class RaftLifecycle implements SmartLifecycle {

    private final RaftNode raftNode;
    private final DnsPeerDiscovery discovery;
    private volatile boolean running = false;

    public RaftLifecycle(RaftNode raftNode, DnsPeerDiscovery discovery) {
        this.raftNode = raftNode;
        this.discovery = discovery;
    }

    @Override
    public void start() {
        try {
            raftNode.start();
            discovery.start();
            running = true;
            log.info("Raft node started successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Raft node", e);
        }
    }

    @Override
    public void stop() {
        discovery.stop();
        raftNode.stop();
        running = false;
        log.info("Raft node stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }
}
