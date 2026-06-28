package learning.spring;

import learning.node.RaftNode;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Spring Boot Actuator health indicator for the Raft node.
 *
 * <p>Reports the node's role, term, commit index, and cluster size at
 * {@code /actuator/health}.
 */
@Component
@RequiredArgsConstructor
public class RaftHealthIndicator implements HealthIndicator {

    private final RaftNode raftNode;

    @Override
    public Health health() {
        Map<String, Object> status = raftNode.getStatus();
        return Health.up()
                .withDetail("nodeId", status.get("nodeId"))
                .withDetail("role", status.get("role"))
                .withDetail("term", status.get("term"))
                .withDetail("commitIndex", status.get("commitIndex"))
                .withDetail("clusterSize", status.get("clusterSize"))
                .build();
    }
}
