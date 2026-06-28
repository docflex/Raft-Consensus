package learning.spring;

import learning.node.RaftNode;
import learning.model.ServerID;
import learning.rpc.ClientRequest;
import learning.rpc.ClientResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * REST API for interacting with the Raft cluster.
 *
 * <p>Exposes endpoints for client commands, linearizable queries, cluster status,
 * dynamic membership changes, and manual snapshot triggers. All state-mutating
 * operations are forwarded to the leader via {@link RaftNode} facade methods.
 */
@RestController
@RequestMapping("/api/raft")
@RequiredArgsConstructor
public class RaftController {

    private final RaftNode raftNode;

    // -------------------- Client Commands --------------------

    @PostMapping("/command")
    public ResponseEntity<Map<String, Object>> command(@RequestBody CommandRequest body) {
        CompletableFuture<ClientResponse> future = raftNode.submitCommand(
                ClientRequest.builder()
                        .clientId(body.clientId())
                        .serialNumber(body.serialNumber())
                        .command(body.command())
                        .build());

        try {
            ClientResponse resp = future.get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(toMap(resp));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("success", false, "result", "TIMEOUT"));
        }
    }

    @GetMapping("/query")
    public ResponseEntity<Map<String, Object>> query(@RequestParam String q) {
        return ResponseEntity.ok(toMap(raftNode.query(q)));
    }

    // -------------------- Status --------------------

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(raftNode.getStatus());
    }

    // -------------------- Membership --------------------

    @PostMapping("/cluster/add/{nodeId}")
    public ResponseEntity<Map<String, Object>> addServer(@PathVariable long nodeId) {
        return ResponseEntity.ok(Map.of("accepted", raftNode.addServer(new ServerID(nodeId))));
    }

    @PostMapping("/cluster/remove/{nodeId}")
    public ResponseEntity<Map<String, Object>> removeServer(@PathVariable long nodeId) {
        return ResponseEntity.ok(Map.of("accepted", raftNode.removeServer(new ServerID(nodeId))));
    }

    // -------------------- Snapshot --------------------

    @PostMapping("/snapshot")
    public ResponseEntity<Map<String, Object>> snapshot() {
        raftNode.triggerSnapshot();
        return ResponseEntity.ok(Map.of("status", "snapshot_triggered"));
    }

    // -------------------- DTOs --------------------

    public record CommandRequest(long clientId, long serialNumber, String command) {}

    private Map<String, Object> toMap(ClientResponse resp) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("success", resp.success());
        if (resp.result() != null) map.put("result", resp.result());
        if (resp.leaderHint() != null) map.put("leaderHint", resp.leaderHint().id());
        return map;
    }
}
