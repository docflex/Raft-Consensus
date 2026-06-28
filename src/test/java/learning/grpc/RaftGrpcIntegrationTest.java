package learning.grpc;

import learning.model.ServerID;
import learning.model.ServerRole;
import learning.node.RaftNode;
import learning.server.RaftServer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that boot 3 real Raft nodes communicating over gRPC on localhost.
 * Tests leader election, log replication, and client interaction over the network.
 */
@DisplayName("gRPC Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaftGrpcIntegrationTest {

    static final ServerID S1 = new ServerID(1);
    static final ServerID S2 = new ServerID(2);
    static final ServerID S3 = new ServerID(3);

    static RaftNode node1, node2, node3;

    @BeforeAll
    static void startCluster() throws Exception {
        node1 = new RaftNode(S1, List.of(S2, S3), 19001, Map.of(
                S2, "localhost:19002", S3, "localhost:19003"));
        node2 = new RaftNode(S2, List.of(S1, S3), 19002, Map.of(
                S1, "localhost:19001", S3, "localhost:19003"));
        node3 = new RaftNode(S3, List.of(S1, S2), 19003, Map.of(
                S1, "localhost:19001", S2, "localhost:19002"));

        node1.start();
        node2.start();
        node3.start();
    }

    @AfterAll
    static void stopCluster() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();
    }

    private static final long TIMEOUT = 10_000;

    private RaftNode findStableLeader() throws InterruptedException {
        // Find a leader and wait for its no-op to commit (proves majority connectivity)
        long deadline = System.currentTimeMillis() + TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode node : List.of(node1, node2, node3)) {
                synchronized (node.getRaftServer()) {
                    if (node.getRaftServer().getRole() == ServerRole.LEADER
                            && node.getRaftServer().getCommitIndex() >= 1) {
                        return node;
                    }
                }
            }
            Thread.sleep(100);
        }
        return null;
    }

    private void waitForCommit(RaftServer server, long minCommitIndex) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            synchronized (server) {
                if (server.getCommitIndex() >= minCommitIndex) return;
            }
            Thread.sleep(50);
        }
    }

    @Test
    @Order(1)
    @DisplayName("cluster elects a leader and commits no-op")
    void leaderElectionAndNoOp() throws Exception {
        RaftNode leader = findStableLeader();
        assertNotNull(leader, "A stable leader with committed no-op should exist within timeout");

        synchronized (leader.getRaftServer()) {
            assertEquals(ServerRole.LEADER, leader.getRaftServer().getRole());
            assertTrue(leader.getRaftServer().getCurrentTerm() >= 1);
            assertTrue(leader.getRaftServer().getCommitIndex() >= 1, "No-op should be committed");
        }
    }

    @Test
    @Order(2)
    @DisplayName("client write replicates to majority")
    void clientWriteReplication() throws Exception {
        RaftNode leader = findStableLeader();
        assertNotNull(leader);
        RaftServer leaderServer = leader.getRaftServer();

        long preCommit;
        synchronized (leaderServer) {
            preCommit = leaderServer.getCommitIndex();
            leaderServer.handleClientRequest(
                    learning.rpc.ClientRequest.builder()
                            .clientId(1).serialNumber(1).command("SET test_key hello").build());
        }

        waitForCommit(leaderServer, preCommit + 1);

        synchronized (leaderServer) {
            assertTrue(leaderServer.getCommitIndex() > preCommit,
                    "Client command should be committed");
        }
    }

    @Test
    @Order(3)
    @DisplayName("all nodes converge on commit index")
    void logConsistency() throws Exception {
        RaftNode leader = findStableLeader();
        assertNotNull(leader);

        long leaderCommit;
        synchronized (leader.getRaftServer()) {
            leaderCommit = leader.getRaftServer().getCommitIndex();
        }

        for (RaftNode node : List.of(node1, node2, node3)) {
            waitForCommit(node.getRaftServer(), leaderCommit);
            synchronized (node.getRaftServer()) {
                assertTrue(node.getRaftServer().getCommitIndex() >= leaderCommit,
                        "S" + node.getRaftServer().getSelfID().id() + " commitIndex should be >= " + leaderCommit);
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("all nodes converge to the same term")
    void termAgreement() throws Exception {
        long deadline = System.currentTimeMillis() + TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            long term1, term2, term3;
            synchronized (node1.getRaftServer()) { term1 = node1.getRaftServer().getCurrentTerm(); }
            synchronized (node2.getRaftServer()) { term2 = node2.getRaftServer().getCurrentTerm(); }
            synchronized (node3.getRaftServer()) { term3 = node3.getRaftServer().getCurrentTerm(); }

            if (term1 == term2 && term2 == term3 && term1 > 0) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Nodes did not converge to the same term within timeout");
    }
}
