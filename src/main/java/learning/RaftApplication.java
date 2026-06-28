package learning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the Raft consensus cluster node.
 *
 * <p>Starts the embedded web server (REST API) and the gRPC server
 * (inter-node Raft RPCs) via {@link learning.spring.RaftLifecycle}.
 */
@SpringBootApplication
public class RaftApplication {

    public static void main(String[] args) {
        SpringApplication.run(RaftApplication.class, args);
    }
}
