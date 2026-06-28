package learning.spring;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.ArrayList;
import java.util.List;

@Data
@Validated
@ConfigurationProperties(prefix = "raft")
public class RaftProperties {

    /**
     * Node ID. Set to 0 for auto-assignment from Docker hostname suffix.
     * e.g. hostname "raft-3" → nodeId=3
     */
    @Min(value = 0, message = "node-id must be >= 0 (0 = auto)")
    private long nodeId = 0;

    @Min(value = 1024, message = "grpc-port must be >= 1024")
    private int grpcPort = 9001;

    /**
     * Static peer list. Optional — leave empty when using DNS discovery.
     */
    @NotNull
    private List<@Valid PeerConfig> peers = new ArrayList<>();

    /**
     * DNS-based peer discovery. Set to a Docker Compose service name
     * (e.g. "raft") to auto-discover peers. Leave blank to use static peers.
     */
    private String discoveryDns = "";

    /**
     * Directory for RiptideKV persistence data.
     * Each node stores its WAL and SSTables under {dataDir}/s{nodeId}.
     * Set to empty string to disable persistence (in-memory only).
     */
    private String dataDir = "";

    @Data
    public static class PeerConfig {
        @Min(value = 1, message = "peer id must be >= 1")
        private long id;

        @NotBlank(message = "peer host must not be blank")
        private String host = "localhost";

        @Min(value = 1024, message = "peer port must be >= 1024")
        private int port;
    }
}
