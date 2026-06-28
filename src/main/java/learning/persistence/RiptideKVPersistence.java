package learning.persistence;

import io.riptidekv.RiptideKVConfig;
import io.riptidekv.RiptideKVServer;
import learning.model.LogEntry;
import learning.model.ServerID;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RiptideKVPersistence implements PersistenceLayer {

    public static final int BASE_PORT = 16400;

    private static final String KEY_CURRENT_TERM = "raft:currentTerm";
    private static final String KEY_VOTED_FOR    = "raft:votedFor";
    private static final String KEY_LOG          = "raft:log";
    private static final String KEY_LOG_SIZE     = "raft:logSize";

    private final RiptideKVServer server;
    private final Jedis jedis;
    private final ServerID serverID;

    public static RiptideKVPersistence forServer(ServerID serverID, Path baseDir) throws IOException {
        int port = BASE_PORT + (int) serverID.id();
        Path dataDir = baseDir.resolve("s" + serverID.id());
        return new RiptideKVPersistence(serverID, port, dataDir);
    }

    private RiptideKVPersistence(ServerID serverID, int port, Path dataDir) throws IOException {
        this.serverID = serverID;

        // Fail-fast if a zombie server from a previous run occupies this port
        ensurePortFree("127.0.0.1", port);

        RiptideKVConfig config = RiptideKVConfig.builder()
                .bind("127.0.0.1:" + port)
                .dataDir(dataDir)
                .walSync(true)
                .build();

        this.server = new RiptideKVServer(config);
        this.server.start();

        this.jedis = new Jedis("127.0.0.1", port, 30_000);
        log.info("[S{}] RiptideKV persistence started on port {} (dataDir={})",
                serverID.id(), port, dataDir);
    }

    public ServerID getServerID() {
        return serverID;
    }

    @Override
    public void saveState(long currentTerm, ServerID votedFor) {
        jedis.mset(
                KEY_CURRENT_TERM, String.valueOf(currentTerm),
                KEY_VOTED_FOR, votedFor == null ? "" : String.valueOf(votedFor.id()));
        log.debug("Persisted state: term={}, votedFor={}", currentTerm,
                votedFor == null ? "null" : votedFor.id());
    }

    @Override
    public void saveLog(List<LogEntry> logEntries) {
        // Read old size BEFORE overwriting, to clean up stale keys
        String oldSizeStr = jedis.get(KEY_LOG_SIZE);
        int oldSize = (oldSizeStr == null) ? 0 : Integer.parseInt(oldSizeStr);

        // Batch-write all entries + size in a single MSET round-trip
        if (!logEntries.isEmpty()) {
            // MSET expects alternating key, value, key, value, ...
            String[] keysAndValues = new String[(logEntries.size() + 1) * 2];
            keysAndValues[0] = KEY_LOG_SIZE;
            keysAndValues[1] = String.valueOf(logEntries.size());
            for (int i = 0; i < logEntries.size(); i++) {
                LogEntry entry = logEntries.get(i);
                keysAndValues[(i + 1) * 2]     = KEY_LOG + ":" + i;
                keysAndValues[(i + 1) * 2 + 1] = entry.index() + "\n" + entry.term() + "\n" + entry.command();
            }
            jedis.mset(keysAndValues);
        } else {
            jedis.set(KEY_LOG_SIZE, "0");
        }

        // Batch-delete stale keys beyond the new log size
        if (oldSize > logEntries.size()) {
            String[] staleKeys = new String[oldSize - logEntries.size()];
            for (int i = 0; i < staleKeys.length; i++) {
                staleKeys[i] = KEY_LOG + ":" + (logEntries.size() + i);
            }
            jedis.del(staleKeys);
        }

        log.debug("Persisted log: {} entries", logEntries.size());
    }

    @Override
    public long loadCurrentTerm() {
        String val = jedis.get(KEY_CURRENT_TERM);
        return val == null ? 0 : Long.parseLong(val);
    }

    @Override
    public ServerID loadVotedFor() {
        String val = jedis.get(KEY_VOTED_FOR);
        if (val == null || val.isEmpty()) return null;
        return new ServerID(Long.parseLong(val));
    }

    @Override
    public List<LogEntry> loadLog() {
        String sizeStr = jedis.get(KEY_LOG_SIZE);
        if (sizeStr == null) return new ArrayList<>();
        int size = Integer.parseInt(sizeStr);
        if (size == 0) return new ArrayList<>();

        // Batch-read all entries in a single MGET round-trip
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = KEY_LOG + ":" + i;
        }
        List<String> values = jedis.mget(keys);

        List<LogEntry> result = new ArrayList<>(size);
        for (String serialized : values) {
            // Parse "index\nterm\ncommand"
            int firstNl = serialized.indexOf('\n');
            int secondNl = serialized.indexOf('\n', firstNl + 1);
            long index = Long.parseLong(serialized.substring(0, firstNl));
            long term = Long.parseLong(serialized.substring(firstNl + 1, secondNl));
            String command = serialized.substring(secondNl + 1);
            result.add(new LogEntry(index, term, command));
        }
        log.info("Loaded log from RiptideKV: {} entries", result.size());
        return result;
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
        if (server != null) server.close();
        log.info("RiptideKV persistence shut down");
    }

    private static void ensurePortFree(String host, int port) throws IOException {
        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress(host, port), 200);
            // Connection succeeded → something is already listening
            throw new IOException(
                    "Port " + port + " is already in use (zombie RiptideKV process?). " +
                    "Kill the stale process before starting a new server.");
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Port ")) {
                throw e; // re-throw our own exception
            }
            // Connection refused → port is free, proceed
        }
    }
}
