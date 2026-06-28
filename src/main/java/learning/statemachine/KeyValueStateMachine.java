package learning.statemachine;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple key-value store state machine.
 * Supports commands: SET key value, GET key, DEL key.
 * Tracks per-client serial numbers for deduplication.
 */
@Slf4j
public class KeyValueStateMachine implements StateMachine {

    private final Map<String, String> store = new HashMap<>();

    // Deduplication: clientId -> (latest serialNumber, result)
    private final Map<Long, ClientSession> sessions = new HashMap<>();

    @Override
    public String apply(String command) {
        if (command == null || command.isBlank()) {
            return "ERR empty command";
        }

        // NO_OP entries (from leader election) are silently applied
        if (command.equals("NO OP")) {
            return "OK";
        }

        String[] parts = command.split(" ", 3);
        String op = parts[0].toUpperCase();

        return switch (op) {
            case "SET" -> {
                if (parts.length < 3) yield "ERR SET requires key and value";
                store.put(parts[1], parts[2]);
                yield "OK";
            }
            case "GET" -> {
                if (parts.length < 2) yield "ERR GET requires key";
                String val = store.get(parts[1]);
                yield val != null ? val : "(nil)";
            }
            case "DEL" -> {
                if (parts.length < 2) yield "ERR DEL requires key";
                String removed = store.remove(parts[1]);
                yield removed != null ? "1" : "0";
            }
            default -> "ERR unknown command: " + op;
        };
    }

    @Override
    public String isDuplicate(long clientId, long serialNumber) {
        ClientSession session = sessions.get(clientId);
        if (session != null && session.serialNumber == serialNumber) {
            return session.result;
        }
        return null;
    }

    @Override
    public void recordExecution(long clientId, long serialNumber, String result) {
        sessions.put(clientId, new ClientSession(serialNumber, result));
    }

    @Override
    public byte[] takeSnapshot() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            // Write store entries
            dos.writeInt(store.size());
            for (Map.Entry<String, String> e : store.entrySet()) {
                dos.writeUTF(e.getKey());
                dos.writeUTF(e.getValue());
            }
            // Write client sessions
            dos.writeInt(sessions.size());
            for (Map.Entry<Long, ClientSession> e : sessions.entrySet()) {
                dos.writeLong(e.getKey());
                dos.writeLong(e.getValue().serialNumber);
                dos.writeUTF(e.getValue().result);
            }
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize snapshot", e);
        }
    }

    @Override
    public void loadFromSnapshot(byte[] data) {
        store.clear();
        sessions.clear();
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int storeSize = dis.readInt();
            for (int i = 0; i < storeSize; i++) {
                store.put(dis.readUTF(), dis.readUTF());
            }
            int sessionSize = dis.readInt();
            for (int i = 0; i < sessionSize; i++) {
                long clientId = dis.readLong();
                long serial = dis.readLong();
                String result = dis.readUTF();
                sessions.put(clientId, new ClientSession(serial, result));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize snapshot", e);
        }
    }

    // Visible for testing
    public Map<String, String> getStore() {
        return store;
    }

    private record ClientSession(long serialNumber, String result) {}
}
