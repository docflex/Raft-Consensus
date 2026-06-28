package learning.persistence;

import learning.model.LogEntry;
import learning.model.ServerID;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RiptideKV Stress Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RiptideKVStressTest {

    static final ServerID STRESS_SERVER = new ServerID(98);
    static Path baseDir;
    static RiptideKVPersistence persistence;

    @BeforeAll
    static void startServer() throws IOException {
        baseDir = Files.createTempDirectory("raft-stress-test");
        // Low flushKb to force memtable → SSTable flushes during tests
        persistence = RiptideKVPersistence.forServer(STRESS_SERVER, baseDir);
    }

    @AfterAll
    static void stopServer() {
        if (persistence != null) persistence.close();
    }

    @BeforeEach
    void clearState() {
        persistence.saveState(0, null);
        persistence.saveLog(List.of());
    }

    // ========================== Bulk Writes — Trigger Memtable Flush ==========================

    @Nested
    @DisplayName("Bulk Writes")
    class BulkWrites {

        @Test
        @Order(1)
        @DisplayName("500 log entries survive save and reload")
        void bulkLogEntries() {
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 1; i <= 500; i++) {
                entries.add(new LogEntry(i, (i / 100) + 1, "SET key" + i + "=value" + i));
            }

            persistence.saveLog(entries);
            List<LogEntry> loaded = persistence.loadLog();

            assertEquals(500, loaded.size());
            assertEquals(1, loaded.get(0).index());
            assertEquals("SET key1=value1", loaded.get(0).command());
            assertEquals(500, loaded.get(499).index());
            assertEquals("SET key500=value500", loaded.get(499).command());
            assertEquals(6, loaded.get(499).term()); // (500/100)+1 = 6
        }

        @Test
        @Order(2)
        @DisplayName("200 log entries with large commands")
        void bulkLargeCommands() {
            String largePayload = "X".repeat(100); // 100-byte command
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 1; i <= 200; i++) {
                entries.add(new LogEntry(i, 1, "SET key" + i + "=" + largePayload));
            }

            persistence.saveLog(entries);
            List<LogEntry> loaded = persistence.loadLog();

            assertEquals(200, loaded.size());
            // Verify first, middle, and last entries
            assertTrue(loaded.get(0).command().endsWith(largePayload));
            assertTrue(loaded.get(99).command().endsWith(largePayload));
            assertTrue(loaded.get(199).command().endsWith(largePayload));
        }
    }

    // ========================== Rapid State Overwrites — WAL + Compaction ==========================

    @Nested
    @DisplayName("Rapid State Overwrites")
    class RapidOverwrites {

        @Test
        @DisplayName("200 term bumps in rapid succession")
        void rapidTermBumps() {
            for (int term = 1; term <= 200; term++) {
                ServerID votedFor = (term % 2 == 0) ? new ServerID(2) : new ServerID(3);
                persistence.saveState(term, votedFor);
            }

            assertEquals(200, persistence.loadCurrentTerm());
            assertEquals(new ServerID(2), persistence.loadVotedFor()); // 200 is even → S2
        }

        @Test
        @DisplayName("100 alternating saveState and saveLog calls")
        void interleavedStateAndLog() {
            List<LogEntry> log = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                log.add(new LogEntry(i, i, "cmd" + i));
                persistence.saveLog(log);
                persistence.saveState(i, new ServerID(i % 5 + 1));
            }

            assertEquals(100, persistence.loadCurrentTerm());
            assertEquals(new ServerID(1), persistence.loadVotedFor()); // 100%5+1 = 1
            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(100, loaded.size());
            assertEquals("cmd100", loaded.get(99).command());
        }

        @Test
        @DisplayName("rapid votedFor flips between null and non-null")
        void rapidVotedForFlips() {
            for (int i = 0; i < 200; i++) {
                persistence.saveState(i, (i % 2 == 0) ? null : new ServerID(7));
            }

            // Last iteration: i=199, 199%2=1, so votedFor=S7
            assertEquals(199, persistence.loadCurrentTerm());
            assertEquals(new ServerID(7), persistence.loadVotedFor());
        }
    }

    // ========================== Log Churn — Grow/Truncate/Grow ==========================

    @Nested
    @DisplayName("Log Churn (Truncation + Regrowth)")
    class LogChurn {

        @Test
        @DisplayName("repeatedly grow to 100, truncate to 10, grow again — 10 cycles")
        void growTruncateGrowCycles() {
            for (int cycle = 0; cycle < 10; cycle++) {
                // Grow to 100
                List<LogEntry> entries = new ArrayList<>();
                for (int i = 1; i <= 100; i++) {
                    entries.add(new LogEntry(i, cycle + 1, "cycle" + cycle + "_cmd" + i));
                }
                persistence.saveLog(entries);
                assertEquals(100, persistence.loadLog().size());

                // Truncate to 10
                persistence.saveLog(entries.subList(0, 10));
                List<LogEntry> truncated = persistence.loadLog();
                assertEquals(10, truncated.size());
                assertEquals("cycle" + cycle + "_cmd10", truncated.get(9).command());
            }

            // Final state: 10 entries from cycle 9
            List<LogEntry> finalLog = persistence.loadLog();
            assertEquals(10, finalLog.size());
            assertEquals(10, finalLog.get(9).term()); // cycle 9 → term 10
        }

        @Test
        @DisplayName("truncate from 500 to 0 (empty log after large write)")
        void truncateToEmpty() {
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 1; i <= 500; i++) {
                entries.add(new LogEntry(i, 1, "data" + i));
            }
            persistence.saveLog(entries);
            assertEquals(500, persistence.loadLog().size());

            // Truncate to empty
            persistence.saveLog(List.of());
            assertTrue(persistence.loadLog().isEmpty());
        }

        @Test
        @DisplayName("incremental growth: append one entry at a time up to 100")
        void incrementalGrowth() {
            List<LogEntry> log = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                log.add(new LogEntry(i, 1, "entry" + i));
                persistence.saveLog(log);
            }

            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(100, loaded.size());
            assertEquals("entry1", loaded.get(0).command());
            assertEquals("entry100", loaded.get(99).command());
        }

        @Test
        @DisplayName("sawtooth pattern: grow by 20, truncate by 10, repeat")
        void sawtoothPattern() {
            List<LogEntry> log = new ArrayList<>();
            int index = 1;

            for (int cycle = 0; cycle < 20; cycle++) {
                // Grow by 20
                for (int i = 0; i < 20; i++) {
                    log.add(new LogEntry(index++, cycle + 1, "saw_" + (index - 1)));
                }
                persistence.saveLog(log);

                // Truncate last 10
                log = new ArrayList<>(log.subList(0, log.size() - 10));
                persistence.saveLog(log);
            }

            // 20 cycles × net 10 entries = 200 entries
            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(200, loaded.size());
        }
    }

    // ========================== Key Stress — DEL + Tombstone Compaction ==========================

    @Nested
    @DisplayName("Key Stress (DEL + Tombstone Behavior)")
    class KeyStress {

        @Test
        @DisplayName("overwrite same log index 200 times (triggers tombstone accumulation)")
        void overwriteSameIndex() {
            for (int i = 0; i < 200; i++) {
                persistence.saveLog(List.of(
                        new LogEntry(1, i + 1, "overwrite_" + i)));
            }

            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(1, loaded.size());
            assertEquals(200, loaded.get(0).term());
            assertEquals("overwrite_199", loaded.get(0).command());
        }

        @Test
        @DisplayName("grow to 200, shrink to 50, verify old keys are truly gone")
        void shrinkVerifyOldKeysGone() {
            // Write 200 entries
            List<LogEntry> big = new ArrayList<>();
            for (int i = 1; i <= 200; i++) {
                big.add(new LogEntry(i, 1, "big_" + i));
            }
            persistence.saveLog(big);
            assertEquals(200, persistence.loadLog().size());

            // Shrink to 50
            persistence.saveLog(big.subList(0, 50));
            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(50, loaded.size());

            // Verify no phantom entries
            assertEquals("big_50", loaded.get(49).command());
        }
    }

    // ========================== Raft-Realistic Workload ==========================

    @Nested
    @DisplayName("Raft-Realistic Workload")
    class RealisticWorkload {

        @Test
        @DisplayName("simulate 20 elections with log growth between each")
        void twentyElections() {
            List<LogEntry> log = new ArrayList<>();
            int logIndex = 1;

            for (int term = 1; term <= 20; term++) {
                // Election: save new term + votedFor
                persistence.saveState(term, new ServerID(term % 5 + 1));

                // Leader appends NO OP
                log.add(new LogEntry(logIndex++, term, "NO OP"));
                persistence.saveLog(log);

                // 3 client commands per term
                for (int cmd = 0; cmd < 3; cmd++) {
                    log.add(new LogEntry(logIndex++, term, "SET k" + logIndex + "=v" + logIndex));
                    persistence.saveLog(log);
                }
            }

            // 20 terms × (1 NO OP + 3 commands) = 80 entries
            assertEquals(20, persistence.loadCurrentTerm());
            assertEquals(new ServerID(1), persistence.loadVotedFor()); // 20%5+1=1

            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(80, loaded.size());
            assertEquals("NO OP", loaded.get(0).command());
            assertEquals(1, loaded.get(0).term());
            assertEquals(20, loaded.get(79).term());
        }

        @Test
        @DisplayName("simulate follower receiving batched AppendEntries (varying batch sizes)")
        void batchedAppendEntries() {
            List<LogEntry> log = new ArrayList<>();
            int index = 1;

            // 15 batches of varying sizes (1 to 15 entries each)
            for (int batch = 1; batch <= 15; batch++) {
                for (int i = 0; i < batch; i++) {
                    log.add(new LogEntry(index++, batch, "batch" + batch + "_" + i));
                }
                persistence.saveLog(log);
            }

            // Total entries: sum(1..15) = 120
            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(120, loaded.size());
            assertEquals("batch1_0", loaded.get(0).command());
            assertEquals(1, loaded.get(0).term());
            assertEquals(15, loaded.get(119).term());
        }

        @Test
        @DisplayName("simulate leader conflict resolution: 5 cycles of diverge + truncate + converge")
        void leaderConflictResolution() {
            List<LogEntry> log = new ArrayList<>();

            for (int cycle = 0; cycle < 5; cycle++) {
                int baseTerm = cycle * 3 + 1;

                // Phase 1: old leader writes 30 entries
                log.clear();
                for (int i = 1; i <= 30; i++) {
                    log.add(new LogEntry(i, baseTerm, "old_leader_" + cycle + "_" + i));
                }
                persistence.saveLog(log);

                // Phase 2: new leader truncates back to 10, writes 40 new entries
                log = new ArrayList<>(log.subList(0, 10));
                for (int i = 11; i <= 50; i++) {
                    log.add(new LogEntry(i, baseTerm + 1, "new_leader_" + cycle + "_" + i));
                }
                persistence.saveLog(log);
                persistence.saveState(baseTerm + 1, new ServerID(cycle + 1));
            }

            List<LogEntry> loaded = persistence.loadLog();
            assertEquals(50, loaded.size());

            // First 10 entries from last old leader (cycle 4, term 13)
            assertEquals(13, loaded.get(0).term());
            // Entries 11-50 from last new leader (cycle 4, term 14)
            assertEquals(14, loaded.get(10).term());
            assertEquals("new_leader_4_11", loaded.get(10).command());
        }
    }
}
