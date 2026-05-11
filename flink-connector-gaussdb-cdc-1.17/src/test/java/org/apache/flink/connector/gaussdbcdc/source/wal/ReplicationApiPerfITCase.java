/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.gaussdbcdc.source.wal;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Performance benchmark for streaming replication API with different parallel-decode-num values.
 *
 * <p>Usage: {@code JAVA_HOME=<jdk17+> mvn test -Dtest=ReplicationApiPerfITCase
 * -Dgaussdb.test.enabled=true -Dcheckstyle.skip=true}
 *
 * <p>Tests parallel-decode-num = 1, 4, 8 with decode-style=b + sending-batch=true.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplicationApiPerfITCase {

    private static final String HOST = "1.92.120.69";
    private static final int PORT = 8000;
    private static final String DATABASE = "test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "GuassDB123";
    private static final String SCHEMA = "public";
    private static final String PERF_TABLE = "flink_cdc_perf_test";

    /** Total rows to insert for each test round. */
    private static final int TOTAL_ROWS = 100_000;

    /** Batch size for inserts. */
    private static final int INSERT_BATCH_SIZE = 5000;

    private Connection connection;

    @BeforeAll
    void setUp() throws Exception {
        assumeThat(Boolean.getBoolean("gaussdb.test.enabled"))
                .as("GaussDB integration test disabled")
                .isTrue();

        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        connection = DriverManager.getConnection(url, USERNAME, PASSWORD);
        connection.setAutoCommit(true);

        // Create perf test table
        createPerfTable();
    }

    private void createPerfTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA, PERF_TABLE));
            stmt.execute(
                    String.format(
                            "CREATE TABLE %s.%s (id SERIAL PRIMARY KEY, name VARCHAR(100), age INT, score DECIMAL(10,2), remark VARCHAR(200))",
                            SCHEMA, PERF_TABLE));
        }
    }

    @Order(1)
    @Test
    void testParallelDecode1() throws Exception {
        // parallel-decode-num=1 does not support decode-style=b, use 'j' (JSON)
        runBenchmark(1, "j", false);
    }

    @Order(2)
    @Test
    void testParallelDecode4() throws Exception {
        runBenchmark(4, "b", true);
    }

    @Order(3)
    @Test
    void testParallelDecode8() throws Exception {
        runBenchmark(8, "b", true);
    }

    @Order(4)
    @Test
    void testParallelDecode4Json() throws Exception {
        runBenchmark(4, "j", true);
    }

    /**
     * Run a single benchmark: insert data, then read via streaming replication API and measure
     * throughput.
     */
    private void runBenchmark(int parallelDecodeNum, String decodeStyle, boolean sendingBatch)
            throws Exception {
        String slotName = String.format("perf_slot_%d_%s", parallelDecodeNum, decodeStyle);

        System.out.printf(
                "%n========== Benchmark: parallel-decode-num=%d, decode-style=%s, sending-batch=%s ==========%n",
                parallelDecodeNum, decodeStyle, sendingBatch);

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create WalReplicationStream
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE),
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        parallelDecodeNum,
                        decodeStyle,
                        sendingBatch,
                        10000);

        long initStart = System.currentTimeMillis();
        stream.initialize();
        long initTime = System.currentTimeMillis() - initStart;
        System.out.printf(
                "Stream initialized in %d ms (useReplicationApi=%s)%n",
                initTime, stream.isUseReplicationApi());

        if (!stream.isUseReplicationApi()) {
            System.out.println(
                    "WARNING: Streaming replication API not available, skipping benchmark");
            stream.close();
            dropSlotIfExists(slotName);
            return;
        }

        // Truncate table to start fresh
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("TRUNCATE %s.%s", SCHEMA, PERF_TABLE));
        }

        // Insert data in batches
        System.out.printf("Inserting %d rows in batches of %d...%n", TOTAL_ROWS, INSERT_BATCH_SIZE);
        long insertStart = System.currentTimeMillis();
        insertData(TOTAL_ROWS, INSERT_BATCH_SIZE);
        long insertTime = System.currentTimeMillis() - insertStart;
        System.out.printf(
                "Insert completed in %d ms (%.0f rows/s)%n",
                insertTime, TOTAL_ROWS * 1000.0 / Math.max(insertTime, 1));

        // Read all changes via replication stream
        System.out.println("Reading changes via streaming replication API...");
        long readStart = System.currentTimeMillis();
        int totalChanges = 0;
        int dataChanges = 0;
        int emptyReadCount = 0;
        long totalBytes = 0;

        // Read loop with timeout
        long readDeadline = System.currentTimeMillis() + 120_000; // 2 min max
        while (System.currentTimeMillis() < readDeadline) {
            List<WalChange> changes = stream.readChanges(10000);
            if (changes.isEmpty()) {
                emptyReadCount++;
                if (emptyReadCount > 30) {
                    // No new data for 30 consecutive reads, assume all consumed
                    break;
                }
                Thread.sleep(100);
                continue;
            }
            emptyReadCount = 0;
            totalChanges += changes.size();
            for (WalChange change : changes) {
                if (change.getType() == WalChange.ChangeType.INSERT
                        || change.getType() == WalChange.ChangeType.UPDATE
                        || change.getType() == WalChange.ChangeType.DELETE) {
                    dataChanges++;
                }
                if (change.getRawData() != null) {
                    totalBytes += change.getRawData().length();
                }
            }

            // Check if we've read enough data changes
            if (dataChanges >= TOTAL_ROWS) {
                break;
            }
        }

        long readTime = System.currentTimeMillis() - readStart;
        double throughputRows = dataChanges * 1000.0 / Math.max(readTime, 1);
        double throughputMB = totalBytes / 1024.0 / 1024.0 / (readTime / 1000.0);

        System.out.println("----- Results -----");
        System.out.printf(
                "  Total changes: %d (data: %d, control: %d)%n",
                totalChanges, dataChanges, totalChanges - dataChanges);
        System.out.printf("  Read time: %d ms%n", readTime);
        System.out.printf("  Throughput: %.0f data rows/s%n", throughputRows);
        System.out.printf(
                "  Data volume: %.2f MB, %.2f MB/s%n", totalBytes / 1024.0 / 1024.0, throughputMB);
        System.out.printf("  Last LSN: %s%n", stream.getLastLsn());
        System.out.println("-------------------");

        // Close stream
        stream.close();
        dropSlotIfExists(slotName);

        // Small delay between tests to let server clean up
        Thread.sleep(2000);
    }

    private void insertData(int totalRows, int batchSize) throws SQLException {
        // Use a separate connection with autoCommit=false for batch inserts
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        try (Connection batchConn = DriverManager.getConnection(url, USERNAME, PASSWORD)) {
            batchConn.setAutoCommit(false);
            try (PreparedStatement stmt =
                    batchConn.prepareStatement(
                            String.format(
                                    "INSERT INTO %s.%s (name, age, score, remark) VALUES (?, ?, ?, ?)",
                                    SCHEMA, PERF_TABLE))) {

                for (int i = 0; i < totalRows; i++) {
                    stmt.setString(1, "user_" + i);
                    stmt.setInt(2, 20 + (i % 50));
                    stmt.setDouble(3, 60.0 + (i % 40));
                    stmt.setString(4, "remark for user " + i);
                    stmt.addBatch();

                    if ((i + 1) % batchSize == 0) {
                        stmt.executeBatch();
                        batchConn.commit();
                    }
                }
                // Remaining rows
                if (totalRows % batchSize != 0) {
                    stmt.executeBatch();
                    batchConn.commit();
                }
            }
        }
    }

    private void dropSlotIfExists(String slotName) {
        try (PreparedStatement stmt =
                connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
            stmt.setString(1, slotName);
            stmt.execute();
        } catch (SQLException e) {
            // Slot may not exist, that's OK
        }
    }
}
