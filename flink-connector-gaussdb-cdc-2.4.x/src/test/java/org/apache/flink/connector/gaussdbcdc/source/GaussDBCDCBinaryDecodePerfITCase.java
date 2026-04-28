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

package org.apache.flink.connector.gaussdbcdc.source;

import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Performance test for binary decode-style (decode-style='b') with different parallel-decode-num
 * values against a real GaussDB instance.
 *
 * <p>Tests 1, 4, 8 threads and measures total decode+read time.
 *
 * <p>Requires {@code -Dgaussdb.test.enabled=true} and a real GaussDB with wal_level=logical.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GaussDBCDCBinaryDecodePerfITCase {

    private static final String HOST = "1.92.120.69";
    private static final int PORT = 8000;
    private static final String DATABASE = "test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "GuassDB123";
    private static final String SCHEMA = "public";
    private static final String PERF_TABLE = "perf_test_binary_decode";

    /** Number of rows to insert for each test run. */
    private static final int BATCH_SIZE = 10000;

    private Connection connection;

    @BeforeAll
    void setUp() throws Exception {
        assumeThat(Boolean.getBoolean("gaussdb.test.enabled"))
                .as("GaussDB perf test disabled - set -Dgaussdb.test.enabled=true")
                .isTrue();

        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        connection = DriverManager.getConnection(url, USERNAME, PASSWORD);
        connection.setAutoCommit(true);

        ensureWalLevelLogical();
        createPerfTable();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA, PERF_TABLE));
            }
            connection.close();
        }
    }

    private void ensureWalLevelLogical() throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            if (rs.next()) {
                String walLevel = rs.getString(1);
                assertThat(walLevel)
                        .as("GaussDB must have wal_level=logical")
                        .isEqualToIgnoringCase("logical");
            }
        }
    }

    private void createPerfTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA, PERF_TABLE));
            stmt.execute(
                    String.format(
                            "CREATE TABLE %s.%s (id INT PRIMARY KEY, name VARCHAR(100), age INT, score NUMERIC(10,2))",
                            SCHEMA, PERF_TABLE));
        }
    }

    /** Baseline: serial decoding with JSON format (decode-style='b' not supported when num=1). */
    @Test
    void testBinaryDecodePerf_1Thread_JsonBaseline() throws Exception {
        runPerfTest(1, "j");
    }

    @Test
    void testBinaryDecodePerf_4Threads() throws Exception {
        runPerfTest(4, "b");
    }

    @Test
    void testBinaryDecodePerf_8Threads() throws Exception {
        runPerfTest(8, "b");
    }

    private void runPerfTest(int parallelDecodeNum, String decodeStyle) throws Exception {
        String slotName =
                String.format("perf_slot_%s_%d", decodeStyle, parallelDecodeNum).toLowerCase();

        System.out.println("\n========================================");
        System.out.printf(
                "PERF TEST: parallel-decode-num=%d, decode-style='%s'%n",
                parallelDecodeNum, decodeStyle);
        System.out.println("========================================");

        // Clean up: truncate table, drop slot
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("TRUNCATE TABLE %s.%s", SCHEMA, PERF_TABLE));
        }
        dropSlotIfExists(slotName);

        // Create WalReplicationStream first (slot must exist before data is inserted)
        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        parallelDecodeNum,
                        decodeStyle,
                        false,
                        1000);

        long initStart = System.currentTimeMillis();
        stream.initialize();
        long initDuration = System.currentTimeMillis() - initStart;
        System.out.printf(
                "  Stream init: %d ms (useReplicationApi=%s)%n",
                initDuration, stream.isUseReplicationApi());

        // Insert BATCH_SIZE rows after slot is created
        long insertStart = System.currentTimeMillis();
        String insertSql =
                String.format(
                        "INSERT INTO %s.%s (id, name, age, score) VALUES (?, ?, ?, ?)",
                        SCHEMA, PERF_TABLE);
        try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
            for (int i = 0; i < BATCH_SIZE; i++) {
                ps.setInt(1, i);
                ps.setString(2, "user_" + i);
                ps.setInt(3, 20 + (i % 50));
                ps.setDouble(4, i * 0.5);
                ps.addBatch();
                if ((i + 1) % 1000 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        long insertDuration = System.currentTimeMillis() - insertStart;
        System.out.printf("  Insert %d rows: %d ms%n", BATCH_SIZE, insertDuration);

        // Read all changes with retry (streaming API needs time to propagate)
        long readStart = System.currentTimeMillis();
        int totalChanges = 0;
        int totalDataChanges = 0;
        int emptyPolls = 0;
        int maxEmptyPolls = 30; // 30s max wait
        while (true) {
            List<WalChange> changes = stream.readChanges(5000);
            if (changes.isEmpty()) {
                emptyPolls++;
                if (emptyPolls >= maxEmptyPolls) {
                    break;
                }
            } else {
                emptyPolls = 0;
                totalChanges += changes.size();
                for (WalChange c : changes) {
                    if (c.getType() == WalChange.ChangeType.INSERT
                            || c.getType() == WalChange.ChangeType.UPDATE
                            || c.getType() == WalChange.ChangeType.DELETE) {
                        totalDataChanges++;
                    }
                }
            }
        }
        long readDuration = System.currentTimeMillis() - readStart;
        long totalDuration = initDuration + readDuration;

        System.out.printf("  Changes read: total=%d, data=%d%n", totalChanges, totalDataChanges);
        System.out.printf("  Read time: %d ms%n", readDuration);
        System.out.printf("  TOTAL: %d ms%n", totalDuration);
        System.out.printf(
                "  Throughput: %.1f changes/sec%n",
                totalChanges * 1000.0 / Math.max(readDuration, 1));

        // Cleanup
        stream.close();
        dropSlotIfExists(slotName);

        // Basic assertion
        assertThat(totalChanges).isGreaterThan(0);
        assertThat(totalDataChanges).isGreaterThan(0);

        // Print summary line for easy parsing
        System.out.printf(
                "[RESULT] decode-style=%s parallel-decode-num=%d total-ms=%d changes=%d data-changes=%d throughput=%.1f%n",
                decodeStyle,
                parallelDecodeNum,
                totalDuration,
                totalChanges,
                totalDataChanges,
                totalChanges * 1000.0 / Math.max(readDuration, 1));
    }

    private void dropSlotIfExists(String slotName) {
        try (PreparedStatement stmt =
                connection.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
            stmt.setString(1, slotName);
            stmt.execute();
        } catch (SQLException e) {
            // Slot may not exist
        }
    }
}
