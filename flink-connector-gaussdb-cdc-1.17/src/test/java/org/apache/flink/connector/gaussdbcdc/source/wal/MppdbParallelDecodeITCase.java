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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for GaussDB mppdb_decoding parallel logical decoding.
 *
 * <p>These tests require a running GaussDB instance with wal_level=logical. Set system property
 * gaussdb.test.enabled=true to enable. Must run with JDK 17+ (GaussDB JDBC driver requirement).
 *
 * <p>Usage: {@code JAVA_HOME=<jdk17+> mvn test -Dtest=MppdbParallelDecodeITCase
 * -Dgaussdb.test.enabled=true -Dcheckstyle.skip=true}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MppdbParallelDecodeITCase {

    private static final String HOST = "1.92.120.69";
    private static final int PORT = 8000;
    private static final String DATABASE = "test";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "GuassDB123";
    private static final String SCHEMA = "public";
    private static final String TEST_TABLE = "flink_cdc_it_test";

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

        // Ensure wal_level = logical
        ensureWalLevelLogical();

        // Create test table
        createTestTable();
    }

    private void ensureWalLevelLogical() throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            if (rs.next()) {
                String walLevel = rs.getString(1);
                assertThat(walLevel)
                        .as("GaussDB must have wal_level=logical for CDC")
                        .isEqualToIgnoringCase("logical");
            }
        }
    }

    private void createTestTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA, TEST_TABLE));
            stmt.execute(
                    String.format(
                            "CREATE TABLE %s.%s (id SERIAL PRIMARY KEY, name VARCHAR(100), age INT)",
                            SCHEMA, TEST_TABLE));
        }
    }

    @Test
    void testSlotCreationWithMppdbDecoding() throws Exception {
        String slotName = "it_slot_mppdb";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create slot with mppdb_decoding plugin
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
            stmt.setString(1, slotName);
            stmt.setString(2, "mppdb_decoding");
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                // GaussDB returns: slot_name, lsn
                ResultSetMetaData meta = rs.getMetaData();
                System.out.printf(
                        "Slot created: %s=%s, %s=%s%n",
                        meta.getColumnName(1),
                        rs.getString(1),
                        meta.getColumnName(2),
                        rs.getString(2));
                assertThat(rs.getString(1)).isNotNull();
            }
        }

        // Verify slot exists with correct plugin
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT plugin FROM pg_replication_slots WHERE slot_name = ?")) {
            stmt.setString(1, slotName);
            try (ResultSet rs = stmt.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("plugin")).isEqualTo("mppdb_decoding");
            }
        }

        // Cleanup
        dropSlotIfExists(slotName);
    }

    @Test
    void testParallelDecodeReadChanges() throws Exception {
        String slotName = "it_slot_parallel";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create slot with mppdb_decoding
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
            stmt.setString(1, slotName);
            stmt.setString(2, "mppdb_decoding");
            stmt.execute();
        }

        // Insert test data to generate WAL changes
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (name, age) VALUES ('Alice', 25)",
                            SCHEMA, TEST_TABLE));
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (name, age) VALUES ('Bob', 30)",
                            SCHEMA, TEST_TABLE));
            stmt.execute(
                    String.format(
                            "UPDATE %s.%s SET age = 26 WHERE name = 'Alice'", SCHEMA, TEST_TABLE));
        }

        // Read changes with parallel decoding using location column (GaussDB)
        String sql =
                String.format(
                        "SELECT location AS lsn, xid, data FROM pg_logical_slot_peek_changes('%s', NULL, 100, 'include-xids', '1', 'parallel-decode-num', '4')",
                        slotName);

        int changeCount = 0;
        int insertCount = 0;
        int updateCount = 0;

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String lsn = rs.getString("lsn");
                String xid = rs.getString("xid");
                String data = rs.getString("data");

                System.out.printf("LSN=%s, XID=%s, DATA=%s%n", lsn, xid, data);

                if (data != null && !data.isEmpty()) {
                    changeCount++;
                    if (data.contains("\"op_type\":\"INSERT\"")) {
                        insertCount++;
                    } else if (data.contains("\"op_type\":\"UPDATE\"")) {
                        updateCount++;
                    }
                }
            }
        }

        // Should have BEGIN/COMMIT + data changes
        assertThat(changeCount).isGreaterThanOrEqualTo(2);
        assertThat(insertCount).isGreaterThanOrEqualTo(1);

        // Cleanup
        dropSlotIfExists(slotName);
    }

    @Test
    void testWalReplicationStreamIntegration() throws Exception {
        String slotName = "it_slot_stream";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE),
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        1000);

        // Initialize should create the slot and start streaming
        stream.initialize();

        assertThat(stream.isRunning()).isTrue();
        assertThat(stream.getLastLsn()).isNotNull();

        // Insert test data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (name, age) VALUES ('Diana', 28)",
                            SCHEMA, TEST_TABLE));
        }

        // Read changes via WalReplicationStream
        List<WalChange> changes = stream.readChanges(100);
        System.out.printf("Read %d changes from WAL stream%n", changes.size());
        for (WalChange change : changes) {
            System.out.printf(
                    "  Change: type=%s, schema=%s, table=%s%n",
                    change.getType(), change.getSchema(), change.getTable());
        }

        // Stream should still be running
        assertThat(stream.isRunning()).isTrue();

        // Close
        stream.close();
        assertThat(stream.isRunning()).isFalse();

        // Cleanup slot
        dropSlotIfExists(slotName);
    }

    @Test
    void testGetCurrentLsnCompatibility() throws Exception {
        // Test that GaussDB compatible function works
        String[] functions = {
            "SELECT pg_current_xlog_location()", "SELECT pg_last_xlog_replay_location()"
        };

        String validLsn = null;
        for (String sql : functions) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String lsn = rs.getString(1);
                    if (lsn != null) {
                        System.out.printf("%s => %s%n", sql, lsn);
                        validLsn = lsn;
                    }
                }
            } catch (SQLException e) {
                System.out.printf("%s => NOT AVAILABLE: %s%n", sql, e.getMessage().split("\n")[0]);
            }
        }

        assertThat(validLsn).as("At least one LSN function should work").isNotNull();
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
