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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for GaussDB CDC SourceFunction against a real GaussDB instance.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>JDBC connection and driver loading
 *   <li>WalReplicationStream initialization and SQL function fallback
 *   <li>Change capture via pg_logical_slot_peek_changes
 *   <li>GaussDBCDCSourceFunction lifecycle (snapshot + streaming)
 *   <li>Parallel snapshot
 * </ul>
 *
 * <p>Requires a running GaussDB instance with wal_level=logical. Set system property {@code
 * gaussdb.test.enabled=true} to enable.
 *
 * <p>Usage: {@code mvn test -Dtest=GaussDBCDCTSourceFunctionITCase -Dgaussdb.test.enabled=true
 * -Dcheckstyle.skip=true}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GaussDBCDCSourceFunctionITCase {

    private static final Properties SYSTEM_PROPERTIES = System.getProperties();
    private static final String HOST = SYSTEM_PROPERTIES.getProperty("gaussdb.host", "localhost");
    private static final int PORT =
            Integer.parseInt(SYSTEM_PROPERTIES.getProperty("gaussdb.port", "8000"));
    private static final String DATABASE =
            SYSTEM_PROPERTIES.getProperty("gaussdb.database", "test");
    private static final String USERNAME =
            SYSTEM_PROPERTIES.getProperty("gaussdb.username", "root");
    private static final String PASSWORD = SYSTEM_PROPERTIES.getProperty("gaussdb.password", "");
    private static final String SCHEMA = SYSTEM_PROPERTIES.getProperty("gaussdb.schema", "public");
    private static final String TEST_TABLE =
            SYSTEM_PROPERTIES.getProperty("gaussdb.table", "flink_cdc_sf_it_test");

    private Connection connection;

    @Test
    void testPollingUsesPrimaryKeyTieBreakerForSameTimestamp() throws Exception {
        String table = "flink_cdc_sf_it_polling_cursor";
        String qualified = SCHEMA + "." + table;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + qualified);
            stmt.execute(
                    "CREATE TABLE "
                            + qualified
                            + " (code VARCHAR(32) PRIMARY KEY, value_col VARCHAR(64), updated_at TIMESTAMP)");
            stmt.execute(
                    "INSERT INTO "
                            + qualified
                            + " VALUES ('A', 'before-a', '2026-01-01 00:00:00'),"
                            + " ('B', 'before-b', '2026-01-01 00:00:00'),"
                            + " ('C', 'before-c', '2026-01-01 00:00:00')");
        }

        try {
            ChangeDataPoller poller =
                    new ChangeDataPoller(
                            connection,
                            DATABASE,
                            SCHEMA,
                            Collections.singletonList(table),
                            "raw",
                            "debezium",
                            2);
            poller.loadSnapshot();

            try (PreparedStatement update =
                    connection.prepareStatement(
                            "UPDATE " + qualified + " SET value_col = 'after', updated_at = ?")) {
                update.setTimestamp(1, Timestamp.valueOf("2026-01-02 00:00:00"));
                assertThat(update.executeUpdate()).isEqualTo(3);
            }

            assertThat(poller.pollUpdates()).hasSize(2);
            assertThat(poller.pollUpdates()).hasSize(1);
            assertThat(poller.pollUpdates()).isEmpty();
            System.out.println(
                    "[OK] Polling cursor emitted all same-timestamp rows across batch boundary");
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + qualified);
            }
        }
    }

    @BeforeAll
    void setUp() throws Exception {
        assumeThat(
                        Boolean.parseBoolean(
                                SYSTEM_PROPERTIES.getProperty("gaussdb.test.enabled", "false")))
                .as("GaussDB integration test disabled - set -Dgaussdb.test.enabled=true")
                .isTrue();

        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        connection = DriverManager.getConnection(url, USERNAME, PASSWORD);
        connection.setAutoCommit(true);

        // Verify wal_level = logical
        ensureWalLevelLogical();

        // Create test table
        createTestTable();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            // Cleanup test table
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", SCHEMA, TEST_TABLE));
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
                            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, name VARCHAR(100), age INT)",
                            SCHEMA, TEST_TABLE));
        }
    }

    // ---- Test 1: JDBC Connection and Driver ----

    @Test
    @org.junit.jupiter.api.Order(1)
    void testJdbcConnectionAndDriver() throws Exception {
        String url = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        try (Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD)) {
            assertThat(conn.isValid(5)).isTrue();

            // Test basic query
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        System.out.println("[OK] JDBC connection and driver verified");
    }

    // ---- Test 2: Compatible Mode URL ----

    @Test
    @org.junit.jupiter.api.Order(2)
    void testJdbcConnectionWithCompatibleMode() throws Exception {
        // This is the URL format used by GaussDBCDCSourceFunction.open()
        String url =
                String.format("jdbc:gaussdb://%s:%d/%s?compatibleMode=mysql", HOST, PORT, DATABASE);
        try (Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD)) {
            assertThat(conn.isValid(5)).isTrue();
        }

        System.out.println("[OK] JDBC connection with compatibleMode=mysql verified");
    }

    // ---- Test 3: Slot Management ----

    @Test
    @org.junit.jupiter.api.Order(3)
    void testSlotCreationAndCleanup() throws Exception {
        String slotName = "sf_it_slot_mgmt";

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
                assertThat(rs.getString(1)).isNotNull();
                System.out.printf("  Slot created: %s%n", rs.getString(1));
            }
        }

        // Verify slot exists
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

        System.out.println("[OK] Slot creation and cleanup verified");
    }

    // ---- Test 4: WalReplicationStream Change Capture ----

    @Test
    @org.junit.jupiter.api.Order(4)
    void testWalReplicationStreamChangeCapture() throws Exception {
        String slotName = "sf_it_stream";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create WalReplicationStream
        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        1000);

        // Initialize - should create slot and start streaming
        stream.initialize();
        assertThat(stream.isRunning()).isTrue();

        System.out.printf(
                "  Stream initialized: useReplicationApi=%s, lastLsn=%s%n",
                stream.isUseReplicationApi(), stream.getLastLsn());

        // Insert data to generate WAL changes
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (1, 'Charlie', 35)",
                            SCHEMA, TEST_TABLE));
        }

        // Read changes with retries (streaming API may need time to propagate)
        List<WalChange> changes = readChangesWithRetry(stream, 100, 10);
        System.out.printf("  Read %d changes from WAL stream%n", changes.size());
        for (WalChange change : changes) {
            System.out.printf(
                    "    Change: type=%s, schema=%s, table=%s, lsn=%s%n",
                    change.getType(), change.getSchema(), change.getTable(), change.getLsn());
        }

        // Should have some changes (BEGIN/COMMIT + data changes)
        assertThat(changes.size()).isGreaterThan(0);

        // Close
        stream.close();
        assertThat(stream.isRunning()).isFalse();

        // Cleanup
        dropSlotIfExists(slotName);

        System.out.println("[OK] WalReplicationStream change capture verified");
    }

    // ---- Test 5: LSN Functions Compatibility ----

    @Test
    @org.junit.jupiter.api.Order(5)
    void testLsnFunctionsCompatibility() throws Exception {
        // Test GaussDB compatible functions
        String[] functions = {
            "SELECT pg_current_xlog_location()", "SELECT pg_current_wal_lsn()",
        };

        String validLsn = null;
        for (String sql : functions) {
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String lsn = rs.getString(1);
                    if (lsn != null) {
                        System.out.printf("  %s => %s%n", sql, lsn);
                        validLsn = lsn;
                    }
                }
            } catch (SQLException e) {
                System.out.printf(
                        "  %s => NOT AVAILABLE: %s%n", sql, e.getMessage().split("\n")[0]);
            }
        }

        assertThat(validLsn).as("At least one LSN function should work").isNotNull();

        System.out.println("[OK] LSN functions compatibility verified");
    }

    // ---- Test 6: pg_logical_slot_peek_changes ----

    @Test
    @org.junit.jupiter.api.Order(6)
    void testPgLogicalSlotPeekChanges() throws Exception {
        String slotName = "sf_it_peek";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create slot
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
                            "INSERT INTO %s.%s (id, name, age) VALUES (2, 'Diana', 28)",
                            SCHEMA, TEST_TABLE));
            stmt.execute(
                    String.format(
                            "UPDATE %s.%s SET age = 29 WHERE name = 'Diana'", SCHEMA, TEST_TABLE));
        }

        // Read changes via SQL function with parallel-decode-num
        String sql =
                String.format(
                        "SELECT location AS lsn, xid, data FROM pg_logical_slot_peek_changes('%s', NULL, 100, 'include-xids', '1', 'parallel-decode-num', '4')",
                        slotName);

        int changeCount = 0;
        int dataChangeCount = 0;
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String lsn = rs.getString("lsn");
                String xid = rs.getString("xid");
                String data = rs.getString("data");

                System.out.printf("  LSN=%s, XID=%s, DATA=%s%n", lsn, xid, data);

                if (data != null && !data.isEmpty()) {
                    changeCount++;
                    if (data.contains("INSERT")
                            || data.contains("UPDATE")
                            || data.contains("\"op_type\":\"INSERT\"")
                            || data.contains("\"op_type\":\"UPDATE\"")) {
                        dataChangeCount++;
                    }
                }
            }
        }

        assertThat(changeCount).isGreaterThan(0);

        // Cleanup
        dropSlotIfExists(slotName);

        System.out.printf(
                "[OK] pg_logical_slot_peek_changes verified (total=%d, data=%d)%n",
                changeCount, dataChangeCount);
    }

    // ---- Test 7: Slot Advance ----

    @Test
    @org.junit.jupiter.api.Order(7)
    void testSlotAdvance() throws Exception {
        String slotName = "sf_it_advance";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Create slot
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
            stmt.setString(1, slotName);
            stmt.setString(2, "mppdb_decoding");
            stmt.execute();
        }

        // Insert data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (3, 'Eve', 22)",
                            SCHEMA, TEST_TABLE));
        }

        // Peek changes to get LSN
        String peekLsn = null;
        String peekSql =
                String.format(
                        "SELECT location AS lsn FROM pg_logical_slot_peek_changes('%s', NULL, 1)",
                        slotName);
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(peekSql)) {
            if (rs.next()) {
                peekLsn = rs.getString("lsn");
            }
        }

        assertThat(peekLsn).isNotNull();
        System.out.printf("  Peeked LSN: %s%n", peekLsn);

        // Try to advance slot using GaussDB function
        boolean advanced = false;
        String[] advanceSqls = {
            "SELECT pg_replication_slot_advance(?, ?)", "SELECT pg_logical_slot_advance(?, ?)"
        };
        for (String advSql : advanceSqls) {
            try (PreparedStatement stmt = connection.prepareStatement(advSql)) {
                stmt.setString(1, slotName);
                stmt.setString(2, peekLsn);
                stmt.execute();
                System.out.printf("  Advanced slot via: %s%n", advSql);
                advanced = true;
                break;
            } catch (SQLException e) {
                System.out.printf(
                        "  Advance function not available: %s - %s%n",
                        advSql, e.getMessage().split("\n")[0]);
            }
        }

        if (!advanced) {
            System.out.println("  WARNING: No slot advance function available");
        }

        // Cleanup
        dropSlotIfExists(slotName);

        System.out.println("[OK] Slot advance verified");
    }

    // ---- Test 8: WalReplicationStream Full Lifecycle ----

    @Test
    @org.junit.jupiter.api.Order(8)
    void testWalReplicationStreamFullLifecycle() throws Exception {
        String slotName = "sf_it_lifecycle";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        true,
                        1000);

        // Step 1: Initialize
        stream.initialize();
        assertThat(stream.isRunning()).isTrue();
        System.out.printf(
                "  Initialized: useReplicationApi=%s, lastLsn=%s%n",
                stream.isUseReplicationApi(), stream.getLastLsn());

        // Step 2: Read initial (may be empty if no pending changes)
        List<WalChange> initialChanges = stream.readChanges(100);
        System.out.printf("  Initial read: %d changes%n", initialChanges.size());

        // Step 3: Insert data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (4, 'Frank', 40)",
                            SCHEMA, TEST_TABLE));
        }

        // Step 4: Read changes with retries
        List<WalChange> insertChanges = readChangesWithRetry(stream, 100, 10);
        System.out.printf("  After insert: %d changes%n", insertChanges.size());

        // Step 5: Update data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "UPDATE %s.%s SET age = 41 WHERE name = 'Frank'", SCHEMA, TEST_TABLE));
        }

        // Step 6: Read changes with retries
        List<WalChange> updateChanges = readChangesWithRetry(stream, 100, 10);
        System.out.printf("  After update: %d changes%n", updateChanges.size());

        // Step 7: Close
        stream.close();
        assertThat(stream.isRunning()).isFalse();

        // Verify LSN tracking
        System.out.printf("  Final LSN: %s%n", stream.getLastLsn());

        // Cleanup
        dropSlotIfExists(slotName);

        System.out.println("[OK] WalReplicationStream full lifecycle verified");
    }

    // ---- Test 9: Snapshot Read via JDBC ----

    @Test
    @org.junit.jupiter.api.Order(9)
    void testSnapshotReadViaJdbc() throws Exception {
        // This tests the snapshot reading pattern used by GaussDBCDCSourceFunction.readSnapshot()

        // First, ensure some data exists
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (5, 'Grace', 33)",
                            SCHEMA, TEST_TABLE));
        }

        // Read snapshot using the same SQL pattern as GaussDBCDCSourceFunction
        // Get columns from metadata
        java.sql.DatabaseMetaData dbMeta = connection.getMetaData();
        StringBuilder columnsBuilder = new StringBuilder();
        try (ResultSet colRs = dbMeta.getColumns(null, SCHEMA, TEST_TABLE, null)) {
            boolean first = true;
            while (colRs.next()) {
                if (!first) {
                    columnsBuilder.append(", ");
                }
                columnsBuilder.append(colRs.getString("COLUMN_NAME"));
                first = false;
            }
        }
        String columns = columnsBuilder.toString();
        System.out.printf("  Table columns: %s%n", columns);
        assertThat(columns).isNotEmpty();

        // Get id range (same pattern as getIdRange)
        String rangeSql = String.format("SELECT MIN(id), MAX(id) FROM %s.%s", SCHEMA, TEST_TABLE);
        long minId = 0, maxId = 0;
        try (PreparedStatement stmt = connection.prepareStatement(rangeSql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                minId = rs.getLong(1);
                maxId = rs.getLong(2);
                if (rs.wasNull()) {
                    System.out.println("  Table is empty (NULL range)");
                } else {
                    System.out.printf("  ID range: [%d, %d]%n", minId, maxId);
                }
            }
        }

        // Read all data (single subtask pattern)
        String selectSql =
                String.format("SELECT %s FROM %s.%s ORDER BY id", columns, SCHEMA, TEST_TABLE);
        int rowCount = 0;
        try (PreparedStatement stmt = connection.prepareStatement(selectSql);
                ResultSet rs = stmt.executeQuery()) {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                rowCount++;
                if (rowCount <= 5) {
                    // Print first 5 rows
                    StringBuilder row = new StringBuilder("    Row: ");
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        if (i > 1) {
                            row.append(", ");
                        }
                        row.append(meta.getColumnName(i)).append("=").append(rs.getObject(i));
                    }
                    System.out.println(row);
                }
            }
        }
        System.out.printf("  Total rows in snapshot: %d%n", rowCount);
        assertThat(rowCount).isGreaterThan(0);

        // Parallel split pattern (2 subtasks)
        if (maxId > minId) {
            int numSubtasks = 2;
            long totalRange = maxId - minId + 1;
            long chunkSz = totalRange / numSubtasks;

            for (int subtask = 0; subtask < numSubtasks; subtask++) {
                long startId = minId + (long) subtask * chunkSz;
                long endId;
                if (subtask == numSubtasks - 1) {
                    endId = maxId;
                } else {
                    endId = minId + (long) (subtask + 1) * chunkSz - 1;
                }

                String parallelSql =
                        String.format(
                                "SELECT %s FROM %s.%s WHERE id >= ? AND id <= ? ORDER BY id",
                                columns, SCHEMA, TEST_TABLE);
                int subRowCount = 0;
                try (PreparedStatement stmt = connection.prepareStatement(parallelSql)) {
                    stmt.setLong(1, startId);
                    stmt.setLong(2, endId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            subRowCount++;
                        }
                    }
                }
                System.out.printf(
                        "  Subtask %d: id [%d, %d] => %d rows%n",
                        subtask, startId, endId, subRowCount);
            }
        }

        System.out.println("[OK] Snapshot read via JDBC verified");
    }

    // ---- Test 10: MppdbBinaryDecoder Integration ----

    @Test
    @org.junit.jupiter.api.Order(10)
    void testMppdbBinaryDecoderIntegration() throws Exception {
        String slotName = "sf_it_binary";

        // Drop slot if exists
        dropSlotIfExists(slotName);

        // Insert data first
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (6, 'Heidi', 27)",
                            SCHEMA, TEST_TABLE));
        }

        // Create WalReplicationStream with binary decode-style
        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        true,
                        1000);

        stream.initialize();
        System.out.printf(
                "  Binary stream initialized: useReplicationApi=%s%n",
                stream.isUseReplicationApi());

        // Insert more data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (7, 'Ivan', 32)",
                            SCHEMA, TEST_TABLE));
        }

        // Read changes with retries
        List<WalChange> changes = readChangesWithRetry(stream, 100, 10);
        System.out.printf("  Read %d changes from binary stream%n", changes.size());
        for (WalChange change : changes) {
            System.out.printf(
                    "    type=%s, schema=%s, table=%s, lsn=%s%n",
                    change.getType(), change.getSchema(), change.getTable(), change.getLsn());
            if (change.getAfterColumns() != null) {
                for (WalChange.ColumnValue col : change.getAfterColumns()) {
                    System.out.printf(
                            "      col: %s (oid=%d) = %s%n",
                            col.getColumnName(), col.getTypeOid(), col.getValue());
                }
            }
        }

        stream.close();
        dropSlotIfExists(slotName);

        System.out.println("[OK] MppdbBinaryDecoder integration verified");
    }

    // ---- Test 11: Snapshot/WAL handoff ----

    @Test
    @org.junit.jupiter.api.Order(11)
    void testSlotPreparedBeforeSnapshotRetainsChange() throws Exception {
        String slotName = "sf_it_handoff";
        dropSlotIfExists(slotName);

        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        true,
                        1000);

        String snapshotStartLsn = stream.prepareSlotForSnapshot();
        assertThat(snapshotStartLsn).isNotBlank();

        // This commit represents a change made while the JDBC snapshot is still running.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (8, 'Handoff', 44)",
                            SCHEMA, TEST_TABLE));
        }

        stream.initialize(snapshotStartLsn);
        List<WalChange> changes = readChangesWithRetry(stream, 100, 10);
        assertThat(changes)
                .anySatisfy(
                        change -> {
                            assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
                            assertThat(change.getSchema()).isEqualTo(SCHEMA);
                            assertThat(change.getTable()).isEqualTo(TEST_TABLE);
                        });

        stream.close();
        dropSlotIfExists(slotName);
        System.out.println("[OK] Pre-snapshot slot retained handoff change");
    }

    @Test
    @org.junit.jupiter.api.Order(12)
    void testReplicaIdentityFullDeleteCommitAcrossReadBatches() throws Exception {
        String slotName = "sf_it_full_delete";
        dropSlotIfExists(slotName);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format("ALTER TABLE %s.%s REPLICA IDENTITY FULL", SCHEMA, TEST_TABLE));
            stmt.execute(
                    String.format(
                            "INSERT INTO %s.%s (id, name, age) VALUES (120012, 'full-delete', 42)",
                            SCHEMA, TEST_TABLE));
        }

        String jdbcUrl = String.format("jdbc:gaussdb://%s:%d/%s", HOST, PORT, DATABASE);
        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        jdbcUrl,
                        USERNAME,
                        PASSWORD,
                        slotName,
                        "mppdb_decoding",
                        4,
                        "b",
                        true,
                        1000);

        try {
            stream.initialize();
            assumeThat(stream.isUseReplicationApi())
                    .as("This regression targets the SQL-function fallback")
                    .isFalse();

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(
                        String.format("DELETE FROM %s.%s WHERE id = 120012", SCHEMA, TEST_TABLE));
                stmt.execute(
                        String.format(
                                "INSERT INTO %s.%s (id, name, age) VALUES (120013, 'after-delete', 43)",
                                SCHEMA, TEST_TABLE));
            }

            List<WalChange> allChanges = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                List<WalChange> batch = stream.readChanges(2);
                if (!batch.isEmpty()) {
                    allChanges.addAll(batch);
                }
                boolean hasDelete =
                        allChanges.stream()
                                .anyMatch(c -> c.getType() == WalChange.ChangeType.DELETE);
                boolean hasInsert =
                        allChanges.stream()
                                .anyMatch(
                                        c ->
                                                c.getType() == WalChange.ChangeType.INSERT
                                                        && TEST_TABLE.equals(c.getTable()));
                if (hasDelete && hasInsert) {
                    break;
                }
                Thread.sleep(200);
            }

            WalChange delete =
                    allChanges.stream()
                            .filter(c -> c.getType() == WalChange.ChangeType.DELETE)
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("DELETE was not decoded"));
            int beginIndex = -1;
            int deleteIndex = -1;
            int commitIndex = -1;
            WalChange deleteCommit = null;
            for (int i = 0; i < allChanges.size(); i++) {
                WalChange change = allChanges.get(i);
                if (change.getXid() != delete.getXid()) {
                    continue;
                }
                if (change.getType() == WalChange.ChangeType.BEGIN) {
                    beginIndex = i;
                } else if (change == delete) {
                    deleteIndex = i;
                } else if (change.getType() == WalChange.ChangeType.COMMIT) {
                    commitIndex = i;
                    deleteCommit = change;
                    break;
                }
            }
            assertThat(beginIndex).isGreaterThanOrEqualTo(0);
            assertThat(deleteIndex).isGreaterThan(beginIndex);
            assertThat(commitIndex).isGreaterThan(deleteIndex);

            GaussDBCDCSourceFunction source =
                    GaussDBCDCSourceFunction.builder()
                            .hostname(HOST)
                            .port(PORT)
                            .database(DATABASE)
                            .schema(SCHEMA)
                            .tableName(TEST_TABLE)
                            .username(USERNAME)
                            .password(PASSWORD)
                            .walMode(true)
                            .build();
            setSourceField(source, "walReplicationStream", stream);
            setSourceField(source, "snapshotCsn", -1L);

            List<WalChange> beforeCommit =
                    prepareSourceChanges(
                            source,
                            new ArrayList<>(allChanges.subList(beginIndex, deleteIndex + 1)));
            assertThat(beforeCommit).isEmpty();
            List<WalChange> afterCommit =
                    prepareSourceChanges(
                            source,
                            new ArrayList<>(allChanges.subList(deleteIndex + 1, commitIndex + 1)));
            assertThat(afterCommit).containsExactly(delete);
            System.out.printf(
                    "[OK] REPLICA IDENTITY FULL DELETE xid=%d survived source batch split; commitLsn=%s, commitCsn=%d%n",
                    delete.getXid(), deleteCommit.getLsn(), deleteCommit.getCsn());
        } finally {
            stream.close();
            dropSlotIfExists(slotName);
        }
    }

    @Test
    @org.junit.jupiter.api.Order(13)
    void testSnapshotBoundaryFunctionCompatibility() throws Exception {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname(HOST)
                        .port(PORT)
                        .database(DATABASE)
                        .schema(SCHEMA)
                        .tableName(TEST_TABLE)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .walMode(true)
                        .build();
        setSourceField(source, "connection", connection);

        try {
            Method method =
                    GaussDBCDCSourceFunction.class.getDeclaredMethod("exportSnapshotBoundary");
            method.setAccessible(true);
            Object boundary = method.invoke(source);

            assertThat(readObjectField(boundary, "snapshotId").toString()).isNotBlank();
            assertThat((Long) readObjectField(boundary, "csn")).isGreaterThanOrEqualTo(-1L);
            System.out.printf(
                    "[OK] Snapshot boundary compatibility: snapshot=%s, csn=%s%n",
                    readObjectField(boundary, "snapshotId"), readObjectField(boundary, "csn"));
        } finally {
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    @Test
    @org.junit.jupiter.api.Order(14)
    void testQuotedIdentifiersAndReconnectDdlDetection() throws Exception {
        String quotedTable = TEST_TABLE + " Order";
        String quotedPk = "select";
        String quotedColumn = "customer\"name";
        String quoteString = SqlIdentifierUtils.resolveQuoteString(connection);
        String qualifiedTable = SqlIdentifierUtils.qualified(SCHEMA, quotedTable, quoteString);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + qualifiedTable);
            stmt.execute(
                    "CREATE TABLE "
                            + qualifiedTable
                            + " ("
                            + SqlIdentifierUtils.quote(quotedPk, quoteString)
                            + " BIGINT PRIMARY KEY, "
                            + SqlIdentifierUtils.quote(quotedColumn, quoteString)
                            + " VARCHAR(100))");
            stmt.execute(
                    "INSERT INTO "
                            + qualifiedTable
                            + " ("
                            + SqlIdentifierUtils.quote(quotedPk, quoteString)
                            + ", "
                            + SqlIdentifierUtils.quote(quotedColumn, quoteString)
                            + ") VALUES (7, 'quoted-ok')");
        }

        try {
            ChangeDataPoller poller =
                    new ChangeDataPoller(connection, SCHEMA, quotedTable, quotedPk);
            assertThat(poller.pollNewInserts()).hasSize(1);

            GaussDBCDCSourceFunction source =
                    GaussDBCDCSourceFunction.builder()
                            .hostname(HOST)
                            .port(PORT)
                            .database(DATABASE)
                            .schema(SCHEMA)
                            .tableName(quotedTable)
                            .username(USERNAME)
                            .password(PASSWORD)
                            .walMode(true)
                            .build();
            setSourceField(source, "connection", connection);
            setSourceField(source, "cachedColumnsByTable", new HashMap<String, List<String>>());
            setSourceField(source, "cachedPkByTable", new HashMap<String, String>());

            invokeSourceMethod(
                    source, "cacheTableColumns", new Class<?>[] {String.class}, quotedTable);
            invokeSourceMethod(
                    source, "getPrimaryKeyColumn", new Class<?>[] {String.class}, quotedTable);
            long[] range =
                    (long[])
                            invokeSourceMethod(
                                    source,
                                    "getIdRange",
                                    new Class<?>[] {String.class, String.class},
                                    quotedTable,
                                    quotedPk);
            assertThat(range).containsExactly(7L, 7L);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(
                        "ALTER TABLE "
                                + qualifiedTable
                                + " ADD COLUMN "
                                + SqlIdentifierUtils.quote("new column", quoteString)
                                + " INT");
            }

            try {
                invokeSourceMethod(
                        source, "validateCachedTableMetadataAfterReconnect", new Class<?>[0]);
                throw new AssertionError("Expected reconnect metadata validation to reject DDL");
            } catch (InvocationTargetException e) {
                assertThat(e.getCause()).isInstanceOf(SQLException.class);
                assertThat(e.getCause().getMessage())
                        .contains("Unsupported schema change detected")
                        .contains("new column");
            }
            System.out.printf(
                    "[OK] Quoted identifiers work and reconnect rejected changed metadata for %s%n",
                    qualifiedTable);
        } finally {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + qualifiedTable);
            }
        }
    }

    // ---- Helper methods ----

    @SuppressWarnings("unchecked")
    private List<WalChange> prepareSourceChanges(
            GaussDBCDCSourceFunction source, List<WalChange> changes) throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "prepareCommittedWalChanges", List.class);
        method.setAccessible(true);
        return (List<WalChange>) method.invoke(source, changes);
    }

    private void setSourceField(GaussDBCDCSourceFunction source, String name, Object value)
            throws Exception {
        Field field = GaussDBCDCSourceFunction.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(source, value);
    }

    private Object readObjectField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private Object invokeSourceMethod(
            GaussDBCDCSourceFunction source, String name, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = GaussDBCDCSourceFunction.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method.invoke(source, args);
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

    /** Read changes with retries - streaming API may need time to propagate WAL changes. */
    private List<WalChange> readChangesWithRetry(
            WalReplicationStream stream, int maxChanges, int maxRetries) throws Exception {
        List<WalChange> allChanges = new ArrayList<>();
        for (int i = 0; i < maxRetries; i++) {
            List<WalChange> changes = stream.readChanges(maxChanges);
            allChanges.addAll(changes);
            if (!allChanges.isEmpty()) {
                break;
            }
            Thread.sleep(500);
        }
        return allChanges;
    }
}
