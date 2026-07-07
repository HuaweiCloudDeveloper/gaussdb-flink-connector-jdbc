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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link ChangeDataPoller}. */
class ChangeDataPollerTest {

    private Connection mockConnection;
    private ChangeDataPoller poller;
    private ResultSetMetaData mockMeta;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = mock(Connection.class);
        // Mock DatabaseMetaData.getColumns() so getTableColumns() works
        DatabaseMetaData mockDbMeta = mock(DatabaseMetaData.class);
        ResultSet mockColRs = mock(ResultSet.class);
        when(mockColRs.next()).thenReturn(true, true, true, true, true, true, true, true, false);
        when(mockColRs.getString("COLUMN_NAME"))
                .thenReturn(
                        "id",
                        "name",
                        "gender",
                        "age",
                        "class_name",
                        "score",
                        "created_date",
                        "updated_at");
        when(mockDbMeta.getColumns(null, "public", "test_table", null)).thenReturn(mockColRs);
        when(mockConnection.getMetaData()).thenReturn(mockDbMeta);

        // Mock ResultSetMetaData for convertToRowDataDynamic
        mockMeta = mock(ResultSetMetaData.class);
        when(mockMeta.getColumnCount()).thenReturn(8);
        when(mockMeta.getColumnName(1)).thenReturn("id");
        when(mockMeta.getColumnName(2)).thenReturn("name");
        when(mockMeta.getColumnName(3)).thenReturn("gender");
        when(mockMeta.getColumnName(4)).thenReturn("age");
        when(mockMeta.getColumnName(5)).thenReturn("class_name");
        when(mockMeta.getColumnName(6)).thenReturn("score");
        when(mockMeta.getColumnName(7)).thenReturn("created_date");
        when(mockMeta.getColumnName(8)).thenReturn("updated_at");
        when(mockMeta.getColumnType(1)).thenReturn(java.sql.Types.INTEGER);
        when(mockMeta.getColumnType(2)).thenReturn(java.sql.Types.VARCHAR);
        when(mockMeta.getColumnType(3)).thenReturn(java.sql.Types.VARCHAR);
        when(mockMeta.getColumnType(4)).thenReturn(java.sql.Types.INTEGER);
        when(mockMeta.getColumnType(5)).thenReturn(java.sql.Types.VARCHAR);
        when(mockMeta.getColumnType(6)).thenReturn(java.sql.Types.DECIMAL);
        when(mockMeta.getColumnType(7)).thenReturn(java.sql.Types.TIMESTAMP);
        when(mockMeta.getColumnType(8)).thenReturn(java.sql.Types.TIMESTAMP);

        poller = new ChangeDataPoller(mockConnection, "public", "test_table", "id");
    }

    @Test
    void testConstructor() {
        assertThat(poller).isNotNull();
    }

    @Test
    void testConstructorWithNullConnection() {
        ChangeDataPoller pollerWithNull = new ChangeDataPoller(null, "public", "test_table", "id");
        assertThat(pollerWithNull).isNotNull();
    }

    @Test
    void testSetAndGetLastPolledId() {
        assertThat(poller.getLastPolledId()).isEqualTo(0L);
        poller.setLastPolledId(42L);
        assertThat(poller.getLastPolledId()).isEqualTo(42L);
        poller.setLastPolledId(100L);
        assertThat(poller.getLastPolledId()).isEqualTo(100L);
    }

    @Test
    void testPollNewInsertsEmpty() throws SQLException {
        PreparedStatement mockStmt = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockRs.next()).thenReturn(false);
        when(mockStmt.executeQuery()).thenReturn(mockRs);
        when(mockRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollNewInserts();
        assertThat(events).isEmpty();
    }

    @Test
    void testPollNewInsertsWithData() throws SQLException {
        PreparedStatement mockStmt = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockRs.next()).thenReturn(true, true, false);
        when(mockRs.getInt("id")).thenReturn(1, 2);
        when(mockRs.getLong("id")).thenReturn(1L, 2L);
        when(mockRs.getString("name")).thenReturn("Alice", "Bob");
        when(mockRs.getString("gender")).thenReturn("F", "M");
        when(mockRs.getInt("age")).thenReturn(20, 25);
        when(mockRs.getString("class_name")).thenReturn("A", "B");
        when(mockRs.getBigDecimal("score"))
                .thenReturn(new BigDecimal("95.50"), new BigDecimal("88.30"));
        when(mockRs.getTimestamp("created_date"))
                .thenReturn(new Timestamp(1000L), new Timestamp(2000L));
        when(mockStmt.executeQuery()).thenReturn(mockRs);
        when(mockRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollNewInserts();
        assertThat(events).hasSize(2);
        assertThat(events.get(0).isInsert()).isTrue();
        assertThat(events.get(1).isInsert()).isTrue();
        assertThat(poller.getLastPolledId()).isEqualTo(2L);
    }

    @Test
    void testPollUpdatesWithExistingRow() throws SQLException {
        // First, add a row to currentSnapshot by polling inserts
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(true, false);
        when(insertRs.getInt("id")).thenReturn(1);
        when(insertRs.getLong("id")).thenReturn(1L);
        when(insertRs.getString("name")).thenReturn("Alice");
        when(insertRs.getString("gender")).thenReturn("F");
        when(insertRs.getInt("age")).thenReturn(20);
        when(insertRs.getString("class_name")).thenReturn("A");
        when(insertRs.getBigDecimal("score")).thenReturn(new BigDecimal("95.50"));
        when(insertRs.getTimestamp("created_date")).thenReturn(new Timestamp(1000L));
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE id > ? ORDER BY id LIMIT 1000"))
                .thenReturn(insertStmt);

        poller.pollNewInserts();

        // Now poll for updates
        PreparedStatement updateStmt = mock(PreparedStatement.class);
        ResultSet updateRs = mock(ResultSet.class);
        when(updateRs.next()).thenReturn(true, false);
        when(updateRs.getLong("id")).thenReturn(1L);
        when(updateRs.getString("name")).thenReturn("AliceUpdated");
        when(updateRs.getString("gender")).thenReturn("F");
        when(updateRs.getInt("age")).thenReturn(21);
        when(updateRs.getInt("id")).thenReturn(1);
        when(updateRs.getString("class_name")).thenReturn("A");
        when(updateRs.getBigDecimal("score")).thenReturn(new BigDecimal("96.00"));
        when(updateRs.getTimestamp("created_date")).thenReturn(new Timestamp(1000L));
        when(updateRs.getTimestamp("updated_at")).thenReturn(new Timestamp(3000L));
        when(updateStmt.executeQuery()).thenReturn(updateRs);
        when(updateRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE updated_at > ? ORDER BY updated_at LIMIT 1000"))
                .thenReturn(updateStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollUpdates();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).isUpdate()).isTrue();
    }

    @Test
    void testPollUpdatesWithNewRow() throws SQLException {
        // No rows in snapshot, so update becomes insert
        PreparedStatement updateStmt = mock(PreparedStatement.class);
        ResultSet updateRs = mock(ResultSet.class);
        when(updateRs.next()).thenReturn(true, false);
        when(updateRs.getLong("id")).thenReturn(5L);
        when(updateRs.getString("name")).thenReturn("NewGuy");
        when(updateRs.getString("gender")).thenReturn("M");
        when(updateRs.getInt("age")).thenReturn(30);
        when(updateRs.getInt("id")).thenReturn(5);
        when(updateRs.getString("class_name")).thenReturn("C");
        when(updateRs.getBigDecimal("score")).thenReturn(new BigDecimal("70.00"));
        when(updateRs.getTimestamp("created_date")).thenReturn(new Timestamp(5000L));
        when(updateRs.getTimestamp("updated_at")).thenReturn(new Timestamp(6000L));
        when(updateStmt.executeQuery()).thenReturn(updateRs);
        when(updateRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE updated_at > ? ORDER BY updated_at LIMIT 1000"))
                .thenReturn(updateStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollUpdates();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).isInsert()).isTrue(); // Missed insert
    }

    @Test
    void testPollUpdatesWithNullTimestamp() throws SQLException {
        PreparedStatement updateStmt = mock(PreparedStatement.class);
        ResultSet updateRs = mock(ResultSet.class);
        when(updateRs.next()).thenReturn(true, false);
        when(updateRs.getLong("id")).thenReturn(5L);
        when(updateRs.getString("name")).thenReturn("NewGuy");
        when(updateRs.getString("gender")).thenReturn("M");
        when(updateRs.getInt("age")).thenReturn(30);
        when(updateRs.getInt("id")).thenReturn(5);
        when(updateRs.getString("class_name")).thenReturn("C");
        when(updateRs.getBigDecimal("score")).thenReturn(new BigDecimal("70.00"));
        when(updateRs.getTimestamp("created_date")).thenReturn(new Timestamp(5000L));
        when(updateRs.getTimestamp("updated_at")).thenReturn(null); // null timestamp
        when(updateStmt.executeQuery()).thenReturn(updateRs);
        when(updateRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE updated_at > ? ORDER BY updated_at LIMIT 1000"))
                .thenReturn(updateStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollUpdates();
        assertThat(events).hasSize(1);
    }

    @Test
    void testPollDeletesWithDeletedRow() throws SQLException {
        // First add a row to the snapshot
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(true, false);
        when(insertRs.getInt("id")).thenReturn(1);
        when(insertRs.getLong("id")).thenReturn(1L);
        when(insertRs.getString("name")).thenReturn("Alice");
        when(insertRs.getString("gender")).thenReturn("F");
        when(insertRs.getInt("age")).thenReturn(20);
        when(insertRs.getString("class_name")).thenReturn("A");
        when(insertRs.getBigDecimal("score")).thenReturn(new BigDecimal("95.50"));
        when(insertRs.getTimestamp("created_date")).thenReturn(new Timestamp(1000L));
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE id > ? ORDER BY id LIMIT 1000"))
                .thenReturn(insertStmt);

        poller.pollNewInserts();

        // Now check deletes - current IDs from DB don't include id=1
        PreparedStatement deleteStmt = mock(PreparedStatement.class);
        ResultSet deleteRs = mock(ResultSet.class);
        when(deleteRs.next()).thenReturn(false); // No rows in DB
        when(deleteStmt.executeQuery()).thenReturn(deleteRs);
        when(deleteRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement("SELECT id FROM public.test_table"))
                .thenReturn(deleteStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollDeletes();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).isDelete()).isTrue();
    }

    @Test
    void testPollDeletesNoDeletions() throws SQLException {
        // Add a row
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(true, false);
        when(insertRs.getInt("id")).thenReturn(1);
        when(insertRs.getLong("id")).thenReturn(1L);
        when(insertRs.getString("name")).thenReturn("Alice");
        when(insertRs.getString("gender")).thenReturn("F");
        when(insertRs.getInt("age")).thenReturn(20);
        when(insertRs.getString("class_name")).thenReturn("A");
        when(insertRs.getBigDecimal("score")).thenReturn(new BigDecimal("95.50"));
        when(insertRs.getTimestamp("created_date")).thenReturn(new Timestamp(1000L));
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE id > ? ORDER BY id LIMIT 1000"))
                .thenReturn(insertStmt);

        poller.pollNewInserts();

        // DB still has id=1
        PreparedStatement deleteStmt = mock(PreparedStatement.class);
        ResultSet deleteRs = mock(ResultSet.class);
        when(deleteRs.next()).thenReturn(true, false);
        when(deleteRs.getLong("id")).thenReturn(1L);
        when(deleteStmt.executeQuery()).thenReturn(deleteRs);
        when(deleteRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement("SELECT id FROM public.test_table"))
                .thenReturn(deleteStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollDeletes();
        assertThat(events).isEmpty();
    }

    @Test
    void testPollAllChanges() throws SQLException {
        // Setup inserts
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(false);
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE id > ? ORDER BY id LIMIT 1000"))
                .thenReturn(insertStmt);

        // Setup updates
        PreparedStatement updateStmt = mock(PreparedStatement.class);
        ResultSet updateRs = mock(ResultSet.class);
        when(updateRs.next()).thenReturn(false);
        when(updateStmt.executeQuery()).thenReturn(updateRs);
        when(updateRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE updated_at > ? ORDER BY updated_at LIMIT 1000"))
                .thenReturn(updateStmt);

        // Setup deletes
        PreparedStatement deleteStmt = mock(PreparedStatement.class);
        ResultSet deleteRs = mock(ResultSet.class);
        when(deleteRs.next()).thenReturn(false);
        when(deleteStmt.executeQuery()).thenReturn(deleteRs);
        when(deleteRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement("SELECT id FROM public.test_table"))
                .thenReturn(deleteStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollAllChanges();
        assertThat(events).isEmpty();
    }

    @Test
    void testPollAllChangesWithUpdateFailure() throws SQLException {
        // Setup inserts
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(false);
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE id > ? ORDER BY id LIMIT 1000"))
                .thenReturn(insertStmt);

        // Setup updates - throws SQLException
        PreparedStatement updateStmt = mock(PreparedStatement.class);
        when(updateStmt.executeQuery()).thenThrow(new SQLException("column updated_at not found"));
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table WHERE updated_at > ? ORDER BY updated_at LIMIT 1000"))
                .thenReturn(updateStmt);

        // Setup deletes
        PreparedStatement deleteStmt = mock(PreparedStatement.class);
        ResultSet deleteRs = mock(ResultSet.class);
        when(deleteRs.next()).thenReturn(false);
        when(deleteStmt.executeQuery()).thenReturn(deleteRs);
        when(deleteRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement("SELECT id FROM public.test_table"))
                .thenReturn(deleteStmt);

        // Should not throw - update failure is caught
        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollAllChanges();
        assertThat(events).isEmpty(); // inserts empty, updates failed, deletes empty
    }

    @Test
    void testLoadSnapshot() throws SQLException {
        PreparedStatement snapshotStmt = mock(PreparedStatement.class);
        ResultSet snapshotRs = mock(ResultSet.class);
        when(snapshotRs.next()).thenReturn(true, true, false);
        when(snapshotRs.getLong("id")).thenReturn(1L, 5L);
        when(snapshotRs.getInt("id")).thenReturn(1, 5);
        when(snapshotRs.getString("name")).thenReturn("Alice", "Bob");
        when(snapshotRs.getString("gender")).thenReturn("F", "M");
        when(snapshotRs.getInt("age")).thenReturn(20, 25);
        when(snapshotRs.getString("class_name")).thenReturn("A", "B");
        when(snapshotRs.getBigDecimal("score"))
                .thenReturn(new BigDecimal("95.50"), new BigDecimal("88.30"));
        when(snapshotRs.getTimestamp("created_date"))
                .thenReturn(new Timestamp(1000L), new Timestamp(2000L));
        when(snapshotStmt.executeQuery()).thenReturn(snapshotRs);
        when(snapshotRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table"))
                .thenReturn(snapshotStmt);

        poller.loadSnapshot();
        assertThat(poller.getLastPolledId()).isEqualTo(5L);
    }

    @Test
    void testLoadSnapshotEmpty() throws SQLException {
        PreparedStatement snapshotStmt = mock(PreparedStatement.class);
        ResultSet snapshotRs = mock(ResultSet.class);
        when(snapshotRs.next()).thenReturn(false);
        when(snapshotStmt.executeQuery()).thenReturn(snapshotRs);
        when(snapshotRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM public.test_table"))
                .thenReturn(snapshotStmt);

        poller.setLastPolledId(10L);
        poller.loadSnapshot();
        assertThat(poller.getLastPolledId()).isEqualTo(10L); // unchanged
    }

    @Test
    void testConvertToRowDataWithNullCreatedDate() throws SQLException {
        PreparedStatement insertStmt = mock(PreparedStatement.class);
        ResultSet insertRs = mock(ResultSet.class);
        when(insertRs.next()).thenReturn(true, false);
        when(insertRs.getInt("id")).thenReturn(1);
        when(insertRs.getLong("id")).thenReturn(1L);
        when(insertRs.getString("name")).thenReturn("Alice");
        when(insertRs.getString("gender")).thenReturn("F");
        when(insertRs.getInt("age")).thenReturn(20);
        when(insertRs.getString("class_name")).thenReturn("A");
        when(insertRs.getBigDecimal("score")).thenReturn(new BigDecimal("95.50"));
        when(insertRs.getTimestamp("created_date")).thenReturn(null);
        when(insertStmt.executeQuery()).thenReturn(insertRs);
        when(insertRs.getMetaData()).thenReturn(mockMeta);
        when(mockConnection.prepareStatement(anyString())).thenReturn(insertStmt);

        java.util.List<ChangeEvent<org.apache.flink.table.data.RowData>> events =
                poller.pollNewInserts();
        assertThat(events).hasSize(1);
    }
}
