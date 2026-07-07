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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBSourceReader}. */
class GaussDBSourceReaderTest {

    private GaussDBSourceReader reader;
    private SourceReaderContext context;

    @BeforeEach
    void setUp() {
        context = mock(SourceReaderContext.class);
        reader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "pgoutput",
                        1000,
                        false,
                        "mppdb_decoding",
                        1,
                        "b",
                        false,
                        "prefer");
    }

    @Test
    void testIsAvailable() {
        assertThat(reader.isAvailable()).isCompletedWithValue(null);
    }

    @Test
    void testPollNextWhenNotRunning() throws Exception {
        reader.close();
        InputStatus status = reader.pollNext(mock(ReaderOutput.class));
        assertThat(status).isEqualTo(InputStatus.END_OF_INPUT);
    }

    @Test
    void testPollNextWhenNoSplitAndSnapshotNotFinished() throws Exception {
        // No split assigned, snapshot not finished -> readAllData will be called
        // readAllData fails because connection is null
        assertThatThrownBy(() -> reader.pollNext(mock(ReaderOutput.class)))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testPollNextWhenNoSplitAndSnapshotFinished() throws Exception {
        setField(reader, "snapshotFinished", true);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    }

    @Test
    void testPollNextReadAllData() throws Exception {
        // Setup mock connection
        Connection conn = setupMockConnectionForReadAll();
        setField(reader, "connection", conn);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);
        assertThat(getField(reader, "snapshotFinished", Boolean.class)).isTrue();
    }

    @Test
    void testPollSnapshotWithSnapshotSplit() throws Exception {
        Connection conn = setupMockConnectionForSnapshot();
        setField(reader, "connection", conn);

        GaussDBSplit snapshotSplit = new GaussDBSplit("snapshot-0", "test_table", 1L, 100L);
        reader.addSplits(Collections.singletonList(snapshotSplit));

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(getField(reader, "snapshotFinished", Boolean.class)).isTrue();
        verify(mockPoller).loadSnapshot();
    }

    @Test
    void testPollSnapshotWithNullPoller() throws Exception {
        Connection conn = setupMockConnectionForSnapshot();
        setField(reader, "connection", conn);

        GaussDBSplit snapshotSplit = new GaussDBSplit("snapshot-0", "test_table", 1L, 100L);
        reader.addSplits(Collections.singletonList(snapshotSplit));

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    }

    @Test
    void testPollChangesWithInsert() throws Exception {
        Connection conn = mock(Connection.class);
        setField(reader, "connection", conn);

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);
        setField(reader, "snapshotFinished", true);

        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        GenericRowData row = new GenericRowData(1);
        row.setField(0, 1);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        events.add(ChangeEvent.insert("test_table", row, System.currentTimeMillis()));
        when(mockPoller.pollAllChanges()).thenReturn(events);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);
        verify(output).collect(any(RowData.class));
    }

    @Test
    void testPollChangesWithUpdate() throws Exception {
        Connection conn = mock(Connection.class);
        setField(reader, "connection", conn);

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);
        setField(reader, "snapshotFinished", true);

        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        GenericRowData beforeRow = new GenericRowData(1);
        beforeRow.setField(0, 1);
        GenericRowData afterRow = new GenericRowData(1);
        afterRow.setField(0, 2);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        events.add(
                ChangeEvent.update("test_table", beforeRow, afterRow, System.currentTimeMillis()));
        when(mockPoller.pollAllChanges()).thenReturn(events);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);
    }

    @Test
    void testPollChangesWithDelete() throws Exception {
        Connection conn = mock(Connection.class);
        setField(reader, "connection", conn);

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);
        setField(reader, "snapshotFinished", true);

        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        GenericRowData beforeRow = new GenericRowData(1);
        beforeRow.setField(0, 1);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        events.add(ChangeEvent.delete("test_table", beforeRow, System.currentTimeMillis()));
        when(mockPoller.pollAllChanges()).thenReturn(events);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);
    }

    @Test
    void testPollChangesWithSnapshot() throws Exception {
        Connection conn = mock(Connection.class);
        setField(reader, "connection", conn);

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);
        setField(reader, "snapshotFinished", true);

        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        GenericRowData row = new GenericRowData(1);
        row.setField(0, 1);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        events.add(ChangeEvent.snapshot("test_table", row, System.currentTimeMillis()));
        when(mockPoller.pollAllChanges()).thenReturn(events);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.MORE_AVAILABLE);
        verify(output).collect(any(RowData.class));
    }

    @Test
    void testPollChangesEmpty() throws Exception {
        Connection conn = mock(Connection.class);
        setField(reader, "connection", conn);

        ChangeDataPoller mockPoller = mock(ChangeDataPoller.class);
        setField(reader, "changeDataPoller", mockPoller);
        setField(reader, "snapshotFinished", true);

        // Use stream split with snapshotFinished=true -> pollChanges
        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        when(mockPoller.pollAllChanges()).thenReturn(Collections.emptyList());

        // pollIntervalMs is 0 to avoid sleep
        setField(reader, "pollIntervalMs", 0);

        ReaderOutput<RowData> output = mock(ReaderOutput.class);
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    }

    @Test
    void testAddSplits() {
        GaussDBSplit split = new GaussDBSplit("split-0", "test_table", 1L, 100L);
        reader.addSplits(Collections.singletonList(split));
        List<GaussDBSplit> state = reader.snapshotState(1);
        assertThat(state).hasSize(1);
        assertThat(state.get(0).splitId()).isEqualTo("split-0");
    }

    @Test
    void testAddSplitsEmptyList() {
        reader.addSplits(Collections.emptyList());
        List<GaussDBSplit> state = reader.snapshotState(1);
        assertThat(state).isEmpty();
    }

    @Test
    void testSnapshotStateNoSplit() {
        List<GaussDBSplit> state = reader.snapshotState(1);
        assertThat(state).isEmpty();
    }

    @Test
    void testSnapshotStateWithSplit() {
        GaussDBSplit split = new GaussDBSplit("split-0", "test_table", 1L, 100L);
        reader.addSplits(Collections.singletonList(split));
        List<GaussDBSplit> state = reader.snapshotState(1);
        assertThat(state).hasSize(1);
    }

    @Test
    void testNotifyNoMoreSplits() {
        reader.notifyNoMoreSplits();
    }

    @Test
    void testCloseWithoutConnection() throws Exception {
        reader.close();
    }

    @Test
    void testStartFailsWithoutDriver() {
        org.assertj.core.api.ThrowableAssert.ThrowingCallable callable = () -> reader.start();
        assertThatThrownBy(callable).isInstanceOfAny(RuntimeException.class, Error.class);
    }

    @Test
    void testCloseWithMockConnection() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.isClosed()).thenReturn(false);
        setField(reader, "connection", conn);

        reader.close();
        verify(conn).close();
    }

    @Test
    void testCloseWithAlreadyClosedConnection() throws Exception {
        Connection conn = mock(Connection.class);
        when(conn.isClosed()).thenReturn(true);
        setField(reader, "connection", conn);

        reader.close();
        verify(conn, never()).close();
    }

    @Test
    void testPollNextWithStreamSplit() throws Exception {
        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "test_table", "flink_cdc_slot", null);
        reader.addSplits(Collections.singletonList(streamSplit));

        // In polling mode (walMode=false) with a stream split and no initialized poller,
        // pollNext returns NOTHING_AVAILABLE rather than throwing
        InputStatus status = reader.pollNext(mock(ReaderOutput.class));
        assertThat(status).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    }

    @Test
    void testConvertToRowDataDynamicInteger() throws Exception {
        // Test INTEGER type
        testConvertToRowDataDynamicType(Types.INTEGER, 42, null, null);
    }

    @Test
    void testConvertToRowDataDynamicSmallInt() throws Exception {
        testConvertToRowDataDynamicType(Types.SMALLINT, 10, null, null);
    }

    @Test
    void testConvertToRowDataDynamicTinyInt() throws Exception {
        testConvertToRowDataDynamicType(Types.TINYINT, 5, null, null);
    }

    @Test
    void testConvertToRowDataDynamicBigInt() throws Exception {
        testConvertToRowDataDynamicType(Types.BIGINT, 12345L, null, null);
    }

    @Test
    void testConvertToRowDataDynamicVarchar() throws Exception {
        testConvertToRowDataDynamicType(
                Types.VARCHAR, "hello", StringData.fromString("hello"), null);
    }

    @Test
    void testConvertToRowDataDynamicChar() throws Exception {
        testConvertToRowDataDynamicType(Types.CHAR, "a", StringData.fromString("a"), null);
    }

    @Test
    void testConvertToRowDataDynamicDecimal() throws Exception {
        testConvertToRowDataDynamicType(Types.DECIMAL, new BigDecimal("3.14"), null, null);
    }

    @Test
    void testConvertToRowDataDynamicNumeric() throws Exception {
        testConvertToRowDataDynamicType(Types.NUMERIC, new BigDecimal("100.5"), null, null);
    }

    @Test
    void testConvertToRowDataDynamicTimestamp() throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        testConvertToRowDataDynamicType(Types.TIMESTAMP, ts, TimestampData.fromTimestamp(ts), null);
    }

    @Test
    void testConvertToRowDataDynamicBoolean() throws Exception {
        testConvertToRowDataDynamicType(Types.BOOLEAN, true, null, null);
    }

    @Test
    void testConvertToRowDataDynamicDouble() throws Exception {
        testConvertToRowDataDynamicType(Types.DOUBLE, 3.14, null, null);
    }

    @Test
    void testConvertToRowDataDynamicFloat() throws Exception {
        testConvertToRowDataDynamicType(Types.FLOAT, 2.5f, null, null);
    }

    @Test
    void testConvertToRowDataDynamicDate() throws Exception {
        java.sql.Date date = java.sql.Date.valueOf("2024-01-15");
        testConvertToRowDataDynamicType(
                Types.DATE, date, (int) date.toLocalDate().toEpochDay(), null);
    }

    @Test
    void testConvertToRowDataDynamicDefault() throws Exception {
        // Unknown type falls back to string
        testConvertToRowDataDynamicType(
                Types.OTHER, "fallback", StringData.fromString("fallback"), null);
    }

    @Test
    void testConvertToRowDataDynamicNullVarchar() throws Exception {
        testConvertToRowDataDynamicTypeNull(Types.VARCHAR);
    }

    @Test
    void testConvertToRowDataDynamicNullInt() throws Exception {
        testConvertToRowDataDynamicTypeNull(Types.INTEGER);
    }

    @Test
    void testGetTableColumnsNoColumns() throws Exception {
        Connection conn = mock(Connection.class);
        DatabaseMetaData dbMeta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(columnsRs.next()).thenReturn(false);
        when(dbMeta.getColumns(any(), anyString(), anyString(), any())).thenReturn(columnsRs);
        when(conn.getMetaData()).thenReturn(dbMeta);
        setField(reader, "connection", conn);

        assertThatThrownBy(
                        () -> {
                            java.lang.reflect.Method method =
                                    GaussDBSourceReader.class.getDeclaredMethod("getTableColumns");
                            method.setAccessible(true);
                            method.invoke(reader);
                        })
                .hasCauseInstanceOf(SQLException.class);
    }

    // ---- Helper methods ----

    private void testConvertToRowDataDynamicType(
            int sqlType, Object rawValue, Object expectedFieldValue, Object ignored)
            throws Exception {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(sqlType);

        setupResultSetByType(rs, sqlType, rawValue);
        when(rs.wasNull()).thenReturn(false);

        java.lang.reflect.Method method =
                GaussDBSourceReader.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);
        RowData row = (RowData) method.invoke(reader, rs, meta);

        assertThat(row).isNotNull();
        assertThat(row instanceof GenericRowData).isTrue();
        GenericRowData genericRow = (GenericRowData) row;
        Object actualValue = genericRow.getField(0);
        if (expectedFieldValue != null) {
            assertThat(actualValue).isEqualTo(expectedFieldValue);
        }
    }

    private void testConvertToRowDataDynamicTypeNull(int sqlType) throws Exception {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(sqlType);

        setupResultSetNullByType(rs, sqlType);
        when(rs.wasNull()).thenReturn(true);

        java.lang.reflect.Method method =
                GaussDBSourceReader.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);
        RowData row = (RowData) method.invoke(reader, rs, meta);

        assertThat(row).isNotNull();
        assertThat(row instanceof GenericRowData).isTrue();
        GenericRowData genericRow = (GenericRowData) row;
        assertThat(genericRow.getField(0)).isNull();
    }

    private void setupResultSetByType(ResultSet rs, int sqlType, Object value) throws SQLException {
        switch (sqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                when(rs.getInt(1)).thenReturn((Integer) value);
                break;
            case Types.BIGINT:
                when(rs.getLong(1)).thenReturn((Long) value);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NVARCHAR:
            case Types.OTHER:
                when(rs.getString(1)).thenReturn((String) value);
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                when(rs.getBigDecimal(1)).thenReturn((BigDecimal) value);
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                when(rs.getTimestamp(1)).thenReturn((Timestamp) value);
                break;
            case Types.DATE:
                when(rs.getDate(1)).thenReturn((java.sql.Date) value);
                break;
            case Types.BOOLEAN:
                when(rs.getBoolean(1)).thenReturn((Boolean) value);
                break;
            case Types.DOUBLE:
            case Types.FLOAT:
                when(rs.getDouble(1)).thenReturn(((Number) value).doubleValue());
                break;
        }
    }

    private void setupResultSetNullByType(ResultSet rs, int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                when(rs.getInt(1)).thenReturn(0);
                break;
            case Types.BIGINT:
                when(rs.getLong(1)).thenReturn(0L);
                break;
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NVARCHAR:
                when(rs.getString(1)).thenReturn(null);
                break;
            case Types.BOOLEAN:
                when(rs.getBoolean(1)).thenReturn(false);
                break;
            case Types.DOUBLE:
            case Types.FLOAT:
                when(rs.getDouble(1)).thenReturn(0.0);
                break;
            default:
                when(rs.getString(1)).thenReturn(null);
                break;
        }
    }

    private Connection setupMockConnectionForReadAll() throws SQLException {
        Connection conn = mock(Connection.class);
        DatabaseMetaData dbMeta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(columnsRs.next()).thenReturn(true, false);
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id");
        when(dbMeta.getColumns(any(), anyString(), anyString(), any())).thenReturn(columnsRs);
        when(conn.getMetaData()).thenReturn(dbMeta);

        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
        when(rsMeta.getColumnCount()).thenReturn(1);
        when(rsMeta.getColumnType(1)).thenReturn(Types.INTEGER);
        when(rs.getMetaData()).thenReturn(rsMeta);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt(1)).thenReturn(1);
        when(rs.wasNull()).thenReturn(false);
        when(stmt.executeQuery()).thenReturn(rs);
        when(conn.prepareStatement(anyString())).thenReturn(stmt);

        return conn;
    }

    private Connection setupMockConnectionForSnapshot() throws SQLException {
        Connection conn = mock(Connection.class);
        DatabaseMetaData dbMeta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(columnsRs.next()).thenReturn(true, false);
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id");
        when(dbMeta.getColumns(any(), anyString(), anyString(), any())).thenReturn(columnsRs);
        when(conn.getMetaData()).thenReturn(dbMeta);

        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
        when(rsMeta.getColumnCount()).thenReturn(1);
        when(rsMeta.getColumnType(1)).thenReturn(Types.INTEGER);
        when(rs.getMetaData()).thenReturn(rsMeta);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt(1)).thenReturn(1);
        when(rs.wasNull()).thenReturn(false);
        when(stmt.executeQuery()).thenReturn(rs);
        when(conn.prepareStatement(anyString())).thenReturn(stmt);

        return conn;
    }

    private static void setField(Object obj, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object obj, String fieldName, Class<T> type) throws Exception {
        java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    // ---- WAL mode tests ----

    @Test
    void testConvertWalColumnsToRowData() throws Exception {
        // Create a WAL mode reader
        GaussDBSourceReader walReader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "mppdb_decoding",
                        1000,
                        true,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        "prefer");

        // Test convertWalColumnsToRowData via reflection
        java.lang.reflect.Method method =
                GaussDBSourceReader.class.getDeclaredMethod(
                        "convertWalColumnsToRowData", List.class);
        method.setAccessible(true);

        // Create column values
        List<WalChange.ColumnValue> columns =
                new ArrayList<>(
                        java.util.Arrays.asList(
                                new WalChange.ColumnValue("id", 23, "1", false),
                                new WalChange.ColumnValue("name", 1043, "Alice", false),
                                new WalChange.ColumnValue("age", 23, "25", false),
                                new WalChange.ColumnValue("salary", 701, "5000.50", false),
                                new WalChange.ColumnValue("active", 16, "true", false),
                                new WalChange.ColumnValue("score", 700, "95.5", false),
                                new WalChange.ColumnValue("data", 25, "hello", false),
                                new WalChange.ColumnValue("ts", 1114, "2024-01-15 10:30:00", false),
                                new WalChange.ColumnValue("dt", 1082, "2024-01-15", false),
                                new WalChange.ColumnValue("big_id", 20, "123456789", false),
                                new WalChange.ColumnValue("amount", 1700, "99.99", false),
                                new WalChange.ColumnValue("code", 21, "5", false)));

        RowData row = (RowData) method.invoke(walReader, columns);
        assertThat(row).isNotNull();
        assertThat(row instanceof GenericRowData).isTrue();
    }

    @Test
    void testConvertWalColumnsToRowDataNull() throws Exception {
        GaussDBSourceReader walReader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "mppdb_decoding",
                        1000,
                        true,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        "prefer");

        java.lang.reflect.Method method =
                GaussDBSourceReader.class.getDeclaredMethod(
                        "convertWalColumnsToRowData", List.class);
        method.setAccessible(true);

        // Test null columns
        RowData row = (RowData) method.invoke(walReader, (List<?>) null);
        assertThat(row).isNull();

        // Test empty columns
        row = (RowData) method.invoke(walReader, java.util.Collections.emptyList());
        assertThat(row).isNull();

        // Test column with null value
        List<WalChange.ColumnValue> columns =
                new ArrayList<>(
                        java.util.Arrays.asList(
                                new WalChange.ColumnValue("id", 23, null, true),
                                new WalChange.ColumnValue("name", 1043, "test", false)));
        row = (RowData) method.invoke(walReader, columns);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertColumnValueByOid() throws Exception {
        GaussDBSourceReader walReader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "mppdb_decoding",
                        1000,
                        true,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        "prefer");

        java.lang.reflect.Method method =
                GaussDBSourceReader.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        // int4 (23)
        assertThat(method.invoke(walReader, 23, "42")).isEqualTo(42);
        // int8 (20)
        assertThat(method.invoke(walReader, 20, "123456789012")).isEqualTo(123456789012L);
        // int2 (21)
        assertThat(method.invoke(walReader, 21, "5")).isEqualTo(5);
        // bool (16)
        assertThat(method.invoke(walReader, 16, "true")).isEqualTo(true);
        // float4 (700)
        assertThat(method.invoke(walReader, 700, "3.14")).isEqualTo(3.14f);
        // float8 (701)
        assertThat(method.invoke(walReader, 701, "3.14159")).isEqualTo(3.14159);
        // varchar (1043)
        assertThat(method.invoke(walReader, 1043, "hello"))
                .isEqualTo(StringData.fromString("hello"));
        // text (25)
        assertThat(method.invoke(walReader, 25, "world")).isEqualTo(StringData.fromString("world"));
        // name (19)
        assertThat(method.invoke(walReader, 19, "col1")).isEqualTo(StringData.fromString("col1"));
        // null value
        assertThat(method.invoke(walReader, 23, null)).isNull();
        // unknown OID -> fallback to string
        assertThat(method.invoke(walReader, 9999, "unknown"))
                .isEqualTo(StringData.fromString("unknown"));
    }

    @Test
    void testWalModeConstructor() {
        GaussDBSourceReader walReader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "mppdb_decoding",
                        1000,
                        true,
                        "mppdb_decoding",
                        4,
                        "b",
                        true,
                        "prefer");
        assertThat(walReader).isNotNull();
    }

    @Test
    void testCloseWithWalReplicationStream() throws Exception {
        GaussDBSourceReader walReader =
                new GaussDBSourceReader(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "pass",
                        "slot1",
                        "mppdb_decoding",
                        1000,
                        true,
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        "prefer");

        // Set a mock WalReplicationStream
        Connection conn = mock(Connection.class);
        setField(walReader, "connection", conn);
        when(conn.isClosed()).thenReturn(false);

        WalReplicationStream mockStream = mock(WalReplicationStream.class);
        setField(walReader, "walReplicationStream", mockStream);

        walReader.close();
        org.mockito.Mockito.verify(mockStream).close();
        org.mockito.Mockito.verify(conn).close();
    }
}
