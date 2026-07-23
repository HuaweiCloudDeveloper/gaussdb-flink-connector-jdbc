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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Additional tests for {@link WalReplicationStream} to improve coverage. */
class WalReplicationStreamAdditionalTest {

    private Connection connection;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
    }

    private WalReplicationStream createStream(int parallelDecodeNum) {
        return new WalReplicationStream(
                connection,
                "jdbc:gaussdb://localhost:8000/test",
                "root",
                "pass",
                "test_slot",
                "mppdb_decoding",
                parallelDecodeNum,
                "b",
                false,
                1000);
    }

    // ---- buildReplicationUrl tests ----

    @Test
    void testBuildReplicationUrlWithoutParams() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("buildReplicationUrl", String.class);
        method.setAccessible(true);

        String result =
                (String)
                        method.invoke(
                                stream, "jdbc:gaussdb://localhost:8000/test?compatibleMode=mysql");
        assertThat(result).contains("replication=database");
        assertThat(result).contains("preferQueryMode=simple");
        assertThat(result).contains("assumeMinServerVersion=9.4");
        assertThat(result).contains("compatibleMode=mysql");
    }

    @Test
    void testBuildReplicationUrlWithoutQuestionMark() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("buildReplicationUrl", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(stream, "jdbc:gaussdb://localhost:8000/test");
        assertThat(result).contains("replication=database");
        assertThat(result).contains("preferQueryMode=simple");
    }

    @Test
    void testBuildReplicationUrlWithExistingReplicationParam() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("buildReplicationUrl", String.class);
        method.setAccessible(true);

        String result =
                (String)
                        method.invoke(
                                stream,
                                "jdbc:gaussdb://localhost:8000/test?replication=database&compatibleMode=mysql");
        // Should not duplicate replication param
        assertThat(result).contains("replication=database");
        assertThat(result).contains("preferQueryMode=simple");
    }

    @Test
    void testBuildReplicationUrlWithAllExistingParams() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("buildReplicationUrl", String.class);
        method.setAccessible(true);

        String result =
                (String)
                        method.invoke(
                                stream,
                                "jdbc:gaussdb://localhost:8000/test?replication=database&preferQueryMode=simple&assumeMinServerVersion=9.4");
        // Should not duplicate any params
        assertThat(result).contains("replication=database");
        assertThat(result).contains("preferQueryMode=simple");
        assertThat(result).contains("assumeMinServerVersion=9.4");
    }

    // ---- parseJsonBatch tests ----

    @Test
    void testParseJsonBatchWithValidJson() throws Exception {
        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/1");
        Method method =
                WalReplicationStream.class.getDeclaredMethod("parseJsonBatch", byte[].class);
        method.setAccessible(true);

        String json =
                "{\"table_name\":\"public.test\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"1\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}";
        byte[] data = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        List<WalChange> changes = (List<WalChange>) method.invoke(stream, (Object) data);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getType()).isEqualTo(WalChange.ChangeType.INSERT);
    }

    @Test
    void testParseJsonBatchWithMultipleJsonObjects() throws Exception {
        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/1");
        Method method =
                WalReplicationStream.class.getDeclaredMethod("parseJsonBatch", byte[].class);
        method.setAccessible(true);

        String json =
                "header{\"table_name\":\"public.t1\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"1\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}"
                        + "separator{\"table_name\":\"public.t2\",\"op_type\":\"DELETE\","
                        + "\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"5\"],"
                        + "\"old_keys_name\":[\"id\"],\"old_keys_type\":[\"integer\"],"
                        + "\"old_keys_val\":[\"5\"]}";
        byte[] data = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        List<WalChange> changes = (List<WalChange>) method.invoke(stream, (Object) data);
        assertThat(changes).hasSize(2);
    }

    @Test
    void testParseJsonBatchWithEmptyData() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("parseJsonBatch", byte[].class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<WalChange> changes = (List<WalChange>) method.invoke(stream, (Object) new byte[0]);
        assertThat(changes).isEmpty();
    }

    @Test
    void testParseJsonBatchWithNoJsonBraces() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("parseJsonBatch", byte[].class);
        method.setAccessible(true);

        byte[] data = "no json here".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        List<WalChange> changes = (List<WalChange>) method.invoke(stream, (Object) data);
        assertThat(changes).isEmpty();
    }

    @Test
    void testParseJsonBatchWithUnclosedBrace() throws Exception {
        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/1");
        Method method =
                WalReplicationStream.class.getDeclaredMethod("parseJsonBatch", byte[].class);
        method.setAccessible(true);

        byte[] data = "{\"unclosed".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        @SuppressWarnings("unchecked")
        List<WalChange> changes = (List<WalChange>) method.invoke(stream, (Object) data);
        assertThat(changes).isEmpty();
    }

    // ---- advanceSlot tests ----

    @Test
    void testAdvanceSlotWithNullLsn() throws Exception {
        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", null);
        Method method = WalReplicationStream.class.getDeclaredMethod("advanceSlot");
        method.setAccessible(true);

        method.invoke(stream);
        // Should return immediately without SQL
        verify(connection, never()).prepareStatement(anyString());
    }

    @Test
    void testAdvanceSlotWithGaussdbFunction() throws Exception {
        PreparedStatement advanceStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT pg_replication_slot_advance(?, ?)"))
                .thenReturn(advanceStmt);

        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/15A3B");
        Method method = WalReplicationStream.class.getDeclaredMethod("advanceSlot");
        method.setAccessible(true);

        method.invoke(stream);
        verify(advanceStmt).setString(1, "test_slot");
        verify(advanceStmt).setString(2, "0/15A3B");
        verify(advanceStmt).execute();
    }

    @Test
    void testAdvanceSlotFallsBackToPostgresFunction() throws Exception {
        PreparedStatement gaussdbStmt = mock(PreparedStatement.class);
        when(gaussdbStmt.execute()).thenThrow(new SQLException("function not found"));
        when(connection.prepareStatement("SELECT pg_replication_slot_advance(?, ?)"))
                .thenReturn(gaussdbStmt);

        PreparedStatement postgresStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT pg_logical_slot_advance(?, ?)"))
                .thenReturn(postgresStmt);

        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/15A3B");
        Method method = WalReplicationStream.class.getDeclaredMethod("advanceSlot");
        method.setAccessible(true);

        method.invoke(stream);
        verify(postgresStmt).execute();
    }

    @Test
    void testAdvanceSlotAllFunctionsFail() throws Exception {
        PreparedStatement gaussdbStmt = mock(PreparedStatement.class);
        when(gaussdbStmt.execute()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_replication_slot_advance(?, ?)"))
                .thenReturn(gaussdbStmt);

        PreparedStatement postgresStmt = mock(PreparedStatement.class);
        when(postgresStmt.execute()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_logical_slot_advance(?, ?)"))
                .thenReturn(postgresStmt);

        WalReplicationStream stream = createStream(1);
        setField(stream, "lastLsn", "0/15A3B");
        Method method = WalReplicationStream.class.getDeclaredMethod("advanceSlot");
        method.setAccessible(true);

        assertThatThrownBy(() -> method.invoke(stream))
                .hasCauseInstanceOf(SQLException.class)
                .hasRootCauseMessage("not found");
    }

    @Test
    void testFailedCheckpointAcknowledgeRetainsSqlPeekPrefix() throws Exception {
        PreparedStatement gaussdbStmt = mock(PreparedStatement.class);
        when(gaussdbStmt.execute()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_replication_slot_advance(?, ?)"))
                .thenReturn(gaussdbStmt);
        PreparedStatement postgresStmt = mock(PreparedStatement.class);
        when(postgresStmt.execute()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_logical_slot_advance(?, ?)"))
                .thenReturn(postgresStmt);

        WalReplicationStream stream = createStream(1);
        Field pendingField = WalReplicationStream.class.getDeclaredField("sqlUnacknowledgedLsns");
        pendingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> pending = (List<String>) pendingField.get(stream);
        pending.add("0/15A3B");

        assertThatThrownBy(() -> stream.acknowledgeLsn("0/15A3B")).isInstanceOf(SQLException.class);
        assertThat(pending).containsExactly("0/15A3B");
    }

    // ---- getCurrentLsn tests ----

    @Test
    void testGetCurrentLsnWithGaussdbFunction() throws Exception {
        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn("0/1A2B3C");
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_xlog_location()")).thenReturn(lsnStmt);

        WalReplicationStream stream = createStream(1);
        Method method = WalReplicationStream.class.getDeclaredMethod("getCurrentLsn");
        method.setAccessible(true);

        String result = (String) method.invoke(stream);
        assertThat(result).isEqualTo("0/1A2B3C");
    }

    @Test
    void testGetCurrentLsnFallsBackToPostgresFunction() throws Exception {
        PreparedStatement gaussdbStmt = mock(PreparedStatement.class);
        when(gaussdbStmt.executeQuery()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_current_xlog_location()"))
                .thenReturn(gaussdbStmt);

        PreparedStatement postgresStmt = mock(PreparedStatement.class);
        ResultSet postgresRs = mock(ResultSet.class);
        when(postgresRs.next()).thenReturn(true);
        when(postgresRs.getString(1)).thenReturn("0/1A2B3C");
        when(postgresStmt.executeQuery()).thenReturn(postgresRs);
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(postgresStmt);

        WalReplicationStream stream = createStream(1);
        Method method = WalReplicationStream.class.getDeclaredMethod("getCurrentLsn");
        method.setAccessible(true);

        String result = (String) method.invoke(stream);
        assertThat(result).isEqualTo("0/1A2B3C");
    }

    @Test
    void testGetCurrentLsnReturnsNullResult() throws Exception {
        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn(null);
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_xlog_location()")).thenReturn(lsnStmt);

        PreparedStatement fallbackStmt = mock(PreparedStatement.class);
        ResultSet fallbackRs = mock(ResultSet.class);
        when(fallbackRs.next()).thenReturn(false);
        when(fallbackStmt.executeQuery()).thenReturn(fallbackRs);
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(fallbackStmt);

        WalReplicationStream stream = createStream(1);
        Method method = WalReplicationStream.class.getDeclaredMethod("getCurrentLsn");
        method.setAccessible(true);

        String result = (String) method.invoke(stream);
        assertThat(result).isEqualTo("0/0");
    }

    @Test
    void testGetCurrentLsnAllFunctionsFail() throws Exception {
        PreparedStatement gaussdbStmt = mock(PreparedStatement.class);
        when(gaussdbStmt.executeQuery()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_current_xlog_location()"))
                .thenReturn(gaussdbStmt);

        PreparedStatement postgresStmt = mock(PreparedStatement.class);
        when(postgresStmt.executeQuery()).thenThrow(new SQLException("not found"));
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(postgresStmt);

        WalReplicationStream stream = createStream(1);
        Method method = WalReplicationStream.class.getDeclaredMethod("getCurrentLsn");
        method.setAccessible(true);

        String result = (String) method.invoke(stream);
        assertThat(result).isEqualTo("0/0");
    }

    // ---- close with replicationConnection ----

    @Test
    void testCloseWithReplicationConnection() throws Exception {
        WalReplicationStream stream = createStream(1);

        Connection replConnection = mock(Connection.class);
        setField(stream, "replicationConnection", replConnection);

        stream.close();
        verify(replConnection).close();
    }

    @Test
    void testCloseWithReplicationConnectionThrowing() throws Exception {
        WalReplicationStream stream = createStream(1);

        Connection replConnection = mock(Connection.class);
        org.mockito.Mockito.doThrow(new SQLException("connection error"))
                .when(replConnection)
                .close();
        setField(stream, "replicationConnection", replConnection);

        // Should not throw
        stream.close();
    }

    @Test
    void testCloseWithReplicationStreamThrowing() throws Exception {
        WalReplicationStream stream = createStream(1);

        Object mockStream = mock(Object.class);
        setField(stream, "replicationStream", mockStream);

        // close() uses reflection to call close() on the stream
        // The mock's close() doesn't throw by default
        stream.close();
        assertThat(stream.isRunning()).isFalse();
    }

    // ---- extractJsonStringField edge cases ----

    @Test
    void testExtractJsonStringFieldWithNoColon() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonStringField", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(stream, "\"field_name\"", "field_name");
        assertThat(result).isNull();
    }

    @Test
    void testExtractJsonStringFieldWithNoValueAfterColon() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonStringField", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(stream, "\"field_name\": ", "field_name");
        assertThat(result).isNull();
    }

    @Test
    void testExtractJsonStringFieldWithNonStringArrayValue() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonStringField", String.class, String.class);
        method.setAccessible(true);

        // Field value is an array, not a string
        String result = (String) method.invoke(stream, "\"field_name\": [1, 2, 3]", "field_name");
        assertThat(result).isNull();
    }

    @Test
    void testExtractJsonStringFieldWithTruncatedJson() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonStringField", String.class, String.class);
        method.setAccessible(true);

        // String value without closing quote
        String result = (String) method.invoke(stream, "\"field_name\": \"truncated", "field_name");
        assertThat(result).isNull();
    }

    // ---- extractJsonArray edge cases ----

    @Test
    void testExtractJsonArrayWithNoColon() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonArray", String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(stream, "\"field_name\"", "field_name");
        assertThat(result).isEmpty();
    }

    @Test
    void testExtractJsonArrayWithNoOpenBracket() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonArray", String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result =
                (List<String>) method.invoke(stream, "\"field_name\": \"value\"", "field_name");
        assertThat(result).isEmpty();
    }

    @Test
    void testExtractJsonArrayWithNoCloseBracket() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonArray", String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result =
                (List<String>) method.invoke(stream, "\"field_name\": [1, 2", "field_name");
        assertThat(result).isEmpty();
    }

    @Test
    void testExtractJsonArrayWithUnquotedValues() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonArray", String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(stream, "\"nums\": [1, 2, 3]", "nums");
        assertThat(result).hasSize(3);
        assertThat(result).containsExactly("1", "2", "3");
    }

    @Test
    void testExtractJsonArrayWithEmptyArray() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "extractJsonArray", String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) method.invoke(stream, "\"nums\": []", "nums");
        assertThat(result).isEmpty();
    }

    // ---- parseChangeData with COMMIT and CSN parsing ----

    @Test
    void testParseCommitWithInvalidCsn() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);

        WalChange change =
                (WalChange) method.invoke(stream, "0/1", 100L, "COMMIT CSN not_a_number");
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.COMMIT);
    }

    @Test
    void testParseInsertWithoutSchema() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);

        WalChange change =
                (WalChange) method.invoke(stream, "0/1", 100L, "INSERT: mytable [id[integer]:1]");
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
        assertThat(change.getTable()).isEqualTo("mytable");
    }

    @Test
    void testParseInsertWithoutBrackets() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);

        WalChange change = (WalChange) method.invoke(stream, "0/1", 100L, "INSERT: public.test");
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
    }

    // ---- parseJsonColumns with missing types/values ----

    @Test
    void testParseJsonColumnsWithNullTypesAndValues() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseJsonColumns", String.class, String.class, String.class, String.class);
        method.setAccessible(true);

        String json = "{\"columns_name\":[\"id\"],\"nonexistent_type\":[],\"nonexistent_val\":[]}";

        @SuppressWarnings("unchecked")
        List<WalChange.ColumnValue> result =
                (List<WalChange.ColumnValue>)
                        method.invoke(
                                stream,
                                json,
                                "columns_name",
                                "nonexistent_type",
                                "nonexistent_val");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getColumnName()).isEqualTo("id");
        assertThat(result.get(0).getTypeOid()).isEqualTo(25); // text fallback
    }

    @Test
    void testParseJsonColumnsWithNullNameArray() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseJsonColumns", String.class, String.class, String.class, String.class);
        method.setAccessible(true);

        String json = "{}";

        @SuppressWarnings("unchecked")
        List<WalChange.ColumnValue> result =
                (List<WalChange.ColumnValue>)
                        method.invoke(
                                stream,
                                json,
                                "nonexistent_name",
                                "nonexistent_type",
                                "nonexistent_val");
        assertThat(result).isEmpty();
    }

    // ---- mapTypeNameToOid additional types ----

    @Test
    void testMapTypeNameToOidAdditionalTypes() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod("mapTypeNameToOid", String.class);
        method.setAccessible(true);

        assertThat(method.invoke(stream, "int")).isEqualTo(23);
        assertThat(method.invoke(stream, "int4")).isEqualTo(23);
        assertThat(method.invoke(stream, "int8")).isEqualTo(20);
        assertThat(method.invoke(stream, "int2")).isEqualTo(21);
        assertThat(method.invoke(stream, "bool")).isEqualTo(16);
        assertThat(method.invoke(stream, "float4")).isEqualTo(700);
        assertThat(method.invoke(stream, "float8")).isEqualTo(701);
        assertThat(method.invoke(stream, "decimal")).isEqualTo(1700);
        assertThat(method.invoke(stream, "varchar")).isEqualTo(1043);
        assertThat(method.invoke(stream, "char")).isEqualTo(1042);
        assertThat(method.invoke(stream, "timestamp")).isEqualTo(1114);
        assertThat(method.invoke(stream, "timestamptz")).isEqualTo(1184);
    }

    // ---- parseJsonChange with unknown op_type ----

    @Test
    void testParseJsonChangeWithUnknownOpType() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);

        String json =
                "{\"table_name\":\"public.t1\",\"op_type\":\"TRUNCATE\","
                        + "\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"1\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}";
        WalChange change = (WalChange) method.invoke(stream, "0/1", 100, json);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.UNKNOWN);
    }

    // ---- parseJsonChange with exception ----

    @Test
    void testParseJsonChangeWithMalformedJson() throws Exception {
        WalReplicationStream stream = createStream(1);
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);

        // Starts with { but has no table_name or op_type
        WalChange change = (WalChange) method.invoke(stream, "0/1", 100, "{invalid json}");
        // table_name and op_type are null, so type is not set by parseJsonChange
        assertThat(change).isNotNull();
    }

    // ---- readChangesFromSqlFunction with parallel decode ----

    @Test
    void testReadChangesFromSqlFunctionWithParallelDecode() throws Exception {
        PreparedStatement checkStmt = mock(PreparedStatement.class);
        ResultSet checkRs = mock(ResultSet.class);
        when(checkStmt.executeQuery()).thenReturn(checkRs);
        when(checkRs.next()).thenReturn(true);
        when(connection.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?"))
                .thenReturn(checkStmt);

        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn("0/1");
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_xlog_location()")).thenReturn(lsnStmt);

        PreparedStatement readStmt = mock(PreparedStatement.class);
        ResultSet readRs = mock(ResultSet.class);
        when(readRs.next()).thenReturn(false);
        when(readStmt.executeQuery()).thenReturn(readRs);
        when(connection.prepareStatement(
                        "SELECT location AS lsn, xid, data FROM pg_logical_slot_peek_changes(?, NULL, ?, 'include-xids', '1', 'parallel-decode-num', '4')"))
                .thenReturn(readStmt);

        WalReplicationStream stream =
                new WalReplicationStream(
                        connection,
                        "jdbc:gaussdb://localhost:8000/test",
                        "root",
                        "pass",
                        "test_slot",
                        "mppdb_decoding",
                        4,
                        "b",
                        false,
                        1000);
        stream.initialize();

        List<WalChange> changes = stream.readChanges(10);
        assertThat(changes).isEmpty();
    }

    // ---- Helper methods ----

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = WalReplicationStream.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
