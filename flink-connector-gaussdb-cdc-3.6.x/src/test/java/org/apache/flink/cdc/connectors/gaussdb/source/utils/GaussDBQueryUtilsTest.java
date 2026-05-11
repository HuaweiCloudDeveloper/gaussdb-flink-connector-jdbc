/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBQueryUtils}. */
class GaussDBQueryUtilsTest {

    @Test
    void testQuoteString() {
        assertThat(GaussDBQueryUtils.quote("hello")).isEqualTo("\"hello\"");
    }

    @Test
    void testQuoteStringWithDoubleQuotes() {
        assertThat(GaussDBQueryUtils.quote("he\"llo")).isEqualTo("\"he\"\"llo\"");
    }

    @Test
    void testQuoteTableId() {
        TableId tableId = new TableId("mydb", "public", "users");
        String quoted = GaussDBQueryUtils.quote(tableId);
        assertThat(quoted).contains("public");
        assertThat(quoted).contains("users");
    }

    @Test
    void testIsUuidColumn() {
        Column uuidCol = mock(Column.class);
        when(uuidCol.typeName()).thenReturn("uuid");
        assertThat(GaussDBQueryUtils.isUUID(uuidCol)).isTrue();

        Column intCol = mock(Column.class);
        when(intCol.typeName()).thenReturn("int4");
        assertThat(GaussDBQueryUtils.isUUID(intCol)).isFalse();
    }

    @Test
    void testBuildSplitScanQueryFirstAndLastSplit() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        true,
                        true,
                        Collections.singletonList("id"),
                        Collections.emptyList());
        assertThat(query).contains("SELECT");
        assertThat(query).contains("FROM");
        assertThat(query).doesNotContain("WHERE");
    }

    @Test
    void testBuildSplitScanQueryFirstSplit() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        true,
                        false,
                        Collections.singletonList("id"),
                        Collections.emptyList());
        assertThat(query).contains("WHERE");
        assertThat(query).contains("<=");
    }

    @Test
    void testBuildSplitScanQueryLastSplit() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        false,
                        true,
                        Collections.singletonList("id"),
                        Collections.emptyList());
        assertThat(query).contains("WHERE");
        assertThat(query).contains(">=");
    }

    @Test
    void testBuildSplitScanQueryMiddleSplit() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        false,
                        false,
                        Collections.singletonList("id"),
                        Collections.emptyList());
        assertThat(query).contains("WHERE");
        assertThat(query).contains(">=");
        assertThat(query).contains("<=");
    }

    @Test
    void testBuildSplitScanQueryWithCompositeKey() {
        RowType pkRowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("id", new IntType()),
                                new RowType.RowField("name", new VarCharType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        false,
                        false,
                        Arrays.asList("id", "name"),
                        Collections.emptyList());
        assertThat(query).contains("WHERE");
        assertThat(query).contains("AND");
    }

    @Test
    void testBuildSplitScanQueryWithUuidField() {
        RowType pkRowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("uuid_col", new VarCharType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        false,
                        false,
                        Collections.singletonList("uuid_col"),
                        Collections.singletonList("uuid_col"));
        assertThat(query).contains("::uuid");
    }

    @Test
    void testBuildSplitScanQueryWithNullColumnNames() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType, tableId, true, true, null, Collections.emptyList());
        assertThat(query).contains("SELECT *");
    }

    @Test
    void testBuildSplitScanQuerySevenParamVersion() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        // Test 7-param version with null extraConditions
        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        true,
                        true,
                        Collections.singletonList("id"),
                        Collections.emptyList(),
                        null);
        assertThat(query).contains("SELECT");
        assertThat(query).contains("FROM");
    }

    @Test
    void testBuildSplitScanQueryMiddleSplitDetailed() {
        RowType pkRowType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        false,
                        false,
                        Collections.singletonList("\"id\""),
                        Collections.emptyList());
        // Middle split should have both >= and <= conditions with NOT (=) in between
        assertThat(query).contains("WHERE");
        assertThat(query).contains(">=");
        assertThat(query).contains("NOT");
        assertThat(query).contains("<=");
    }

    @Test
    void testBuildSplitScanQueryFirstSplitWithUuid() {
        RowType pkRowType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("uuid_id", new VarCharType())));
        TableId tableId = new TableId("mydb", "public", "users");

        String query =
                GaussDBQueryUtils.buildSplitScanQuery(
                        pkRowType,
                        tableId,
                        true,
                        false,
                        Collections.singletonList("uuid_id"),
                        Collections.singletonList("uuid_id"));
        assertThat(query).contains("::uuid");
        assertThat(query).contains("<=");
    }

    @Test
    void testReadTableSplitDataStatementFirstAndLastSplit() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT * FROM \"public\".\"users\""))
                .thenReturn(statement);

        PreparedStatement result =
                GaussDBQueryUtils.readTableSplitDataStatement(
                        jdbc, "SELECT * FROM \"public\".\"users\"", true, true, null, null, 1, 100);
        assertThat(result).isNotNull();
    }

    @Test
    void testReadTableSplitDataStatementFirstSplit() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        Object[] splitEnd = new Object[] {100};
        PreparedStatement result =
                GaussDBQueryUtils.readTableSplitDataStatement(
                        jdbc, "SELECT * FROM t WHERE id <= ?", true, false, null, splitEnd, 1, 100);
        assertThat(result).isNotNull();
    }

    @Test
    void testReadTableSplitDataStatementLastSplit() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        Object[] splitStart = new Object[] {100};
        PreparedStatement result =
                GaussDBQueryUtils.readTableSplitDataStatement(
                        jdbc,
                        "SELECT * FROM t WHERE id >= ?",
                        false,
                        true,
                        splitStart,
                        null,
                        1,
                        100);
        assertThat(result).isNotNull();
    }

    @Test
    void testReadTableSplitDataStatementMiddleSplit() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        Object[] splitStart = new Object[] {100};
        Object[] splitEnd = new Object[] {200};
        PreparedStatement result =
                GaussDBQueryUtils.readTableSplitDataStatement(
                        jdbc,
                        "SELECT * FROM t WHERE id >= ? AND id <= ?",
                        false,
                        false,
                        splitStart,
                        splitEnd,
                        1,
                        100);
        assertThat(result).isNotNull();
    }

    @Test
    void testReadTableSplitDataStatementWithException() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        when(jdbc.connection()).thenThrow(new RuntimeException("Connection failed"));

        assertThatThrownBy(
                        () ->
                                GaussDBQueryUtils.readTableSplitDataStatement(
                                        jdbc, "SELECT * FROM t", true, true, null, null, 1, 100))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to build the split data read statement");
    }

    @Test
    void testQueryTableSchemaReturnsNullForMissingTable() {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        TableId tableId = new TableId("testdb", "public", "nonexistent");
        // queryTableSchema catches SQLException and returns null
        var result = GaussDBQueryUtils.queryTableSchema(jdbc, tableId);
        assertThat(result).isNull();
    }

    @Test
    void testQueryTableSchemaWithMultipleTables() throws Exception {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        TableId tableId1 = new TableId("testdb", "public", "table1");
        // Should handle errors gracefully and return empty map
        var result = GaussDBQueryUtils.queryTableSchema(jdbc, java.util.Arrays.asList(tableId1));
        assertThat(result).isNotNull();
    }

    @Test
    void testCastParamMethod() throws Exception {
        Method castParam = GaussDBQueryUtils.class.getDeclaredMethod("castParam", boolean.class);
        castParam.setAccessible(true);

        String resultUuid = (String) castParam.invoke(null, true);
        assertThat(resultUuid).contains("::uuid");

        String resultNormal = (String) castParam.invoke(null, false);
        assertThat(resultNormal).isEqualTo("?");
    }

    @Test
    void testCastToUuidMethod() throws Exception {
        Method castToUuid = GaussDBQueryUtils.class.getDeclaredMethod("castToUuid", String.class);
        castToUuid.setAccessible(true);

        String result = (String) castToUuid.invoke(null, "?");
        assertThat(result).isEqualTo("(?)::uuid");
    }

    @Test
    void testCastToTextMethod() throws Exception {
        Method castToText = GaussDBQueryUtils.class.getDeclaredMethod("castToText", String.class);
        castToText.setAccessible(true);

        String result = (String) castToText.invoke(null, "\"col\"");
        assertThat(result).isEqualTo("(\"col\")::text");
    }

    @Test
    void testQuoteForMinMaxMethod() throws Exception {
        Method quoteForMinMax =
                GaussDBQueryUtils.class.getDeclaredMethod("quoteForMinMax", Column.class);
        quoteForMinMax.setAccessible(true);

        Column uuidCol = mock(Column.class);
        when(uuidCol.typeName()).thenReturn("uuid");
        when(uuidCol.name()).thenReturn("uuid_id");
        String resultUuid = (String) quoteForMinMax.invoke(null, uuidCol);
        assertThat(resultUuid).contains("::text");

        Column intCol = mock(Column.class);
        when(intCol.typeName()).thenReturn("int4");
        when(intCol.name()).thenReturn("id");
        String resultInt = (String) quoteForMinMax.invoke(null, intCol);
        assertThat(resultInt).isEqualTo("\"id\"");
    }

    @Test
    void testBuildSelectWithRowLimitsMethod() throws Exception {
        Method buildSelect =
                GaussDBQueryUtils.class.getDeclaredMethod(
                        "buildSelectWithRowLimits",
                        TableId.class,
                        String.class,
                        java.util.Optional.class,
                        java.util.Optional.class);
        buildSelect.setAccessible(true);

        TableId tableId = new TableId("mydb", "public", "users");

        // With condition and orderBy
        String result =
                (String)
                        buildSelect.invoke(
                                null,
                                tableId,
                                "*",
                                java.util.Optional.of("id > 0"),
                                java.util.Optional.of("id ASC"));
        assertThat(result).contains("SELECT *");
        assertThat(result).contains("WHERE");
        assertThat(result).contains("ORDER BY");

        // Without condition and orderBy
        String resultNoCondition =
                (String)
                        buildSelect.invoke(
                                null,
                                tableId,
                                "col1,col2",
                                java.util.Optional.empty(),
                                java.util.Optional.empty());
        assertThat(resultNoCondition).contains("SELECT col1,col2");
        assertThat(resultNoCondition).doesNotContain("WHERE");
        assertThat(resultNoCondition).doesNotContain("ORDER BY");
    }

    private static String anyString() {
        return org.mockito.ArgumentMatchers.anyString();
    }

    @Test
    void testInitStatementMethod() throws Exception {
        // Test the private initStatement method which sets autocommit and fetch size
        Method initStatement =
                GaussDBQueryUtils.class.getDeclaredMethod(
                        "initStatement", JdbcConnection.class, String.class, int.class);
        initStatement.setAccessible(true);

        // With a mock JdbcConnection that has a mock Connection
        JdbcConnection jdbc = mock(JdbcConnection.class);
        java.sql.Connection sqlConn = mock(java.sql.Connection.class);
        java.sql.PreparedStatement pstmt = mock(java.sql.PreparedStatement.class);
        when(jdbc.connection()).thenReturn(sqlConn);
        when(sqlConn.prepareStatement(anyString())).thenReturn(pstmt);

        java.sql.PreparedStatement result =
                (java.sql.PreparedStatement) initStatement.invoke(null, jdbc, "SELECT 1", 100);
        assertThat(result).isNotNull();
    }

    @Test
    void testAddPrimaryKeyColumnsToConditionMethod() throws Exception {
        // Test the private method that builds condition with PK columns
        Method addPkCondition =
                GaussDBQueryUtils.class.getDeclaredMethod(
                        "addPrimaryKeyColumnsToCondition",
                        org.apache.flink.table.types.logical.RowType.class,
                        StringBuilder.class,
                        String.class,
                        java.util.List.class);
        addPkCondition.setAccessible(true);

        org.apache.flink.table.types.logical.RowType pkRowType =
                new org.apache.flink.table.types.logical.RowType(
                        java.util.Arrays.asList(
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "id", new org.apache.flink.table.types.logical.IntType()),
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "name",
                                        new org.apache.flink.table.types.logical.VarCharType(
                                                255))));

        StringBuilder sql = new StringBuilder();
        java.util.List<String> uuidFields = java.util.Collections.emptyList();
        addPkCondition.invoke(null, pkRowType, sql, " >= ", uuidFields);
        assertThat(sql.toString()).contains("\"id\" >= ?");
        assertThat(sql.toString()).contains("\"name\" >= ?");
        assertThat(sql.toString()).contains(" AND ");
    }

    @Test
    void testAddPrimaryKeyColumnsToConditionWithUuid() throws Exception {
        Method addPkCondition =
                GaussDBQueryUtils.class.getDeclaredMethod(
                        "addPrimaryKeyColumnsToCondition",
                        org.apache.flink.table.types.logical.RowType.class,
                        StringBuilder.class,
                        String.class,
                        java.util.List.class);
        addPkCondition.setAccessible(true);

        org.apache.flink.table.types.logical.RowType pkRowType =
                new org.apache.flink.table.types.logical.RowType(
                        java.util.Collections.singletonList(
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "uuid_col",
                                        new org.apache.flink.table.types.logical.VarCharType(36))));

        StringBuilder sql = new StringBuilder();
        java.util.List<String> uuidFields = java.util.Collections.singletonList("uuid_col");
        addPkCondition.invoke(null, pkRowType, sql, " <= ", uuidFields);
        assertThat(sql.toString()).contains("::uuid");
    }
}
