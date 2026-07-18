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
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Comprehensive tests for {@link GaussDBCDCSourceFunction} runtime logic. */
class GaussDBCDCSourceFunctionRuntimeTest {

    /** Test convertColumnValueByOid via reflection for common type OIDs. */
    @Test
    void testConvertColumnValueByOid() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        // int4 (OID 23)
        Object result = method.invoke(source, 23, "42");
        assertThat(result).isEqualTo(42);

        // int2 (OID 21)
        result = method.invoke(source, 21, "100");
        assertThat(result).isEqualTo(100);

        // int8 (OID 20)
        result = method.invoke(source, 20, "9999999999");
        assertThat(result).isEqualTo(9999999999L);

        // bool (OID 16)
        result = method.invoke(source, 16, "true");
        assertThat(result).isEqualTo(true);

        // float4 (OID 700)
        result = method.invoke(source, 700, "3.14");
        assertThat((Float) result).isCloseTo(3.14f, org.assertj.core.data.Offset.offset(0.01f));

        // float8 (OID 701)
        result = method.invoke(source, 701, "3.141592653589793");
        assertThat((Double) result)
                .isCloseTo(3.141592653589793, org.assertj.core.data.Offset.offset(0.0001));

        // text (OID 25)
        result = method.invoke(source, 25, "hello");
        assertThat(result.toString()).isEqualTo("hello");

        // varchar (OID 1043)
        result = method.invoke(source, 1043, "world");
        assertThat(result.toString()).isEqualTo("world");

        // name (OID 19)
        result = method.invoke(source, 19, "col_name");
        assertThat(result.toString()).isEqualTo("col_name");

        // null value
        result = method.invoke(source, 23, null);
        assertThat(result).isNull();

        // unknown OID -> fallback to string
        result = method.invoke(source, 9999, "unknown_type");
        assertThat(result.toString()).isEqualTo("unknown_type");
    }

    /** Test convertWalColumnsToRowData via reflection. */
    @Test
    void testConvertWalColumnsToRowData() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        initTransientMaps(source);
        // Pre-populate column cache for test_table so convertWalColumnsToRowData finds it
        java.lang.reflect.Field colsField =
                GaussDBCDCSourceFunction.class.getDeclaredField("cachedColumnsByTable");
        colsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, java.util.List<String>> colsMap =
                (java.util.Map<String, java.util.List<String>>) colsField.get(source);
        java.util.List<String> cols = new ArrayList<>();
        cols.add("id");
        cols.add("name");
        cols.add("age");
        cols.add("score");
        colsMap.put("test_table", cols);
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertWalColumnsToRowData", List.class, String.class);
        method.setAccessible(true);

        // Null list
        Object result = method.invoke(source, (Object) null, "test_table");
        assertThat(result).isNull();

        // Empty list
        result = method.invoke(source, Collections.emptyList(), "test_table");
        assertThat(result).isNull();

        // List with values
        List<WalChange.ColumnValue> columns = new ArrayList<>();
        columns.add(new WalChange.ColumnValue("id", 23, "1", false));
        columns.add(new WalChange.ColumnValue("name", 25, "Alice", false));
        columns.add(new WalChange.ColumnValue("age", 23, "20", false));
        columns.add(new WalChange.ColumnValue("score", 701, "95.5", false));

        result = method.invoke(source, columns, "test_table");
        assertThat(result).isInstanceOf(RowData.class);
        RowData row = (RowData) result;
        assertThat(row.getArity()).isEqualTo(4);
    }

    /** Test convertWalColumnsToRowData with null column. */
    @Test
    void testConvertWalColumnsToRowDataWithNull() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        initTransientMaps(source);
        // Pre-populate column cache for test_table
        java.lang.reflect.Field colsField =
                GaussDBCDCSourceFunction.class.getDeclaredField("cachedColumnsByTable");
        colsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, java.util.List<String>> colsMap =
                (java.util.Map<String, java.util.List<String>>) colsField.get(source);
        java.util.List<String> cols = new ArrayList<>();
        cols.add("id");
        cols.add("name");
        colsMap.put("test_table", cols);
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertWalColumnsToRowData", List.class, String.class);
        method.setAccessible(true);

        List<WalChange.ColumnValue> columns = new ArrayList<>();
        columns.add(new WalChange.ColumnValue("id", 23, "1", false));
        columns.add(new WalChange.ColumnValue("name", 25, null, true));

        Object result = method.invoke(source, columns, "test_table");
        assertThat(result).isInstanceOf(RowData.class);
        RowData row = (RowData) result;
        assertThat(row.getArity()).isEqualTo(2);
    }

    /** Test getTableColumns via reflection with mocked connection. */
    @Test
    void testGetTableColumns() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        initTransientMaps(source);

        // Mock connection
        Connection conn = mock(Connection.class);
        DatabaseMetaData meta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);

        when(conn.getMetaData()).thenReturn(meta);
        when(meta.getColumns(null, "public", "test_table", null)).thenReturn(columnsRs);
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");

        // Set connection field
        Field connField = GaussDBCDCSourceFunction.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(source, conn);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod("getTableColumns", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(source, "test_table");
        assertThat(result).isEqualTo("id, name");
    }

    /** Test getIdRange via reflection with mocked connection. */
    @Test
    void testGetIdRange() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();

        Connection conn = mock(Connection.class);
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getLong(1)).thenReturn(1L);
        when(rs.getLong(2)).thenReturn(1000L);
        when(rs.wasNull()).thenReturn(false);

        Field connField = GaussDBCDCSourceFunction.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(source, conn);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "getIdRange", String.class, String.class);
        method.setAccessible(true);

        long[] result = (long[]) method.invoke(source, "test_table", "id");
        assertThat(result).containsExactly(1L, 1000L);
    }

    /** Test getIdRange with empty table. */
    @Test
    void testGetIdRangeEmptyTable() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();

        Connection conn = mock(Connection.class);
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(conn.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getLong(1)).thenReturn(0L);
        when(rs.wasNull()).thenReturn(true);

        Field connField = GaussDBCDCSourceFunction.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(source, conn);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "getIdRange", String.class, String.class);
        method.setAccessible(true);

        long[] result = (long[]) method.invoke(source, "test_table", "id");
        assertThat(result).containsExactly(0L, -1L);
    }

    /** Test convertToRowDataDynamic with mocked ResultSet. */
    @Test
    void testConvertToRowDataDynamic() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(4);
        when(meta.getColumnType(1)).thenReturn(Types.INTEGER);
        when(meta.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(meta.getColumnType(3)).thenReturn(Types.BIGINT);
        when(meta.getColumnType(4)).thenReturn(Types.BOOLEAN);

        when(rs.getInt(1)).thenReturn(42);
        when(rs.wasNull()).thenReturn(false);
        when(rs.getString(2)).thenReturn("hello");
        when(rs.getLong(3)).thenReturn(999999L);
        when(rs.getBoolean(4)).thenReturn(true);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
        assertThat(row.getArity()).isEqualTo(4);
    }

    /** Test convertToRowDataDynamic with null values. */
    @Test
    void testConvertToRowDataDynamicWithNulls() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(2);
        when(meta.getColumnType(1)).thenReturn(Types.INTEGER);
        when(meta.getColumnType(2)).thenReturn(Types.VARCHAR);

        when(rs.getInt(1)).thenReturn(0);
        when(rs.wasNull()).thenReturn(true);
        when(rs.getString(2)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test convertToRowDataDynamic with decimal type. */
    @Test
    void testConvertToRowDataDynamicDecimal() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.DECIMAL);
        when(meta.getPrecision(1)).thenReturn(10);
        when(meta.getScale(1)).thenReturn(2);
        when(rs.getBigDecimal(1)).thenReturn(new java.math.BigDecimal("123.45"));

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test convertToRowDataDynamic with timestamp type. */
    @Test
    void testConvertToRowDataDynamicTimestamp() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.TIMESTAMP);
        when(rs.getTimestamp(1)).thenReturn(new java.sql.Timestamp(System.currentTimeMillis()));

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test convertToRowDataDynamic with date type. */
    @Test
    void testConvertToRowDataDynamicDate() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.DATE);
        when(rs.getDate(1)).thenReturn(java.sql.Date.valueOf("2024-01-15"));

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test convertToRowDataDynamic with double/float type. */
    @Test
    void testConvertToRowDataDynamicDouble() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.DOUBLE);
        when(rs.getDouble(1)).thenReturn(3.14159);
        when(rs.wasNull()).thenReturn(false);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test convertToRowDataDynamic with unknown type (fallback to string). */
    @Test
    void testConvertToRowDataDynamicUnknownType() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.OTHER);
        when(rs.getString(1)).thenReturn("some_blob_data");

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    /** Test numeric OID conversion. */
    @Test
    void testConvertColumnValueByOidNumeric() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        // numeric (OID 1700)
        Object result = method.invoke(source, 1700, "12345.67");
        assertThat(result).isNotNull();
    }

    /** Test date OID conversion. */
    @Test
    void testConvertColumnValueByOidDate() throws Exception {
        GaussDBCDCSourceFunction source = createMinimalSource();
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        // date (OID 1082)
        Object result = method.invoke(source, 1082, "2024-01-15");
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(Integer.class);
    }

    private GaussDBCDCSourceFunction createMinimalSource() {
        return GaussDBCDCSourceFunction.builder()
                .hostname("localhost")
                .port(8000)
                .database("testdb")
                .schema("public")
                .tableName("test_table")
                .username("root")
                .password("pass")
                .build();
    }

    /** Initialize transient Map fields that are normally set in open(). */
    private void initTransientMaps(GaussDBCDCSourceFunction source) throws Exception {
        java.lang.reflect.Field colsField =
                GaussDBCDCSourceFunction.class.getDeclaredField("cachedColumnsByTable");
        colsField.setAccessible(true);
        colsField.set(source, new java.util.HashMap<>());

        java.lang.reflect.Field pkField =
                GaussDBCDCSourceFunction.class.getDeclaredField("cachedPkByTable");
        pkField.setAccessible(true);
        pkField.set(source, new java.util.HashMap<>());
    }
}
