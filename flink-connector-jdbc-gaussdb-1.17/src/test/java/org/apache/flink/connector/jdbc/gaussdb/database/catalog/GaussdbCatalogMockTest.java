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

package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for {@link GaussdbCatalog} with mocked database connections. */
class GaussdbCatalogMockTest {

    private GaussdbCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog =
                new GaussdbCatalog(
                        Thread.currentThread().getContextClassLoader(),
                        "test_catalog",
                        "postgres",
                        "user",
                        "password",
                        "jdbc:gaussdb://localhost:8000/");
    }

    @Test
    void testFromJDBCTypeWithInt4() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("int4");
        when(metadata.getPrecision(1)).thenReturn(0);
        when(metadata.getScale(1)).thenReturn(0);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.INT());
    }

    @Test
    void testFromJDBCTypeWithBool() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("bool");
        when(metadata.getPrecision(1)).thenReturn(0);
        when(metadata.getScale(1)).thenReturn(0);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    @Test
    void testFromJDBCTypeWithVarchar() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("varchar");
        when(metadata.getPrecision(1)).thenReturn(255);
        when(metadata.getScale(1)).thenReturn(0);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.VARCHAR(255));
    }

    @Test
    void testFromJDBCTypeWithTimestamp() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("timestamp");
        when(metadata.getPrecision(1)).thenReturn(0);
        when(metadata.getScale(1)).thenReturn(6);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.TIMESTAMP(6));
    }

    @Test
    void testFromJDBCTypeWithNumeric() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("numeric");
        when(metadata.getPrecision(1)).thenReturn(10);
        when(metadata.getScale(1)).thenReturn(2);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.DECIMAL(10, 2));
    }

    @Test
    void testFromJDBCTypeWithFloat8() throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn("float8");
        when(metadata.getPrecision(1)).thenReturn(0);
        when(metadata.getScale(1)).thenReturn(0);

        Method method =
                GaussdbCatalog.class.getDeclaredMethod(
                        "fromJDBCType", ObjectPath.class, ResultSetMetaData.class, int.class);
        method.setAccessible(true);

        ObjectPath tablePath = new ObjectPath("testdb", "testtable");
        DataType result = (DataType) method.invoke(catalog, tablePath, metadata, 1);
        assertThat(result).isEqualTo(org.apache.flink.table.api.DataTypes.DOUBLE());
    }

    @Test
    void testListTablesBlankDatabase() {
        assertThatThrownBy(() -> catalog.listTables("")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testListTablesDatabaseNotExistThrows() {
        assertThatThrownBy(() -> catalog.listTables("nonexistent"))
                .isInstanceOf(Exception.class); // CatalogException due to no DB connection
    }
}
