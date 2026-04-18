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

package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresDialect;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link GaussdbDynamicTableSource}. */
class GaussdbDynamicTableSourceTest {

    private InternalJdbcConnectionOptions jdbcOptions;
    private JdbcReadOptions readOptions;
    private DataType physicalRowDataType;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        InternalJdbcConnectionOptions.Builder builder =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:postgresql://localhost:5432/test")
                        .setTableName("test_table")
                        .setDialect(new PostgresDialect());
        jdbcOptions = builder.build();
        readOptions = JdbcReadOptions.builder().setFetchSize(100).setAutoCommit(true).build();
        physicalRowDataType =
                org.apache.flink.table.api.DataTypes.ROW(
                        org.apache.flink.table.api.DataTypes.FIELD(
                                "id", org.apache.flink.table.api.DataTypes.INT()),
                        org.apache.flink.table.api.DataTypes.FIELD(
                                "name", org.apache.flink.table.api.DataTypes.VARCHAR(255)));
    }

    @Test
    void testGetChangelogMode() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source.getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void testGetScanRuntimeProvider() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class));
        assertThat(provider).isNotNull();
    }

    @Test
    void testGetScanRuntimeProviderWithNullReadOptions() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, null, physicalRowDataType);
        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class));
        assertThat(provider).isNotNull();
    }

    @Test
    void testGetScanRuntimeProviderWithZeroFetchSize() {
        JdbcReadOptions readOptsZero =
                JdbcReadOptions.builder().setFetchSize(0).setAutoCommit(true).build();
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptsZero, physicalRowDataType);
        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class));
        assertThat(provider).isNotNull();
    }

    @Test
    void testCopy() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        DynamicTableSource copy = source.copy();
        assertThat(copy).isInstanceOf(GaussdbDynamicTableSource.class);
        assertThat(copy).isNotSameAs(source);
    }

    @Test
    void testAsSummaryString() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source.asSummaryString()).startsWith("GaussDB:");
    }

    @Test
    void testEqualsSameInstance() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source.equals(source)).isTrue();
    }

    @Test
    void testEqualsDifferentInstance() {
        GaussdbDynamicTableSource source1 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        GaussdbDynamicTableSource source2 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source1.equals(source2)).isTrue();
    }

    @Test
    void testEqualsDifferentOptions() {
        GaussdbDynamicTableSource source1 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);

        InternalJdbcConnectionOptions otherJdbcOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl("jdbc:postgresql://other:5432/test")
                        .setTableName("other_table")
                        .setDialect(new PostgresDialect())
                        .build();
        GaussdbDynamicTableSource source2 =
                new GaussdbDynamicTableSource(otherJdbcOptions, readOptions, physicalRowDataType);
        assertThat(source1.equals(source2)).isFalse();
    }

    @Test
    void testEqualsNull() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source.equals(null)).isFalse();
    }

    @Test
    void testEqualsDifferentClass() {
        GaussdbDynamicTableSource source =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source.equals("not a source")).isFalse();
    }

    @Test
    void testHashCode() {
        GaussdbDynamicTableSource source1 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        GaussdbDynamicTableSource source2 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        assertThat(source1.hashCode()).isEqualTo(source2.hashCode());
    }

    @Test
    void testEqualsDifferentReadOptions() {
        GaussdbDynamicTableSource source1 =
                new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
        JdbcReadOptions otherReadOptions =
                JdbcReadOptions.builder().setFetchSize(200).setAutoCommit(false).build();
        GaussdbDynamicTableSource source2 =
                new GaussdbDynamicTableSource(jdbcOptions, otherReadOptions, physicalRowDataType);
        assertThat(source1.equals(source2)).isFalse();
    }
}
