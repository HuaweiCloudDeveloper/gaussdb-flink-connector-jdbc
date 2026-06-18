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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresDialect;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

/**
 * A {@link DynamicTableSource} for GaussDB.
 *
 * <p>This class is based on Flink's JdbcDynamicTableSource but uses GaussDB dialect.
 */
@Internal
public class GaussdbDynamicTableSource implements ScanTableSource {

    private final InternalJdbcConnectionOptions jdbcOptions;
    private final JdbcReadOptions readOptions;
    private final DataType physicalRowDataType;
    private final String dialectName;

    public GaussdbDynamicTableSource(
            InternalJdbcConnectionOptions jdbcOptions,
            JdbcReadOptions readOptions,
            DataType physicalRowDataType) {
        this.jdbcOptions = jdbcOptions;
        this.readOptions = readOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = jdbcOptions.getDialect().dialectName();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        // Use PostgresRowConverter for lib directory compatibility
        // GaussDB is based on PostgreSQL, so PostgresRowConverter works
        final PostgresDialect postgresDialect = new PostgresDialect();

        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(jdbcOptions.getDriverName())
                        .setDBUrl(jdbcOptions.getDbURL())
                        .setUsername(jdbcOptions.getUsername().orElse(null))
                        .setPassword(jdbcOptions.getPassword().orElse(null))
                        .setQuery(
                                jdbcOptions
                                        .getDialect()
                                        .getSelectFromStatement(
                                                jdbcOptions.getTableName(),
                                                DataType.getFieldNames(physicalRowDataType)
                                                        .toArray(new String[0]),
                                                new String[0]))
                        .setRowConverter(postgresDialect.getRowConverter(rowType))
                        .setRowDataTypeInfo(context.createTypeInformation(physicalRowDataType));

        if (readOptions != null) {
            if (readOptions.getFetchSize() != 0) {
                builder.setFetchSize(readOptions.getFetchSize());
            }
            builder.setAutoCommit(readOptions.getAutoCommit());
        }

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        return new GaussdbDynamicTableSource(jdbcOptions, readOptions, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "GaussDB:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GaussdbDynamicTableSource)) {
            return false;
        }
        GaussdbDynamicTableSource that = (GaussdbDynamicTableSource) o;
        return Objects.equals(jdbcOptions, that.jdbcOptions)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcOptions, readOptions, physicalRowDataType, dialectName);
    }
}
