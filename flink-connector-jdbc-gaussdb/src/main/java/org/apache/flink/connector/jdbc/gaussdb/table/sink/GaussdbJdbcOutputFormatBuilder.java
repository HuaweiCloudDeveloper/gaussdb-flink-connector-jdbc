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

package org.apache.flink.connector.jdbc.gaussdb.table.sink;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.gaussdb.table.GaussdbExtendOptions;
import org.apache.flink.connector.jdbc.gaussdb.table.executor.GaussdbUpsertStatementExecutor;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.executor.TableSimpleStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for {@link JdbcOutputFormat} for Table/SQL. */
public class GaussdbJdbcOutputFormatBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private InternalJdbcConnectionOptions jdbcOptions;
    private JdbcExecutionOptions executionOptions;
    private JdbcDmlOptions dmlOptions;
    private GaussdbExtendOptions gaussdbExtendOptions;
    private DataType[] fieldDataTypes;

    public GaussdbJdbcOutputFormatBuilder() {}

    public GaussdbJdbcOutputFormatBuilder setJdbcOptions(
            InternalJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
        return this;
    }

    public GaussdbJdbcOutputFormatBuilder setGaussdbSinkOptions(
            GaussdbExtendOptions gaussdbExtendOptions) {
        this.gaussdbExtendOptions = gaussdbExtendOptions;
        return this;
    }

    public GaussdbJdbcOutputFormatBuilder setJdbcExecutionOptions(
            JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public GaussdbJdbcOutputFormatBuilder setJdbcDmlOptions(JdbcDmlOptions dmlOptions) {
        this.dmlOptions = dmlOptions;
        return this;
    }

    public GaussdbJdbcOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        return this;
    }

    public JdbcOutputFormat<RowData, ?, ?> build() {
        checkNotNull(jdbcOptions, "jdbc options can not be null");
        checkNotNull(dmlOptions, "jdbc dml options can not be null");
        checkNotNull(executionOptions, "jdbc execution options can not be null");

        final LogicalType[] logicalTypes =
                Arrays.stream(fieldDataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new);
        if (dmlOptions.getKeyFields().isPresent() && dmlOptions.getKeyFields().get().length > 0) {
            // upsert query
            return new JdbcOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    () ->
                            createBufferReduceExecutor(
                                    dmlOptions, gaussdbExtendOptions, logicalTypes));
        } else {
            // append only query
            final String sql =
                    dmlOptions
                            .getDialect()
                            .getInsertIntoStatement(
                                    dmlOptions.getTableName(), dmlOptions.getFieldNames());
            return new JdbcOutputFormat<>(
                    new SimpleJdbcConnectionProvider(jdbcOptions),
                    executionOptions,
                    () ->
                            createSimpleBufferedExecutor(
                                    dmlOptions.getDialect(),
                                    dmlOptions.getFieldNames(),
                                    logicalTypes,
                                    sql));
        }
    }

    private static JdbcBatchStatementExecutor<RowData> createBufferReduceExecutor(
            JdbcDmlOptions opt, GaussdbExtendOptions spt, LogicalType[] fieldTypes) {
        checkArgument(opt.getKeyFields().isPresent());
        JdbcDialect dialect = opt.getDialect();
        String tableName = opt.getTableName();
        String[] pkNames = opt.getKeyFields().get();
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(Arrays.asList(opt.getFieldNames())::indexOf)
                        .toArray();
        LogicalType[] pkTypes =
                Arrays.stream(pkFields).mapToObj(f -> fieldTypes[f]).toArray(LogicalType[]::new);

        return new TableBufferReducedStatementExecutor(
                new GaussdbUpsertStatementExecutor(opt, spt, fieldTypes),
                createDeleteExecutor(dialect, tableName, pkNames, pkTypes),
                createRowKeyExtractor(fieldTypes, pkFields));
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleBufferedExecutor(
            JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, String sql) {

        return new TableBufferedStatementExecutor(
                createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql));
    }

    private static JdbcBatchStatementExecutor<RowData> createDeleteExecutor(
            JdbcDialect dialect, String tableName, String[] pkNames, LogicalType[] pkTypes) {
        String deleteSql = dialect.getDeleteStatement(tableName, pkNames);
        return createSimpleRowExecutor(dialect, pkNames, pkTypes, deleteSql);
    }

    private static JdbcBatchStatementExecutor<RowData> createSimpleRowExecutor(
            JdbcDialect dialect, String[] fieldNames, LogicalType[] fieldTypes, final String sql) {
        final JdbcDialectConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));
        return new TableSimpleStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames),
                rowConverter);
    }

    private static Function<RowData, RowData> createRowKeyExtractor(
            LogicalType[] logicalTypes, int[] pkFields) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
        }
        return row -> getPrimaryKey(row, fieldGetters);
    }

    private static RowData getPrimaryKey(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData pkRow = new GenericRowData(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return pkRow;
    }
}
