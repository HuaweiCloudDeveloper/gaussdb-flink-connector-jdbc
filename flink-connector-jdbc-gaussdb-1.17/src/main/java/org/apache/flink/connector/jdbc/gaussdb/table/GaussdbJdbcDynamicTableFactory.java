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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.gaussdb.dialect.GaussdbDialect;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.DRIVER;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;

/**
 * Factory for creating configured instances of {@link DynamicTableSource} and {@link
 * DynamicTableSink} for GaussDB.
 *
 * <p><b>NOTE:</b> This factory uses the standard Flink JDBC Connector's {@link
 * JdbcDynamicTableSink} to avoid serialization issues with custom StatementExecutorFactory in Flink
 * 1.17. As a result, the {@code sink.ignore-null-when-update} option is NOT supported in this
 * version.
 */
@Internal
public class GaussdbJdbcDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "gaussdb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        InternalJdbcConnectionOptions jdbcOptions =
                getJdbcOptions(config, context.getClassLoader());
        JdbcExecutionOptions executionOptions = getJdbcExecutionOptions(config);
        JdbcDmlOptions dmlOptions =
                getJdbcDmlOptions(
                        jdbcOptions,
                        context.getPhysicalRowDataType(),
                        context.getPrimaryKeyIndexes());

        // Use standard JdbcDynamicTableSink to avoid serialization issues
        return new JdbcDynamicTableSink(
                jdbcOptions, executionOptions, dmlOptions, context.getPhysicalRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // For source, use the GaussDB source
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();

        InternalJdbcConnectionOptions jdbcOptions =
                getJdbcOptions(config, context.getClassLoader());

        // Use GaussDB DynamicTableSource
        return new GaussdbDynamicTableSource(
                jdbcOptions, getJdbcReadOptions(config), context.getPhysicalRowDataType());
    }

    private org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions getJdbcReadOptions(
            ReadableConfig config) {
        org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions.Builder builder =
                org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions.builder();
        builder.setFetchSize(
                config.get(
                        org.apache.flink.connector.jdbc.table.JdbcConnectorOptions
                                .SCAN_FETCH_SIZE));
        builder.setAutoCommit(
                config.get(
                        org.apache.flink.connector.jdbc.table.JdbcConnectorOptions
                                .SCAN_AUTO_COMMIT));
        return builder.build();
    }

    private InternalJdbcConnectionOptions getJdbcOptions(
            ReadableConfig readableConfig, ClassLoader classLoader) {
        final String url = readableConfig.get(URL);
        final InternalJdbcConnectionOptions.Builder builder =
                InternalJdbcConnectionOptions.builder()
                        .setClassLoader(classLoader)
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setDialect(new GaussdbDialect(url.contains("compatibleMode=mysql")))
                        .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                        .setConnectionCheckTimeoutSeconds(
                                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);

        return builder.build();
    }

    private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
        return builder.build();
    }

    private JdbcDmlOptions getJdbcDmlOptions(
            InternalJdbcConnectionOptions jdbcOptions, DataType dataType, int[] primaryKeyIndexes) {

        String[] keyFields =
                Arrays.stream(primaryKeyIndexes)
                        .mapToObj(i -> DataType.getFieldNames(dataType).get(i))
                        .toArray(String[]::new);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(DataType.getFieldNames(dataType).toArray(new String[0]))
                .withKeyFields(keyFields.length > 0 ? keyFields : null)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DRIVER);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(MAX_RETRY_TIMEOUT);
        return optionalOptions;
    }
}
