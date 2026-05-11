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

package org.apache.flink.cdc.connectors.gaussdb.source.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.SCHEMA_NAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for creating configured instance of {@link GaussDBTableSource}. */
public class GaussDBTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "gaussdb-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(GaussDBSourceOptions.PORT);
        Integer replicationPort =
                config.getOptional(GaussDBSourceOptions.REPLICATION_PORT).orElse(null);
        String pluginName = config.get(GaussDBSourceOptions.DECODING_PLUGIN_NAME);
        String slotName = config.get(GaussDBSourceOptions.SLOT_NAME);
        DebeziumChangelogMode changelogMode = config.get(GaussDBSourceOptions.CHANGELOG_MODE);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        if (changelogMode == DebeziumChangelogMode.UPSERT) {
            checkArgument(
                    physicalSchema.getPrimaryKey().isPresent(),
                    "Primary key must be present when upsert mode is selected.");
        }
        boolean enableParallelRead =
                config.get(GaussDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        StartupOptions startupOptions = getStartupOptions(config);
        int splitSize = config.get(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(JdbcSourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(JdbcSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(JdbcSourceOptions.CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        double distributionFactorUpper =
                config.get(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        Duration heartbeatInterval = config.get(GaussDBSourceOptions.HEARTBEAT_INTERVAL);
        String chunkKeyColumn =
                config.getOptional(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);

        boolean closeIdlerReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean isScanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        int lsnCommitCheckpointsDelay =
                config.get(GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        boolean assignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        // GaussDB-specific parameters
        int parallelDecodeNum = config.get(GaussDBSourceOptions.PARALLEL_DECODE_NUM);
        String decodeStyle = config.get(GaussDBSourceOptions.DECODE_STYLE);
        boolean sendingBatch = config.get(GaussDBSourceOptions.SENDING_BATCH);
        boolean includeDatabaseInTableId =
                config.get(GaussDBSourceOptions.TABLE_ID_INCLUDE_DATABASE);

        if (enableParallelRead) {
            validateIntegerOption(
                    JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(JdbcSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(JdbcSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
            validateIntegerOption(JdbcSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
            validateIntegerOption(JdbcSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
            validateDistributionFactorUpper(distributionFactorUpper);
            validateDistributionFactorLower(distributionFactorLower);
        }

        return new GaussDBTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                changelogMode,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                enableParallelRead,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                heartbeatInterval,
                startupOptions,
                chunkKeyColumn,
                closeIdlerReaders,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch,
                includeDatabaseInTableId,
                replicationPort);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        options.add(GaussDBSourceOptions.SLOT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GaussDBSourceOptions.PORT);
        options.add(GaussDBSourceOptions.REPLICATION_PORT);
        options.add(GaussDBSourceOptions.DECODING_PLUGIN_NAME);
        options.add(GaussDBSourceOptions.CHANGELOG_MODE);
        options.add(JdbcSourceOptions.SCAN_STARTUP_MODE);
        options.add(GaussDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(JdbcSourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(JdbcSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(JdbcSourceOptions.CONNECT_TIMEOUT);
        options.add(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        options.add(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        options.add(GaussDBSourceOptions.HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        // GaussDB-specific options
        options.add(GaussDBSourceOptions.PARALLEL_DECODE_NUM);
        options.add(GaussDBSourceOptions.DECODE_STYLE);
        options.add(GaussDBSourceOptions.SENDING_BATCH);
        options.add(GaussDBSourceOptions.TABLE_ID_INCLUDE_DATABASE);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_COMMITTED = "committed-offset";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(JdbcSourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_COMMITTED:
                return StartupOptions.committed();
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                JdbcSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_COMMITTED,
                                modeString));
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        JdbcSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
