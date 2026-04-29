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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a GaussDB source from a logical
 * description.
 */
public class GaussDBTableSource implements ScanTableSource {

    private final ResolvedSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String schemaName;
    private final String tableName;
    private final String username;
    private final String password;
    private final String pluginName;
    private final String slotName;
    private final DebeziumChangelogMode changelogMode;
    private final Properties dbzProperties;
    private final boolean enableParallelRead;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final Duration heartbeatInterval;
    private final StartupOptions startupOptions;
    private final String chunkKeyColumn;
    private final boolean closeIdleReaders;
    private final boolean skipSnapshotBackfill;
    private final boolean scanNewlyAddedTableEnabled;
    private final int lsnCommitCheckpointsDelay;
    private final boolean assignUnboundedChunkFirst;
    // GaussDB-specific parameters
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final boolean includeDatabaseInTableId;

    // Mutable attributes
    protected org.apache.flink.table.types.DataType producedDataType;
    protected java.util.List<String> metadataKeys;

    public GaussDBTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String schemaName,
            String tableName,
            String username,
            String password,
            String pluginName,
            String slotName,
            DebeziumChangelogMode changelogMode,
            Properties dbzProperties,
            boolean enableParallelRead,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            Duration heartbeatInterval,
            StartupOptions startupOptions,
            @Nullable String chunkKeyColumn,
            boolean closeIdleReaders,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            int lsnCommitCheckpointsDelay,
            boolean assignUnboundedChunkFirst,
            int parallelDecodeNum,
            String decodeStyle,
            boolean sendingBatch,
            boolean includeDatabaseInTableId) {
        this.physicalSchema = physicalSchema;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.schemaName = checkNotNull(schemaName);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.pluginName = checkNotNull(pluginName);
        this.slotName = slotName;
        this.changelogMode = changelogMode;
        this.dbzProperties = dbzProperties;
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.heartbeatInterval = heartbeatInterval;
        this.startupOptions = startupOptions;
        this.chunkKeyColumn = chunkKeyColumn;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
        this.closeIdleReaders = closeIdleReaders;
        this.skipSnapshotBackfill = skipSnapshotBackfill;
        this.scanNewlyAddedTableEnabled = isScanNewlyAddedTableEnabled;
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
        this.parallelDecodeNum = parallelDecodeNum;
        this.decodeStyle = decodeStyle;
        this.sendingBatch = sendingBatch;
        this.includeDatabaseInTableId = includeDatabaseInTableId;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        switch (changelogMode) {
            case UPSERT:
                return ChangelogMode.upsert();
            case ALL:
                return ChangelogMode.all();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported changelog mode: " + changelogMode);
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);

        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setResultTypeInfo(typeInfo)
                        .setChangelogMode(changelogMode)
                        .build();

        JdbcIncrementalSource<RowData> parallelSource =
                GaussDBSourceBuilder.GaussDBIncrementalSource.<RowData>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .schemaList(schemaName)
                        .tableList(schemaName + "." + tableName)
                        .username(username)
                        .password(password)
                        .decodingPluginName(pluginName)
                        .slotName(slotName)
                        .debeziumProperties(dbzProperties)
                        .deserializer(deserializer)
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .startupOptions(startupOptions)
                        .chunkKeyColumn(chunkKeyColumn)
                        .heartbeatInterval(heartbeatInterval)
                        .closeIdleReaders(closeIdleReaders)
                        .skipSnapshotBackfill(skipSnapshotBackfill)
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                        .lsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay)
                        .parallelDecodeNum(parallelDecodeNum)
                        .decodeStyle(decodeStyle)
                        .sendingBatch(sendingBatch)
                        .includeDatabaseInTableId(includeDatabaseInTableId)
                        .build();
        return SourceProvider.of(parallelSource);
    }

    @Override
    public DynamicTableSource copy() {
        GaussDBTableSource source =
                new GaussDBTableSource(
                        physicalSchema,
                        port,
                        hostname,
                        database,
                        schemaName,
                        tableName,
                        username,
                        password,
                        pluginName,
                        slotName,
                        changelogMode,
                        dbzProperties,
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
                        closeIdleReaders,
                        skipSnapshotBackfill,
                        scanNewlyAddedTableEnabled,
                        lsnCommitCheckpointsDelay,
                        assignUnboundedChunkFirst,
                        parallelDecodeNum,
                        decodeStyle,
                        sendingBatch,
                        includeDatabaseInTableId);
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GaussDBTableSource that = (GaussDBTableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(pluginName, that.pluginName)
                && Objects.equals(slotName, that.slotName)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(changelogMode, that.changelogMode)
                && Objects.equals(enableParallelRead, that.enableParallelRead)
                && Objects.equals(splitSize, that.splitSize)
                && Objects.equals(splitMetaGroupSize, that.splitMetaGroupSize)
                && Objects.equals(fetchSize, that.fetchSize)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(distributionFactorUpper, that.distributionFactorUpper)
                && Objects.equals(distributionFactorLower, that.distributionFactorLower)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(chunkKeyColumn, that.chunkKeyColumn)
                && Objects.equals(closeIdleReaders, that.closeIdleReaders)
                && Objects.equals(skipSnapshotBackfill, that.skipSnapshotBackfill)
                && Objects.equals(scanNewlyAddedTableEnabled, that.scanNewlyAddedTableEnabled)
                && Objects.equals(lsnCommitCheckpointsDelay, that.lsnCommitCheckpointsDelay)
                && Objects.equals(assignUnboundedChunkFirst, that.assignUnboundedChunkFirst)
                && Objects.equals(parallelDecodeNum, that.parallelDecodeNum)
                && Objects.equals(decodeStyle, that.decodeStyle)
                && Objects.equals(sendingBatch, that.sendingBatch)
                && Objects.equals(includeDatabaseInTableId, that.includeDatabaseInTableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostname,
                database,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                dbzProperties,
                producedDataType,
                metadataKeys,
                changelogMode,
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
                closeIdleReaders,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch,
                includeDatabaseInTableId);
    }

    @Override
    public String asSummaryString() {
        return "GaussDB-CDC";
    }
}
