/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the
 * "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.enumerator.GaussDBSourceEnumerator;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.reader.GaussDBSourceRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The source builder for GaussDBIncrementalSource. */
public class GaussDBSourceBuilder<T> {

    private final GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    private GaussDBSourceBuilder() {}

    public GaussDBSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    public GaussDBSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    public GaussDBSourceBuilder<T> database(String database) {
        this.configFactory.database(database);
        return this;
    }

    public GaussDBSourceBuilder<T> schemaList(String... schemaList) {
        this.configFactory.schemaList(schemaList);
        return this;
    }

    public GaussDBSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    public GaussDBSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    public GaussDBSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    public GaussDBSourceBuilder<T> decodingPluginName(String name) {
        this.configFactory.decodingPluginName(name);
        return this;
    }

    public GaussDBSourceBuilder<T> slotName(String slotName) {
        this.configFactory.slotName(slotName);
        return this;
    }

    public GaussDBSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    public GaussDBSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    public GaussDBSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    public GaussDBSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    public GaussDBSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    public GaussDBSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    public GaussDBSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    public GaussDBSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    public GaussDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    public GaussDBSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    public GaussDBSourceBuilder<T> debeziumProperties(Properties properties) {
        this.configFactory.debeziumProperties(properties);
        return this;
    }

    public GaussDBSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    public GaussDBSourceBuilder<T> heartbeatInterval(Duration heartbeatInterval) {
        this.configFactory.heartbeatInterval(heartbeatInterval);
        return this;
    }

    public GaussDBSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    public GaussDBSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    public GaussDBSourceBuilder<T> lsnCommitCheckpointsDelay(int lsnCommitDelay) {
        this.configFactory.setLsnCommitCheckpointsDelay(lsnCommitDelay);
        return this;
    }

    public GaussDBSourceBuilder<T> includeDatabaseInTableId(boolean includeDatabaseInTableId) {
        this.configFactory.setIncludeDatabaseInTableId(includeDatabaseInTableId);
        return this;
    }

    public GaussDBSourceBuilder<T> parallelDecodeNum(int parallelDecodeNum) {
        this.configFactory.setParallelDecodeNum(parallelDecodeNum);
        return this;
    }

    public GaussDBSourceBuilder<T> decodeStyle(String decodeStyle) {
        this.configFactory.setDecodeStyle(decodeStyle);
        return this;
    }

    public GaussDBSourceBuilder<T> sendingBatch(boolean sendingBatch) {
        this.configFactory.setSendingBatch(sendingBatch);
        return this;
    }

    public GaussDBSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public GaussDBIncrementalSource<T> build() {
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));
        return new GaussDBIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    /** The GaussDB source based on the incremental snapshot framework. */
    public static class GaussDBIncrementalSource<T> extends JdbcIncrementalSource<T> {

        public GaussDBIncrementalSource(
                GaussDBSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                GaussDBOffsetFactory offsetFactory,
                GaussDBDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }

        @Override
        public GaussDBSourceEnumerator createEnumerator(
                org.apache.flink.api.connector.source.SplitEnumeratorContext<SourceSplitBase>
                        enumContext) {
            final SplitAssigner splitAssigner;
            GaussDBSourceConfig sourceConfig = (GaussDBSourceConfig) configFactory.create(0);
            if (!sourceConfig.getStartupOptions().isStreamOnly()) {
                try {
                    final List<TableId> remainingTables =
                            dataSourceDialect.discoverDataCollections(sourceConfig);
                    boolean isTableIdCaseSensitive =
                            dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                    splitAssigner =
                            new HybridSplitAssigner<>(
                                    sourceConfig,
                                    enumContext.currentParallelism(),
                                    remainingTables,
                                    isTableIdCaseSensitive,
                                    dataSourceDialect,
                                    offsetFactory,
                                    enumContext);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(
                            "Failed to discover captured tables for enumerator", e);
                }
            } else {
                splitAssigner =
                        new StreamSplitAssigner(
                                sourceConfig, dataSourceDialect, offsetFactory, enumContext);
            }
            return new GaussDBSourceEnumerator(
                    enumContext, sourceConfig, splitAssigner, getBoundedness());
        }

        @Override
        public GaussDBSourceEnumerator restoreEnumerator(
                org.apache.flink.api.connector.source.SplitEnumeratorContext<SourceSplitBase>
                        enumContext,
                PendingSplitsState checkpoint) {
            final SplitAssigner splitAssigner;
            GaussDBSourceConfig sourceConfig = (GaussDBSourceConfig) configFactory.create(0);
            if (checkpoint instanceof HybridPendingSplitsState) {
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                (HybridPendingSplitsState) checkpoint,
                                dataSourceDialect,
                                offsetFactory,
                                enumContext);
            } else if (checkpoint instanceof StreamPendingSplitsState) {
                splitAssigner =
                        new StreamSplitAssigner(
                                sourceConfig,
                                (StreamPendingSplitsState) checkpoint,
                                dataSourceDialect,
                                offsetFactory,
                                enumContext);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported restored PendingSplitsState: " + checkpoint);
            }
            return new GaussDBSourceEnumerator(
                    enumContext, sourceConfig, splitAssigner, getBoundedness());
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new GaussDBSourceRecordEmitter<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    sourceConfig.isIncludeSchemaChanges(),
                    offsetFactory);
        }

        public static <T> GaussDBSourceBuilder<T> builder() {
            return new GaussDBSourceBuilder<>();
        }
    }
}
