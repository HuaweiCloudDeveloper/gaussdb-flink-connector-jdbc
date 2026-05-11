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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.gaussdbcdc.GaussDBCDCOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

/**
 * A CDC Source implementation for GaussDB that captures change events from GaussDB WAL.
 *
 * <p>This source supports:
 *
 * <ul>
 *   <li>Initial snapshot of the table
 *   <li>Continuous streaming of changes (INSERT, UPDATE, DELETE)
 *   <li>Exactly-once semantics with checkpointing
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>
 * GaussDBCDCSource source = GaussDBCDCSource.builder()
 *     .hostname("localhost")
 *     .port(8000)
 *     .database("test")
 *     .tableName("student")
 *     .username("root")
 *     .password("password")
 *     .build();
 * </pre>
 */
@PublicEvolving
public class GaussDBCDCSource
        implements Source<RowData, GaussDBSplit, GaussDBCheckpoint>, ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final String slotName;
    private final String pluginName;
    private final boolean snapshotMode;
    private final int chunkSize;
    private final int connectTimeoutMs;
    private final int pollIntervalMs;
    private final boolean walMode;
    private final String decodePlugin;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final String sslMode;

    private GaussDBCDCSource(Builder builder) {
        this.hostname = Preconditions.checkNotNull(builder.hostname, "hostname must not be null");
        this.port = builder.port;
        this.database = Preconditions.checkNotNull(builder.database, "database must not be null");
        this.schema = builder.schema;
        this.tableName =
                Preconditions.checkNotNull(builder.tableName, "tableName must not be null");
        this.username = Preconditions.checkNotNull(builder.username, "username must not be null");
        this.password = Preconditions.checkNotNull(builder.password, "password must not be null");
        this.slotName = builder.slotName;
        this.pluginName = builder.pluginName;
        this.snapshotMode = builder.snapshotMode;
        this.chunkSize = builder.chunkSize;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.walMode = builder.walMode;
        this.decodePlugin = builder.decodePlugin;
        this.parallelDecodeNum = builder.parallelDecodeNum;
        this.decodeStyle = builder.decodeStyle;
        this.sendingBatch = builder.sendingBatch;
        this.sslMode = builder.sslMode;
    }

    @Override
    public Boundedness getBoundedness() {
        // CDC source is unbounded (continuous streaming)
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, GaussDBSplit> createReader(SourceReaderContext readerContext) {
        return new GaussDBSourceReader(
                readerContext,
                hostname,
                port,
                database,
                schema,
                tableName,
                username,
                password,
                slotName,
                pluginName,
                pollIntervalMs,
                walMode,
                decodePlugin,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch,
                sslMode);
    }

    @Override
    public SplitEnumerator<GaussDBSplit, GaussDBCheckpoint> createEnumerator(
            SplitEnumeratorContext<GaussDBSplit> enumContext) {
        return new GaussDBSplitEnumerator(
                enumContext,
                hostname,
                port,
                database,
                schema,
                tableName,
                username,
                password,
                snapshotMode,
                chunkSize,
                connectTimeoutMs,
                sslMode);
    }

    @Override
    public SplitEnumerator<GaussDBSplit, GaussDBCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<GaussDBSplit> enumContext, GaussDBCheckpoint checkpoint) {
        return new GaussDBSplitEnumerator(
                enumContext,
                checkpoint,
                hostname,
                port,
                database,
                schema,
                tableName,
                username,
                password,
                snapshotMode,
                chunkSize,
                connectTimeoutMs,
                sslMode);
    }

    @Override
    public SimpleVersionedSerializer<GaussDBSplit> getSplitSerializer() {
        return new GaussDBSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<GaussDBCheckpoint> getEnumeratorCheckpointSerializer() {
        return new GaussDBCheckpointSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        // Return RowData type information
        return TypeInformation.of(RowData.class);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for GaussDBCDCSource. */
    public static class Builder {
        private String hostname;
        private int port = GaussDBCDCOptions.PORT.defaultValue();
        private String database;
        private String schema = GaussDBCDCOptions.SCHEMA.defaultValue();
        private String tableName;
        private String username;
        private String password;
        private String slotName = GaussDBCDCOptions.SLOT_NAME.defaultValue();
        private String pluginName = GaussDBCDCOptions.PLUGIN_NAME.defaultValue();
        private boolean snapshotMode = GaussDBCDCOptions.SNAPSHOT_MODE.defaultValue();
        private int chunkSize = GaussDBCDCOptions.CHUNK_SIZE.defaultValue();
        private int connectTimeoutMs = GaussDBCDCOptions.CONNECT_TIMEOUT_MS.defaultValue();
        private int pollIntervalMs = GaussDBCDCOptions.POLL_INTERVAL_MS.defaultValue();
        private boolean walMode = GaussDBCDCOptions.WAL_MODE.defaultValue();
        private String decodePlugin = GaussDBCDCOptions.DECODE_PLUGIN.defaultValue();
        private int parallelDecodeNum = GaussDBCDCOptions.PARALLEL_DECODE_NUM.defaultValue();
        private String decodeStyle = GaussDBCDCOptions.DECODE_STYLE.defaultValue();
        private boolean sendingBatch = GaussDBCDCOptions.SENDING_BATCH.defaultValue();
        private String sslMode = GaussDBCDCOptions.SSL_MODE.defaultValue();

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        public Builder pluginName(String pluginName) {
            this.pluginName = pluginName;
            return this;
        }

        public Builder snapshotMode(boolean snapshotMode) {
            this.snapshotMode = snapshotMode;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder pollIntervalMs(int pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        public Builder walMode(boolean walMode) {
            this.walMode = walMode;
            return this;
        }

        public Builder decodePlugin(String decodePlugin) {
            this.decodePlugin = decodePlugin;
            return this;
        }

        public Builder parallelDecodeNum(int parallelDecodeNum) {
            this.parallelDecodeNum = parallelDecodeNum;
            return this;
        }

        public Builder decodeStyle(String decodeStyle) {
            this.decodeStyle = decodeStyle;
            return this;
        }

        public Builder sendingBatch(boolean sendingBatch) {
            this.sendingBatch = sendingBatch;
            return this;
        }

        public Builder sslMode(String sslMode) {
            this.sslMode = sslMode;
            return this;
        }

        public GaussDBCDCSource build() {
            return new GaussDBCDCSource(this);
        }
    }
}
