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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.decoder.MppdbBinaryMessageDecoder;
import org.apache.flink.cdc.connectors.gaussdb.source.decoder.MppdbDecodingMessageDecoder;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBStreamFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.GaussDBQueryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.TopicSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/** The dialect for GaussDB, extends PostgreSQL dialect behavior. */
public class GaussDBDialect implements JdbcDataSourceDialect {
    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "gaussdb-cdc-connector";
    private static final Logger LOG = LoggerFactory.getLogger(GaussDBDialect.class);

    private final GaussDBSourceConfig sourceConfig;
    @Nullable private GaussDBStreamFetchTask streamFetchTask;

    public GaussDBDialect(GaussDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        GaussDBSourceConfig gaussDBSourceConfig = (GaussDBSourceConfig) sourceConfig;
        PostgresConnectorConfig dbzConfig = gaussDBSourceConfig.getDbzConnectorConfig();

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbc =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(), valueConverterBuilder, CONNECTION_NAME);

        skipVersionValidation(jdbc);

        try {
            jdbc.connect();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    /**
     * Replaces the initial validation operation in JdbcConnection with a no-op.
     *
     * <p>GaussDB's JDBC driver reports database version as 9.2.4 for compatibility, but GaussDB
     * fully supports PostgreSQL 9.4+ features including logical replication. The default
     * validateServerVersion check would reject this version, so we bypass it using Unsafe to
     * reliably modify the final field across all JDK versions.
     */
    public static void skipVersionValidation(JdbcConnection connection) {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

            Field initialOpsField = JdbcConnection.class.getDeclaredField("initialOps");
            long offset = unsafe.objectFieldOffset(initialOpsField);
            unsafe.putObject(connection, offset, (JdbcConnection.Operations) statement -> {});
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to skip version validation for GaussDB connection", e);
        }
    }

    public PostgresReplicationConnection openReplicationConnection(
            PostgresConnection jdbcConnection) {
        try {
            PostgresConnectorConfig pgConnectorConfig = sourceConfig.getDbzConnectorConfig();

            // If a dedicated replication port is configured (e.g. GaussDB HA port when
            // enable_thread_pool=on), derive a new PostgresConnectorConfig whose
            // database.port is overridden. The regular JDBC connection (jdbcConnection)
            // keeps using the main port for discovery/snapshot queries.
            Integer replicationPort = sourceConfig.getReplicationPort();
            if (replicationPort != null && replicationPort != sourceConfig.getPort()) {
                io.debezium.config.Configuration replConfig =
                        pgConnectorConfig
                                .getConfig()
                                .edit()
                                .with("database.port", String.valueOf(replicationPort))
                                .build();
                pgConnectorConfig = new PostgresConnectorConfig(replConfig);
                LOG.info(
                        "Using dedicated replication port {} for GaussDB streaming (main JDBC port = {}).",
                        replicationPort,
                        sourceConfig.getPort());
            }

            TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(pgConnectorConfig);
            PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                    newPostgresValueConverterBuilder(pgConnectorConfig);
            PostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            pgConnectorConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
            PostgresTaskContext taskContext =
                    PostgresObjectUtils.newTaskContext(pgConnectorConfig, schema, topicSelector);
            PostgresReplicationConnection replConn =
                    (PostgresReplicationConnection)
                            createReplicationConnection(
                                    taskContext, jdbcConnection, false, pgConnectorConfig);

            // If using mppdb_decoding plugin, replace Debezium's PgOutputMessageDecoder
            // with our custom MppdbDecodingMessageDecoder that can parse mppdb_decoding JSON
            // output.
            if ("mppdb_decoding".equals(getPluginName())) {
                replaceMessageDecoder(replConn);
            }

            return replConn;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize GaussDB replication connection", e);
        }
    }

    /**
     * Replaces the {@code messageDecoder} field in {@link PostgresReplicationConnection} with the
     * appropriate mppdb_decoding decoder based on the {@code decode-style} configuration.
     *
     * <p>decode-style='j' (JSON) uses {@link MppdbDecodingMessageDecoder}, decode-style='b'
     * (binary) uses {@link MppdbBinaryMessageDecoder}. This is necessary because Debezium's {@code
     * LogicalDecoder} enum does not have a MPPDB_DECODING constant.
     */
    private void replaceMessageDecoder(PostgresReplicationConnection replConn) {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

            Field decoderField =
                    PostgresReplicationConnection.class.getDeclaredField("messageDecoder");
            long offset = unsafe.objectFieldOffset(decoderField);

            int parallelDecodeNum = sourceConfig.getParallelDecodeNum();
            String decodeStyle = sourceConfig.getDecodeStyle();
            Object decoder;
            // In serial mode (parallelDecodeNum <= 1), mppdb_decoding always outputs
            // JSON format regardless of the decode-style parameter. Only in parallel
            // mode (parallelDecodeNum > 1) does the decode-style parameter take effect.
            if (parallelDecodeNum > 1 && "b".equals(decodeStyle)) {
                String schemaName =
                        sourceConfig.getDbzProperties().getProperty("schema.include.list");
                String databaseName =
                        sourceConfig.getDbzProperties().getProperty("database.dbname");
                decoder = new MppdbBinaryMessageDecoder(schemaName, databaseName);
                LOG.info(
                        "Replaced messageDecoder with MppdbBinaryMessageDecoder for mppdb_decoding "
                                + "(parallel-decode-num={}, decode-style=b, schemaName={}, catalogName={})",
                        parallelDecodeNum,
                        schemaName,
                        databaseName);
            } else {
                // Pass the expected schema name so that the decoder can correct
                // the schema in mppdb_decoding serial mode output (e.g., root.table ->
                // public.table)
                String schemaName =
                        sourceConfig.getDbzProperties().getProperty("schema.include.list");
                String databaseName =
                        sourceConfig.getDbzProperties().getProperty("database.dbname");
                decoder = new MppdbDecodingMessageDecoder(schemaName, databaseName);
                LOG.info(
                        "Replaced messageDecoder with MppdbDecodingMessageDecoder for mppdb_decoding "
                                + "(parallel-decode-num={}, decode-style={}, schemaName={})",
                        parallelDecodeNum,
                        decodeStyle,
                        schemaName);
            }
            unsafe.putObject(replConn, offset, decoder);

            // Replace the plugin field's decoderName from "decoderbufs" to "mppdb_decoding"
            // so that initReplicationSlot/createReplicationSlot uses the correct plugin name
            // when creating the logical replication slot on GaussDB.
            // We use plugin.getClass().getSuperclass() instead of LogicalDecoder.class to avoid
            // Flink classloader issues where the compile-time class reference may differ from
            // the runtime class loaded by Flink's child-first classloader.
            Field pluginField = PostgresReplicationConnection.class.getDeclaredField("plugin");
            long pluginOffset = unsafe.objectFieldOffset(pluginField);
            Object plugin = unsafe.getObject(replConn, pluginOffset);
            if (plugin != null) {
                Field decoderNameField =
                        plugin.getClass().getSuperclass().getDeclaredField("decoderName");
                long decoderNameOffset = unsafe.objectFieldOffset(decoderNameField);
                String currentName = (String) unsafe.getObject(plugin, decoderNameOffset);
                if (!"mppdb_decoding".equals(currentName)) {
                    unsafe.putObject(plugin, decoderNameOffset, "mppdb_decoding");
                    LOG.info(
                            "Replaced plugin decoderName from '{}' to 'mppdb_decoding'",
                            currentName);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to replace message decoder for mppdb_decoding plugin", e);
        }
    }

    @Override
    public String getName() {
        return "GaussDB";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return GaussDBQueryUtils.currentOffset((PostgresConnection) jdbc);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public Offset displayCommittedOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return GaussDBQueryUtils.committedOffset(
                    (PostgresConnection) jdbc, getSlotName(), getPluginName());
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new org.apache.flink.cdc.connectors.gaussdb.source.GaussDBChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new org.apache.flink.cdc.connectors.gaussdb.source.GaussDBChunkSplitter(
                sourceConfig, this, chunkSplitterState);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return GaussDBQueryUtils.listTables(
                    sourceConfig.getDatabaseList().get(0),
                    jdbc,
                    sourceConfig.getTableFilters().dataCollectionFilter());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error discovering tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return GaussDBQueryUtils.queryTableSchema(jdbc, capturedTableIds);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error discovering table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new GaussDBConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        return GaussDBQueryUtils.queryTableSchema(jdbc, tableId);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new GaussDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new GaussDBStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new GaussDBSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        return sourceConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId);
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        // Return the real plugin name (e.g., mppdb_decoding) stored separately
        // from the Debezium-facing plugin.name (which is always pgoutput).
        String realName = sourceConfig.getDbzProperties().getProperty("real.plugin.name");
        if (realName != null) {
            return realName;
        }
        return sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name());
    }

    public boolean removeSlot(String slotName) {
        try (PostgresConnection jdbc = (PostgresConnection) openJdbcConnection(sourceConfig)) {
            return jdbc.dropReplicationSlot(slotName);
        } catch (Exception e) {
            return false;
        }
    }
}
