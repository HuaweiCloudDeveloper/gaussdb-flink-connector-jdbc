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

package org.apache.flink.cdc.connectors.gaussdb.source.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.decoder.MppdbBinaryMessageDecoder;
import org.apache.flink.cdc.connectors.gaussdb.source.decoder.MppdbDecodingMessageDecoder;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Map;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.DROP_SLOT_ON_STOP;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/** The context of {@link GaussDBScanFetchTask} and {@link GaussDBStreamFetchTask}. */
public class GaussDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final String CONNECTION_NAME = "gaussdb-fetch-task-connection";

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceFetchTaskContext.class);

    private PostgresTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresConnection jdbcConnection;
    private ReplicationConnection replicationConnection;
    private PostgresOffsetContext offsetContext;
    private PostgresPartition partition;
    private PostgresSchema schema;
    private ErrorHandler errorHandler;
    private PostgresEventDispatcher<TableId> eventDispatcher;
    private EventMetadataProvider metadataProvider;
    private SnapshotChangeEventSourceMetrics<PostgresPartition> snapshotChangeEventSourceMetrics;
    private Snapshotter snapShotter;

    private final GaussDBDialect dialect;

    public GaussDBSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, GaussDBDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.dialect = dataSourceDialect;
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return (PostgresConnectorConfig) super.getDbzConnectorConfig();
    }

    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? new GaussDBOffsetFactory().createInitialOffset()
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return GaussDBOffsetUtils.getPostgresOffsetContext(loader, offset);
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        LOG.debug("Configuring GaussDBSourceFetchTaskContext for split: {}", sourceSplitBase);
        PostgresConnectorConfig dbzConfig = getDbzConnectorConfig();

        // If a dedicated replication port is configured (e.g. GaussDB HA port when
        // enable_thread_pool=on), override database.port so that taskContext and
        // replicationConnection built below use the replication port, while regular
        // JDBC queries keep using the main port (via sourceConfig.jdbcConfig).
        Integer replicationPort = ((GaussDBSourceConfig) sourceConfig).getReplicationPort();
        boolean needReplicationPortOverride =
                replicationPort != null && replicationPort.intValue() != sourceConfig.getPort();

        // Register the main business port as a JVM-wide override for plain JDBC
        // PostgresConnection instances. This ensures Debezium's
        // PostgresReplicationConnection internals (getSlotInfo, close->Debezium Drop Slot,
        // heartbeat, etc.) that internally do `new
        // PostgresConnection(connectorConfig.getJdbcConfig())`
        // with the HA port get rewritten to the main port, which is the only port that
        // accepts non-replication JDBC traffic under GaussDB enable_thread_pool=on.
        // The replication stream on PostgresReplicationConnection itself keeps using the HA
        // port (it applies its own addDefaultSettings independently).
        if (needReplicationPortOverride) {
            PostgresConnection.setMainJdbcPortOverride(sourceConfig.getPort());
        } else {
            PostgresConnection.setMainJdbcPortOverride(null);
        }

        if (sourceSplitBase instanceof SnapshotSplit) {
            io.debezium.config.Configuration.Builder snapshotBuilder =
                    dbzConfig
                            .getConfig()
                            .edit()
                            .with(
                                    "table.include.list",
                                    getTableList(((SnapshotSplit) sourceSplitBase).getTableId()))
                            .with(
                                    SLOT_NAME.name(),
                                    ((GaussDBSourceConfig) sourceConfig)
                                            .getSlotNameForBackfillTask())
                            // drop slot for backfill stream split
                            .with(DROP_SLOT_ON_STOP.name(), true)
                            // Disable heartbeat event in snapshot split fetcher
                            .with(Heartbeat.HEARTBEAT_INTERVAL, 0);
            if (needReplicationPortOverride) {
                snapshotBuilder.with("database.port", String.valueOf(replicationPort));
                LOG.info(
                        "Using dedicated replication port {} for GaussDB streaming (main JDBC port = {}) in snapshot split.",
                        replicationPort,
                        sourceConfig.getPort());
            }
            dbzConfig = new PostgresConnectorConfig(snapshotBuilder.build());
        } else {
            io.debezium.config.Configuration.Builder builder = dbzConfig.getConfig().edit();
            if (isBackFillSplit(sourceSplitBase)) {
                builder.with(
                        "table.include.list",
                        getTableList(
                                sourceSplitBase
                                        .asStreamSplit()
                                        .getTableSchemas()
                                        .keySet()
                                        .iterator()
                                        .next()));
            }
            if (needReplicationPortOverride) {
                builder.with("database.port", String.valueOf(replicationPort));
                LOG.info(
                        "Using dedicated replication port {} for GaussDB streaming (main JDBC port = {}) in stream split.",
                        replicationPort,
                        sourceConfig.getPort());
            }
            dbzConfig =
                    new PostgresConnectorConfig(
                            builder
                                    // never drop slot for stream split
                                    .with(DROP_SLOT_ON_STOP.name(), false)
                                    .build());
        }
        setDbzConnectorConfig(dbzConfig);

        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        dbzConfig.getConfig().getString(SNAPSHOT_MODE));
        this.snapShotter = snapshotMode.getSnapshotter(dbzConfig.getConfig());

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        this.jdbcConnection =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        CONNECTION_NAME,
                        new org.apache.flink.cdc.connectors.base.relational.connection
                                .JdbcConnectionFactory(
                                sourceConfig, dialect.getPooledDataSourceFactory()));

        GaussDBDialect.skipVersionValidation(jdbcConnection);

        try {
            jdbcConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to GaussDB", e);
        }

        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);

        try {
            String tableIncludeList = dbzConfig.tableIncludeList();
            String schemaIncludeList = dbzConfig.getConfig().getString("schema.include.list");
            String databaseIncludeList = dbzConfig.getConfig().getString("database.include.list");
            LOG.info(
                    "Initializing PostgresSchema with table schemas: {}, table.include.list={}, schema.include.list={}, database.include.list={}",
                    sourceSplitBase.getTableSchemas().keySet(),
                    tableIncludeList,
                    schemaIncludeList,
                    databaseIncludeList);
            this.schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()),
                            sourceSplitBase.getTableSchemas().values());
            LOG.info("PostgresSchema after newSchema(): tableIds={}", schema.tableIds());

            // Debug: check if schemaFor returns anything for our tables
            for (TableId tid : sourceSplitBase.getTableSchemas().keySet()) {
                LOG.info(
                        "schemaFor({}) = {}, tableFor({}) = {}",
                        tid,
                        schema.schemaFor(tid),
                        tid,
                        schema.tableFor(tid));
            }

            // PostgresObjectUtils.newSchema() calls buildAndRegisterSchema() which
            // populates schemasByTableId. However, the Table objects from
            // queryTableSchema() have 2-part TableIds (without catalog) because
            // GaussDB JDBC's readSchema() doesn't include the catalog in TableId.
            // The snapshot split uses 3-part TableIds (with catalog), so lookups
            // like tableFor() and schemaFor() fail because TableId.equals() compares
            // all three parts.
            //
            // Fix: For each table change, rebuild the Table with the correct 3-part
            // TableId (including the catalog/database name), then register it in
            // both the Tables map and schemasByTableId.
            String databaseName = dbzConfig.getJdbcConfig().getDatabase();
            try {
                java.lang.reflect.Method tablesMethod =
                        io.debezium.relational.RelationalDatabaseSchema.class.getDeclaredMethod(
                                "tables");
                tablesMethod.setAccessible(true);
                io.debezium.relational.Tables tables =
                        (io.debezium.relational.Tables) tablesMethod.invoke(schema);

                // Also get the schemasByTableId field to register schemas
                java.lang.reflect.Field schemasByTableIdField =
                        io.debezium.relational.RelationalDatabaseSchema.class.getDeclaredField(
                                "schemasByTableId");
                schemasByTableIdField.setAccessible(true);
                Object schemasByTableId = schemasByTableIdField.get(schema);
                java.lang.reflect.Method putMethod =
                        schemasByTableId
                                .getClass()
                                .getDeclaredMethod(
                                        "put",
                                        io.debezium.relational.TableId.class,
                                        io.debezium.relational.TableSchema.class);
                putMethod.setAccessible(true);

                // Get the schemaBuilder to create TableSchema objects
                java.lang.reflect.Field schemaBuilderField =
                        io.debezium.relational.RelationalDatabaseSchema.class.getDeclaredField(
                                "schemaBuilder");
                schemaBuilderField.setAccessible(true);
                io.debezium.relational.TableSchemaBuilder schemaBuilder =
                        (io.debezium.relational.TableSchemaBuilder) schemaBuilderField.get(schema);

                // Get schemaPrefix
                java.lang.reflect.Field schemaPrefixField =
                        io.debezium.relational.RelationalDatabaseSchema.class.getDeclaredField(
                                "schemaPrefix");
                schemaPrefixField.setAccessible(true);
                String schemaPrefix = (String) schemaPrefixField.get(schema);

                // Get the tableFilter used in buildAndRegisterSchema
                java.lang.reflect.Field tableFilterField =
                        io.debezium.relational.RelationalDatabaseSchema.class.getDeclaredField(
                                "tableFilter");
                tableFilterField.setAccessible(true);
                io.debezium.relational.Tables.TableFilter tableFilter =
                        (io.debezium.relational.Tables.TableFilter) tableFilterField.get(schema);

                for (io.debezium.relational.history.TableChanges.TableChange tableChange :
                        sourceSplitBase.getTableSchemas().values()) {
                    io.debezium.relational.Table table = tableChange.getTable();
                    TableId originalId = table.id();
                    LOG.info(
                            "Processing tableChange: originalId={}, catalog={}, schema={}, table={}",
                            originalId,
                            originalId.catalog(),
                            originalId.schema(),
                            originalId.table());

                    // GaussDB JDBC's readSchema() may report the schema name
                    // as the catalog name (e.g., TableId("public", null, "test_cdc"))
                    // instead of the expected format
                    // TableId("postgres", "public", "test_cdc"). We need to
                    // detect this and rebuild the table with the correct TableId.
                    TableId correctId = originalId;
                    boolean needsRebuild = false;
                    if (!databaseName.equals(originalId.catalog()) && originalId.schema() == null) {
                        // catalog has the schema name, schema is null
                        // -> rebuild as TableId(databaseName, schemaName, table)
                        correctId =
                                new TableId(databaseName, originalId.catalog(), originalId.table());
                        needsRebuild = true;
                    } else if (originalId.catalog() == null || originalId.catalog().isEmpty()) {
                        // catalog is empty, schema has the schema name
                        // -> rebuild as TableId(databaseName, schemaName, table)
                        correctId =
                                new TableId(databaseName, originalId.schema(), originalId.table());
                        needsRebuild = true;
                    }
                    if (needsRebuild) {
                        table = table.edit().tableId(correctId).create();
                        LOG.info(
                                "Rebuilt table with correct TableId: {} -> {}",
                                originalId,
                                correctId);
                    }

                    // Register the table in the Tables map
                    tables.overwriteTable(table);

                    // Build and register the TableSchema if not already present
                    if (tableFilter.isIncluded(correctId) && schema.schemaFor(correctId) == null) {
                        io.debezium.relational.TableSchema tableSchema =
                                schemaBuilder.create(
                                        schemaPrefix,
                                        "gaussdb_cdc_source",
                                        table,
                                        dbzConfig.getColumnFilter(),
                                        io.debezium.relational.mapping.ColumnMappers.create(
                                                dbzConfig),
                                        dbzConfig.getKeyMapper());
                        putMethod.invoke(schemasByTableId, correctId, tableSchema);
                        LOG.info("Registered TableSchema for {} in schemasByTableId", correctId);
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(
                        "Failed to register tables in PostgresSchema via reflection", e);
            }
            LOG.info(
                    "PostgresSchema after registration: tableIds={}, tableFor={}",
                    schema.tableIds(),
                    schema.tableFor(sourceSplitBase.getTableSchemas().keySet().iterator().next()));

            // Verify filter matching for each table
            io.debezium.relational.Tables.TableFilter filter =
                    dbzConfig.getTableFilters().dataCollectionFilter();
            for (TableId tid : sourceSplitBase.getTableSchemas().keySet()) {
                LOG.info("TableFilter.isIncluded({}) = {}", tid, filter.isIncluded(tid));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresSchema", e);
        }

        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(dbzConfig), sourceSplitBase);
        this.partition = new PostgresPartition(dbzConfig.getLogicalName());
        this.taskContext = PostgresObjectUtils.newTaskContext(dbzConfig, schema, topicSelector);

        if (replicationConnection == null) {
            try {
                replicationConnection =
                        createReplicationConnection(
                                this.taskContext,
                                jdbcConnection,
                                this.snapShotter.shouldSnapshot(),
                                dbzConfig);
                // If using mppdb_decoding, replace the message decoder
                if ("mppdb_decoding".equals(getPluginName())) {
                    replaceMessageDecoder();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to create replication connection", e);
            }
        }

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(dbzConfig.getPollInterval())
                        .maxBatchSize(dbzConfig.getMaxBatchSize())
                        .maxQueueSize(dbzConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(dbzConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "gaussdb-cdc-connector-task"))
                        .build();

        this.errorHandler = new PostgresErrorHandler(getDbzConnectorConfig(), queue);
        this.metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        PostgresConnectorConfig finalDbzConfig = dbzConfig;
        try {
            this.eventDispatcher =
                    new PostgresEventDispatcher<>(
                            finalDbzConfig,
                            topicSelector,
                            schema,
                            queue,
                            finalDbzConfig.getTableFilters().dataCollectionFilter(),
                            DataChangeEvent::new,
                            metadataProvider,
                            schemaNameAdjuster);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PostgresEventDispatcher", e);
        }

        ChangeEventSourceMetricsFactory<PostgresPartition> metricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                metricsFactory.getSnapshotMetrics(taskContext, queue, metadataProvider);
    }

    @Override
    public PostgresSchema getDatabaseSchema() {
        return schema;
    }

    @Override
    public RowType getSplitType(io.debezium.relational.Table table) {
        io.debezium.relational.Column splitColumn =
                org.apache.flink.cdc.connectors.gaussdb.source.utils.ChunkUtils.getSplitColumn(
                        table, sourceConfig.getChunkKeyColumn());
        return org.apache.flink.cdc.connectors.gaussdb.source.utils.ChunkUtils.getSplitType(
                splitColumn);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public PostgresEventDispatcher<TableId> getEventDispatcher() {
        return eventDispatcher;
    }

    /**
     * Returns a {@link WatermarkDispatcher} implementation that delegates watermark events to the
     * Debezium {@link ChangeEventQueue}.
     *
     * <p>This is needed because {@link PostgresEventDispatcher} does not implement {@link
     * WatermarkDispatcher}, unlike the base {@code JdbcSourceEventDispatcher} used by other CDC
     * connectors. The watermark events are used by the snapshot scan task to coordinate the
     * transition between snapshot and stream phases.
     */
    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return new WatermarkDispatcher() {
            @Override
            public void dispatchWatermarkEvent(
                    Map<String, ?> sourcePartition,
                    org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase split,
                    org.apache.flink.cdc.connectors.base.source.meta.offset.Offset offset,
                    WatermarkKind kind)
                    throws InterruptedException {
                String topicName =
                        sourceConfig
                                .getDbzProperties()
                                .getProperty(
                                        "topic.prefix",
                                        sourceConfig.getDbzConnectorConfig().getLogicalName());
                SourceRecord watermarkRecord =
                        WatermarkEvent.create(
                                sourcePartition, topicName, split.splitId(), kind, offset);
                queue.enqueue(new DataChangeEvent(watermarkRecord));
            }
        };
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public PostgresPartition getPartition() {
        return partition;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return GaussDBOffset.of(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (replicationConnection != null) {
            replicationConnection.close();
        }
    }

    public PostgresConnection getConnection() {
        return jdbcConnection;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    public ReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    public SnapshotChangeEventSourceMetrics<PostgresPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public Snapshotter getSnapShotter() {
        return snapShotter;
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        // Read the real plugin name (e.g., mppdb_decoding), not the Debezium-facing
        // plugin.name which is always pgoutput.
        String realName = sourceConfig.getDbzProperties().getProperty("real.plugin.name");
        if (realName != null) {
            return realName;
        }
        return sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name());
    }

    /**
     * Replaces the messageDecoder field in the replication connection for mppdb_decoding, selecting
     * JSON or binary decoder based on the decode-style configuration.
     */
    private void replaceMessageDecoder() {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

            Field decoderField =
                    replicationConnection.getClass().getDeclaredField("messageDecoder");
            long decoderOffset = unsafe.objectFieldOffset(decoderField);

            int parallelDecodeNum = ((GaussDBSourceConfig) sourceConfig).getParallelDecodeNum();
            String decodeStyle = ((GaussDBSourceConfig) sourceConfig).getDecodeStyle();
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
            unsafe.putObject(replicationConnection, decoderOffset, decoder);

            // Replace the plugin field's decoderName from "decoderbufs" to "mppdb_decoding"
            // so that initReplicationSlot/createReplicationSlot uses the correct plugin name
            // when creating the logical replication slot on GaussDB.
            // We use plugin.getClass().getSuperclass() instead of LogicalDecoder.class to avoid
            // Flink classloader issues where the compile-time class reference may differ from
            // the runtime class loaded by Flink's child-first classloader.
            Field pluginField = replicationConnection.getClass().getDeclaredField("plugin");
            long pluginOffset = unsafe.objectFieldOffset(pluginField);
            Object plugin = unsafe.getObject(replicationConnection, pluginOffset);
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
            throw new RuntimeException("Failed to replace message decoder for mppdb_decoding", e);
        }
    }

    private boolean isBackFillSplit(SourceSplitBase sourceSplitBase) {
        return sourceSplitBase.isStreamSplit()
                && !StreamSplit.STREAM_SPLIT_ID.equalsIgnoreCase(
                        sourceSplitBase.asStreamSplit().splitId());
    }

    private String getTableList(TableId tableId) {
        // Use 2-part format (schema.table) for table.include.list because Debezium's
        // PostgreSQL TableIdToStringMapper converts TableId to "schema.table" format
        // (ignoring catalog). Using 3-part format (catalog.schema.table) would not
        // match because the mapper strips the catalog part before comparison.
        // See: PostgresConnectorConfig.lambda$new$3 which does tableId.schema() + "." +
        // tableId.table()
        if (tableId.schema() != null && !tableId.schema().isEmpty()) {
            return tableId.schema() + "." + tableId.table();
        }
        return tableId.table();
    }
}
