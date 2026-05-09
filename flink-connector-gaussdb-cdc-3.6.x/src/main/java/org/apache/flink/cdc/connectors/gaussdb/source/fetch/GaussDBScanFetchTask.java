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

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetUtils;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.GaussDBQueryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.Utils;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A {@link FetchTask} implementation for GaussDB to read snapshot split. */
public class GaussDBScanFetchTask extends AbstractScanFetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBScanFetchTask.class);

    public GaussDBScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    @Override
    public void execute(Context context) throws Exception {

        GaussDBSourceFetchTaskContext ctx = (GaussDBSourceFetchTaskContext) context;
        GaussDBSourceConfig sourceConfig = (GaussDBSourceConfig) context.getSourceConfig();
        try {
            maybeCreateSlotForBackFillReadTask(
                    ctx.getConnection(),
                    ctx.getReplicationConnection(),
                    sourceConfig.getSlotNameForBackfillTask(),
                    ctx.getPluginName(),
                    sourceConfig.isSkipSnapshotBackfill());
            super.execute(context);
        } finally {
            maybeDropSlotForBackFillReadTask(
                    (PostgresReplicationConnection) ctx.getReplicationConnection(),
                    sourceConfig.isSkipSnapshotBackfill());
        }
    }

    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        GaussDBSourceFetchTaskContext ctx = (GaussDBSourceFetchTaskContext) context;

        GaussDBSnapshotSplitReadTask snapshotSplitReadTask =
                new GaussDBSnapshotSplitReadTask(
                        ctx.getConnection(),
                        ctx.getDbzConnectorConfig(),
                        ctx.getDatabaseSchema(),
                        ctx.getOffsetContext(),
                        ctx.getEventDispatcher(),
                        ctx.getSnapshotChangeEventSourceMetrics(),
                        snapshotSplit);

        StoppableChangeEventSourceContext changeEventSourceContext =
                new StoppableChangeEventSourceContext();
        SnapshotResult<PostgresOffsetContext> snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, ctx.getPartition(), ctx.getOffsetContext());

        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for GaussDB split %s fail", snapshotResult));
        }
    }

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {
        GaussDBSourceFetchTaskContext ctx = (GaussDBSourceFetchTaskContext) context;

        final PostgresOffsetContext.Loader loader =
                new PostgresOffsetContext.Loader(ctx.getDbzConnectorConfig());
        final PostgresOffsetContext postgresOffsetContext =
                GaussDBOffsetUtils.getPostgresOffsetContext(
                        loader, backfillStreamSplit.getStartingOffset());

        final GaussDBStreamFetchTask.StreamSplitReadTask backfillReadTask =
                new GaussDBStreamFetchTask.StreamSplitReadTask(
                        ctx.getDbzConnectorConfig(),
                        ctx.getSnapShotter(),
                        ctx.getConnection(),
                        ctx.getEventDispatcher(),
                        ctx.getWaterMarkDispatcher(),
                        ctx.getErrorHandler(),
                        ctx.getTaskContext().getClock(),
                        ctx.getDatabaseSchema(),
                        ctx.getTaskContext(),
                        ctx.getReplicationConnection(),
                        backfillStreamSplit);
        LOG.info(
                "Execute backfillReadTask for split {} with slot name {}",
                snapshotSplit,
                ((GaussDBSourceConfig) ctx.getSourceConfig()).getSlotNameForBackfillTask());
        backfillReadTask.execute(
                new StoppableChangeEventSourceContext(), ctx.getPartition(), postgresOffsetContext);
    }

    private void maybeCreateSlotForBackFillReadTask(
            PostgresConnection jdbcConnection,
            ReplicationConnection replicationConnection,
            String slotName,
            String pluginName,
            boolean skipSnapshotBackfill) {
        if (skipSnapshotBackfill) {
            return;
        }

        try {
            SlotState slotInfo = null;
            try {
                slotInfo = jdbcConnection.getReplicationSlotState(slotName, pluginName);
            } catch (SQLException e) {
                LOG.info("Unable to load info of replication slot, will try to create the slot");
            }
            if (slotInfo == null) {
                try {
                    replicationConnection.createReplicationSlot().orElse(null);
                } catch (SQLException ex) {
                    String message = "Creation of replication slot failed";
                    if (ex.getMessage().contains("already exists")) {
                        message +=
                                "; when setting up multiple connectors for the same database host, "
                                        + "please make sure to use a distinct replication slot name for each.";
                    }
                    throw new FlinkRuntimeException(message, ex);
                }
            }
            waitForReplicationSlotReady(jdbcConnection, slotName, pluginName);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    /** Wait until replication slot is ready by polling slot state. */
    private void waitForReplicationSlotReady(
            PostgresConnection jdbcConnection, String slotName, String pluginName)
            throws SQLException, InterruptedException {
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
            try {
                SlotState slotState = jdbcConnection.getReplicationSlotState(slotName, pluginName);
                if (slotState != null) {
                    LOG.info("Replication slot {} is ready", slotName);
                    return;
                }
            } catch (SQLException e) {
                // slot not yet visible, retry
            }
            Thread.sleep(1000);
        }
        LOG.warn(
                "Replication slot {} may not be fully ready after {} retries",
                slotName,
                maxRetries);
    }

    private void maybeDropSlotForBackFillReadTask(
            PostgresReplicationConnection replicationConnection, boolean skipSnapshotBackfill) {
        if (skipSnapshotBackfill) {
            return;
        }

        try {
            replicationConnection.close(true);
        } catch (Throwable t) {
            LOG.error("Unexpected error while dropping replication slot", t);
            throw new FlinkRuntimeException(t);
        }
    }

    /** Refresh the schema from the database using the Debezium Utils accessor. */
    private static void refreshSchema(
            PostgresSchema schema,
            PostgresConnection jdbcConnection,
            boolean printReplicaIdentityInfo)
            throws SQLException {
        Utils.refreshSchema(schema, jdbcConnection, printReplicaIdentityInfo);
    }

    /** A SnapshotChangeEventSource implementation for GaussDB to read snapshot split. */
    public static class GaussDBSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {
        private static final Logger LOG =
                LoggerFactory.getLogger(GaussDBSnapshotSplitReadTask.class);

        private final PostgresConnection jdbcConnection;
        private final PostgresConnectorConfig connectorConfig;
        private final PostgresEventDispatcher<TableId> eventDispatcher;
        private final SnapshotSplit snapshotSplit;
        private final PostgresOffsetContext offsetContext;
        private final PostgresSchema databaseSchema;
        private final SnapshotProgressListener<PostgresPartition> snapshotProgressListener;
        private final Clock clock;

        public GaussDBSnapshotSplitReadTask(
                PostgresConnection jdbcConnection,
                PostgresConnectorConfig connectorConfig,
                PostgresSchema databaseSchema,
                PostgresOffsetContext previousOffset,
                PostgresEventDispatcher<TableId> eventDispatcher,
                SnapshotProgressListener snapshotProgressListener,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.jdbcConnection = jdbcConnection;
            this.connectorConfig = connectorConfig;
            this.snapshotProgressListener = snapshotProgressListener;
            this.databaseSchema = databaseSchema;
            this.eventDispatcher = eventDispatcher;
            this.snapshotSplit = snapshotSplit;
            this.offsetContext = previousOffset;
            this.clock = Clock.SYSTEM;
        }

        @Override
        protected SnapshotResult<PostgresOffsetContext> doExecute(
                ChangeEventSourceContext context,
                PostgresOffsetContext previousOffset,
                SnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final GaussDBSnapshotContext ctx = (GaussDBSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            // Skip refreshSchema() for GaussDB - the schema is already initialized
            // in GaussDBSourceFetchTaskContext.configure() with the correct TableId
            // that includes the catalog (database) name. refreshSchema() would
            // clear the existing schema and re-read from DatabaseMetaData, which
            // may produce TableIds without the catalog, causing tableFor() to
            // return null.
            // refreshSchema(databaseSchema, jdbcConnection, true);
            createDataEvents(ctx, snapshotSplit.getTableId());

            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(GaussDBSnapshotContext snapshotContext, TableId tableId)
                throws InterruptedException {
            EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver =
                    eventDispatcher.getSnapshotChangeEventReceiver();
            LOG.info("Snapshotting table {}", tableId);
            Table table = databaseSchema.tableFor(tableId);
            if (table == null) {
                LOG.warn(
                        "Table {} not found in databaseSchema, trying schema.table match", tableId);
                // Try to find the table with a TableId that doesn't include the catalog
                for (TableId registeredId : databaseSchema.tableIds()) {
                    if (registeredId.table().equals(tableId.table())
                            && registeredId.schema().equals(tableId.schema())) {
                        LOG.info(
                                "Found matching table by schema+table name: {} -> {}",
                                tableId,
                                registeredId);
                        table = databaseSchema.tableFor(registeredId);
                        break;
                    }
                }
            }
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, Objects.requireNonNull(table));
            snapshotReceiver.completeSnapshot();
        }

        private void createDataEventsForTable(
                GaussDBSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<PostgresPartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

            List<String> uuidFields =
                    snapshotSplit.getSplitKeyType().getFieldNames().stream()
                            .filter(field -> table.columnWithName(field).typeName().equals("uuid"))
                            .collect(Collectors.toList());

            List<String> columnNames =
                    table.columns().stream()
                            .map(column -> jdbcConnection.quotedColumnIdString(column.name()))
                            .collect(Collectors.toList());
            final String selectSql =
                    GaussDBQueryUtils.buildSplitScanQuery(
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null,
                            columnNames,
                            uuidFields);
            LOG.debug(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                            GaussDBQueryUtils.readTableSplitDataStatement(
                                    jdbcConnection,
                                    selectSql,
                                    snapshotSplit.getSplitStart() == null,
                                    snapshotSplit.getSplitEnd() == null,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd(),
                                    snapshotSplit.getSplitKeyType().getFieldCount(),
                                    connectorConfig.getSnapshotFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                    for (int i = 0; i < columnArray.getColumns().length; i++) {
                        row[columnArray.getColumns()[i].position() - 1] = rs.getObject(i + 1);
                    }
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    snapshotContext.offset.event(table.id(), clock.currentTime());
                    SnapshotChangeRecordEmitter<PostgresPartition> emitter =
                            new SnapshotChangeRecordEmitter<>(
                                    snapshotContext.partition, snapshotContext.offset, row, clock);
                    eventDispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition, table.id(), emitter, snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new FlinkRuntimeException(
                        "Snapshotting of table " + table.id() + " failed", e);
            }
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                PostgresPartition partition, PostgresOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected GaussDBSnapshotContext prepare(PostgresPartition partition) throws Exception {
            return new GaussDBSnapshotContext(partition);
        }

        private static class GaussDBSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                        PostgresPartition, PostgresOffsetContext> {

            public GaussDBSnapshotContext(PostgresPartition partition) throws SQLException {
                super(partition, "");
            }
        }
    }
}
