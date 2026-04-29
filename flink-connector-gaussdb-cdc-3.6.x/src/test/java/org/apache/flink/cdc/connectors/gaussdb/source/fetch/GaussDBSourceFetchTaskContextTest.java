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

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBSourceFetchTaskContext} using reflection and mocking. */
class GaussDBSourceFetchTaskContextTest {

    private GaussDBSourceFetchTaskContext context;
    private GaussDBSourceConfig sourceConfig;

    @BeforeEach
    void setUp() {
        GaussDBSourceConfigFactory factory = new GaussDBSourceConfigFactory();
        factory.hostname("localhost");
        factory.database("testdb");
        factory.username("testuser");
        factory.password("testpass");
        factory.port(8000);
        factory.tableList("public.test_table");
        factory.slotName("test_slot");
        sourceConfig = factory.create(0);
        GaussDBDialect dialect = new GaussDBDialect(sourceConfig);
        context = new GaussDBSourceFetchTaskContext(sourceConfig, dialect);
    }

    private void setField(String fieldName, Object value) throws Exception {
        Field field = GaussDBSourceFetchTaskContext.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(context, value);
    }

    @Test
    void testGetDbzConnectorConfig() {
        var config = context.getDbzConnectorConfig();
        assertThat(config).isNotNull();
    }

    @Test
    void testGetSplitType() {
        Table table = mock(Table.class);
        Column splitColumn = mock(Column.class);
        when(splitColumn.name()).thenReturn("id");
        when(splitColumn.typeName()).thenReturn("int4");
        when(table.columnWithName("id")).thenReturn(splitColumn);
        when(table.primaryKeyColumns()).thenReturn(java.util.Arrays.asList(splitColumn));

        RowType rowType = context.getSplitType(table);
        assertThat(rowType).isNotNull();
        assertThat(rowType.getFieldNames()).containsExactly("id");
    }

    @Test
    void testGetSplitTypeWithBigIntColumn() {
        Table table = mock(Table.class);
        Column splitColumn = mock(Column.class);
        when(splitColumn.name()).thenReturn("seq_no");
        when(splitColumn.typeName()).thenReturn("int8");
        when(table.columnWithName("seq_no")).thenReturn(splitColumn);
        when(table.primaryKeyColumns()).thenReturn(java.util.Arrays.asList(splitColumn));

        RowType rowType = context.getSplitType(table);
        assertThat(rowType).isNotNull();
        assertThat(rowType.getFieldNames()).containsExactly("seq_no");
    }

    @Test
    void testGetStreamOffset() {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("lsn", Schema.INT64_SCHEMA)
                        .field("schema_name", Schema.STRING_SCHEMA)
                        .field("table_name", Schema.STRING_SCHEMA)
                        .build();
        Struct sourceStruct = new Struct(sourceSchema);
        sourceStruct.put("lsn", 12345L);
        sourceStruct.put("schema_name", "public");
        sourceStruct.put("table_name", "test_table");

        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("source", sourceSchema)
                        .field("op", Schema.STRING_SCHEMA)
                        .build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put("source", sourceStruct);
        valueStruct.put("op", "c");

        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.value()).thenReturn(valueStruct);

        Offset offset = context.getStreamOffset(sourceRecord);
        assertThat(offset).isInstanceOf(GaussDBOffset.class);
    }

    @Test
    void testGetTableId() throws Exception {
        // Use reflection to call getTableId with a mocked SourceRecord
        // The real method uses Envelope.FieldName.SOURCE, SCHEMA_NAME_KEY, TABLE_NAME_KEY
        // These are "source", "schema", "table" in Kafka Connect Struct
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("schema", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .build();
        Struct sourceStruct = new Struct(sourceSchema);
        sourceStruct.put("schema", "public");
        sourceStruct.put("table", "users");

        Schema valueSchema = SchemaBuilder.struct().field("source", sourceSchema).build();
        Struct valueStruct = new Struct(valueSchema);
        valueStruct.put("source", sourceStruct);

        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.value()).thenReturn(valueStruct);

        TableId tableId = context.getTableId(sourceRecord);
        assertThat(tableId).isNotNull();
        assertThat(tableId.schema()).isEqualTo("public");
        assertThat(tableId.table()).isEqualTo("users");
    }

    @Test
    void testGetWaterMarkDispatcher() {
        assertThat(context.getWaterMarkDispatcher()).isNull();
    }

    @Test
    void testGetDatabaseSchemaBeforeConfigure() throws Exception {
        // Before configure, schema is null
        assertThat(context.getDatabaseSchema()).isNull();
    }

    @Test
    void testGetDatabaseSchemaAfterSet() throws Exception {
        PostgresSchema schema = mock(PostgresSchema.class);
        setField("schema", schema);
        assertThat(context.getDatabaseSchema()).isSameAs(schema);
    }

    @Test
    void testGetErrorHandlerAfterSet() throws Exception {
        ErrorHandler errorHandler = mock(ErrorHandler.class);
        setField("errorHandler", errorHandler);
        assertThat(context.getErrorHandler()).isSameAs(errorHandler);
    }

    @Test
    void testGetOffsetContextAfterSet() throws Exception {
        PostgresOffsetContext offsetContext = mock(PostgresOffsetContext.class);
        setField("offsetContext", offsetContext);
        assertThat(context.getOffsetContext()).isSameAs(offsetContext);
    }

    @Test
    void testGetPartitionAfterSet() throws Exception {
        PostgresPartition partition = mock(PostgresPartition.class);
        setField("partition", partition);
        assertThat(context.getPartition()).isSameAs(partition);
    }

    @Test
    void testGetQueueAfterSet() throws Exception {
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        setField("queue", queue);
        assertThat(context.getQueue()).isSameAs(queue);
    }

    @Test
    void testGetTableFilter() {
        var tableFilter = context.getTableFilter();
        assertThat(tableFilter).isNotNull();
    }

    @Test
    void testGetConnectionAfterSet() throws Exception {
        PostgresConnection connection = mock(PostgresConnection.class);
        setField("jdbcConnection", connection);
        assertThat(context.getConnection()).isSameAs(connection);
    }

    @Test
    void testGetTaskContextAfterSet() throws Exception {
        PostgresTaskContext taskContext = mock(PostgresTaskContext.class);
        setField("taskContext", taskContext);
        assertThat(context.getTaskContext()).isSameAs(taskContext);
    }

    @Test
    void testGetReplicationConnectionAfterSet() throws Exception {
        ReplicationConnection replConnection = mock(ReplicationConnection.class);
        setField("replicationConnection", replConnection);
        assertThat(context.getReplicationConnection()).isSameAs(replConnection);
    }

    @Test
    void testGetSnapshotChangeEventSourceMetricsAfterSet() throws Exception {
        SnapshotChangeEventSourceMetrics<PostgresPartition> metrics =
                mock(SnapshotChangeEventSourceMetrics.class);
        setField("snapshotChangeEventSourceMetrics", metrics);
        assertThat(context.getSnapshotChangeEventSourceMetrics()).isSameAs(metrics);
    }

    @Test
    void testGetSlotName() {
        String slotName = context.getSlotName();
        assertThat(slotName).isEqualTo("test_slot");
    }

    @Test
    void testGetPluginName() {
        String pluginName = context.getPluginName();
        assertThat(pluginName).isEqualTo("mppdb_decoding");
    }

    @Test
    void testCloseWithNullConnections() throws Exception {
        // Should not throw when connections are null
        context.close();
    }

    @Test
    void testCloseWithMockConnections() throws Exception {
        PostgresConnection jdbcConnection = mock(PostgresConnection.class);
        ReplicationConnection replicationConnection = mock(ReplicationConnection.class);
        setField("jdbcConnection", jdbcConnection);
        setField("replicationConnection", replicationConnection);

        context.close();
        // No exception thrown means success
    }

    @Test
    void testGetTableListWithSchema() throws Exception {
        Method getTableList =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "getTableList", TableId.class);
        getTableList.setAccessible(true);

        TableId tableId = new TableId("testdb", "public", "users");
        String result = (String) getTableList.invoke(context, tableId);
        assertThat(result).isEqualTo("public.users");
    }

    @Test
    void testGetTableListWithoutSchema() throws Exception {
        Method getTableList =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "getTableList", TableId.class);
        getTableList.setAccessible(true);

        TableId tableId = new TableId("testdb", null, "users");
        String result = (String) getTableList.invoke(context, tableId);
        assertThat(result).isEqualTo("users");
    }

    @Test
    void testGetTableListWithEmptySchema() throws Exception {
        Method getTableList =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "getTableList", TableId.class);
        getTableList.setAccessible(true);

        TableId tableId = new TableId("testdb", "", "users");
        String result = (String) getTableList.invoke(context, tableId);
        assertThat(result).isEqualTo("users");
    }

    @Test
    void testIsBackFillSplitWithStreamSplitId() throws Exception {
        Method isBackFillSplit =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "isBackFillSplit",
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase
                                .class);
        isBackFillSplit.setAccessible(true);

        // Regular stream split (not backfill) - uses STREAM_SPLIT_ID = "stream-split"
        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit regularStreamSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit(
                        "stream-split",
                        GaussDBOffset.INITIAL_OFFSET,
                        GaussDBOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        boolean result =
                (boolean)
                        isBackFillSplit.invoke(
                                context,
                                (org.apache.flink.cdc.connectors.base.source.meta.split
                                                .SourceSplitBase)
                                        regularStreamSplit);
        assertThat(result).isFalse();
    }

    @Test
    void testIsBackFillSplitWithBackFillSplit() throws Exception {
        Method isBackFillSplit =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "isBackFillSplit",
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase
                                .class);
        isBackFillSplit.setAccessible(true);

        // Backfill stream split (splitId != STREAM_SPLIT_ID)
        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit backfillSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit(
                        "backfill-split-0",
                        GaussDBOffset.INITIAL_OFFSET,
                        GaussDBOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        boolean result =
                (boolean)
                        isBackFillSplit.invoke(
                                context,
                                (org.apache.flink.cdc.connectors.base.source.meta.split
                                                .SourceSplitBase)
                                        backfillSplit);
        assertThat(result).isTrue();
    }

    @Test
    void testIsBackFillSplitWithSnapshotSplit() throws Exception {
        Method isBackFillSplit =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "isBackFillSplit",
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase
                                .class);
        isBackFillSplit.setAccessible(true);

        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit snapshotSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        new TableId("testdb", "public", "test_table"),
                        0,
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new IntType()))),
                        null,
                        null,
                        null,
                        new HashMap<>());

        boolean result = (boolean) isBackFillSplit.invoke(context, snapshotSplit);
        assertThat(result).isFalse();
    }

    @Test
    void testLoadStartingOffsetStateForSnapshot() throws Exception {
        Method loadStartingOffsetState =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "loadStartingOffsetState",
                        PostgresOffsetContext.Loader.class,
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase
                                .class);
        loadStartingOffsetState.setAccessible(true);

        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit snapshotSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        new TableId("testdb", "public", "test_table"),
                        0,
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new IntType()))),
                        null,
                        null,
                        null,
                        new HashMap<>());

        PostgresOffsetContext.Loader loader = mock(PostgresOffsetContext.Loader.class);
        PostgresOffsetContext mockOffset = mock(PostgresOffsetContext.class);
        when(loader.load(org.mockito.ArgumentMatchers.anyMap())).thenReturn(mockOffset);

        PostgresOffsetContext result =
                (PostgresOffsetContext)
                        loadStartingOffsetState.invoke(context, loader, snapshotSplit);
        assertThat(result).isNotNull();
    }

    @Test
    void testLoadStartingOffsetStateForStream() throws Exception {
        Method loadStartingOffsetState =
                GaussDBSourceFetchTaskContext.class.getDeclaredMethod(
                        "loadStartingOffsetState",
                        PostgresOffsetContext.Loader.class,
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase
                                .class);
        loadStartingOffsetState.setAccessible(true);

        // Stream split with a starting offset
        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit streamSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit(
                        "stream-split-1",
                        new GaussDBOffset(100L, null, null),
                        GaussDBOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        PostgresOffsetContext.Loader loader = mock(PostgresOffsetContext.Loader.class);
        PostgresOffsetContext mockOffset = mock(PostgresOffsetContext.class);
        when(loader.load(org.mockito.ArgumentMatchers.anyMap())).thenReturn(mockOffset);

        PostgresOffsetContext result =
                (PostgresOffsetContext)
                        loadStartingOffsetState.invoke(context, loader, streamSplit);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetSnapShotterBeforeConfigure() {
        // Before configure, snapShotter is null
        assertThat(context.getSnapShotter()).isNull();
    }

    @Test
    void testGetSnapShotterAfterSet() throws Exception {
        io.debezium.connector.postgresql.spi.Snapshotter snapShotter =
                mock(io.debezium.connector.postgresql.spi.Snapshotter.class);
        setField("snapShotter", snapShotter);
        assertThat(context.getSnapShotter()).isSameAs(snapShotter);
    }

    /**
     * Migrated from official Flink CDC 3.6 PostgresSourceFetchTaskContextTest. Verifies that when
     * LAST_COMMIT_LSN_KEY is null, the lastKnownLsn should still return the LSN value instead of
     * being reset.
     */
    @Test
    void shouldNotResetLsnWhenLastCommitLsnIsNull() {
        // Build a minimal PostgresConnectorConfig for testing
        // Use the same config as setUp() to ensure all required properties are present
        GaussDBSourceConfigFactory factory = new GaussDBSourceConfigFactory();
        factory.hostname("localhost");
        factory.database("testdb");
        factory.username("testuser");
        factory.password("testpass");
        factory.port(8000);
        factory.tableList("public.test_table");
        factory.slotName("test_slot");
        GaussDBSourceConfig gaussDBSourceConfig = factory.create(0);
        PostgresConnectorConfig connectorConfig =
                (PostgresConnectorConfig) gaussDBSourceConfig.getDbzConnectorConfig();
        PostgresOffsetContext.Loader offsetLoader =
                new PostgresOffsetContext.Loader(connectorConfig);

        final java.util.Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);
        assertThat(lastKnownLsn(offsetContext)).isEqualTo(Lsn.valueOf(12345L));
    }
}
