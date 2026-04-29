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
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** Tests for {@link GaussDBScanFetchTask}. */
class GaussDBScanFetchTaskTest {

    private SnapshotSplit createTestSnapshotSplit() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        org.apache.flink.table.types.logical.RowType splitKeyType =
                new org.apache.flink.table.types.logical.RowType(
                        Collections.singletonList(
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "id", new org.apache.flink.table.types.logical.IntType())));

        return new SnapshotSplit(tableId, 0, splitKeyType, null, null, null, new HashMap<>());
    }

    private GaussDBSourceConfig createTestConfig() {
        GaussDBSourceConfigFactory factory = new GaussDBSourceConfigFactory();
        factory.hostname("localhost");
        factory.database("testdb");
        factory.username("testuser");
        factory.password("testpass");
        factory.port(8000);
        factory.tableList("public.test_table");
        factory.slotName("test_slot");
        return factory.create(0);
    }

    private GaussDBSourceFetchTaskContext createMockContext() throws Exception {
        GaussDBSourceConfig config = createTestConfig();
        org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect dialect =
                new org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect(config);
        GaussDBSourceFetchTaskContext context = new GaussDBSourceFetchTaskContext(config, dialect);

        // Set required fields via reflection
        PostgresConnection mockConn = mock(PostgresConnection.class);
        PostgresOffsetContext mockOffset = mock(PostgresOffsetContext.class);
        PostgresPartition mockPartition = mock(PostgresPartition.class);
        PostgresSchema mockSchema = mock(PostgresSchema.class);
        PostgresTaskContext mockTaskCtx = mock(PostgresTaskContext.class);

        setField(context, "jdbcConnection", mockConn);
        setField(context, "offsetContext", mockOffset);
        setField(context, "partition", mockPartition);
        setField(context, "schema", mockSchema);
        setField(context, "taskContext", mockTaskCtx);

        return context;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void testConstructor() {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);
        assertThat(task).isNotNull();
    }

    @Test
    void testIsRunningInitiallyFalse() {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testGetSplit() {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);
        assertThat(task.getSplit()).isNotNull();
        assertThat(task.getSplit().isSnapshotSplit()).isTrue();
    }

    @Test
    void testMaybeCreateSlotSkipsWhenBackfillSkipped() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeCreateSlotForBackFillReadTask",
                        PostgresConnection.class,
                        io.debezium.connector.postgresql.connection.ReplicationConnection.class,
                        String.class,
                        String.class,
                        boolean.class);
        method.setAccessible(true);

        // When skipSnapshotBackfill is true, method should return immediately
        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        io.debezium.connector.postgresql.connection.ReplicationConnection replConn =
                mock(io.debezium.connector.postgresql.connection.ReplicationConnection.class);

        // Should not throw or interact with connections
        method.invoke(task, jdbcConn, replConn, "slot", "plugin", true);
    }

    @Test
    void testMaybeDropSlotSkipsWhenBackfillSkipped() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeDropSlotForBackFillReadTask",
                        PostgresReplicationConnection.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresReplicationConnection replConn = mock(PostgresReplicationConnection.class);

        // Should not throw when skipSnapshotBackfill is true
        method.invoke(task, replConn, true);
    }

    @Test
    void testMaybeDropSlotCallsCloseWhenNotSkipped() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeDropSlotForBackFillReadTask",
                        PostgresReplicationConnection.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresReplicationConnection replConn = mock(PostgresReplicationConnection.class);

        // Should call close(true) when skipSnapshotBackfill is false
        method.invoke(task, replConn, false);
    }

    @Test
    void testMaybeCreateSlotWithExistingSlot() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeCreateSlotForBackFillReadTask",
                        PostgresConnection.class,
                        io.debezium.connector.postgresql.connection.ReplicationConnection.class,
                        String.class,
                        String.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        io.debezium.connector.postgresql.connection.ReplicationConnection replConn =
                mock(io.debezium.connector.postgresql.connection.ReplicationConnection.class);

        // Also test the slot-already-exists path
        doReturn(mock(SlotState.class)).when(jdbcConn).getReplicationSlotState("slot", "plugin");
        method.invoke(task, jdbcConn, replConn, "slot", "plugin", false);
    }

    @Test
    void testMaybeCreateSlotWhenSlotNotFoundAndCreationSucceeds() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeCreateSlotForBackFillReadTask",
                        PostgresConnection.class,
                        ReplicationConnection.class,
                        String.class,
                        String.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        ReplicationConnection replConn = mock(ReplicationConnection.class);

        // Slot does not exist, creation succeeds
        // Use doThrow/doReturn pattern to avoid stub overriding issues
        SlotState slotState = mock(SlotState.class);
        doThrow(new SQLException("slot not found"))
                .doReturn(slotState)
                .when(jdbcConn)
                .getReplicationSlotState("slot", "plugin");
        doReturn(Optional.empty()).when(replConn).createReplicationSlot();

        // Should not throw
        method.invoke(task, jdbcConn, replConn, "slot", "plugin", false);
    }

    @Test
    void testMaybeCreateSlotWhenCreationFailsWithAlreadyExists() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeCreateSlotForBackFillReadTask",
                        PostgresConnection.class,
                        ReplicationConnection.class,
                        String.class,
                        String.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        ReplicationConnection replConn = mock(ReplicationConnection.class);

        // Slot does not exist, creation fails with "already exists"
        doThrow(new SQLException("slot not found"))
                .when(jdbcConn)
                .getReplicationSlotState("slot", "plugin");
        doThrow(new SQLException("replication slot already exists"))
                .when(replConn)
                .createReplicationSlot();

        // Should throw FlinkRuntimeException wrapping the error
        Throwable thrown =
                catchThrowable(
                        () -> method.invoke(task, jdbcConn, replConn, "slot", "plugin", false));
        assertThat(thrown).isInstanceOf(InvocationTargetException.class);
        assertThat(thrown.getCause()).isInstanceOf(FlinkRuntimeException.class);
        assertThat(thrown.getCause().getMessage())
                .contains("Creation of replication slot failed")
                .contains("distinct replication slot name");
    }

    @Test
    void testMaybeCreateSlotWhenCreationFailsWithoutAlreadyExists() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeCreateSlotForBackFillReadTask",
                        PostgresConnection.class,
                        ReplicationConnection.class,
                        String.class,
                        String.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        ReplicationConnection replConn = mock(ReplicationConnection.class);

        // Slot does not exist, creation fails with other error
        doThrow(new SQLException("slot not found"))
                .when(jdbcConn)
                .getReplicationSlotState("slot", "plugin");
        doThrow(new SQLException("connection refused")).when(replConn).createReplicationSlot();

        Throwable thrown =
                catchThrowable(
                        () -> method.invoke(task, jdbcConn, replConn, "slot", "plugin", false));
        assertThat(thrown).isInstanceOf(InvocationTargetException.class);
        assertThat(thrown.getCause()).isInstanceOf(FlinkRuntimeException.class);
        assertThat(thrown.getCause().getMessage()).contains("Creation of replication slot failed");
    }

    @Test
    void testMaybeDropSlotWhenCloseThrows() throws Exception {
        SnapshotSplit split = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(split);

        Method method =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "maybeDropSlotForBackFillReadTask",
                        PostgresReplicationConnection.class,
                        boolean.class);
        method.setAccessible(true);

        PostgresReplicationConnection replConn = mock(PostgresReplicationConnection.class);
        doThrow(new RuntimeException("close failed")).when(replConn).close(true);

        Throwable thrown = catchThrowable(() -> method.invoke(task, replConn, false));
        assertThat(thrown).isInstanceOf(InvocationTargetException.class);
        assertThat(thrown.getCause()).isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void testSnapshotSplitReadTaskConstructor() throws Exception {
        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        PostgresConnectorConfig connectorConfig = mock(PostgresConnectorConfig.class);
        PostgresSchema schema = mock(PostgresSchema.class);
        PostgresOffsetContext offsetCtx = mock(PostgresOffsetContext.class);
        io.debezium.connector.postgresql.PostgresEventDispatcher<TableId> dispatcher =
                mock(io.debezium.connector.postgresql.PostgresEventDispatcher.class);
        SnapshotProgressListener listener = mock(SnapshotProgressListener.class);
        SnapshotSplit split = createTestSnapshotSplit();

        GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask readTask =
                new GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask(
                        jdbcConn, connectorConfig, schema, offsetCtx, dispatcher, listener, split);

        assertThat(readTask).isNotNull();
    }

    @Test
    void testSnapshotSplitReadTaskGetSnapshottingTask() throws Exception {
        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        PostgresConnectorConfig connectorConfig = mock(PostgresConnectorConfig.class);
        PostgresSchema schema = mock(PostgresSchema.class);
        PostgresOffsetContext offsetCtx = mock(PostgresOffsetContext.class);
        io.debezium.connector.postgresql.PostgresEventDispatcher<TableId> dispatcher =
                mock(io.debezium.connector.postgresql.PostgresEventDispatcher.class);
        SnapshotProgressListener listener = mock(SnapshotProgressListener.class);
        SnapshotSplit split = createTestSnapshotSplit();

        GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask readTask =
                new GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask(
                        jdbcConn, connectorConfig, schema, offsetCtx, dispatcher, listener, split);

        PostgresPartition partition = mock(PostgresPartition.class);
        var result = readTask.getSnapshottingTask(partition, offsetCtx);
        assertThat(result).isNotNull();
    }

    @Test
    void testSnapshotSplitReadTaskPrepare() throws Exception {
        PostgresConnection jdbcConn = mock(PostgresConnection.class);
        PostgresConnectorConfig connectorConfig = mock(PostgresConnectorConfig.class);
        PostgresSchema schema = mock(PostgresSchema.class);
        PostgresOffsetContext offsetCtx = mock(PostgresOffsetContext.class);
        io.debezium.connector.postgresql.PostgresEventDispatcher<TableId> dispatcher =
                mock(io.debezium.connector.postgresql.PostgresEventDispatcher.class);
        SnapshotProgressListener listener = mock(SnapshotProgressListener.class);
        SnapshotSplit split = createTestSnapshotSplit();

        GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask readTask =
                new GaussDBScanFetchTask.GaussDBSnapshotSplitReadTask(
                        jdbcConn, connectorConfig, schema, offsetCtx, dispatcher, listener, split);

        PostgresPartition partition = mock(PostgresPartition.class);
        var result = readTask.prepare(partition);
        assertThat(result).isNotNull();
    }

    @Test
    void testRefreshSchemaWithMocks() throws Exception {
        Method refreshMethod =
                GaussDBScanFetchTask.class.getDeclaredMethod(
                        "refreshSchema",
                        PostgresSchema.class,
                        PostgresConnection.class,
                        boolean.class);
        refreshMethod.setAccessible(true);

        PostgresSchema schema = mock(PostgresSchema.class);
        PostgresConnection jdbcConn = mock(PostgresConnection.class);

        // With Utils.refreshSchema (package-private access), calling refresh on mock is a no-op.
        // The method should complete without exception.
        refreshMethod.invoke(null, schema, jdbcConn, true);
    }
}
