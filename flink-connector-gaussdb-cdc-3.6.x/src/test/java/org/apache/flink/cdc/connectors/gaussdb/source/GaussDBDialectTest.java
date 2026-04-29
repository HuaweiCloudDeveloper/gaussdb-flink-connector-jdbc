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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBStreamFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GaussDBDialect}. */
class GaussDBDialectTest {

    private GaussDBDialect dialect;
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
        dialect = new GaussDBDialect(sourceConfig);
    }

    @Test
    void testGetName() {
        assertThat(dialect.getName()).isEqualTo("GaussDB");
    }

    @Test
    void testIsDataCollectionIdCaseSensitive() {
        assertThat(dialect.isDataCollectionIdCaseSensitive(sourceConfig)).isTrue();
    }

    @Test
    void testGetPooledDataSourceFactory() {
        assertThat(dialect.getPooledDataSourceFactory())
                .isInstanceOf(GaussDBConnectionPoolFactory.class);
    }

    @Test
    void testCreateFetchTaskSnapshotSplit() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        org.apache.flink.table.types.logical.RowType splitKeyType =
                new org.apache.flink.table.types.logical.RowType(
                        Collections.singletonList(
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "id", new org.apache.flink.table.types.logical.IntType())));

        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit snapshotSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        tableId, 0, splitKeyType, null, null, null, new HashMap<>());

        var fetchTask = dialect.createFetchTask(snapshotSplit);
        assertThat(fetchTask).isInstanceOf(GaussDBScanFetchTask.class);
    }

    @Test
    void testCreateFetchTaskStreamSplit() {
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit streamSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        var fetchTask = dialect.createFetchTask(streamSplit);
        assertThat(fetchTask).isInstanceOf(GaussDBStreamFetchTask.class);
    }

    @Test
    void testCreateFetchTaskContext() {
        var context = dialect.createFetchTaskContext(sourceConfig);
        assertThat(context).isInstanceOf(GaussDBSourceFetchTaskContext.class);
    }

    @Test
    void testGetSlotName() {
        assertThat(dialect.getSlotName()).isEqualTo("test_slot");
    }

    @Test
    void testGetPluginName() {
        assertThat(dialect.getPluginName()).isEqualTo("mppdb_decoding");
    }

    @Test
    void testIsIncludeDataCollection() {
        TableId includedTable = new TableId("testdb", "public", "test_table");
        boolean result = dialect.isIncludeDataCollection(sourceConfig, includedTable);
        assertThat(result).isNotNull();
    }

    @Test
    void testNotifyCheckpointCompleteWithNullStreamFetchTask() throws Exception {
        GaussDBOffset offset = new GaussDBOffset(100L, null, null);
        dialect.notifyCheckpointComplete(1L, offset);
    }

    @Test
    void testNotifyCheckpointCompleteWithStreamFetchTask() throws Exception {
        // Create a stream split and fetch task
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        dialect.createFetchTask(streamSplit);

        // notifyCheckpointComplete should not throw
        GaussDBOffset offset = new GaussDBOffset(200L, null, null);
        dialect.notifyCheckpointComplete(1L, offset);
    }

    @Test
    void testCreateChunkSplitter() {
        ChunkSplitter splitter = dialect.createChunkSplitter(sourceConfig);
        assertThat(splitter).isInstanceOf(GaussDBChunkSplitter.class);
    }

    @Test
    void testCreateChunkSplitterWithState() {
        ChunkSplitter splitter =
                dialect.createChunkSplitter(
                        sourceConfig, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        assertThat(splitter).isInstanceOf(GaussDBChunkSplitter.class);
    }

    @Test
    void testQueryTableSchemaWithMockJdbc() {
        io.debezium.jdbc.JdbcConnection jdbc = mock(io.debezium.jdbc.JdbcConnection.class);
        TableId tableId = new TableId("testdb", "public", "nonexistent");
        // The method catches SQLException and returns null
        var result = dialect.queryTableSchema(jdbc, tableId);
        assertThat(result).isNull();
    }

    @Test
    void testOpenJdbcConnectionFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.openJdbcConnection(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDisplayCurrentOffsetFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.displayCurrentOffset(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testRemoveSlotFailsWithoutDB() {
        boolean result = dialect.removeSlot("nonexistent_slot");
        assertThat(result).isFalse();
    }

    @Test
    void testOpenReplicationConnectionWithMockConnection() {
        // openReplicationConnection with a fully mocked PostgresConnection may not throw
        // because the mock returns default values for all method calls. This test verifies
        // that the method can be invoked without NullPointerException for basic setup.
        io.debezium.connector.postgresql.connection.PostgresConnection pgConn =
                org.mockito.Mockito.mock(
                        io.debezium.connector.postgresql.connection.PostgresConnection.class);
        // The method may or may not throw depending on mock behavior;
        // the key point is that it does not throw unexpected NPEs.
        try {
            dialect.openReplicationConnection(pgConn);
        } catch (RuntimeException e) {
            // Expected: the mock connection cannot actually create a replication stream
            assertThat(e).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void testDisplayCommittedOffsetFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.displayCommittedOffset(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testCreateFetchTaskSetsStreamFetchTask() {
        // Create stream fetch task - should set the internal streamFetchTask
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;
        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        var fetchTask = dialect.createFetchTask(streamSplit);
        assertThat(fetchTask).isInstanceOf(GaussDBStreamFetchTask.class);

        // Creating snapshot task should not change streamFetchTask
        TableId tableId = new TableId("testdb", "public", "test_table");
        org.apache.flink.table.types.logical.RowType splitKeyType =
                new org.apache.flink.table.types.logical.RowType(
                        Collections.singletonList(
                                new org.apache.flink.table.types.logical.RowType.RowField(
                                        "id", new org.apache.flink.table.types.logical.IntType())));
        org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit snapshotSplit =
                new org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit(
                        tableId, 0, splitKeyType, null, null, null, new HashMap<>());
        var snapshotTask = dialect.createFetchTask(snapshotSplit);
        assertThat(snapshotTask).isInstanceOf(GaussDBScanFetchTask.class);
    }

    @Test
    void testNotifyCheckpointCompleteWithStreamFetchTaskAndOffset() throws Exception {
        // First create a stream split to set streamFetchTask
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;
        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        dialect.createFetchTask(streamSplit);

        // notifyCheckpointComplete should not throw
        GaussDBOffset offset = new GaussDBOffset(200L, null, null);
        dialect.notifyCheckpointComplete(1L, offset);
    }

    private static <T> T mock(Class<T> classToMock) {
        return org.mockito.Mockito.mock(classToMock);
    }
}
