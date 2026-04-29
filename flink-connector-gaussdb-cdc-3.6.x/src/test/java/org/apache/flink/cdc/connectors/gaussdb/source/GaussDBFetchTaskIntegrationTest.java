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

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBStreamFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Comprehensive tests for fetch task integration scenarios. */
class GaussDBFetchTaskIntegrationTest {

    private GaussDBSourceConfig sourceConfig;
    private GaussDBDialect dialect;

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
    void testCreateFetchTaskAndCloseSnapshotTask() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        RowType splitKeyType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));

        SnapshotSplit snapshotSplit =
                new SnapshotSplit(tableId, 0, splitKeyType, null, null, null, new HashMap<>());

        GaussDBScanFetchTask task = (GaussDBScanFetchTask) dialect.createFetchTask(snapshotSplit);
        assertThat(task).isNotNull();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testStreamFetchTaskCommitWithNullOffset() {
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        GaussDBStreamFetchTask task = (GaussDBStreamFetchTask) dialect.createFetchTask(streamSplit);
        assertThat(task).isNotNull();

        // commitCurrentOffset with null should not throw
        task.commitCurrentOffset(null);

        // commitCurrentOffset with offset should not throw
        GaussDBOffset offset = new GaussDBOffset(200L, null, null);
        task.commitCurrentOffset(offset);
    }

    @Test
    void testNotifyCheckpointCompleteWithStreamFetchTask() throws Exception {
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        dialect.createFetchTask(streamSplit);

        // Should not throw
        GaussDBOffset offset = new GaussDBOffset(200L, null, null);
        dialect.notifyCheckpointComplete(1L, offset);
    }

    @Test
    void testFetchTaskContextCreationAndAccessors() {
        GaussDBSourceFetchTaskContext context =
                (GaussDBSourceFetchTaskContext) dialect.createFetchTaskContext(sourceConfig);

        // Test accessors that don't require DB connection
        assertThat(context.getDbzConnectorConfig()).isNotNull();
        assertThat(context.getWaterMarkDispatcher()).isNull();
        assertThat(context.getSlotName()).isEqualTo("test_slot");
        assertThat(context.getPluginName()).isEqualTo("mppdb_decoding");
    }

    @Test
    void testFetchTaskContextCloseWithNullConnections() throws Exception {
        GaussDBSourceFetchTaskContext context =
                (GaussDBSourceFetchTaskContext) dialect.createFetchTaskContext(sourceConfig);

        // Should not throw when connections are null
        context.close();
    }

    @Test
    void testFetchTaskContextGetTableFilter() {
        GaussDBSourceFetchTaskContext context =
                (GaussDBSourceFetchTaskContext) dialect.createFetchTaskContext(sourceConfig);

        assertThat(context.getTableFilter()).isNotNull();
    }

    @Test
    void testDialectDiscoverDataCollectionsFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.discoverDataCollections(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDialectDiscoverDataCollectionSchemasFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.discoverDataCollectionSchemas(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDialectDisplayCommittedOffsetFailsWithoutDB() {
        assertThatThrownBy(() -> dialect.displayCommittedOffset(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testMultipleFetchTaskCreation() {
        // Create snapshot fetch task
        TableId tableId = new TableId("testdb", "public", "test_table");
        RowType splitKeyType =
                new RowType(Collections.singletonList(new RowType.RowField("id", new IntType())));

        SnapshotSplit snapshotSplit =
                new SnapshotSplit(tableId, 0, splitKeyType, null, null, null, new HashMap<>());
        var scanTask = dialect.createFetchTask(snapshotSplit);
        assertThat(scanTask).isInstanceOf(GaussDBScanFetchTask.class);

        // Create stream fetch task
        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split",
                        GaussDBOffset.INITIAL_OFFSET,
                        GaussDBOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);
        var streamTask = dialect.createFetchTask(streamSplit);
        assertThat(streamTask).isInstanceOf(GaussDBStreamFetchTask.class);
    }

    @Test
    void testStreamFetchTaskCloseStopsRunning() {
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        GaussDBOffset endingOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);

        GaussDBStreamFetchTask task = (GaussDBStreamFetchTask) dialect.createFetchTask(streamSplit);
        assertThat(task.isRunning()).isFalse();

        task.close();
        assertThat(task.isRunning()).isFalse();
    }
}
