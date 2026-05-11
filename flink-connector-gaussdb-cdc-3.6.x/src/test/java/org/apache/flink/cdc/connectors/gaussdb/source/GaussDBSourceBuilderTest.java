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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.enumerator.GaussDBSourceEnumerator;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffsetFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.reader.GaussDBSourceRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBSourceBuilder} and its inner class. */
class GaussDBSourceBuilderTest {

    private GaussDBSourceBuilder.GaussDBIncrementalSource<String> source;

    @BeforeEach
    void setUp() {
        // We can't actually call build() without a real deserializer,
        // but we can test builder methods
    }

    @Test
    void testBuilderCreation() {
        GaussDBSourceBuilder<String> builder =
                GaussDBSourceBuilder.GaussDBIncrementalSource.builder();
        assertThat(builder).isNotNull();
    }

    @Test
    void testBuilderChaining() {
        GaussDBSourceBuilder<String> builder =
                GaussDBSourceBuilder.GaussDBIncrementalSource.builder();
        GaussDBSourceBuilder<String> result =
                builder.hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schemaList("public")
                        .tableList("public.test_table")
                        .username("testuser")
                        .password("testpass")
                        .decodingPluginName("mppdb_decoding")
                        .slotName("flink_slot")
                        .splitSize(8096)
                        .splitMetaGroupSize(1000)
                        .fetchSize(1024)
                        .connectTimeout(java.time.Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .distributionFactorUpper(1000.0d)
                        .distributionFactorLower(0.05d)
                        .startupOptions(StartupOptions.initial())
                        .chunkKeyColumn("id")
                        .closeIdleReaders(false)
                        .heartbeatInterval(java.time.Duration.ofSeconds(30))
                        .skipSnapshotBackfill(false)
                        .scanNewlyAddedTableEnabled(false)
                        .lsnCommitCheckpointsDelay(3)
                        .parallelDecodeNum(1)
                        .decodeStyle("b")
                        .sendingBatch(false)
                        .includeDatabaseInTableId(true);

        assertThat(result).isSameAs(builder);
    }

    @Test
    void testBuilderWithDebeziumProperties() {
        GaussDBSourceBuilder<String> builder =
                GaussDBSourceBuilder.GaussDBIncrementalSource.builder();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("snapshot.mode", "never");
        GaussDBSourceBuilder<String> result = builder.debeziumProperties(props);
        assertThat(result).isSameAs(builder);
    }

    @Test
    void testGaussDBOffsetFactoryCreateInitialOffset() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        GaussDBOffset offset = (GaussDBOffset) factory.createInitialOffset();
        assertThat(offset).isEqualTo(GaussDBOffset.INITIAL_OFFSET);
    }

    @Test
    void testGaussDBOffsetFactoryCreateNoStoppingOffset() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        GaussDBOffset offset = (GaussDBOffset) factory.createNoStoppingOffset();
        assertThat(offset).isEqualTo(GaussDBOffset.NO_STOPPING_OFFSET);
    }

    @Test
    void testGaussDBOffsetFactoryNewOffsetFromMap() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        java.util.Map<String, String> offsetMap = new java.util.HashMap<>();
        offsetMap.put("lsn", "12345");
        GaussDBOffset offset = (GaussDBOffset) factory.newOffset(offsetMap);
        assertThat(offset).isNotNull();
        assertThat(offset.getLsn().asLong()).isEqualTo(12345L);
    }

    @Test
    void testGaussDBOffsetFactoryNewOffsetFromFilenamePositionThrows() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        assertThatThrownBy(() -> factory.newOffset("file", 0L))
                .isInstanceOf(org.apache.flink.util.FlinkRuntimeException.class);
    }

    @Test
    void testGaussDBOffsetFactoryNewOffsetFromPositionThrows() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        assertThatThrownBy(() -> factory.newOffset(0L))
                .isInstanceOf(org.apache.flink.util.FlinkRuntimeException.class);
    }

    @Test
    void testGaussDBOffsetFactoryCreateTimestampOffsetThrows() {
        GaussDBOffsetFactory factory = new GaussDBOffsetFactory();
        assertThatThrownBy(() -> factory.createTimestampOffset(0L))
                .isInstanceOf(org.apache.flink.util.FlinkRuntimeException.class);
    }

    @Test
    void testIncrementalSourceCreateRecordEmitter() throws Exception {
        // Build a minimal source to test createRecordEmitter
        GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.database("testdb");
        configFactory.username("testuser");
        configFactory.password("testpass");
        configFactory.port(8000);
        configFactory.tableList("public.test_table");
        configFactory.slotName("test_slot");

        DebeziumDeserializationSchema<RowData> deserializer =
                mock(DebeziumDeserializationSchema.class);
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));

        GaussDBSourceBuilder.GaussDBIncrementalSource<RowData> source =
                new GaussDBSourceBuilder.GaussDBIncrementalSource<>(
                        configFactory, deserializer, offsetFactory, dialect);

        // Test createRecordEmitter
        org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics metrics =
                mock(org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics.class);
        var emitter = source.createRecordEmitter(configFactory.create(0), metrics);
        assertThat(emitter).isInstanceOf(GaussDBSourceRecordEmitter.class);
    }

    @Test
    void testIncrementalSourceRestoreEnumeratorWithHybridState() {
        GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.database("testdb");
        configFactory.username("testuser");
        configFactory.password("testpass");
        configFactory.port(8000);
        configFactory.tableList("public.test_table");
        configFactory.slotName("test_slot");

        DebeziumDeserializationSchema<RowData> deserializer =
                mock(DebeziumDeserializationSchema.class);
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));

        GaussDBSourceBuilder.GaussDBIncrementalSource<RowData> source =
                new GaussDBSourceBuilder.GaussDBIncrementalSource<>(
                        configFactory, deserializer, offsetFactory, dialect);

        org.apache.flink.api.connector.source.SplitEnumeratorContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase>
                enumContext =
                        mock(org.apache.flink.api.connector.source.SplitEnumeratorContext.class);
        when(enumContext.currentParallelism()).thenReturn(1);

        // Test with HybridPendingSplitsState using real state objects
        org.apache.flink.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState
                snapshotState =
                        new org.apache.flink.cdc.connectors.base.source.assigner.state
                                .SnapshotPendingSplitsState(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                new HashMap<>(),
                                new HashMap<>(),
                                org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus
                                        .INITIAL_ASSIGNING_FINISHED,
                                Collections.emptyList(),
                                true,
                                false,
                                new HashMap<>(),
                                org.apache.flink.cdc.connectors.base.source.assigner.state
                                        .ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        HybridPendingSplitsState hybridState = new HybridPendingSplitsState(snapshotState, true);
        var enumerator = source.restoreEnumerator(enumContext, hybridState);
        assertThat(enumerator).isInstanceOf(GaussDBSourceEnumerator.class);
    }

    @Test
    void testIncrementalSourceRestoreEnumeratorWithStreamState() {
        GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.database("testdb");
        configFactory.username("testuser");
        configFactory.password("testpass");
        configFactory.port(8000);
        configFactory.tableList("public.test_table");
        configFactory.slotName("test_slot");
        configFactory.startupOptions(StartupOptions.latest());

        DebeziumDeserializationSchema<RowData> deserializer =
                mock(DebeziumDeserializationSchema.class);
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));

        GaussDBSourceBuilder.GaussDBIncrementalSource<RowData> source =
                new GaussDBSourceBuilder.GaussDBIncrementalSource<>(
                        configFactory, deserializer, offsetFactory, dialect);

        org.apache.flink.api.connector.source.SplitEnumeratorContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase>
                enumContext =
                        mock(org.apache.flink.api.connector.source.SplitEnumeratorContext.class);
        when(enumContext.currentParallelism()).thenReturn(1);

        // Test with StreamPendingSplitsState
        StreamPendingSplitsState streamState = mock(StreamPendingSplitsState.class);
        var enumerator = source.restoreEnumerator(enumContext, streamState);
        assertThat(enumerator).isInstanceOf(GaussDBSourceEnumerator.class);
    }

    @Test
    void testIncrementalSourceRestoreEnumeratorWithUnsupportedState() {
        GaussDBSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.database("testdb");
        configFactory.username("testuser");
        configFactory.password("testpass");
        configFactory.port(8000);
        configFactory.tableList("public.test_table");
        configFactory.slotName("test_slot");

        DebeziumDeserializationSchema<RowData> deserializer =
                mock(DebeziumDeserializationSchema.class);
        GaussDBOffsetFactory offsetFactory = new GaussDBOffsetFactory();
        GaussDBDialect dialect = new GaussDBDialect(configFactory.create(0));

        GaussDBSourceBuilder.GaussDBIncrementalSource<RowData> source =
                new GaussDBSourceBuilder.GaussDBIncrementalSource<>(
                        configFactory, deserializer, offsetFactory, dialect);

        org.apache.flink.api.connector.source.SplitEnumeratorContext<
                        org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase>
                enumContext =
                        mock(org.apache.flink.api.connector.source.SplitEnumeratorContext.class);

        // Test with unsupported PendingSplitsState
        org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState
                unsupportedState =
                        mock(
                                org.apache.flink.cdc.connectors.base.source.assigner.state
                                        .PendingSplitsState.class);
        assertThatThrownBy(() -> source.restoreEnumerator(enumContext, unsupportedState))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
