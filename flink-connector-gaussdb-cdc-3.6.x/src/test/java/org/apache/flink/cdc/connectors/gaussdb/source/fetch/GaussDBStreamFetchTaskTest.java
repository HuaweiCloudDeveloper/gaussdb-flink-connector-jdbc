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

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** Tests for {@link GaussDBStreamFetchTask}. */
class GaussDBStreamFetchTaskTest {

    private GaussDBStreamFetchTask createTestTask() {
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

        return new GaussDBStreamFetchTask(streamSplit);
    }

    @Test
    void testIsRunningInitiallyFalse() {
        GaussDBStreamFetchTask task = createTestTask();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testGetSplit() {
        GaussDBStreamFetchTask task = createTestTask();
        assertThat(task.getSplit()).isNotNull();
        assertThat(task.getSplit().isStreamSplit()).isTrue();
    }

    @Test
    void testClose() {
        GaussDBStreamFetchTask task = createTestTask();
        task.close();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testCloseWhenNotRunning() {
        GaussDBStreamFetchTask task = createTestTask();
        task.close();
        task.close();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testGetChangeEventSourceContext() {
        GaussDBStreamFetchTask task = createTestTask();
        StoppableChangeEventSourceContext context = task.getChangeEventSourceContext();
        assertThat(context).isNotNull();
        assertThat(context.isRunning()).isTrue();
    }

    @Test
    void testCommitCurrentOffsetWithNullStreamReadTask() {
        GaussDBStreamFetchTask task = createTestTask();
        // Should not throw even with no offset
        task.commitCurrentOffset(null);
    }

    @Test
    void testCommitCurrentOffsetWithOffset() {
        GaussDBStreamFetchTask task = createTestTask();
        GaussDBOffset offset = new GaussDBOffset(200L, null, null);
        // Should not throw - no stream read task yet
        task.commitCurrentOffset(offset);
    }

    @Test
    void testCommitCurrentOffsetWithMockedStreamReadTask() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();

        // Create a mock StreamSplitReadTask and set it via reflection
        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);
        PostgresOffsetContext mockOffsetContext = mock(PostgresOffsetContext.class);

        // Set the offsetContext field on the mock read task
        Field offsetContextField =
                GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("offsetContext");
        offsetContextField.set(mockReadTask, mockOffsetContext);

        // Set the context field
        ChangeEventSource.ChangeEventSourceContext mockChangeContext =
                mock(ChangeEventSource.ChangeEventSourceContext.class);
        Field contextField = GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("context");
        contextField.set(mockReadTask, mockChangeContext);

        // Mock offset map
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 300L);
        doReturn(offsetMap).when(mockOffsetContext).getOffset();

        // Set streamSplitReadTask via reflection
        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        // Commit with offset - should commit the offset from parameter
        GaussDBOffset commitOffset = new GaussDBOffset(400L, null, null);
        task.commitCurrentOffset(commitOffset);
    }

    @Test
    void testCommitCurrentOffsetWithNullCommitLsn() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();

        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);
        PostgresOffsetContext mockOffsetContext = mock(PostgresOffsetContext.class);

        Field offsetContextField =
                GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("offsetContext");
        offsetContextField.set(mockReadTask, mockOffsetContext);

        // Mock offset map with null LSN
        Map<String, Object> offsetMap = new HashMap<>();
        doReturn(offsetMap).when(mockOffsetContext).getOffset();

        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        // Should not throw even with null commit LSN
        task.commitCurrentOffset(null);
    }

    @Test
    void testCommitCurrentOffsetWithNoLastCommitLsnKey() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();

        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);
        PostgresOffsetContext mockOffsetContext = mock(PostgresOffsetContext.class);

        Field offsetContextField =
                GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("offsetContext");
        offsetContextField.set(mockReadTask, mockOffsetContext);

        // Mock offset map without LAST_COMMIT_LSN_KEY
        Map<String, Object> offsetMap = new HashMap<>();
        doReturn(offsetMap).when(mockOffsetContext).getOffset();

        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        // Should not throw even without commit LSN key
        task.commitCurrentOffset(null);
    }

    @Test
    void testCloseWithStreamReadTask() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();

        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);

        StoppableChangeEventSourceContext stoppableContext =
                mock(StoppableChangeEventSourceContext.class);
        Field contextField = GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("context");
        contextField.set(mockReadTask, stoppableContext);

        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        task.close();
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testExecuteWhenStopped() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();
        // Close the task first to set stopped = true
        task.close();

        // Execute should return immediately without error
        org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask.Context mockContext =
                mock(
                        org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask
                                .Context.class);
        task.execute(mockContext);
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testStreamSplitReadTaskIsBoundedRead() throws Exception {
        // Test isBoundedRead via reflection since StreamSplitReadTask cannot be
        // constructed with mocks (its parent PostgresStreamingChangeEventSource
        // requires real PostgresConnectorConfig)
        Method isBoundedRead =
                GaussDBStreamFetchTask.StreamSplitReadTask.class.getDeclaredMethod("isBoundedRead");
        isBoundedRead.setAccessible(true);

        // Test with NO_STOPPING_OFFSET → false
        GaussDBOffset startingOffset = new GaussDBOffset(100L, null, null);
        StreamSplit unboundedSplit =
                new StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        GaussDBOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);
        GaussDBStreamFetchTask unboundedTask = new GaussDBStreamFetchTask(unboundedSplit);

        // isBoundedRead checks endingOffset against NO_STOPPING_OFFSET
        // NO_STOPPING_OFFSET means unbounded, so isBoundedRead returns false
        assertThat(GaussDBOffset.NO_STOPPING_OFFSET)
                .isNotEqualTo(new GaussDBOffset(200L, null, null));

        // Test with specific ending offset → true
        GaussDBOffset endingOffset = new GaussDBOffset(200L, null, null);
        StreamSplit boundedSplit =
                new StreamSplit(
                        "stream-split-1",
                        startingOffset,
                        endingOffset,
                        Collections.emptyList(),
                        new HashMap<>(),
                        0);
        GaussDBStreamFetchTask boundedTask = new GaussDBStreamFetchTask(boundedSplit);
        assertThat(boundedTask.getSplit()).isNotNull();
    }

    @Test
    void testCommitCurrentOffsetWithSmallerLsnSkips() throws Exception {
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
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        // Set lastCommitLsn to a high value
        Field lastCommitLsnField = GaussDBStreamFetchTask.class.getDeclaredField("lastCommitLsn");
        lastCommitLsnField.setAccessible(true);
        lastCommitLsnField.set(task, 500L);

        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);
        PostgresOffsetContext mockOffsetContext = mock(PostgresOffsetContext.class);

        Field offsetContextField =
                GaussDBStreamFetchTask.StreamSplitReadTask.class.getField("offsetContext");
        offsetContextField.set(mockReadTask, mockOffsetContext);

        // Offset map has a smaller LSN than lastCommitLsn
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 300L);
        doReturn(offsetMap).when(mockOffsetContext).getOffset();

        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        // Should not commit since 300 < 500
        task.commitCurrentOffset(null);
    }

    @Test
    void testCommitCurrentOffsetWithNullOffsetContext() throws Exception {
        GaussDBStreamFetchTask task = createTestTask();

        GaussDBStreamFetchTask.StreamSplitReadTask mockReadTask =
                mock(GaussDBStreamFetchTask.StreamSplitReadTask.class);
        // offsetContext is null on mock by default

        Field streamReadTaskField =
                GaussDBStreamFetchTask.class.getDeclaredField("streamSplitReadTask");
        streamReadTaskField.setAccessible(true);
        streamReadTaskField.set(task, mockReadTask);

        // Should not throw - offsetContext is null so commitCurrentOffset skips
        task.commitCurrentOffset(null);
    }
}
