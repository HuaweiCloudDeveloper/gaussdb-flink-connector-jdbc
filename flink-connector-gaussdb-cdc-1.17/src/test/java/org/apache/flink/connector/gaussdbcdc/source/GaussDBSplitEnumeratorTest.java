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

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link GaussDBSplitEnumerator}. */
class GaussDBSplitEnumeratorTest {

    @Test
    @SuppressWarnings("unchecked")
    void testEnumeratorCreation() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        assertThat(enumerator).isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testEnumeratorCreationWithRestore() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        List<GaussDBSplit> assigned =
                Arrays.asList(new GaussDBSplit("snapshot-0", "test_table", 1L, 100L));
        List<GaussDBSplit> unassigned =
                Arrays.asList(new GaussDBSplit("snapshot-1", "test_table", 101L, 200L));
        GaussDBCheckpoint checkpoint = new GaussDBCheckpoint(assigned, unassigned, "0/0", true);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        checkpoint,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        assertThat(enumerator).isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testEnumeratorCreationWithNullCheckpoint() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        null,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        assertThat(enumerator).isNotNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCloseWithoutStart() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAddSplitsBack() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");

        GaussDBSplit split1 = new GaussDBSplit("snapshot-1", "test_table", 101L, 200L);
        GaussDBSplit split2 = new GaussDBSplit("snapshot-0", "test_table", 1L, 100L);
        enumerator.addSplitsBack(Arrays.asList(split1, split2), 0);

        // After addSplitsBack, nextSplitIndex should be 0
        // Verify by calling snapshotState
        GaussDBCheckpoint checkpoint = enumerator.snapshotState(1);
        // The splits added back should be in the unassigned list
        assertThat(checkpoint.getUnassignedSplits()).hasSize(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleSourceEvent() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.handleSourceEvent(0, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleSplitRequestWithNoSplits() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        // Start the enumerator (creates stream split)
        enumerator.start();
        // Request split when no snapshot splits, snapshotCompleted=false
        enumerator.handleSplitRequest(0, "host");
        // Should assign stream split and set snapshotCompleted=true
        verify(context).assignSplit(any(GaussDBSplit.class), anyInt());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleSplitRequestSignalNoMore() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.start();
        // First request: assigns stream split, sets snapshotCompleted=true
        enumerator.handleSplitRequest(0, "host");
        // Second request: no more splits, should signalNoMoreSplits
        enumerator.handleSplitRequest(1, "host");
        verify(context).signalNoMoreSplits(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleSplitRequestWithSnapshotSplits() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.start();

        // Manually add snapshot splits via reflection
        java.lang.reflect.Field snapshotSplitsField =
                GaussDBSplitEnumerator.class.getDeclaredField("snapshotSplits");
        snapshotSplitsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<GaussDBSplit> splits = (List<GaussDBSplit>) snapshotSplitsField.get(enumerator);
        splits.add(new GaussDBSplit("snapshot-0", "test_table", 1L, 100L));
        splits.add(new GaussDBSplit("snapshot-1", "test_table", 101L, 200L));

        // Request first split
        enumerator.handleSplitRequest(0, "host");
        verify(context).assignSplit(any(GaussDBSplit.class), anyInt());

        // Request second split
        enumerator.handleSplitRequest(0, "host");

        // Third request should assign stream split (snapshotCompleted becomes true)
        enumerator.handleSplitRequest(0, "host");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotState() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.start();

        // Add splits manually
        java.lang.reflect.Field snapshotSplitsField =
                GaussDBSplitEnumerator.class.getDeclaredField("snapshotSplits");
        snapshotSplitsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<GaussDBSplit> splits = (List<GaussDBSplit>) snapshotSplitsField.get(enumerator);
        splits.add(new GaussDBSplit("snapshot-0", "test_table", 1L, 100L));
        splits.add(new GaussDBSplit("snapshot-1", "test_table", 101L, 200L));

        // No splits assigned yet
        GaussDBCheckpoint checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getUnassignedSplits()).hasSize(2);
        assertThat(checkpoint.getAssignedSplits()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotStateAfterAssignment() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.start();

        // Add splits and assign one
        java.lang.reflect.Field snapshotSplitsField =
                GaussDBSplitEnumerator.class.getDeclaredField("snapshotSplits");
        snapshotSplitsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<GaussDBSplit> splits = (List<GaussDBSplit>) snapshotSplitsField.get(enumerator);
        splits.add(new GaussDBSplit("snapshot-0", "test_table", 1L, 100L));
        splits.add(new GaussDBSplit("snapshot-1", "test_table", 101L, 200L));

        enumerator.handleSplitRequest(0, "host"); // assigns snapshot-0

        GaussDBCheckpoint checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getAssignedSplits()).hasSize(1);
        assertThat(checkpoint.getUnassignedSplits()).hasSize(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStartWithSnapshotModeNoDb() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        true,
                        1000,
                        30000,
                        "prefer");
        // start() will try to connect to DB and fail
        assertThatThrownBy(() -> enumerator.start())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to create snapshot splits");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStartWithoutSnapshotMode() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.start();
        // Should create stream split
        java.lang.reflect.Field streamSplitField =
                GaussDBSplitEnumerator.class.getDeclaredField("streamSplit");
        streamSplitField.setAccessible(true);
        GaussDBSplit streamSplit = (GaussDBSplit) streamSplitField.get(enumerator);
        assertThat(streamSplit).isNotNull();
        assertThat(streamSplit.isStreamSplit()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAddReader() {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");
        enumerator.addReader(0); // Should not throw
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRestoreEnumeratorWithCheckpoint() throws Exception {
        SplitEnumeratorContext<GaussDBSplit> context = mock(SplitEnumeratorContext.class);
        List<GaussDBSplit> assigned =
                Collections.singletonList(new GaussDBSplit("snapshot-0", "test_table", 1L, 100L));
        List<GaussDBSplit> unassigned =
                Collections.singletonList(new GaussDBSplit("snapshot-1", "test_table", 101L, 200L));
        GaussDBCheckpoint checkpoint = new GaussDBCheckpoint(assigned, unassigned, "0/1A", false);

        GaussDBSplitEnumerator enumerator =
                new GaussDBSplitEnumerator(
                        context,
                        checkpoint,
                        "localhost",
                        5432,
                        "testdb",
                        "public",
                        "test_table",
                        "user",
                        "password",
                        false,
                        1000,
                        30000,
                        "prefer");

        // Verify the restored splits are present
        java.lang.reflect.Field snapshotSplitsField =
                GaussDBSplitEnumerator.class.getDeclaredField("snapshotSplits");
        snapshotSplitsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<GaussDBSplit> splits = (List<GaussDBSplit>) snapshotSplitsField.get(enumerator);
        assertThat(splits).hasSize(1); // unassigned splits from checkpoint

        java.lang.reflect.Field snapshotCompletedField =
                GaussDBSplitEnumerator.class.getDeclaredField("snapshotCompleted");
        snapshotCompletedField.setAccessible(true);
        boolean completed = (boolean) snapshotCompletedField.get(enumerator);
        assertThat(completed).isFalse();
    }
}
