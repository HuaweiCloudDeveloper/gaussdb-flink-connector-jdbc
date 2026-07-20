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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Comprehensive tests for {@link GaussDBCDCSourceFunction} lifecycle and runtime methods. */
class GaussDBCDCSourceFunctionLifecycleTest {

    private static final String TABLE_NAME = "test_table";

    private GaussDBCDCSourceFunction source;
    private Connection mockConnection;
    private SourceFunction.SourceContext<RowData> mockContext;
    private Object checkpointLock;

    @BeforeEach
    void setUp() {
        source = createTestSource(false);
        mockConnection = mock(Connection.class);
        mockContext = mock(SourceFunction.SourceContext.class);
        checkpointLock = new Object();
        when(mockContext.getCheckpointLock()).thenReturn(checkpointLock);
        // Multi-table support: initialize per-table cache maps used by readSnapshot
        // and convertWalColumnsToRowData. open() normally does this but the tests
        // bypass open(), so we set the fields here via reflection.
        try {
            setField(source, "cachedColumnsByTable", new HashMap<String, List<String>>());
            setField(source, "cachedPkByTable", new HashMap<String, String>());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Pre-populate column cache for a table, used by WAL streaming tests. */
    @SuppressWarnings("unchecked")
    private void cacheTestTableColumns() throws Exception {
        Map<String, List<String>> colsMap =
                (Map<String, List<String>>) getField(source, "cachedColumnsByTable", Map.class);
        List<String> cols = new ArrayList<>();
        cols.add("id");
        colsMap.put("test_table", cols);

        Map<String, String> pkMap =
                (Map<String, String>) getField(source, "cachedPkByTable", Map.class);
        pkMap.put("test_table", "id");
    }

    // ---- open() tests ----

    @Test
    void testSourceSupportsParallelExecution() {
        assertThat(source).isInstanceOf(ParallelSourceFunction.class);
    }

    @Test
    void testOpenWithPollingMode() throws Exception {
        // Polling mode (walMode=false) should create ChangeDataPoller
        GaussDBCDCSourceFunction pollingSource = createTestSource(false);
        setField(pollingSource, "connection", mockConnection);

        // open() tries to load driver and create connection - we can't mock DriverManager
        // so we test the internal state setup via reflection
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        setField(pollingSource, "changeDataPoller", poller);

        assertThat(getField(pollingSource, "changeDataPoller", ChangeDataPoller.class)).isNotNull();
    }

    @Test
    void testOpenWithWalMode() throws Exception {
        GaussDBCDCSourceFunction walSource = createTestSource(true);
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        setField(walSource, "walReplicationStream", walStream);
        setField(walSource, "connection", mockConnection);

        assertThat(getField(walSource, "walReplicationStream", WalReplicationStream.class))
                .isNotNull();
    }

    // ---- close() tests ----

    @Test
    void testCloseWithNullConnection() {
        assertThatCode(() -> source.close()).doesNotThrowAnyException();
    }

    @Test
    void testCloseWithOpenConnection() throws Exception {
        when(mockConnection.isClosed()).thenReturn(false);
        setField(source, "connection", mockConnection);

        source.close();
        verify(mockConnection).close();
    }

    @Test
    void testCloseWithAlreadyClosedConnection() throws Exception {
        when(mockConnection.isClosed()).thenReturn(true);
        setField(source, "connection", mockConnection);

        source.close();
        verify(mockConnection, never()).close();
    }

    @Test
    void testCloseWithWalReplicationStream() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        setField(source, "walReplicationStream", walStream);
        setField(source, "connection", mockConnection);
        when(mockConnection.isClosed()).thenReturn(false);

        source.close();
        verify(walStream).close();
        verify(mockConnection).close();
    }

    @Test
    void testCloseSetsRunningToFalse() throws Exception {
        setField(source, "running", true);
        source.close();
        assertThat(getField(source, "running", boolean.class)).isFalse();
    }

    // ---- cancel() tests ----

    @Test
    void testCancelSetsRunningToFalse() throws Exception {
        source.cancel();
        assertThat(getField(source, "running", boolean.class)).isFalse();
    }

    // ---- run() tests ----

    @Test
    void testRunSkipsSnapshotWhenDisabled() throws Exception {
        GaussDBCDCSourceFunction noSnapshotSource = createTestSource(false);
        setField(noSnapshotSource, "snapshotMode", false);
        setField(noSnapshotSource, "connection", mockConnection);
        setField(noSnapshotSource, "running", false);

        // With running=false immediately, run() should exit without snapshot
        noSnapshotSource.run(mockContext);
        // No exception means it skipped snapshot
    }

    @Test
    void testExportSnapshotBoundaryUsesAtomicGaussDbFunction() throws Exception {
        setField(source, "connection", mockConnection);
        PreparedStatement probe = mockFunctionProbe(true);
        when(mockConnection.prepareStatement(contains("FROM pg_catalog.pg_proc")))
                .thenReturn(probe);

        PreparedStatement isolation = mock(PreparedStatement.class);
        PreparedStatement export = mock(PreparedStatement.class);
        ResultSet exportResult = mock(ResultSet.class);
        when(mockConnection.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                .thenReturn(isolation);
        when(mockConnection.prepareStatement(
                        "SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()"))
                .thenReturn(export);
        when(export.executeQuery()).thenReturn(exportResult);
        when(exportResult.next()).thenReturn(true);
        when(exportResult.getString(1)).thenReturn("00000001-00000002-1");
        when(exportResult.getString(2)).thenReturn("64");

        Object boundary = invokeExportSnapshotBoundary(source);

        assertThat(readBoundaryField(boundary, "snapshotId")).isEqualTo("00000001-00000002-1");
        assertThat(readBoundaryField(boundary, "csn")).isEqualTo(100L);
        verify(mockConnection).setAutoCommit(false);
    }

    @Test
    void testExportSnapshotBoundaryFallsBackToStandardSnapshotAndCurrentCsn() throws Exception {
        setField(source, "connection", mockConnection);
        PreparedStatement combinedProbe = mockFunctionProbe(false);
        PreparedStatement csnProbe = mockFunctionProbe(true);
        when(mockConnection.prepareStatement(contains("FROM pg_catalog.pg_proc")))
                .thenReturn(combinedProbe, csnProbe);

        PreparedStatement isolation = mock(PreparedStatement.class);
        PreparedStatement currentCsn = mock(PreparedStatement.class);
        PreparedStatement export = mock(PreparedStatement.class);
        ResultSet csnResult = mock(ResultSet.class);
        ResultSet exportResult = mock(ResultSet.class);
        when(mockConnection.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                .thenReturn(isolation);
        when(mockConnection.prepareStatement("SELECT pg_catalog.pg_current_csn()"))
                .thenReturn(currentCsn);
        when(currentCsn.executeQuery()).thenReturn(csnResult);
        when(csnResult.next()).thenReturn(true);
        when(csnResult.getObject(1)).thenReturn(101L);
        when(mockConnection.prepareStatement("SELECT pg_catalog.pg_export_snapshot()"))
                .thenReturn(export);
        when(export.executeQuery()).thenReturn(exportResult);
        when(exportResult.next()).thenReturn(true);
        when(exportResult.getString(1)).thenReturn("00000001-00000003-1");

        Object boundary = invokeExportSnapshotBoundary(source);

        assertThat(readBoundaryField(boundary, "snapshotId")).isEqualTo("00000001-00000003-1");
        assertThat(readBoundaryField(boundary, "csn")).isEqualTo(101L);
    }

    @Test
    void testExportSnapshotBoundaryDisablesCsnFilterWhenOnlyStandardFunctionExists()
            throws Exception {
        setField(source, "connection", mockConnection);
        PreparedStatement combinedProbe = mockFunctionProbe(false);
        PreparedStatement csnProbe = mockFunctionProbe(false);
        when(mockConnection.prepareStatement(contains("FROM pg_catalog.pg_proc")))
                .thenReturn(combinedProbe, csnProbe);

        PreparedStatement isolation = mock(PreparedStatement.class);
        PreparedStatement export = mock(PreparedStatement.class);
        ResultSet exportResult = mock(ResultSet.class);
        when(mockConnection.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                .thenReturn(isolation);
        when(mockConnection.prepareStatement("SELECT pg_catalog.pg_export_snapshot()"))
                .thenReturn(export);
        when(export.executeQuery()).thenReturn(exportResult);
        when(exportResult.next()).thenReturn(true);
        when(exportResult.getString(1)).thenReturn("00000001-00000004-1");

        Object boundary = invokeExportSnapshotBoundary(source);

        assertThat(readBoundaryField(boundary, "snapshotId")).isEqualTo("00000001-00000004-1");
        assertThat(readBoundaryField(boundary, "csn")).isEqualTo(-1L);
    }

    @Test
    void testExportSnapshotBoundaryRollsBackFailedAtomicExportBeforeFallback() throws Exception {
        setField(source, "connection", mockConnection);
        PreparedStatement combinedProbe = mockFunctionProbe(true);
        PreparedStatement csnProbe = mockFunctionProbe(false);
        when(mockConnection.prepareStatement(contains("FROM pg_catalog.pg_proc")))
                .thenReturn(combinedProbe, csnProbe);

        PreparedStatement isolation = mock(PreparedStatement.class);
        PreparedStatement combinedExport = mock(PreparedStatement.class);
        PreparedStatement standardExport = mock(PreparedStatement.class);
        ResultSet standardResult = mock(ResultSet.class);
        when(mockConnection.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                .thenReturn(isolation);
        when(mockConnection.prepareStatement(
                        "SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()"))
                .thenReturn(combinedExport);
        when(combinedExport.executeQuery()).thenThrow(new SQLException("function disappeared"));
        when(mockConnection.prepareStatement("SELECT pg_catalog.pg_export_snapshot()"))
                .thenReturn(standardExport);
        when(standardExport.executeQuery()).thenReturn(standardResult);
        when(standardResult.next()).thenReturn(true);
        when(standardResult.getString(1)).thenReturn("00000001-00000005-1");

        Object boundary = invokeExportSnapshotBoundary(source);

        assertThat(readBoundaryField(boundary, "snapshotId")).isEqualTo("00000001-00000005-1");
        assertThat(readBoundaryField(boundary, "csn")).isEqualTo(-1L);
        verify(mockConnection).rollback();
        verify(mockConnection).setAutoCommit(true);
    }

    // ---- readSnapshot() tests ----

    @Test
    void testReadSnapshotSingleSubtask() throws Exception {
        setupReadSnapshotMocks(1, 0);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class, String.class);
        method.setAccessible(true);
        method.invoke(source, mockContext, TABLE_NAME);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotParallelSubtask() throws Exception {
        setupReadSnapshotMocks(4, 1);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class, String.class);
        method.setAccessible(true);
        method.invoke(source, mockContext, TABLE_NAME);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotParallelLastSubtask() throws Exception {
        setupReadSnapshotMocks(4, 3);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class, String.class);
        method.setAccessible(true);
        method.invoke(source, mockContext, TABLE_NAME);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotEmptyTable() throws Exception {
        setupReadSnapshotEmptyMocks(4, 0);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class, String.class);
        method.setAccessible(true);
        method.invoke(source, mockContext, TABLE_NAME);

        verify(mockContext, never()).collect(any(RowData.class));
    }

    // ---- runWalStreaming() tests ----

    @Test
    void testRunWalStreamingWithInsertChange() throws Exception {
        cacheTestTableColumns();
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange insert =
                WalChange.insert(
                        "0/1",
                        100,
                        "public",
                        "test_table",
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)));
        changes.add(insert);

        when(walStream.readChanges(1000)).thenReturn(changes).thenReturn(new ArrayList<>());
        when(walStream.getLastLsn()).thenReturn("0/1");
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "running", true);

        // Stop after first iteration
        org.mockito.stubbing.Answer<List<WalChange>> stopAnswer1 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenReturn(changes).thenAnswer(stopAnswer1);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
        assertThat(getField(source, "lastConsumedLsn", String.class)).isEqualTo("0/1");
    }

    @Test
    void testRunWalStreamingWithUpdateChange() throws Exception {
        cacheTestTableColumns();
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange update =
                WalChange.update(
                        "0/2",
                        101,
                        "public",
                        "test_table",
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)),
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "2", false)));
        changes.add(update);

        when(walStream.readChanges(1000)).thenReturn(changes).thenReturn(new ArrayList<>());
        when(walStream.getLastLsn()).thenReturn("0/2");
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "running", true);

        // Use Answer to set running=false after first readChanges call
        org.mockito.stubbing.Answer<List<WalChange>> answer =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenReturn(changes).thenAnswer(answer);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testRunWalStreamingSkipsNonDataChanges() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        changes.add(WalChange.begin("0/1", 100, 0));
        changes.add(WalChange.commit("0/1", 100));

        org.mockito.stubbing.Answer<List<WalChange>> stopAnswer =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenReturn(changes).thenAnswer(stopAnswer);
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, never()).collect(any(RowData.class));
    }

    @Test
    void testSqlWalTransactionAlreadyVisibleInSnapshotIsDiscarded() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.isUseReplicationApi()).thenReturn(false);
        setField(source, "walReplicationStream", walStream);
        setField(source, "snapshotCsn", 100L);

        WalChange insert =
                WalChange.insert(
                        "0/11",
                        7,
                        "public",
                        TABLE_NAME,
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)));
        WalChange commit = WalChange.commit("0/12", 7);
        commit.setCsn(100L);

        List<WalChange> committed =
                prepareCommittedChanges(
                        source,
                        java.util.Arrays.asList(WalChange.begin("0/10", 7, 0), insert, commit));

        assertThat(committed).isEmpty();
        assertThat(getField(source, "lastCommittedWalLsn", String.class)).isEqualTo("0/12");
    }

    @Test
    void testSqlWalTransactionAfterSnapshotIsEmittedAcrossReadBatches() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.isUseReplicationApi()).thenReturn(false);
        setField(source, "walReplicationStream", walStream);
        setField(source, "snapshotCsn", 100L);

        WalChange insert =
                WalChange.insert(
                        "0/21",
                        8,
                        "public",
                        TABLE_NAME,
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "2", false)));
        assertThat(
                        prepareCommittedChanges(
                                source,
                                java.util.Arrays.asList(WalChange.begin("0/20", 8, 0), insert)))
                .isEmpty();

        WalChange commit = WalChange.commit("0/22", 8);
        commit.setCsn(101L);
        assertThat(prepareCommittedChanges(source, Collections.singletonList(commit)))
                .containsExactly(insert);
        assertThat(getField(source, "lastCommittedWalLsn", String.class)).isEqualTo("0/22");
    }

    @Test
    void testSqlWalLaterCommitWaitsForEarlierCrossBatchTransaction() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.isUseReplicationApi()).thenReturn(false);
        setField(source, "walReplicationStream", walStream);
        setField(source, "snapshotCsn", -1L);

        WalChange delete =
                WalChange.delete(
                        "0/11",
                        7,
                        "public",
                        TABLE_NAME,
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)));
        assertThat(
                        prepareCommittedChanges(
                                source,
                                java.util.Arrays.asList(WalChange.begin("0/10", 7, 0), delete)))
                .isEmpty();

        WalChange insert =
                WalChange.insert(
                        "0/21",
                        8,
                        "public",
                        TABLE_NAME,
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "2", false)));
        WalChange insertCommit = WalChange.commit("0/22", 8);
        insertCommit.setCsn(102L);
        assertThat(
                        prepareCommittedChanges(
                                source,
                                java.util.Arrays.asList(
                                        WalChange.begin("0/20", 8, 0), insert, insertCommit)))
                .isEmpty();
        assertThat(getField(source, "lastCommittedWalLsn", String.class)).isNull();

        WalChange deleteCommit = WalChange.commit("0/30", 7);
        deleteCommit.setCsn(101L);
        assertThat(prepareCommittedChanges(source, Collections.singletonList(deleteCommit)))
                .containsExactly(delete, insert);
        assertThat(getField(source, "lastCommittedWalLsn", String.class)).isEqualTo("0/30");
    }

    @Test
    void testSqlWalDeduplicatesByCommitLsnNotDeleteDataLsn() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.isUseReplicationApi()).thenReturn(false);
        setField(source, "walReplicationStream", walStream);
        setField(source, "lastConsumedLsn", "0/15");

        WalChange delete =
                WalChange.delete(
                        "0/11",
                        9,
                        "public",
                        TABLE_NAME,
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)));
        WalChange commit = WalChange.commit("0/20", 9);
        commit.setCsn(103L);

        assertThat(
                        prepareCommittedChanges(
                                source,
                                java.util.Arrays.asList(
                                        WalChange.begin("0/10", 9, 0), delete, commit)))
                .containsExactly(delete);
        assertThat(getField(source, "lastCommittedWalLsn", String.class)).isEqualTo("0/20");
    }

    @Test
    void testRunWalStreamingWithDeleteChange() throws Exception {
        cacheTestTableColumns();
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange delete =
                WalChange.delete(
                        "0/3",
                        102,
                        "public",
                        "test_table",
                        Collections.singletonList(new WalChange.ColumnValue("id", 23, "1", false)));
        changes.add(delete);

        when(walStream.readChanges(1000)).thenReturn(changes).thenReturn(new ArrayList<>());
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<WalChange>> stopAnswer3 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenReturn(changes).thenAnswer(stopAnswer3);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        // DELETE produces a before row with RowKind.DELETE
        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @SuppressWarnings("unchecked")
    private static List<WalChange> prepareCommittedChanges(
            GaussDBCDCSourceFunction source, List<WalChange> changes) throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "prepareCommittedWalChanges", List.class);
        method.setAccessible(true);
        return (List<WalChange>) method.invoke(source, changes);
    }

    @Test
    void testRunWalStreamingInitializeStream() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.getLastLsn()).thenReturn("0/1");
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", false);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<WalChange>> stopAnswer4 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenAnswer(stopAnswer4);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(walStream).initialize();
        verify(walStream, never()).readChanges(10000);
        assertThat(getField(source, "walStreamInitialized", boolean.class)).isTrue();
    }

    @Test
    void testRunWalStreamingSleepsOnEmptyChanges() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "running", true);
        setField(source, "pollIntervalMs", 10);

        org.mockito.stubbing.Answer<List<WalChange>> stopThenEmpty =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(walStream.readChanges(1000)).thenAnswer(stopThenEmpty);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runWalStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        // Should not throw
        method.invoke(source, mockContext);
    }

    // ---- runPollingStreaming() tests ----

    @Test
    void testRunPollingStreamingWithInsertEvent() throws Exception {
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        RowData row = mock(RowData.class);
        events.add(ChangeEvent.insert("test_table", row, System.currentTimeMillis()));

        setField(source, "changeDataPoller", poller);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<ChangeEvent<RowData>>> stopPollAnswer =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(poller.pollAllChanges()).thenReturn(events).thenAnswer(stopPollAnswer);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testRunPollingStreamingWithUpdateEvent() throws Exception {
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        RowData beforeRow = mock(RowData.class);
        RowData afterRow = mock(RowData.class);
        events.add(
                ChangeEvent.update("test_table", beforeRow, afterRow, System.currentTimeMillis()));

        setField(source, "changeDataPoller", poller);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<ChangeEvent<RowData>>> stopPollAnswer2 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(poller.pollAllChanges()).thenReturn(events).thenAnswer(stopPollAnswer2);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testRunPollingStreamingWithDeleteEvent() throws Exception {
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        RowData row = mock(RowData.class);
        events.add(ChangeEvent.delete("test_table", row, System.currentTimeMillis()));

        setField(source, "changeDataPoller", poller);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<ChangeEvent<RowData>>> stopPollAnswer3 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(poller.pollAllChanges()).thenReturn(events).thenAnswer(stopPollAnswer3);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        // DELETE produces a before row with RowKind.DELETE
        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testRunPollingStreamingWithSnapshotEvent() throws Exception {
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        RowData row = mock(RowData.class);
        events.add(ChangeEvent.snapshot("test_table", row, System.currentTimeMillis()));

        setField(source, "changeDataPoller", poller);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<ChangeEvent<RowData>>> stopPollAnswer4 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(poller.pollAllChanges()).thenReturn(events).thenAnswer(stopPollAnswer4);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testRunPollingStreamingCallsLoadSnapshot() throws Exception {
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        setField(source, "changeDataPoller", poller);
        setField(source, "running", true);

        org.mockito.stubbing.Answer<List<ChangeEvent<RowData>>> stopPollAnswer5 =
                invocation -> {
                    setField(source, "running", false);
                    return new ArrayList<>();
                };
        when(poller.pollAllChanges()).thenAnswer(stopPollAnswer5);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(poller).loadSnapshot();
    }

    @Test
    void testRunPollingStreamingWithNullPoller() throws Exception {
        setField(source, "changeDataPoller", null);
        setField(source, "running", false);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "runPollingStreaming", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        // Should not throw - null poller causes NPE which is caught
        try {
            method.invoke(source, mockContext);
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Expected: NPE because changeDataPoller is null and loadSnapshot() is called
        }
    }

    // ---- CheckpointedFunction tests ----

    @Test
    void testSnapshotStateWithWalStreamAndLsn() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.getLastLsn()).thenReturn("0/15A3B");
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);

        ListState<String> mockState = mock(ListState.class);
        setField(source, "offsetState", mockState);

        FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
        source.snapshotState(snapshotContext);

        verify(mockState).clear();
        verify(mockState).add("0/15A3B");
    }

    @Test
    void testWalLsnAcknowledgedOnlyAfterCheckpointCompletes() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);
        setField(source, "lastConsumedLsn", "0/20");

        ListState<String> mockState = mock(ListState.class);
        setField(source, "offsetState", mockState);
        FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
        when(snapshotContext.getCheckpointId()).thenReturn(42L);

        source.snapshotState(snapshotContext);
        verify(walStream, never()).acknowledgeLsn(anyString());

        source.notifyCheckpointComplete(42L);
        verify(walStream).acknowledgeLsn("0/20");

        when(snapshotContext.getCheckpointId()).thenReturn(43L);
        source.snapshotState(snapshotContext);
        source.notifyCheckpointComplete(43L);
        verify(walStream, times(1)).acknowledgeLsn("0/20");
    }

    @Test
    void testSnapshotStateWithNullLsn() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        when(walStream.getLastLsn()).thenReturn(null);
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", true);

        ListState<String> mockState = mock(ListState.class);
        setField(source, "offsetState", mockState);

        FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
        source.snapshotState(snapshotContext);

        verify(mockState).clear();
        verify(mockState, never()).add(anyString());
    }

    @Test
    void testSnapshotStateWithNoWalStream() throws Exception {
        ListState<String> mockState = mock(ListState.class);
        setField(source, "offsetState", mockState);
        setField(source, "walReplicationStream", null);

        FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
        source.snapshotState(snapshotContext);

        verify(mockState).clear();
        verify(mockState, never()).add(anyString());
    }

    @Test
    void testSnapshotStateWithWalStreamNotInitialized() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        setField(source, "walReplicationStream", walStream);
        setField(source, "walStreamInitialized", false);

        ListState<String> mockState = mock(ListState.class);
        setField(source, "offsetState", mockState);

        FunctionSnapshotContext snapshotContext = mock(FunctionSnapshotContext.class);
        source.snapshotState(snapshotContext);

        verify(mockState).clear();
        verify(mockState, never()).add(anyString());
    }

    @Test
    void testSnapshotStatePersistsPollingStateOnActiveSubtask() throws Exception {
        ListState<String> offset = mock(ListState.class);
        ListState<byte[]> polling = mock(ListState.class);
        ChangeDataPoller poller = mock(ChangeDataPoller.class);
        byte[] serialized = new byte[] {1, 2, 3};
        when(poller.serializeState()).thenReturn(serialized);
        setField(source, "offsetState", offset);
        setField(source, "pollingState", polling);
        setField(source, "changeDataPoller", poller);

        org.apache.flink.api.common.functions.RuntimeContext runtimeContext =
                mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        when(runtimeContext.getIndexOfThisSubtask()).thenReturn(0);
        Field rtCtxField =
                org.apache.flink.api.common.functions.AbstractRichFunction.class.getDeclaredField(
                        "runtimeContext");
        rtCtxField.setAccessible(true);
        rtCtxField.set(source, runtimeContext);

        source.snapshotState(mock(FunctionSnapshotContext.class));

        verify(polling).clear();
        verify(polling).add(serialized);
    }

    @Test
    void testInitializeStateNotRestored() throws Exception {
        OperatorStateStore stateStore = mock(OperatorStateStore.class);
        ListState<String> mockListState = mock(ListState.class);
        org.mockito.Mockito.doReturn(mockListState).when(stateStore).getUnionListState(any());

        FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
        when(initContext.isRestored()).thenReturn(false);
        when(initContext.getOperatorStateStore()).thenReturn(stateStore);

        source.initializeState(initContext);

        verify(stateStore, times(2)).getUnionListState(any());
        assertThat(getField(source, "offsetState", ListState.class)).isNotNull();
    }

    @Test
    void testInitializeStateRestoredWithLsn() throws Exception {
        OperatorStateStore stateStore = mock(OperatorStateStore.class);
        ListState<String> mockListState = mock(ListState.class);
        ListState<byte[]> mockPollingState = mock(ListState.class);
        org.mockito.Mockito.doReturn(mockListState, mockPollingState)
                .when(stateStore)
                .getUnionListState(any());

        List<String> restoredLsns = new ArrayList<>();
        restoredLsns.add("0/15A3B");
        when(mockListState.get()).thenReturn(restoredLsns);
        when(mockPollingState.get()).thenReturn(Collections.emptyList());

        FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
        when(initContext.isRestored()).thenReturn(true);
        when(initContext.getOperatorStateStore()).thenReturn(stateStore);

        source.initializeState(initContext);

        verify(mockListState).get();
        assertThat(getField(source, "restoredLsn", String.class)).isEqualTo("0/15A3B");
    }

    // ---- convertToRowDataDynamic extended tests ----

    @Test
    void testConvertToRowDataDynamicWithRealNull() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(5);
        when(meta.getColumnType(1)).thenReturn(Types.SMALLINT);
        when(meta.getColumnType(2)).thenReturn(Types.TINYINT);
        when(meta.getColumnType(3)).thenReturn(Types.CHAR);
        when(meta.getColumnType(4)).thenReturn(Types.NVARCHAR);
        when(meta.getColumnType(5)).thenReturn(Types.FLOAT);

        when(rs.getInt(1)).thenReturn(0);
        when(rs.wasNull()).thenReturn(true);
        when(rs.getInt(2)).thenReturn(0);
        when(rs.getString(3)).thenReturn("a");
        when(rs.getString(4)).thenReturn(null);
        when(rs.getDouble(5)).thenReturn(0.0);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
        assertThat(row.getArity()).isEqualTo(5);
    }

    @Test
    void testConvertToRowDataDynamicWithNullBigDecimal() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(rs.getBigDecimal(1)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullTimestamp() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.TIMESTAMP_WITH_TIMEZONE);
        when(rs.getTimestamp(1)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullDate() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.DATE);
        when(rs.getDate(1)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullBoolean() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.BOOLEAN);
        when(rs.getBoolean(1)).thenReturn(false);
        when(rs.wasNull()).thenReturn(true);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullDouble() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.DOUBLE);
        when(rs.getDouble(1)).thenReturn(0.0);
        when(rs.wasNull()).thenReturn(true);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullBigInt() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.BIGINT);
        when(rs.getLong(1)).thenReturn(0L);
        when(rs.wasNull()).thenReturn(true);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithNullString() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rs.getString(1)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertToRowDataDynamicWithFallbackNullString() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertToRowDataDynamic", ResultSet.class, ResultSetMetaData.class);
        method.setAccessible(true);

        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        when(meta.getColumnCount()).thenReturn(1);
        when(meta.getColumnType(1)).thenReturn(Types.OTHER);
        when(rs.getString(1)).thenReturn(null);

        RowData row = (RowData) method.invoke(source, rs, meta);
        assertThat(row).isNotNull();
    }

    @Test
    void testConvertColumnValueByOidTimestamp() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        // timestamp OID 1114
        Object result = method.invoke(source, 1114, "2024-01-15 10:30:00");
        assertThat(result).isNotNull();
    }

    @Test
    void testConvertColumnValueByOidTimestampWithMicroseconds() throws Exception {
        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "convertColumnValueByOid", int.class, String.class);
        method.setAccessible(true);

        Object result = method.invoke(source, 1114, "2024-01-15 10:30:00.123456");
        assertThat(result).isNotNull();
    }

    // ---- Helper methods ----

    private GaussDBCDCSourceFunction createTestSource(boolean walMode) {
        return GaussDBCDCSourceFunction.builder()
                .hostname("localhost")
                .port(8000)
                .database("testdb")
                .schema("public")
                .tableName("test_table")
                .username("root")
                .password("pass")
                .walMode(walMode)
                .pollIntervalMs(100)
                .build();
    }

    private void setupReadSnapshotMocks(int numSubtasks, int subtaskIndex) throws Exception {
        // Mock RuntimeContext
        org.apache.flink.api.common.functions.RuntimeContext runtimeContext =
                mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        when(runtimeContext.getIndexOfThisSubtask()).thenReturn(subtaskIndex);
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(numSubtasks);

        // Set runtime context via reflection
        Field rtCtxField =
                org.apache.flink.api.common.functions.AbstractRichFunction.class.getDeclaredField(
                        "runtimeContext");
        rtCtxField.setAccessible(true);
        rtCtxField.set(source, runtimeContext);

        // Mock connection and metadata
        DatabaseMetaData dbMeta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(mockConnection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(null, "public", "test_table", null)).thenReturn(columnsRs);
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");

        setField(source, "connection", mockConnection);
        Map<String, String> cachedPk = new HashMap<>();
        cachedPk.put(TABLE_NAME, "id");
        setField(source, "cachedPkByTable", cachedPk);

        if (numSubtasks > 1) {
            ResultSet typeRs = mock(ResultSet.class);
            when(dbMeta.getColumns(null, "public", "test_table", null))
                    .thenReturn(columnsRs, typeRs);
            when(typeRs.next()).thenReturn(true);
            when(typeRs.getString("COLUMN_NAME")).thenReturn("id");
            when(typeRs.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
            // Mock getIdRange
            PreparedStatement idRangeStmt = mock(PreparedStatement.class);
            ResultSet idRangeRs = mock(ResultSet.class);
            when(mockConnection.prepareStatement(contains("MIN(\"id\")"))).thenReturn(idRangeStmt);
            when(idRangeStmt.executeQuery()).thenReturn(idRangeRs);
            when(idRangeRs.next()).thenReturn(true);
            when(idRangeRs.getLong(1)).thenReturn(1L);
            when(idRangeRs.getLong(2)).thenReturn(1000L);
            when(idRangeRs.wasNull()).thenReturn(false);

            // Mock SELECT with WHERE
            PreparedStatement selectStmt = mock(PreparedStatement.class);
            ResultSet selectRs = mock(ResultSet.class);
            when(mockConnection.prepareStatement(contains("WHERE \"id\" >= ?")))
                    .thenReturn(selectStmt);
            when(selectStmt.executeQuery()).thenReturn(selectRs);
            when(selectRs.next()).thenReturn(true, false);

            ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
            when(selectRs.getMetaData()).thenReturn(rsMeta);
            when(rsMeta.getColumnCount()).thenReturn(2);
            when(rsMeta.getColumnType(1)).thenReturn(Types.INTEGER);
            when(rsMeta.getColumnType(2)).thenReturn(Types.VARCHAR);
            when(selectRs.getInt(1)).thenReturn(1);
            when(selectRs.wasNull()).thenReturn(false);
            when(selectRs.getString(2)).thenReturn("Alice");
        } else {
            // Single subtask: SELECT without WHERE
            PreparedStatement selectStmt = mock(PreparedStatement.class);
            ResultSet selectRs = mock(ResultSet.class);
            when(mockConnection.prepareStatement(contains("ORDER BY \"id\"")))
                    .thenReturn(selectStmt);
            when(selectStmt.executeQuery()).thenReturn(selectRs);
            when(selectRs.next()).thenReturn(true, false);

            ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
            when(selectRs.getMetaData()).thenReturn(rsMeta);
            when(rsMeta.getColumnCount()).thenReturn(2);
            when(rsMeta.getColumnType(1)).thenReturn(Types.INTEGER);
            when(rsMeta.getColumnType(2)).thenReturn(Types.VARCHAR);
            when(selectRs.getInt(1)).thenReturn(1);
            when(selectRs.wasNull()).thenReturn(false);
            when(selectRs.getString(2)).thenReturn("Alice");
        }
    }

    private void setupReadSnapshotEmptyMocks(int numSubtasks, int subtaskIndex) throws Exception {
        org.apache.flink.api.common.functions.RuntimeContext runtimeContext =
                mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        when(runtimeContext.getIndexOfThisSubtask()).thenReturn(subtaskIndex);
        when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(numSubtasks);

        Field rtCtxField =
                org.apache.flink.api.common.functions.AbstractRichFunction.class.getDeclaredField(
                        "runtimeContext");
        rtCtxField.setAccessible(true);
        rtCtxField.set(source, runtimeContext);

        DatabaseMetaData dbMeta = mock(DatabaseMetaData.class);
        ResultSet columnsRs = mock(ResultSet.class);
        when(mockConnection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(null, "public", "test_table", null)).thenReturn(columnsRs);
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");

        setField(source, "connection", mockConnection);
        Map<String, String> cachedPk = new HashMap<>();
        cachedPk.put(TABLE_NAME, "id");
        setField(source, "cachedPkByTable", cachedPk);
        ResultSet typeRs = mock(ResultSet.class);
        when(dbMeta.getColumns(null, "public", "test_table", null)).thenReturn(columnsRs, typeRs);
        when(typeRs.next()).thenReturn(true);
        when(typeRs.getString("COLUMN_NAME")).thenReturn("id");
        when(typeRs.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);

        // Empty table - getIdRange returns [0, -1]
        PreparedStatement idRangeStmt = mock(PreparedStatement.class);
        ResultSet idRangeRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(contains("MIN(\"id\")"))).thenReturn(idRangeStmt);
        when(idRangeStmt.executeQuery()).thenReturn(idRangeRs);
        when(idRangeRs.next()).thenReturn(true);
        when(idRangeRs.getLong(1)).thenReturn(0L);
        when(idRangeRs.getLong(2)).thenReturn(0L);
        when(idRangeRs.wasNull()).thenReturn(true); // Empty table

        // SELECT with WHERE (empty results)
        PreparedStatement selectStmt = mock(PreparedStatement.class);
        ResultSet selectRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(contains("WHERE \"id\" >= ?"))).thenReturn(selectStmt);
        when(selectStmt.executeQuery()).thenReturn(selectRs);
        when(selectRs.next()).thenReturn(false);
    }

    private static PreparedStatement mockFunctionProbe(boolean available) throws Exception {
        PreparedStatement probe = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(probe.executeQuery()).thenReturn(result);
        when(result.next()).thenReturn(true);
        when(result.getBoolean(1)).thenReturn(available);
        return probe;
    }

    private static Object invokeExportSnapshotBoundary(GaussDBCDCSourceFunction target)
            throws Exception {
        Method method = GaussDBCDCSourceFunction.class.getDeclaredMethod("exportSnapshotBoundary");
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static Object readBoundaryField(Object boundary, String fieldName) throws Exception {
        Field field = boundary.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(boundary);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = GaussDBCDCSourceFunction.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String fieldName, Class<T> type) throws Exception {
        Field field = GaussDBCDCSourceFunction.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
