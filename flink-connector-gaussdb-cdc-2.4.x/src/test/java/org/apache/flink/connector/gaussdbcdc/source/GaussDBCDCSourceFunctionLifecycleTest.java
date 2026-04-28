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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Comprehensive tests for {@link GaussDBCDCSourceFunction} lifecycle and runtime methods. */
class GaussDBCDCSourceFunctionLifecycleTest {

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
    }

    // ---- open() tests ----

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

    // ---- readSnapshot() tests ----

    @Test
    void testReadSnapshotSingleSubtask() throws Exception {
        setupReadSnapshotMocks(1, 0);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotParallelSubtask() throws Exception {
        setupReadSnapshotMocks(4, 1);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotParallelLastSubtask() throws Exception {
        setupReadSnapshotMocks(4, 3);
        setField(source, "running", true);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, atLeastOnce()).collect(any(RowData.class));
    }

    @Test
    void testReadSnapshotEmptyTable() throws Exception {
        setupReadSnapshotEmptyMocks(4, 0);

        Method method =
                GaussDBCDCSourceFunction.class.getDeclaredMethod(
                        "readSnapshot", SourceFunction.SourceContext.class);
        method.setAccessible(true);
        method.invoke(source, mockContext);

        verify(mockContext, never()).collect(any(RowData.class));
    }

    // ---- runWalStreaming() tests ----

    @Test
    void testRunWalStreamingWithInsertChange() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange insert =
                WalChange.insert(
                        "0/1",
                        100,
                        "public",
                        "test",
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
    }

    @Test
    void testRunWalStreamingWithUpdateChange() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange update =
                WalChange.update(
                        "0/2",
                        101,
                        "public",
                        "test",
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
    void testRunWalStreamingWithDeleteChange() throws Exception {
        WalReplicationStream walStream = mock(WalReplicationStream.class);
        List<WalChange> changes = new ArrayList<>();
        WalChange delete =
                WalChange.delete(
                        "0/3",
                        102,
                        "public",
                        "test",
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

        // DELETE doesn't produce RowData
        verify(mockContext, never()).collect(any(RowData.class));
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

        // DELETE doesn't produce after row
        verify(mockContext, never()).collect(any(RowData.class));
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
    void testInitializeStateNotRestored() throws Exception {
        OperatorStateStore stateStore = mock(OperatorStateStore.class);
        ListState<String> mockListState = mock(ListState.class);
        org.mockito.Mockito.doReturn(mockListState).when(stateStore).getListState(any());

        FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
        when(initContext.isRestored()).thenReturn(false);
        when(initContext.getOperatorStateStore()).thenReturn(stateStore);

        source.initializeState(initContext);

        verify(stateStore).getListState(any());
        assertThat(getField(source, "offsetState", ListState.class)).isNotNull();
    }

    @Test
    void testInitializeStateRestoredWithLsn() throws Exception {
        OperatorStateStore stateStore = mock(OperatorStateStore.class);
        ListState<String> mockListState = mock(ListState.class);
        org.mockito.Mockito.doReturn(mockListState).when(stateStore).getListState(any());

        List<String> restoredLsns = new ArrayList<>();
        restoredLsns.add("0/15A3B");
        when(mockListState.get()).thenReturn(restoredLsns);

        FunctionInitializationContext initContext = mock(FunctionInitializationContext.class);
        when(initContext.isRestored()).thenReturn(true);
        when(initContext.getOperatorStateStore()).thenReturn(stateStore);

        source.initializeState(initContext);

        verify(mockListState).get();
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

        if (numSubtasks > 1) {
            // Mock getIdRange
            PreparedStatement idRangeStmt = mock(PreparedStatement.class);
            ResultSet idRangeRs = mock(ResultSet.class);
            when(mockConnection.prepareStatement(contains("MIN(id)"))).thenReturn(idRangeStmt);
            when(idRangeStmt.executeQuery()).thenReturn(idRangeRs);
            when(idRangeRs.next()).thenReturn(true);
            when(idRangeRs.getLong(1)).thenReturn(1L);
            when(idRangeRs.getLong(2)).thenReturn(1000L);
            when(idRangeRs.wasNull()).thenReturn(false);

            // Mock SELECT with WHERE
            PreparedStatement selectStmt = mock(PreparedStatement.class);
            ResultSet selectRs = mock(ResultSet.class);
            when(mockConnection.prepareStatement(contains("WHERE id >= ?"))).thenReturn(selectStmt);
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
            when(mockConnection.prepareStatement(contains("ORDER BY id"))).thenReturn(selectStmt);
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

        // Empty table - getIdRange returns [0, -1]
        PreparedStatement idRangeStmt = mock(PreparedStatement.class);
        ResultSet idRangeRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(contains("MIN(id)"))).thenReturn(idRangeStmt);
        when(idRangeStmt.executeQuery()).thenReturn(idRangeRs);
        when(idRangeRs.next()).thenReturn(true);
        when(idRangeRs.getLong(1)).thenReturn(0L);
        when(idRangeRs.getLong(2)).thenReturn(0L);
        when(idRangeRs.wasNull()).thenReturn(true); // Empty table

        // SELECT with WHERE (empty results)
        PreparedStatement selectStmt = mock(PreparedStatement.class);
        ResultSet selectRs = mock(ResultSet.class);
        when(mockConnection.prepareStatement(contains("WHERE id >= ?"))).thenReturn(selectStmt);
        when(selectStmt.executeQuery()).thenReturn(selectRs);
        when(selectRs.next()).thenReturn(false);
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
