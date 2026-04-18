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

package org.apache.flink.connector.gaussdbcdc.source.wal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link WalReplicationStream}. */
class WalReplicationStreamTest {

    private Connection connection;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
    }

    @Test
    void testConstructor() {
        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        assertThat(stream.isRunning()).isFalse();
        assertThat(stream.getLastLsn()).isNull();
    }

    @Test
    void testClose() {
        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        stream.close();
        assertThat(stream.isRunning()).isFalse();
    }

    @Test
    void testInitializeSlotNotExists() throws Exception {
        // Mock slotExists check returning false
        PreparedStatement checkStmt = mock(PreparedStatement.class);
        ResultSet checkRs = mock(ResultSet.class);
        when(checkStmt.executeQuery()).thenReturn(checkRs);
        when(checkRs.next()).thenReturn(false);
        when(connection.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?"))
                .thenReturn(checkStmt);

        // Mock createSlot
        PreparedStatement createStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT pg_create_logical_replication_slot(?, ?)"))
                .thenReturn(createStmt);

        // Mock getCurrentLsn
        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn("0/1A2B3C");
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(lsnStmt);

        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        stream.initialize();

        assertThat(stream.isRunning()).isTrue();
        assertThat(stream.getLastLsn()).isEqualTo("0/1A2B3C");
    }

    @Test
    void testInitializeSlotExists() throws Exception {
        // Mock slotExists check returning true
        PreparedStatement checkStmt = mock(PreparedStatement.class);
        ResultSet checkRs = mock(ResultSet.class);
        when(checkStmt.executeQuery()).thenReturn(checkRs);
        when(checkRs.next()).thenReturn(true);
        when(connection.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?"))
                .thenReturn(checkStmt);

        // Mock getCurrentLsn
        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn("0/0");
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(lsnStmt);

        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        stream.initialize();

        assertThat(stream.isRunning()).isTrue();
    }

    @Test
    void testDropSlot() throws Exception {
        PreparedStatement dropStmt = mock(PreparedStatement.class);
        when(connection.prepareStatement("SELECT pg_drop_replication_slot(?)"))
                .thenReturn(dropStmt);

        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        stream.dropSlot();
        // No exception means success
    }

    @Test
    void testReadChanges() throws Exception {
        // First initialize
        PreparedStatement checkStmt = mock(PreparedStatement.class);
        ResultSet checkRs = mock(ResultSet.class);
        when(checkStmt.executeQuery()).thenReturn(checkRs);
        when(checkRs.next()).thenReturn(true);
        when(connection.prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?"))
                .thenReturn(checkStmt);

        PreparedStatement lsnStmt = mock(PreparedStatement.class);
        ResultSet lsnRs = mock(ResultSet.class);
        when(lsnRs.next()).thenReturn(true);
        when(lsnRs.getString(1)).thenReturn("0/1");
        when(lsnStmt.executeQuery()).thenReturn(lsnRs);
        when(connection.prepareStatement("SELECT pg_current_wal_lsn()")).thenReturn(lsnStmt);

        // Mock readChanges
        PreparedStatement readStmt = mock(PreparedStatement.class);
        ResultSet readRs = mock(ResultSet.class);
        when(readRs.next()).thenReturn(true, true, false);
        when(readRs.getString("lsn")).thenReturn("0/2", "0/3");
        when(readRs.getLong("xid")).thenReturn(100L, 101L);
        when(readRs.getString("data"))
                .thenReturn("BEGIN 100", "table public.test: INSERT: id[integer]:1");
        when(readStmt.executeQuery()).thenReturn(readRs);
        when(connection.prepareStatement(
                        "SELECT lsn, xid, data FROM pg_logical_slot_get_changes(?, ?, ?, 'include-xids', '1')"))
                .thenReturn(readStmt);

        WalReplicationStream stream = new WalReplicationStream(connection, "test_slot", "pgoutput");
        stream.initialize();

        java.util.List<WalChange> changes = stream.readChanges(10);
        assertThat(changes).hasSize(2);
        assertThat(changes.get(0).getType()).isEqualTo(WalChange.ChangeType.BEGIN);
        assertThat(changes.get(1).getType()).isEqualTo(WalChange.ChangeType.INSERT);
    }
}
