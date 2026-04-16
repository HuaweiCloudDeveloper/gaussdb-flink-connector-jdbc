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

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * WAL replication stream for GaussDB logical decoding.
 *
 * <p>Reads changes from the database's Write-Ahead Log (WAL) using logical replication.
 *
 * <p>Requirements:
 *
 * <ul>
 *   <li>wal_level = logical
 *   <li>Replication slot created
 *   <li>Appropriate decoding plugin (pgoutput)
 * </ul>
 */
@Internal
public class WalReplicationStream {

    private static final Logger LOG = LoggerFactory.getLogger(WalReplicationStream.class);

    private final Connection connection;
    private final String slotName;
    private final String pluginName;

    private String lastLsn;
    private boolean running = false;

    public WalReplicationStream(Connection connection, String slotName, String pluginName) {
        this.connection = connection;
        this.slotName = slotName;
        this.pluginName = pluginName;
    }

    /**
     * Initialize the replication stream.
     *
     * <p>Creates the replication slot if it doesn't exist.
     */
    public void initialize() throws SQLException {
        LOG.info(
                "Initializing WAL replication stream for slot: {}, plugin: {}",
                slotName,
                pluginName);

        // Check if slot exists
        if (!slotExists()) {
            createSlot();
        }

        // Get current LSN position
        lastLsn = getCurrentLsn();
        LOG.info("Starting from LSN: {}", lastLsn);

        running = true;
    }

    /**
     * Read pending changes from the WAL.
     *
     * @param maxChanges maximum number of changes to read
     * @return list of WAL changes
     */
    public List<WalChange> readChanges(int maxChanges) throws SQLException {
        List<WalChange> changes = new ArrayList<>();

        String sql =
                "SELECT lsn, xid, data FROM pg_logical_slot_get_changes(?, ?, ?, 'include-xids', '1')";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotName);
            stmt.setString(2, lastLsn);
            stmt.setInt(3, maxChanges);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String lsn = rs.getString("lsn");
                    long xid = rs.getLong("xid");
                    String data = rs.getString("data");

                    WalChange change = parseChange(lsn, xid, data);
                    if (change != null) {
                        changes.add(change);
                        lastLsn = lsn;
                    }
                }
            }
        }

        return changes;
    }

    /** Check if the replication slot exists. */
    private boolean slotExists() throws SQLException {
        String sql = "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** Create a new logical replication slot. */
    private void createSlot() throws SQLException {
        LOG.info("Creating logical replication slot: {} with plugin: {}", slotName, pluginName);

        String sql = "SELECT pg_create_logical_replication_slot(?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotName);
            stmt.setString(2, pluginName);
            stmt.execute();
        }

        LOG.info("Successfully created replication slot: {}", slotName);
    }

    /** Get the current WAL LSN position. */
    private String getCurrentLsn() throws SQLException {
        String sql = "SELECT pg_current_wal_lsn()";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "0/0";
    }

    /** Parse WAL change data. */
    private WalChange parseChange(String lsn, long xid, String data) {
        // Parse pgoutput format
        // Format: BEGIN XID
        //         TABLE schema.table: operation [key=value,...]
        //         COMMIT XID

        if (data == null || data.isEmpty()) {
            return null;
        }

        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setRawData(data);

        // Simple parsing for demonstration
        // In production, use proper pgoutput protocol parsing
        if (data.startsWith("BEGIN")) {
            change.setType(WalChange.ChangeType.BEGIN);
        } else if (data.startsWith("COMMIT")) {
            change.setType(WalChange.ChangeType.COMMIT);
        } else if (data.contains("INSERT")) {
            change.setType(WalChange.ChangeType.INSERT);
        } else if (data.contains("UPDATE")) {
            change.setType(WalChange.ChangeType.UPDATE);
        } else if (data.contains("DELETE")) {
            change.setType(WalChange.ChangeType.DELETE);
        } else {
            change.setType(WalChange.ChangeType.UNKNOWN);
        }

        return change;
    }

    /** Drop the replication slot. */
    public void dropSlot() throws SQLException {
        LOG.info("Dropping replication slot: {}", slotName);

        String sql = "SELECT pg_drop_replication_slot(?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotName);
            stmt.execute();
        }

        LOG.info("Successfully dropped replication slot: {}", slotName);
    }

    /** Close the replication stream. */
    public void close() {
        running = false;
        LOG.info("WAL replication stream closed");
    }

    public boolean isRunning() {
        return running;
    }

    public String getLastLsn() {
        return lastLsn;
    }
}
