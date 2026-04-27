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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Poller for change data capture from GaussDB.
 *
 * <p>This implementation uses a change tracking table approach:
 *
 * <ul>
 *   <li>Requires a trigger or application to write changes to a shadow table
 *   <li>Or uses timestamp/version columns to detect changes
 * </ul>
 *
 * <p>Alternative: Query-based CDC using history table
 */
@Internal
public class ChangeDataPoller {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeDataPoller.class);

    private final Connection connection;
    private final String schema;
    private final String tableName;
    private final String primaryKeyColumn;

    // Tracking state
    private long lastPolledId = 0;
    private Timestamp lastPolledTimestamp = new Timestamp(0);
    private final Map<Object, RowData> currentSnapshot = new HashMap<>();

    public ChangeDataPoller(
            Connection connection, String schema, String tableName, String primaryKeyColumn) {
        this.connection = connection;
        this.schema = schema;
        this.tableName = tableName;
        this.primaryKeyColumn = primaryKeyColumn;
    }

    /** Poll for new inserts (based on auto-increment ID). */
    public List<ChangeEvent<RowData>> pollNewInserts() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        String sql =
                String.format(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM %s.%s WHERE id > ? ORDER BY id LIMIT 1000",
                        schema, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, lastPolledId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RowData row = convertToRowData(rs);
                    long id = rs.getLong("id");

                    events.add(ChangeEvent.insert(tableName, row, System.currentTimeMillis()));
                    currentSnapshot.put(id, row);
                    lastPolledId = id;
                }
            }
        }

        return events;
    }

    /** Poll for updates (based on updated_at timestamp). */
    public List<ChangeEvent<RowData>> pollUpdates() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        // This requires an 'updated_at' column on the table
        String sql =
                String.format(
                        "SELECT id, name, gender, age, class_name, score, created_date, updated_at "
                                + "FROM %s.%s WHERE updated_at > ? ORDER BY updated_at LIMIT 1000",
                        schema, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setTimestamp(1, lastPolledTimestamp);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RowData newRow = convertToRowData(rs);
                    long id = rs.getLong("id");
                    Timestamp updatedAt = rs.getTimestamp("updated_at");

                    RowData oldRow = currentSnapshot.get(id);

                    if (oldRow != null) {
                        // This is an UPDATE
                        events.add(
                                ChangeEvent.update(
                                        tableName, oldRow, newRow, System.currentTimeMillis()));
                    } else {
                        // This might be an INSERT that we missed, or initial load
                        events.add(
                                ChangeEvent.insert(tableName, newRow, System.currentTimeMillis()));
                    }

                    currentSnapshot.put(id, newRow);
                    if (updatedAt != null) {
                        lastPolledTimestamp = updatedAt;
                    }
                }
            }
        }

        return events;
    }

    /**
     * Poll for deletes by checking missing IDs. This requires querying all current IDs and
     * comparing with snapshot.
     */
    public List<ChangeEvent<RowData>> pollDeletes() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        // Get all current IDs from database
        String sql = String.format("SELECT id FROM %s.%s", schema, tableName);
        List<Long> currentIds = new ArrayList<>();

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                currentIds.add(rs.getLong("id"));
            }
        }

        // Find deleted IDs (in snapshot but not in current)
        List<Object> deletedIds = new ArrayList<>();
        for (Object id : currentSnapshot.keySet()) {
            if (!currentIds.contains(id)) {
                deletedIds.add(id);
            }
        }

        // Emit delete events
        for (Object id : deletedIds) {
            RowData deletedRow = currentSnapshot.remove(id);
            if (deletedRow != null) {
                events.add(ChangeEvent.delete(tableName, deletedRow, System.currentTimeMillis()));
            }
        }

        return events;
    }

    /** Comprehensive poll for all change types. */
    public List<ChangeEvent<RowData>> pollAllChanges() throws SQLException {
        List<ChangeEvent<RowData>> allEvents = new ArrayList<>();

        // Poll new inserts
        allEvents.addAll(pollNewInserts());

        // Poll updates (requires updated_at column)
        try {
            allEvents.addAll(pollUpdates());
        } catch (SQLException e) {
            LOG.warn(
                    "Could not poll updates, 'updated_at' column may not exist: {}",
                    e.getMessage());
        }

        // Poll deletes (expensive operation, do less frequently)
        allEvents.addAll(pollDeletes());

        return allEvents;
    }

    /** Load initial snapshot into memory for change detection. */
    public void loadSnapshot() throws SQLException {
        currentSnapshot.clear();

        String sql =
                String.format(
                        "SELECT id, name, gender, age, class_name, score, created_date FROM %s.%s",
                        schema, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                long id = rs.getLong("id");
                RowData row = convertToRowData(rs);
                currentSnapshot.put(id, row);

                if (id > lastPolledId) {
                    lastPolledId = id;
                }
            }
        }

        LOG.info(
                "Loaded snapshot with {} rows, lastPolledId={}",
                currentSnapshot.size(),
                lastPolledId);
    }

    private RowData convertToRowData(ResultSet rs) throws SQLException {
        GenericRowData row = new GenericRowData(7);
        row.setField(0, rs.getInt("id"));
        row.setField(1, StringData.fromString(rs.getString("name")));
        row.setField(2, StringData.fromString(rs.getString("gender")));
        row.setField(3, rs.getInt("age"));
        row.setField(4, StringData.fromString(rs.getString("class_name")));
        row.setField(5, DecimalData.fromBigDecimal(rs.getBigDecimal("score"), 5, 2));

        Timestamp createdDate = rs.getTimestamp("created_date");
        if (createdDate != null) {
            row.setField(6, TimestampData.fromLocalDateTime(createdDate.toLocalDateTime()));
        }

        return row;
    }

    public void setLastPolledId(long lastPolledId) {
        this.lastPolledId = lastPolledId;
    }

    public long getLastPolledId() {
        return lastPolledId;
    }
}
