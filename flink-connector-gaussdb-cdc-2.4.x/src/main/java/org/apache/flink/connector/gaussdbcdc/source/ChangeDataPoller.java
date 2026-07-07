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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

    // Cached column list (resolved once on first access)
    private String cachedColumns;

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

    /**
     * Dynamically resolve the column list for the target table via JDBC metadata. The result is
     * cached after the first call.
     */
    private String getTableColumns() throws SQLException {
        if (cachedColumns != null) {
            return cachedColumns;
        }
        DatabaseMetaData meta = connection.getMetaData();
        List<String> columns = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("No columns found for table " + schema + "." + tableName);
        }
        cachedColumns = String.join(", ", columns);
        return cachedColumns;
    }

    /** Poll for new inserts (based on auto-increment ID). */
    public List<ChangeEvent<RowData>> pollNewInserts() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        String columns = getTableColumns();
        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE %s > ? ORDER BY %s LIMIT 1000",
                        columns, schema, tableName, primaryKeyColumn, primaryKeyColumn);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, lastPolledId);

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData row = convertToRowDataDynamic(rs, meta);
                    long id = rs.getLong(primaryKeyColumn);

                    events.add(ChangeEvent.insert(tableName, row, System.currentTimeMillis()));
                    currentSnapshot.put(id, row);
                    lastPolledId = id;
                }
            }
        }

        return events;
    }

    /** Poll for updates (based on updated_at timestamp, if available). */
    public List<ChangeEvent<RowData>> pollUpdates() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        // Check if table has an 'updated_at' column; skip if not
        String columns = getTableColumns();
        if (!columns.toLowerCase().contains("updated_at")) {
            LOG.warn(
                    "Table {}.{} has no 'updated_at' column; skipping update polling",
                    schema,
                    tableName);
            return events;
        }
        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE updated_at > ? ORDER BY updated_at LIMIT 1000",
                        columns, schema, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setTimestamp(1, lastPolledTimestamp);

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData newRow = convertToRowDataDynamic(rs, meta);
                    long id = rs.getLong(primaryKeyColumn);
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
        String sql = String.format("SELECT %s FROM %s.%s", primaryKeyColumn, schema, tableName);
        List<Long> currentIds = new ArrayList<>();

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                currentIds.add(rs.getLong(primaryKeyColumn));
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

        String columns = getTableColumns();
        String sql = String.format("SELECT %s FROM %s.%s", columns, schema, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                long id = rs.getLong(primaryKeyColumn);
                RowData row = convertToRowDataDynamic(rs, meta);
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

    /**
     * Dynamically convert a ResultSet row to GenericRowData using ResultSetMetaData. This replaces
     * the old hardcoded convertToRowData that assumed a fixed schema.
     */
    private RowData convertToRowDataDynamic(ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        int colCount = meta.getColumnCount();
        GenericRowData row = new GenericRowData(colCount);

        for (int i = 1; i <= colCount; i++) {
            int sqlType = meta.getColumnType(i);
            Object value;

            switch (sqlType) {
                case java.sql.Types.INTEGER:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    value = rs.getInt(i);
                    if (rs.wasNull()) {
                        value = null;
                    }
                    break;
                case java.sql.Types.BIGINT:
                    value = rs.getLong(i);
                    if (rs.wasNull()) {
                        value = null;
                    }
                    break;
                case java.sql.Types.VARCHAR:
                case java.sql.Types.CHAR:
                case java.sql.Types.NVARCHAR:
                    String str = rs.getString(i);
                    value = str != null ? StringData.fromString(str) : null;
                    break;
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    java.math.BigDecimal bd = rs.getBigDecimal(i);
                    if (bd != null) {
                        int p = meta.getPrecision(i);
                        int s = meta.getScale(i);
                        value = DecimalData.fromBigDecimal(bd, p, s);
                        if (value == null) {
                            value = DecimalData.fromBigDecimal(bd, bd.precision(), s);
                        }
                    } else {
                        value = null;
                    }
                    break;
                case java.sql.Types.TIMESTAMP:
                case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                    Timestamp ts = rs.getTimestamp(i);
                    value = ts != null ? TimestampData.fromTimestamp(ts) : null;
                    break;
                case java.sql.Types.DATE:
                    java.sql.Date date = rs.getDate(i);
                    value = date != null ? (int) date.toLocalDate().toEpochDay() : null;
                    break;
                case java.sql.Types.BOOLEAN:
                    value = rs.getBoolean(i);
                    if (rs.wasNull()) {
                        value = null;
                    }
                    break;
                case java.sql.Types.DOUBLE:
                case java.sql.Types.FLOAT:
                case java.sql.Types.REAL:
                    value = rs.getDouble(i);
                    if (rs.wasNull()) {
                        value = null;
                    }
                    break;
                default:
                    String fallback = rs.getString(i);
                    value = fallback != null ? StringData.fromString(fallback) : null;
                    break;
            }

            row.setField(i - 1, value);
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
