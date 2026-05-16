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
import java.sql.DriverManager;
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
 * <p>Dynamically discovers table columns from database metadata, so it adapts to any table schema
 * without hardcoded column names.
 */
@Internal
public class ChangeDataPoller {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeDataPoller.class);

    private final Connection connection;
    private final String schema;
    private final String tableName;
    private final String primaryKeyColumn;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String sslMode;

    // Cached column names, lazily loaded
    private List<String> columnNames;

    // Tracking state
    private long lastPolledId = 0;
    private final Map<Object, RowData> currentSnapshot = new HashMap<>();

    public ChangeDataPoller(
            Connection connection, String schema, String tableName, String primaryKeyColumn) {
        this(connection, schema, tableName, primaryKeyColumn, null, null, null, null);
        LOG.warn("ChangeDataPoller created without JDBC credentials - reusing passed connection");
    }

    public ChangeDataPoller(
            Connection connection,
            String schema,
            String tableName,
            String primaryKeyColumn,
            String jdbcUrl,
            String username,
            String password,
            String sslMode) {
        this.connection = connection;
        this.schema = schema;
        this.tableName = tableName;
        this.primaryKeyColumn = primaryKeyColumn;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.sslMode = sslMode;
    }

    /**
     * Get a JDBC connection. If JDBC url & credentials are available, creates a new connection
     * (avoids Flink classloader issues with GaussDB internal classes). Otherwise falls back to the
     * shared connection passed at construction.
     */
    private Connection getConnection() throws SQLException {
        if (jdbcUrl != null && username != null && password != null) {
            try {
                Class.forName("com.huawei.gaussdb.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                throw new SQLException("GaussDB JDBC driver not found", e);
            }
            return DriverManager.getConnection(
                    jdbcUrl + "?sslmode=" + (sslMode != null ? sslMode : "prefer"),
                    username,
                    password);
        }
        return connection;
    }

    /** Get column names from table metadata, cached after first call. */
    private List<String> getColumnNames() throws SQLException {
        if (columnNames == null) {
            columnNames = new ArrayList<>();
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet rs = meta.getColumns(null, schema, tableName, null)) {
                while (rs.next()) {
                    columnNames.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (columnNames.isEmpty()) {
                throw new SQLException("No columns found for table " + schema + "." + tableName);
            }
            LOG.info("Discovered columns for {}.{}: {}", schema, tableName, columnNames);
        }
        return columnNames;
    }

    /** Build a comma-separated column list for SQL queries. */
    private String getColumnList() throws SQLException {
        return String.join(", ", getColumnNames());
    }

    /** Poll for new inserts (based on auto-increment ID / primary key). */
    public List<ChangeEvent<RowData>> pollNewInserts() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        String columns = getColumnList();
        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE %s > %d ORDER BY %s LIMIT 1000",
                        columns,
                        schema,
                        tableName,
                        primaryKeyColumn,
                        lastPolledId,
                        primaryKeyColumn);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            LOG.info("pollNewInserts: sql={}, lastPolledId={}", sql, lastPolledId);
            while (rs.next()) {
                RowData row = convertToRowData(rs, meta);
                long id = rs.getLong(primaryKeyColumn);

                events.add(ChangeEvent.insert(tableName, row, System.currentTimeMillis()));
                currentSnapshot.put(id, row);
                lastPolledId = id;
            }
        }

        return events;
    }

    /**
     * Poll for updates by comparing current snapshot with latest rows. Uses a full table scan of
     * primary keys and their values to detect changes.
     *
     * <p>Note: This is a simplified approach that queries the table by primary key range. For
     * production use, a dedicated timestamp/version column is recommended.
     */
    public List<ChangeEvent<RowData>> pollUpdates() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        if (currentSnapshot.isEmpty()) {
            return events;
        }

        // Get all current rows to detect changes
        String columns = getColumnList();
        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE %s > 0 ORDER BY %s LIMIT 1000",
                        columns, schema, tableName, primaryKeyColumn, primaryKeyColumn);

        // Use a high water mark approach: re-fetch rows with PK > lastPolledId
        // and rows that might have been updated (we re-check known IDs)
        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                long id = rs.getLong(primaryKeyColumn);
                RowData newRow = convertToRowData(rs, meta);

                RowData oldRow = currentSnapshot.get(id);
                if (oldRow != null && !rowsEqual(oldRow, newRow)) {
                    events.add(
                            ChangeEvent.update(
                                    tableName, oldRow, newRow, System.currentTimeMillis()));
                    currentSnapshot.put(id, newRow);
                } else if (oldRow == null) {
                    // This row was not in snapshot (new insert from a previous poll cycle)
                    events.add(ChangeEvent.insert(tableName, newRow, System.currentTimeMillis()));
                    currentSnapshot.put(id, newRow);
                }
            }
        }

        return events;
    }

    /**
     * Simple row comparison. Compares all fields of two RowData instances. Uses toString() for
     * comparison since RowData fields may be of different types.
     */
    private boolean rowsEqual(RowData a, RowData b) {
        int arity = a.getArity();
        if (arity != b.getArity()) {
            return false;
        }
        // Use field-by-field string comparison since we're dealing with GenericRowData
        GenericRowData ga = (GenericRowData) a;
        GenericRowData gb = (GenericRowData) b;
        for (int i = 0; i < arity; i++) {
            Object fieldA = ga.getField(i);
            Object fieldB = gb.getField(i);
            if (fieldA == null && fieldB == null) {
                continue;
            }
            if (fieldA == null || fieldB == null) {
                return false;
            }
            if (!fieldA.toString().equals(fieldB.toString())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Poll for deletes by checking missing IDs. This requires querying all current IDs and
     * comparing with snapshot.
     */
    public List<ChangeEvent<RowData>> pollDeletes() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();

        if (currentSnapshot.isEmpty()) {
            return events;
        }

        // Get all current IDs from database
        String sql = String.format("SELECT %s FROM %s.%s", primaryKeyColumn, schema, tableName);
        List<Long> currentIds = new ArrayList<>();

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
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
        LOG.info(
                "pollDeletes: snapshot keys={}, current ids={}, deleted={}",
                currentSnapshot.keySet(),
                currentIds,
                deletedIds);

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

        // Poll updates
        try {
            allEvents.addAll(pollUpdates());
        } catch (SQLException e) {
            LOG.warn("Could not poll updates: {}", e.getMessage());
        }

        // Poll deletes (expensive operation)
        allEvents.addAll(pollDeletes());

        return allEvents;
    }

    /** Load initial snapshot into memory for change detection. */
    public void loadSnapshot() throws SQLException {
        currentSnapshot.clear();

        String columns = getColumnList();
        String sql = String.format("SELECT %s FROM %s.%s", columns, schema, tableName);

        try (Connection conn = getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                long id = rs.getLong(primaryKeyColumn);
                RowData row = convertToRowData(rs, meta);
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

    /** Convert ResultSet to RowData dynamically based on ResultSetMetaData. */
    private RowData convertToRowData(ResultSet rs, ResultSetMetaData meta) throws SQLException {
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
                    if (rs.wasNull()) value = null;
                    break;
                case java.sql.Types.BIGINT:
                    value = rs.getLong(i);
                    if (rs.wasNull()) value = null;
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
                    if (rs.wasNull()) value = null;
                    break;
                case java.sql.Types.DOUBLE:
                case java.sql.Types.FLOAT:
                    value = rs.getDouble(i);
                    if (rs.wasNull()) value = null;
                    break;
                default:
                    // Fallback to string for unknown types
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
