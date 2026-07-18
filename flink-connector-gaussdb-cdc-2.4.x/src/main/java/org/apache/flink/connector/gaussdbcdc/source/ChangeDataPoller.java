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
import org.apache.flink.types.RowKind;

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
 * <p>This implementation supports multi-table polling. Each table maintains its own:
 *
 * <ul>
 *   <li>Cached column list
 *   <li>Primary key column name
 *   <li>Last polled ID (for INSERT detection)
 *   <li>Last polled timestamp (for UPDATE detection)
 *   <li>In-memory snapshot (for DELETE detection)
 * </ul>
 *
 * <p>When outputFormat is "json", each change event is output as a single STRING column containing
 * a JSON string with table name, operation type, and column values, instead of a fixed-schema
 * RowData.
 */
@Internal
public class ChangeDataPoller {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeDataPoller.class);

    private final Connection connection;
    private final String schema;
    private final List<String> tableNames;
    private final String outputFormat;

    // Per-table state
    private final Map<String, String> cachedColumns = new HashMap<>();
    private final Map<String, String> cachedPkColumns = new HashMap<>();
    private final Map<String, Long> lastPolledIds = new HashMap<>();
    private final Map<String, Timestamp> lastPolledTimestamps = new HashMap<>();
    private final Map<String, Map<Object, RowData>> snapshots = new HashMap<>();

    public ChangeDataPoller(
            Connection connection, String schema, List<String> tableNames, String outputFormat) {
        this.connection = connection;
        this.schema = schema;
        this.tableNames = tableNames;
        this.outputFormat = outputFormat;
        for (String tbl : tableNames) {
            lastPolledIds.put(tbl, 0L);
            lastPolledTimestamps.put(tbl, new Timestamp(0));
            snapshots.put(tbl, new HashMap<>());
        }
    }

    /** Backward-compatible single-table constructor. */
    public ChangeDataPoller(
            Connection connection, String schema, String tableName, String primaryKeyColumn) {
        this(connection, schema, java.util.Collections.singletonList(tableName), "raw");
        if (primaryKeyColumn != null) {
            cachedPkColumns.put(tableName, primaryKeyColumn);
        }
    }

    /** Backward-compatible getter for first table's lastPolledId. */
    public long getLastPolledId() {
        return lastPolledIds.getOrDefault(tableNames.get(0), 0L);
    }

    /** Backward-compatible setter for first table's lastPolledId. */
    public void setLastPolledId(long lastPolledId) {
        lastPolledIds.put(tableNames.get(0), lastPolledId);
    }

    /** Resolve the column list for a table, cached after first access. */
    private String getTableColumns(String tbl) throws SQLException {
        if (cachedColumns.containsKey(tbl)) {
            return cachedColumns.get(tbl);
        }
        DatabaseMetaData meta = connection.getMetaData();
        List<String> columns = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, tbl, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("No columns found for table " + schema + "." + tbl);
        }
        String joined = String.join(", ", columns);
        cachedColumns.put(tbl, joined);
        return joined;
    }

    /** Detect primary key column for a table, cached after first access. */
    private String getPrimaryKeyColumn(String tbl) throws SQLException {
        if (cachedPkColumns.containsKey(tbl)) {
            return cachedPkColumns.get(tbl);
        }
        // Try DatabaseMetaData
        try {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet pkRs = meta.getPrimaryKeys(null, schema, tbl);
            if (pkRs != null) {
                try (ResultSet rs = pkRs) {
                    if (rs.next()) {
                        String pkName = rs.getString("COLUMN_NAME");
                        if (pkName != null && !pkName.isEmpty()) {
                            cachedPkColumns.put(tbl, pkName);
                            return pkName;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn("Failed to detect PK for {}.{}: {}", schema, tbl, e.getMessage());
        }
        // Fallback: pg_index
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT a.attname FROM pg_index i "
                                + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                                + "WHERE i.indrelid = ?::regclass AND i.indisprimary LIMIT 1")) {
            stmt.setString(1, schema + "." + tbl);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String pkName = rs.getString(1);
                    if (pkName != null && !pkName.isEmpty()) {
                        cachedPkColumns.put(tbl, pkName);
                        return pkName;
                    }
                }
            }
        } catch (SQLException e) {
            LOG.warn("pg_index PK detection failed for {}.{}: {}", schema, tbl, e.getMessage());
        }
        LOG.warn("No PK found for {}.{}; falling back to \"id\"", schema, tbl);
        cachedPkColumns.put(tbl, "id");
        return "id";
    }

    /** Poll for new inserts across all tables. */
    public List<ChangeEvent<RowData>> pollNewInserts() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        for (String tbl : tableNames) {
            events.addAll(pollInsertsForTable(tbl));
        }
        return events;
    }

    private List<ChangeEvent<RowData>> pollInsertsForTable(String tbl) throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        String columns = getTableColumns(tbl);
        String pkCol = getPrimaryKeyColumn(tbl);
        long lastId = lastPolledIds.get(tbl);
        Map<Object, RowData> snapshot = snapshots.get(tbl);

        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE %s > ? ORDER BY %s LIMIT 1000",
                        columns, schema, tbl, pkCol, pkCol);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, lastId);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData row;
                    if ("json".equalsIgnoreCase(outputFormat)) {
                        row = convertToJsonRow(tbl, "INSERT", null, rs, meta);
                    } else {
                        row = convertToRowDataDynamic(rs, meta);
                    }
                    events.add(ChangeEvent.insert(tbl, row, System.currentTimeMillis()));
                    long id = rs.getLong(pkCol);
                    snapshot.put(id, row);
                    lastPolledIds.put(tbl, id);
                }
            }
        }
        return events;
    }

    /** Poll for updates across all tables. */
    public List<ChangeEvent<RowData>> pollUpdates() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        for (String tbl : tableNames) {
            try {
                events.addAll(pollUpdatesForTable(tbl));
            } catch (SQLException e) {
                LOG.warn("Could not poll updates for {}.{}: {}", schema, tbl, e.getMessage());
            }
        }
        return events;
    }

    private List<ChangeEvent<RowData>> pollUpdatesForTable(String tbl) throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        String columns = getTableColumns(tbl);
        if (!columns.toLowerCase().contains("updated_at")) {
            return events;
        }
        String pkCol = getPrimaryKeyColumn(tbl);
        Timestamp lastTs = lastPolledTimestamps.get(tbl);
        Map<Object, RowData> snapshot = snapshots.get(tbl);

        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE updated_at > ? ORDER BY updated_at LIMIT 1000",
                        columns, schema, tbl);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setTimestamp(1, lastTs);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData newRow = convertToRowDataDynamic(rs, meta);
                    long id = rs.getLong(pkCol);
                    Timestamp updatedAt = rs.getTimestamp("updated_at");
                    RowData oldRow = snapshot.get(id);

                    if (oldRow != null) {
                        if ("json".equalsIgnoreCase(outputFormat)) {
                            RowData jsonRow = convertToJsonRow(tbl, "UPDATE", oldRow, rs, meta);
                            events.add(
                                    ChangeEvent.update(
                                            tbl, oldRow, jsonRow, System.currentTimeMillis()));
                        } else {
                            events.add(
                                    ChangeEvent.update(
                                            tbl, oldRow, newRow, System.currentTimeMillis()));
                        }
                    } else {
                        if ("json".equalsIgnoreCase(outputFormat)) {
                            RowData jsonRow = convertToJsonRow(tbl, "INSERT", null, rs, meta);
                            events.add(
                                    ChangeEvent.insert(tbl, jsonRow, System.currentTimeMillis()));
                        } else {
                            events.add(ChangeEvent.insert(tbl, newRow, System.currentTimeMillis()));
                        }
                    }
                    snapshot.put(id, newRow);
                    if (updatedAt != null) {
                        lastPolledTimestamps.put(tbl, updatedAt);
                    }
                }
            }
        }
        return events;
    }

    /** Poll for deletes across all tables. */
    public List<ChangeEvent<RowData>> pollDeletes() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        for (String tbl : tableNames) {
            events.addAll(pollDeletesForTable(tbl));
        }
        return events;
    }

    private List<ChangeEvent<RowData>> pollDeletesForTable(String tbl) throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        String pkCol = getPrimaryKeyColumn(tbl);
        Map<Object, RowData> snapshot = snapshots.get(tbl);

        String sql = String.format("SELECT %s FROM %s.%s", pkCol, schema, tbl);
        List<Long> currentIds = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                currentIds.add(rs.getLong(pkCol));
            }
        }
        List<Object> deletedIds = new ArrayList<>();
        for (Object id : snapshot.keySet()) {
            if (!currentIds.contains(id)) {
                deletedIds.add(id);
            }
        }
        for (Object id : deletedIds) {
            RowData deletedRow = snapshot.remove(id);
            if (deletedRow != null) {
                if ("json".equalsIgnoreCase(outputFormat)) {
                    RowData jsonRow = convertToJsonRowForDelete(tbl, deletedRow);
                    events.add(ChangeEvent.delete(tbl, jsonRow, System.currentTimeMillis()));
                } else {
                    if (deletedRow instanceof GenericRowData) {
                        ((GenericRowData) deletedRow).setRowKind(RowKind.DELETE);
                    }
                    events.add(ChangeEvent.delete(tbl, deletedRow, System.currentTimeMillis()));
                }
            }
        }
        return events;
    }

    /** Comprehensive poll for all change types across all tables. */
    public List<ChangeEvent<RowData>> pollAllChanges() throws SQLException {
        List<ChangeEvent<RowData>> allEvents = new ArrayList<>();
        allEvents.addAll(pollNewInserts());
        allEvents.addAll(pollUpdates());
        allEvents.addAll(pollDeletes());
        return allEvents;
    }

    /** Load initial snapshot for all tables into memory. */
    public void loadSnapshot() throws SQLException {
        for (String tbl : tableNames) {
            loadSnapshotForTable(tbl);
        }
    }

    private void loadSnapshotForTable(String tbl) throws SQLException {
        Map<Object, RowData> snapshot = snapshots.get(tbl);
        snapshot.clear();
        String columns = getTableColumns(tbl);
        String pkCol = getPrimaryKeyColumn(tbl);
        long maxId = 0;

        String sql = String.format("SELECT %s FROM %s.%s", columns, schema, tbl);
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                long id = rs.getLong(pkCol);
                RowData row = convertToRowDataDynamic(rs, meta);
                snapshot.put(id, row);
                if (id > maxId) {
                    maxId = id;
                }
            }
        }
        // Only update lastPolledId if we actually loaded rows;
        // preserve existing value for empty tables (backward compatible)
        if (snapshot.size() > 0 || lastPolledIds.get(tbl) == 0L) {
            lastPolledIds.put(tbl, maxId);
        }
        LOG.info(
                "Loaded snapshot for {}.{}: {} rows, lastId={}",
                schema,
                tbl,
                snapshot.size(),
                lastPolledIds.get(tbl));
    }

    // ---- JSON output helpers ----

    private RowData convertToJsonRow(
            String tbl, String op, RowData oldRow, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"schema\":\"").append(escapeJson(schema)).append('"');
        sb.append(",\"op\":\"").append(op).append('"');
        // before
        if (oldRow != null) {
            sb.append(",\"before\":");
            appendRowDataAsJson(sb, oldRow, meta);
        } else {
            sb.append(",\"before\":{}");
        }
        // after
        sb.append(",\"after\":");
        appendResultSetAsJson(sb, rs, meta);
        sb.append('}');

        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(sb.toString()));
        return row;
    }

    private RowData convertToJsonRowForDelete(String tbl, RowData oldRow) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"schema\":\"").append(escapeJson(schema)).append('"');
        sb.append(",\"op\":\"DELETE\"");
        sb.append(",\"before\":");
        // For delete, we don't have ResultSetMetaData here; serialize what we have
        if (oldRow instanceof GenericRowData) {
            GenericRowData gd = (GenericRowData) oldRow;
            sb.append("{\"row\":\"").append(gd.toString()).append("\"}");
        } else {
            sb.append("{}");
        }
        sb.append(",\"after\":{}}");

        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(sb.toString()));
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    private void appendResultSetAsJson(StringBuilder sb, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        sb.append('{');
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) {
                sb.append(',');
            }
            String colName = meta.getColumnName(i);
            sb.append('"').append(escapeJson(colName)).append("\":");
            String val = rs.getString(i);
            if (rs.wasNull()) {
                sb.append("null");
            } else {
                sb.append('"').append(escapeJson(val)).append('"');
            }
        }
        sb.append('}');
    }

    private void appendRowDataAsJson(StringBuilder sb, RowData row, ResultSetMetaData meta)
            throws SQLException {
        if (!(row instanceof GenericRowData)) {
            sb.append("{}");
            return;
        }
        GenericRowData gd = (GenericRowData) row;
        sb.append('{');
        for (int i = 0; i < gd.getArity() && i < meta.getColumnCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String colName = meta.getColumnName(i + 1);
            sb.append('"').append(escapeJson(colName)).append("\":");
            Object val = gd.getField(i);
            if (val == null) {
                sb.append("null");
            } else if (val instanceof StringData) {
                sb.append('"').append(escapeJson(((StringData) val).toString())).append('"');
            } else {
                sb.append('"').append(escapeJson(String.valueOf(val))).append('"');
            }
        }
        sb.append('}');
    }

    private String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    // ---- Standard RowData conversion (raw mode) ----

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
}
