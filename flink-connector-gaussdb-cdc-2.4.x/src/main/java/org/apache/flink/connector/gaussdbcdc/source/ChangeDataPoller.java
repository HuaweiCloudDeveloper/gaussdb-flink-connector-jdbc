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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private final Connection connection;
    private final String database;
    private final String schema;
    private final List<String> tableNames;
    private final String outputFormat;
    private final String outputJsonFormat;
    private final int batchSize;
    private String identifierQuoteString;

    // Per-table state
    private final Map<String, String> cachedColumns = new HashMap<>();
    private final Map<String, List<String>> cachedColumnNames = new HashMap<>();
    private final Map<String, String> cachedPkColumns = new HashMap<>();
    private final Map<String, Object> lastPolledKeys = new HashMap<>();
    private final Map<String, Timestamp> lastPolledTimestamps = new HashMap<>();
    private final Map<String, Object> lastPolledUpdateKeys = new HashMap<>();
    private final Map<String, Map<Object, RowData>> snapshots = new HashMap<>();

    public ChangeDataPoller(
            Connection connection, String schema, List<String> tableNames, String outputFormat) {
        this(connection, null, schema, tableNames, outputFormat, "debezium", DEFAULT_BATCH_SIZE);
    }

    public ChangeDataPoller(
            Connection connection,
            String database,
            String schema,
            List<String> tableNames,
            String outputFormat,
            String outputJsonFormat) {
        this(
                connection,
                database,
                schema,
                tableNames,
                outputFormat,
                outputJsonFormat,
                DEFAULT_BATCH_SIZE);
    }

    public ChangeDataPoller(
            Connection connection,
            String database,
            String schema,
            List<String> tableNames,
            String outputFormat,
            String outputJsonFormat,
            int batchSize) {
        this.connection = connection;
        this.database = database;
        this.schema = schema;
        this.tableNames = tableNames;
        this.outputFormat = outputFormat;
        this.outputJsonFormat = outputJsonFormat;
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        this.batchSize = batchSize;
        for (String tbl : tableNames) {
            lastPolledKeys.put(tbl, null);
            lastPolledTimestamps.put(tbl, new Timestamp(0));
            lastPolledUpdateKeys.put(tbl, null);
            snapshots.put(tbl, new HashMap<>());
        }
    }

    /** Backward-compatible single-table constructor. */
    public ChangeDataPoller(
            Connection connection, String schema, String tableName, String primaryKeyColumn) {
        this(connection, schema, java.util.Collections.singletonList(tableName), "raw");
        if (primaryKeyColumn != null) {
            cachedPkColumns.put(tableName, primaryKeyColumn);
            // Preserve the legacy single-table poller's numeric starting cursor. Generic
            // multi-table construction starts from null and supports non-numeric keys.
            lastPolledKeys.put(tableName, 0L);
        }
    }

    /** Backward-compatible getter for first table's lastPolledId. */
    public long getLastPolledId() {
        Object value = lastPolledKeys.get(tableNames.get(0));
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    /** Backward-compatible setter for first table's lastPolledId. */
    public void setLastPolledId(long lastPolledId) {
        lastPolledKeys.put(tableNames.get(0), lastPolledId);
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
        String joined = quoteColumnList(columns);
        cachedColumnNames.put(tbl, columns);
        cachedColumns.put(tbl, joined);
        return joined;
    }

    private String identifierQuoteString() throws SQLException {
        if (identifierQuoteString == null) {
            identifierQuoteString = SqlIdentifierUtils.resolveQuoteString(connection);
        }
        return identifierQuoteString;
    }

    private String quoteIdentifier(String identifier) throws SQLException {
        return SqlIdentifierUtils.quote(identifier, identifierQuoteString());
    }

    private String qualifiedTable(String tbl) throws SQLException {
        return SqlIdentifierUtils.qualified(schema, tbl, identifierQuoteString());
    }

    private String quoteColumnList(List<String> columns) throws SQLException {
        return SqlIdentifierUtils.columnList(columns, identifierQuoteString());
    }

    private boolean hasColumn(String tbl, String columnName) throws SQLException {
        return findColumnName(tbl, columnName) != null;
    }

    private String findColumnName(String tbl, String columnName) throws SQLException {
        getTableColumns(tbl);
        for (String column : cachedColumnNames.get(tbl)) {
            if (column.equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    /** Detect primary key column for a table, cached after first access. */
    private String getPrimaryKeyColumn(String tbl) throws SQLException {
        if (cachedPkColumns.containsKey(tbl)) {
            return cachedPkColumns.get(tbl);
        }
        // Polling cursors require one deterministic key. Reject composite/no-PK tables explicitly
        // instead of silently using the first key column or a possibly non-existent "id" column.
        try {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet pkRs = meta.getPrimaryKeys(null, schema, tbl);
            if (pkRs != null) {
                try (ResultSet rs = pkRs) {
                    List<String> pkColumns = new ArrayList<>();
                    while (rs.next()) {
                        String pkName = rs.getString("COLUMN_NAME");
                        if (pkName != null && !pkName.isEmpty()) {
                            pkColumns.add(pkName);
                        }
                    }
                    if (pkColumns.size() == 1) {
                        cachedPkColumns.put(tbl, pkColumns.get(0));
                        return pkColumns.get(0);
                    }
                    if (pkColumns.size() > 1) {
                        throw new SQLException(
                                "Polling CDC requires a single-column primary key for "
                                        + schema
                                        + "."
                                        + tbl
                                        + "; found composite key "
                                        + pkColumns);
                    }
                }
            }
        } catch (SQLException e) {
            if (e.getMessage() != null
                    && e.getMessage().startsWith("Polling CDC requires a single-column")) {
                throw e;
            }
            LOG.warn("Failed to detect PK for {}.{}: {}", schema, tbl, e.getMessage());
        }
        // Fallback: pg_index
        try (PreparedStatement stmt =
                connection.prepareStatement(
                        "SELECT a.attname FROM pg_index i "
                                + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                                + "JOIN pg_class c ON c.oid = i.indrelid "
                                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                                + "WHERE n.nspname = ? AND c.relname = ? AND i.indisprimary "
                                + "ORDER BY a.attnum")) {
            stmt.setString(1, schema);
            stmt.setString(2, tbl);
            try (ResultSet rs = stmt.executeQuery()) {
                List<String> pkColumns = new ArrayList<>();
                while (rs.next()) {
                    String pkName = rs.getString(1);
                    if (pkName != null && !pkName.isEmpty()) {
                        pkColumns.add(pkName);
                    }
                }
                if (pkColumns.size() == 1) {
                    cachedPkColumns.put(tbl, pkColumns.get(0));
                    return pkColumns.get(0);
                }
                if (pkColumns.size() > 1) {
                    throw new SQLException(
                            "Polling CDC requires a single-column primary key for "
                                    + schema
                                    + "."
                                    + tbl
                                    + "; found composite key "
                                    + pkColumns);
                }
            }
        } catch (SQLException e) {
            if (e.getMessage() != null
                    && e.getMessage().startsWith("Polling CDC requires a single-column")) {
                throw e;
            }
            LOG.warn("pg_index PK detection failed for {}.{}: {}", schema, tbl, e.getMessage());
        }
        throw new SQLException(
                "Polling CDC requires a primary key, but none was found for " + schema + "." + tbl);
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
        Object lastKey = lastPolledKeys.get(tbl);
        Map<Object, RowData> snapshot = snapshots.get(tbl);

        String sql;
        if (lastKey == null) {
            sql =
                    String.format(
                            "SELECT %s FROM %s ORDER BY %s LIMIT %d",
                            columns, qualifiedTable(tbl), quoteIdentifier(pkCol), batchSize);
        } else {
            sql =
                    String.format(
                            "SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
                            columns,
                            qualifiedTable(tbl),
                            quoteIdentifier(pkCol),
                            quoteIdentifier(pkCol),
                            batchSize);
        }
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setFetchSize(batchSize);
            if (lastKey != null) {
                stmt.setObject(1, lastKey);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData rawRow = convertToRowDataDynamic(rs, meta);
                    Object key = readPrimaryKey(rs, pkCol);
                    // Update polling runs first and may already have discovered this newly inserted
                    // row through updated_at. Do not emit it a second time when advancing the PK
                    // cursor in the same cycle.
                    if (!snapshot.containsKey(key)) {
                        RowData emittedRow;
                        if ("json".equalsIgnoreCase(outputFormat)) {
                            emittedRow = convertToJsonRow(tbl, "INSERT", null, rs, meta);
                        } else {
                            emittedRow = rawRow;
                        }
                        events.add(ChangeEvent.insert(tbl, emittedRow, System.currentTimeMillis()));
                    }
                    // Always keep the raw row in the snapshot. JSON envelopes are output records,
                    // not comparable table state and cannot be used to build UPDATE/DELETE images.
                    snapshot.put(key, rawRow);
                    lastPolledKeys.put(tbl, key);
                }
            }
        }
        return events;
    }

    /** Poll for updates across all tables. */
    public List<ChangeEvent<RowData>> pollUpdates() throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        for (String tbl : tableNames) {
            events.addAll(pollUpdatesForTable(tbl));
        }
        return events;
    }

    private List<ChangeEvent<RowData>> pollUpdatesForTable(String tbl) throws SQLException {
        List<ChangeEvent<RowData>> events = new ArrayList<>();
        String columns = getTableColumns(tbl);
        if (!hasColumn(tbl, "updated_at")) {
            return events;
        }
        String updatedAtColumn = findColumnName(tbl, "updated_at");
        String pkCol = getPrimaryKeyColumn(tbl);
        Timestamp lastTs = lastPolledTimestamps.get(tbl);
        Object lastUpdateKey = lastPolledUpdateKeys.get(tbl);
        Map<Object, RowData> snapshot = snapshots.get(tbl);

        String sql;
        if (lastUpdateKey == null) {
            sql =
                    String.format(
                            "SELECT %s FROM %s WHERE %s > ? ORDER BY %s, %s LIMIT %d",
                            columns,
                            qualifiedTable(tbl),
                            quoteIdentifier(updatedAtColumn),
                            quoteIdentifier(updatedAtColumn),
                            quoteIdentifier(pkCol),
                            batchSize);
        } else {
            sql =
                    String.format(
                            "SELECT %s FROM %s WHERE %s > ? OR (%s = ? AND %s > ?) "
                                    + "ORDER BY %s, %s LIMIT %d",
                            columns,
                            qualifiedTable(tbl),
                            quoteIdentifier(updatedAtColumn),
                            quoteIdentifier(updatedAtColumn),
                            quoteIdentifier(pkCol),
                            quoteIdentifier(updatedAtColumn),
                            quoteIdentifier(pkCol),
                            batchSize);
        }
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setFetchSize(batchSize);
            stmt.setTimestamp(1, lastTs);
            if (lastUpdateKey != null) {
                stmt.setTimestamp(2, lastTs);
                stmt.setObject(3, lastUpdateKey);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData newRow = convertToRowDataDynamic(rs, meta);
                    Object key = readPrimaryKey(rs, pkCol);
                    Timestamp updatedAt = rs.getTimestamp(updatedAtColumn);
                    RowData oldRow = snapshot.get(key);

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
                    snapshot.put(key, newRow);
                    if (updatedAt != null) {
                        lastPolledTimestamps.put(tbl, updatedAt);
                        lastPolledUpdateKeys.put(tbl, key);
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

        String sql =
                String.format("SELECT %s FROM %s", quoteIdentifier(pkCol), qualifiedTable(tbl));
        Set<Object> currentIds = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setFetchSize(batchSize);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    currentIds.add(readPrimaryKey(rs, pkCol));
                }
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
        // Poll timestamp-based changes first. If that query discovers a new row, it advances the
        // ID watermark before insert polling and prevents a duplicate INSERT in the same cycle.
        allEvents.addAll(pollUpdates());
        allEvents.addAll(pollNewInserts());
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
        Object maxKey = null;
        Object maxUpdatedAtKey = null;
        Timestamp maxUpdatedAt = new Timestamp(0);
        String updatedAtColumn = findColumnName(tbl, "updated_at");
        boolean tracksUpdates = updatedAtColumn != null;

        String sql =
                String.format(
                        "SELECT %s FROM %s ORDER BY %s",
                        columns, qualifiedTable(tbl), quoteIdentifier(pkCol));
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setFetchSize(batchSize);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    Object key = readPrimaryKey(rs, pkCol);
                    RowData row = convertToRowDataDynamic(rs, meta);
                    snapshot.put(key, row);
                    maxKey = key;
                    if (tracksUpdates) {
                        Timestamp updatedAt = rs.getTimestamp(updatedAtColumn);
                        if (updatedAt != null && updatedAt.after(maxUpdatedAt)) {
                            maxUpdatedAt = updatedAt;
                            maxUpdatedAtKey = key;
                        } else if (updatedAt != null && updatedAt.equals(maxUpdatedAt)) {
                            // Rows are ordered by PK, so the last key at this timestamp is the
                            // correct secondary cursor for the next page.
                            maxUpdatedAtKey = key;
                        }
                    }
                }
            }
        }
        if (maxKey != null || lastPolledKeys.get(tbl) == null) {
            lastPolledKeys.put(tbl, maxKey);
        }
        if (tracksUpdates) {
            // The JDBC snapshot was already emitted by the source. Start update polling after
            // the newest row included in that snapshot so it is not emitted a second time.
            lastPolledTimestamps.put(tbl, maxUpdatedAt);
            lastPolledUpdateKeys.put(tbl, maxUpdatedAtKey);
        }
        LOG.info(
                "Loaded snapshot for {}.{}: {} rows, lastKey={}",
                schema,
                tbl,
                snapshot.size(),
                lastPolledKeys.get(tbl));
    }

    private Object readPrimaryKey(ResultSet rs, String pkCol) throws SQLException {
        Object key = rs.getObject(pkCol);
        if (key == null && !rs.wasNull()) {
            // Some older/mocked JDBC result sets do not implement getObject(String) correctly.
            long numericKey = rs.getLong(pkCol);
            if (!rs.wasNull()) {
                key = numericKey;
            }
        }
        if (key == null) {
            throw new SQLException("Primary key " + pkCol + " returned null");
        }
        if (!(key instanceof Serializable)) {
            key = key.toString();
        }
        return key;
    }

    /** Serialize polling cursors and delete-detection rows for Flink operator state. */
    byte[] serializeState() throws IOException {
        PollingState state = new PollingState();
        state.lastPolledKeys.putAll(lastPolledKeys);
        for (Map.Entry<String, Timestamp> entry : lastPolledTimestamps.entrySet()) {
            state.lastPolledTimestamps.put(entry.getKey(), entry.getValue().getTime());
        }
        state.lastPolledUpdateKeys.putAll(lastPolledUpdateKeys);
        for (Map.Entry<String, Map<Object, RowData>> tableEntry : snapshots.entrySet()) {
            Map<Object, StoredRow> rows = new HashMap<>();
            for (Map.Entry<Object, RowData> rowEntry : tableEntry.getValue().entrySet()) {
                rows.put(rowEntry.getKey(), StoredRow.from(rowEntry.getValue()));
            }
            state.snapshots.put(tableEntry.getKey(), rows);
        }

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(state);
            output.flush();
            return bytes.toByteArray();
        }
    }

    /** Restore polling state before the streaming loop starts. */
    void restoreState(byte[] serializedState) throws IOException {
        if (serializedState == null || serializedState.length == 0) {
            return;
        }
        PollingState state;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serializedState))) {
            Object value = input.readObject();
            if (!(value instanceof PollingState)) {
                throw new IOException("Unexpected polling state type: " + value.getClass());
            }
            state = (PollingState) value;
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not deserialize polling state", e);
        }

        lastPolledKeys.clear();
        lastPolledKeys.putAll(state.lastPolledKeys);
        lastPolledTimestamps.clear();
        for (String tbl : tableNames) {
            Long timestamp = state.lastPolledTimestamps.get(tbl);
            lastPolledTimestamps.put(tbl, new Timestamp(timestamp == null ? 0L : timestamp));
        }
        lastPolledUpdateKeys.clear();
        lastPolledUpdateKeys.putAll(state.lastPolledUpdateKeys);
        snapshots.clear();
        for (String tbl : tableNames) {
            Map<Object, RowData> rows = new HashMap<>();
            Map<Object, StoredRow> storedRows = state.snapshots.get(tbl);
            if (storedRows != null) {
                for (Map.Entry<Object, StoredRow> entry : storedRows.entrySet()) {
                    rows.put(entry.getKey(), entry.getValue().toRowData());
                }
            }
            snapshots.put(tbl, rows);
            lastPolledKeys.putIfAbsent(tbl, null);
            lastPolledUpdateKeys.putIfAbsent(tbl, null);
        }
    }

    private static final class PollingState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, Object> lastPolledKeys = new HashMap<>();
        private final Map<String, Long> lastPolledTimestamps = new HashMap<>();
        private final Map<String, Object> lastPolledUpdateKeys = new HashMap<>();
        private final Map<String, Map<Object, StoredRow>> snapshots = new HashMap<>();
    }

    private static final class StoredRow implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte rowKind;
        private final List<StoredValue> fields;

        private StoredRow(byte rowKind, List<StoredValue> fields) {
            this.rowKind = rowKind;
            this.fields = fields;
        }

        private static StoredRow from(RowData row) throws IOException {
            if (!(row instanceof GenericRowData)) {
                throw new IOException(
                        "Polling state only supports GenericRowData, found "
                                + row.getClass().getName());
            }
            GenericRowData genericRow = (GenericRowData) row;
            List<StoredValue> fields = new ArrayList<>(genericRow.getArity());
            for (int i = 0; i < genericRow.getArity(); i++) {
                fields.add(StoredValue.from(genericRow.getField(i)));
            }
            return new StoredRow(row.getRowKind().toByteValue(), fields);
        }

        private RowData toRowData() {
            GenericRowData row = new GenericRowData(RowKind.fromByteValue(rowKind), fields.size());
            for (int i = 0; i < fields.size(); i++) {
                row.setField(i, fields.get(i).toValue());
            }
            return row;
        }
    }

    private static final class StoredValue implements Serializable {
        private static final long serialVersionUID = 1L;

        private enum Kind {
            NULL,
            STRING,
            DECIMAL,
            TIMESTAMP,
            OBJECT
        }

        private final Kind kind;
        private final Serializable value;
        private final int precision;
        private final int scale;

        private StoredValue(Kind kind, Serializable value, int precision, int scale) {
            this.kind = kind;
            this.value = value;
            this.precision = precision;
            this.scale = scale;
        }

        private static StoredValue from(Object value) {
            if (value == null) {
                return new StoredValue(Kind.NULL, null, 0, 0);
            }
            if (value instanceof StringData) {
                return new StoredValue(Kind.STRING, value.toString(), 0, 0);
            }
            if (value instanceof DecimalData) {
                DecimalData decimal = (DecimalData) value;
                return new StoredValue(
                        Kind.DECIMAL, decimal.toBigDecimal(), decimal.precision(), decimal.scale());
            }
            if (value instanceof TimestampData) {
                TimestampData timestamp = (TimestampData) value;
                return new StoredValue(Kind.TIMESTAMP, timestamp.toTimestamp(), 0, 0);
            }
            Serializable serializable =
                    value instanceof Serializable ? (Serializable) value : value.toString();
            return new StoredValue(Kind.OBJECT, serializable, 0, 0);
        }

        private Object toValue() {
            switch (kind) {
                case NULL:
                    return null;
                case STRING:
                    return StringData.fromString((String) value);
                case DECIMAL:
                    return DecimalData.fromBigDecimal(
                            (java.math.BigDecimal) value, precision, scale);
                case TIMESTAMP:
                    return TimestampData.fromTimestamp((Timestamp) value);
                case OBJECT:
                default:
                    return value;
            }
        }
    }

    // ---- JSON output helpers ----

    private RowData convertToJsonRow(
            String tbl, String op, RowData oldRow, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        String json;
        if ("canal".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCanalJson(tbl, op, oldRow, rs, meta);
        } else if ("he".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCustomJson(tbl, op, oldRow, rs, meta);
        } else {
            json = buildDebeziumJson(tbl, op, oldRow, rs, meta);
        }
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(json));
        return row;
    }

    private RowData convertToJsonRowForDelete(String tbl, RowData oldRow) throws SQLException {
        String json;
        if ("canal".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCanalJson(tbl, "DELETE", oldRow, null, null);
        } else if ("he".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCustomJson(tbl, "DELETE", oldRow, null, null);
        } else {
            json = buildDebeziumJson(tbl, "DELETE", oldRow, null, null);
        }
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(json));
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    private String buildDebeziumJson(
            String tbl, String operation, RowData oldRow, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"before\":");
        appendBeforeJson(sb, tbl, oldRow);
        sb.append(",\"after\":");
        appendAfterJson(sb, rs, meta);
        sb.append(",\"source\":{\"connector\":\"gaussdb\"");
        sb.append(",\"db\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"schema\":\"").append(escapeJson(schema)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"ts_ms\":").append(now).append(",\"snapshot\":false}");
        sb.append(",\"op\":\"").append(debeziumOperation(operation)).append('"');
        sb.append(",\"ts_ms\":").append(now).append(",\"transaction\":null}");
        return sb.toString();
    }

    private String buildCanalJson(
            String tbl, String operation, RowData oldRow, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"data\":");
        appendAfterJson(sb, rs, meta);
        sb.append(",\"old\":");
        appendBeforeJson(sb, tbl, oldRow);
        sb.append(",\"database\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"type\":\"").append(operation).append('"');
        appendPrimaryKeyNames(sb, tbl);
        sb.append(",\"es\":").append(now).append(",\"ts\":").append(now);
        sb.append(",\"isDdl\":false,\"sqlType\":{},\"mysqlType\":{}}");
        return sb.toString();
    }

    private String buildCustomJson(
            String tbl, String operation, RowData oldRow, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        long now = System.currentTimeMillis();
        String pkCol = getPrimaryKeyColumn(tbl);
        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"database\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"optType\":\"").append(operation).append('"');
        appendPrimaryKeyNames(sb, tbl);
        String pkValue = rs != null ? rs.getString(pkCol) : extractValueFromRow(tbl, oldRow, pkCol);
        if (pkValue == null) {
            sb.append(",\"pkValues\":null");
        } else {
            sb.append(",\"pkValues\":\"").append(escapeJson(pkValue)).append('"');
        }
        sb.append(",\"es\":").append(now).append(",\"ts\":").append(now);
        sb.append(",\"data\":");
        appendAfterJson(sb, rs, meta);
        sb.append(",\"old\":");
        appendBeforeJson(sb, tbl, oldRow);
        sb.append('}');
        return sb.toString();
    }

    private void appendPrimaryKeyNames(StringBuilder sb, String tbl) throws SQLException {
        String pkCol = getPrimaryKeyColumn(tbl);
        sb.append(",\"pkNames\":[\"").append(escapeJson(pkCol)).append("\"]");
    }

    private String debeziumOperation(String operation) {
        if ("INSERT".equals(operation)) {
            return "c";
        }
        if ("DELETE".equals(operation)) {
            return "d";
        }
        return "u";
    }

    private void appendBeforeJson(StringBuilder sb, String tbl, RowData oldRow) {
        if (oldRow == null) {
            sb.append("null");
        } else {
            appendRowDataAsJson(sb, tbl, oldRow);
        }
    }

    private void appendAfterJson(StringBuilder sb, ResultSet rs, ResultSetMetaData meta)
            throws SQLException {
        if (rs == null) {
            sb.append("null");
        } else {
            appendResultSetAsJson(sb, rs, meta);
        }
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

    private void appendRowDataAsJson(StringBuilder sb, String tbl, RowData row) {
        if (!(row instanceof GenericRowData)) {
            sb.append("{}");
            return;
        }
        GenericRowData gd = (GenericRowData) row;
        List<String> columns = cachedColumnNames.get(tbl);
        sb.append('{');
        for (int i = 0; i < gd.getArity() && i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String colName = columns.get(i);
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

    private String extractValueFromRow(String tbl, RowData row, String columnName) {
        if (!(row instanceof GenericRowData)) {
            return null;
        }
        List<String> columns = cachedColumnNames.get(tbl);
        GenericRowData genericRow = (GenericRowData) row;
        for (int i = 0; i < columns.size() && i < genericRow.getArity(); i++) {
            if (columnName.equalsIgnoreCase(columns.get(i))) {
                Object value = genericRow.getField(i);
                return value == null ? null : String.valueOf(value);
            }
        }
        return null;
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
