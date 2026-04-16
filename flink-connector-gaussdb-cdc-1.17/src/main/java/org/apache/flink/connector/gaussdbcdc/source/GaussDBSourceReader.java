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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Source reader for GaussDB CDC that reads snapshot and streaming changes.
 *
 * <p>This implementation uses a polling-based approach for CDC:
 *
 * <ul>
 *   <li>First reads the initial snapshot
 *   <li>Then polls for changes based on a timestamp or version column
 * </ul>
 */
@Internal
public class GaussDBSourceReader implements SourceReader<RowData, GaussDBSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceReader.class);

    private final SourceReaderContext context;
    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final String slotName;
    private final String pluginName;
    private final int pollIntervalMs;

    private Connection connection;
    private GaussDBSplit currentSplit;
    private boolean snapshotFinished = false;
    private long lastPolledId = 0;
    private volatile boolean running = true;

    // Change data poller for capturing INSERT/UPDATE/DELETE
    private ChangeDataPoller changeDataPoller;

    public GaussDBSourceReader(
            SourceReaderContext context,
            String hostname,
            int port,
            String database,
            String schema,
            String tableName,
            String username,
            String password,
            String slotName,
            String pluginName,
            int pollIntervalMs) {
        this.context = context;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.slotName = slotName;
        this.pluginName = pluginName;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void start() {
        try {
            // Load GaussDB driver
            Class.forName("com.huawei.gaussdb.jdbc.Driver");

            // Create connection
            String url =
                    String.format(
                            "jdbc:gaussdb://%s:%d/%s?compatibleMode=mysql",
                            hostname, port, database);
            this.connection = DriverManager.getConnection(url, username, password);

            // Initialize change data poller
            this.changeDataPoller = new ChangeDataPoller(connection, schema, tableName, "id");

            LOG.info(
                    "Connected to GaussDB at {}:{}/{} for table {}",
                    hostname,
                    port,
                    database,
                    tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to GaussDB", e);
        }
    }

    /** Read all data from table (for initial snapshot). */
    private void readAllData(ReaderOutput<RowData> output) throws SQLException {
        String columns = getTableColumns();
        String sql = String.format("SELECT %s FROM %s.%s ORDER BY id", columns, schema, tableName);
        
        LOG.info("Reading all data from {}.{} with query: {}", schema, tableName, sql);
        
        int count = 0;
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                RowData row = convertToRowDataDynamic(rs, meta);
                output.collect(row);
                count++;
            }
        }
        
        LOG.info("Read {} rows from {}.{}", count, schema, tableName);
        snapshotFinished = true;
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!running) {
            return InputStatus.END_OF_INPUT;
        }

        // If no split assigned, read all data directly (simplified approach)
        if (currentSplit == null && !snapshotFinished) {
            readAllData(output);
            return InputStatus.MORE_AVAILABLE;
        }

        if (currentSplit == null) {
            // No split assigned and snapshot finished
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (currentSplit.isSnapshotSplit() && !snapshotFinished) {
            // Read snapshot chunk
            return pollSnapshot(output);
        } else {
            // Poll for changes
            return pollChanges(output);
        }
    }

    private InputStatus pollSnapshot(ReaderOutput<RowData> output) throws SQLException {
        // Dynamically build SELECT query based on table metadata
        String columns = getTableColumns();
        String sql =
                String.format(
                        "SELECT %s FROM %s.%s WHERE id >= ? AND id <= ? ORDER BY id",
                        columns, schema, tableName);

        int count = 0;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, currentSplit.getStartId());
            stmt.setLong(2, currentSplit.getEndId());

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData row = convertToRowDataDynamic(rs, meta);
                    output.collect(row);
                    count++;
                }
            }
        }

        // Load snapshot into poller for change detection
        if (changeDataPoller != null) {
            changeDataPoller.loadSnapshot();
        }

        snapshotFinished = true;
        LOG.info("Finished reading snapshot split: {} ({} rows)", currentSplit.splitId(), count);
        return InputStatus.NOTHING_AVAILABLE;
    }

    /** Get column names from table metadata. */
    private String getTableColumns() throws SQLException {
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
        return String.join(", ", columns);
    }

    private InputStatus pollChanges(ReaderOutput<RowData> output) throws Exception {
        // Use ChangeDataPoller to capture INSERT/UPDATE/DELETE
        List<ChangeEvent<RowData>> events = changeDataPoller.pollAllChanges();

        int insertCount = 0;
        int updateCount = 0;
        int deleteCount = 0;

        for (ChangeEvent<RowData> event : events) {
            // Emit the appropriate row based on change type
            switch (event.getChangeType()) {
                case INSERT:
                    output.collect(event.getAfter());
                    insertCount++;
                    break;
                case UPDATE:
                    // For UPDATE, emit the after image
                    output.collect(event.getAfter());
                    updateCount++;
                    break;
                case DELETE:
                    // For DELETE, we could emit a tombstone or skip
                    // Current implementation: skip (or emit before image if needed)
                    LOG.debug("Detected DELETE for id: {}", event.getBefore().getInt(0));
                    deleteCount++;
                    break;
                case SNAPSHOT:
                    output.collect(event.getAfter());
                    break;
            }
        }

        if (!events.isEmpty()) {
            LOG.debug(
                    "Polled {} events: {} inserts, {} updates, {} deletes",
                    events.size(),
                    insertCount,
                    updateCount,
                    deleteCount);
            return InputStatus.MORE_AVAILABLE;
        } else {
            // No new changes, wait before polling again
            Thread.sleep(pollIntervalMs);
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    /** Convert ResultSet to RowData dynamically based on metadata. */
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
                    value = DecimalData.fromBigDecimal(rs.getBigDecimal(i), 38, 18);
                    break;
                case java.sql.Types.TIMESTAMP:
                case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                    java.sql.Timestamp ts = rs.getTimestamp(i);
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

    @Override
    public List<GaussDBSplit> snapshotState(long checkpointId) {
        // Return current split for checkpoint
        List<GaussDBSplit> splits = new ArrayList<>();
        if (currentSplit != null) {
            splits.add(currentSplit);
        }
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<GaussDBSplit> splits) {
        if (!splits.isEmpty()) {
            this.currentSplit = splits.get(0);
            LOG.info("Assigned split: {}", currentSplit);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits to assign");
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("Closed GaussDB connection");
        }
    }
}
