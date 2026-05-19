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
import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;
import org.apache.flink.core.io.InputStatus;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
    private final boolean walMode;
    private final String decodePlugin;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final String sslMode;
    private final Integer replicationPort;

    private Connection connection;
    private GaussDBSplit currentSplit;
    private boolean snapshotFinished = false;
    private long lastPolledId = 0;
    private volatile boolean running = true;

    // Change data poller for polling-based CDC
    private ChangeDataPoller changeDataPoller;

    // WAL replication stream for WAL-based CDC
    private WalReplicationStream walReplicationStream;

    // Whether the WAL stream has been initialized
    private boolean walStreamInitialized = false;

    // LSN recorded after snapshot to avoid WAL replay of already-snapshot data
    private String snapshotStartLsn = null;
    private String lastConsumedLsn = null;

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
            int pollIntervalMs,
            boolean walMode,
            String decodePlugin,
            int parallelDecodeNum,
            String decodeStyle,
            boolean sendingBatch,
            String sslMode,
            Integer replicationPort) {
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
        this.walMode = walMode;
        this.decodePlugin = decodePlugin;
        this.parallelDecodeNum = parallelDecodeNum;
        this.decodeStyle = decodeStyle;
        this.sendingBatch = sendingBatch;
        this.sslMode = sslMode;
        this.replicationPort = replicationPort;
    }

    @Override
    public void start() {
        try {
            // Load GaussDB driver
            Class.forName("com.huawei.gaussdb.jdbc.Driver");

            // Create connection
            String url =
                    String.format(
                            "jdbc:gaussdb://%s:%d/%s?sslmode=%s",
                            hostname, port, database, sslMode);
            LOG.info("Connecting to GaussDB SourceReader: {}", url);
            this.connection = DriverManager.getConnection(url, username, password);

            // Initialize CDC mode based on walMode setting
            if (walMode) {
                // WAL logical decoding mode
                this.walReplicationStream =
                        new WalReplicationStream(
                                connection,
                                url,
                                username,
                                password,
                                slotName,
                                decodePlugin,
                                parallelDecodeNum,
                                decodeStyle,
                                sendingBatch,
                                1000,
                                replicationPort);
                LOG.info(
                        "Using WAL mode with plugin={}, parallel-decode-num={}, decode-style={}",
                        decodePlugin,
                        parallelDecodeNum,
                        decodeStyle);
            } else {
                // Polling-based CDC mode
                this.changeDataPoller =
                        new ChangeDataPoller(
                                connection,
                                schema,
                                tableName,
                                "id",
                                String.format("jdbc:gaussdb://%s:%d/%s", hostname, port, database),
                                username,
                                password,
                                sslMode);
                LOG.info("Using polling-based CDC mode");
            }

            LOG.info(
                    "Connected to GaussDB at {}:{}/{} for table {}",
                    hostname,
                    port,
                    database,
                    tableName);
        } catch (Exception e) {
            LOG.error("Failed to connect to GaussDB at {}:{}/{}", hostname, port, database, e);
            throw new RuntimeException("Failed to connect to GaussDB: " + e.getMessage(), e);
        }
    }

    /** Read all data from table (for initial snapshot). */
    private void readAllData(ReaderOutput<RowData> output) throws SQLException {
        // Record WAL position BEFORE snapshot for diagnostics.
        if (walMode) {
            String preSnapshotLsn = getCurrentWalLsn();
            LOG.info("Pre-snapshot WAL LSN: {}", preSnapshotLsn);
        }

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

        // Load snapshot into ChangeDataPoller so incremental polling works
        if (changeDataPoller != null) {
            changeDataPoller.loadSnapshot();
        }

        // Capture WAL position AFTER snapshot completes. This ensures the
        // WAL stream starts from a position that is strictly after all
        // data already captured by the snapshot, preventing duplicates.
        if (walMode) {
            snapshotStartLsn = getCurrentWalLsn();
            LOG.info("Recorded post-snapshot WAL start LSN: {}", snapshotStartLsn);
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

        if (currentSplit == null && snapshotFinished) {
            // Snapshot finished but no stream split assigned yet,
            // still poll for incremental changes
            return pollChanges(output);
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
        if (walMode) {
            return pollChangesFromWal(output);
        } else {
            return pollChangesFromPoller(output);
        }
    }

    /** Poll changes from WAL logical decoding stream. */
    private InputStatus pollChangesFromWal(ReaderOutput<RowData> output) throws Exception {
        // Lazy initialize the WAL stream on first poll after snapshot completion
        if (!walStreamInitialized) {
            LOG.info("Initializing WAL replication stream for incremental phase");
            // Start WAL streaming from the snapshot start LSN to avoid
            // replaying data already emitted during the initial snapshot
            if (snapshotStartLsn != null) {
                LOG.info(
                        "Starting WAL stream from snapshot start LSN: {}, skipping already-snapshot data",
                        snapshotStartLsn);
                walReplicationStream.initialize(snapshotStartLsn);
            } else {
                walReplicationStream.initialize();
            }
            walStreamInitialized = true;
            LOG.info(
                    "WAL stream initialized, starting from LSN: {}",
                    walReplicationStream.getLastLsn());

            // Discard stale WAL data from the slot's existing position,
            // which may be earlier than the snapshot LSN. This handles both
            // JDBC Replication API and SQL function fallback.
            try {
                int discarded = walReplicationStream.readChanges(10000).size();
                if (discarded > 0) {
                    LOG.info("Discarded {} stale WAL changes after snapshot", discarded);
                }
                lastConsumedLsn = walReplicationStream.getLastLsn();
            } catch (Exception e) {
                LOG.warn("Failed to discard stale WAL data: {}", e.getMessage());
            }
        }

        List<WalChange> changes = walReplicationStream.readChanges(1000);

        for (WalChange change : changes) {
            if (!change.isDataChange()) {
                continue;
            }

            // Skip changes at or before the last consumed LSN.
            if (lastConsumedLsn != null
                    && change.getLsn() != null
                    && !WalReplicationStream.isLsnNewer(change.getLsn(), lastConsumedLsn)) {
                continue;
            }

            WalChange.ChangeType changeType = change.getType();
            if (changeType == WalChange.ChangeType.INSERT) {
                RowData insertRow = convertWalColumnsToRowData(change.getAfterColumns());
                if (insertRow != null) {
                    output.collect(insertRow);
                }
            } else if (changeType == WalChange.ChangeType.UPDATE) {
                // For UPDATE, emit the after image as INSERT
                RowData updateRow = convertWalColumnsToRowData(change.getAfterColumns());
                if (updateRow != null) {
                    output.collect(updateRow);
                }
            } else if (changeType == WalChange.ChangeType.DELETE) {
                // For DELETE, emit the before image with DELETE RowKind
                RowData deleteRow = convertWalColumnsToRowData(change.getBeforeColumns());
                if (deleteRow != null) {
                    if (deleteRow instanceof GenericRowData) {
                        ((GenericRowData) deleteRow).setRowKind(RowKind.DELETE);
                    }
                    LOG.info("Emitting WAL DELETE: {}", deleteRow);
                    output.collect(deleteRow);
                }
            }
        }

        if (!changes.isEmpty()) {
            LOG.debug("Polled {} WAL changes", changes.size());
            return InputStatus.MORE_AVAILABLE;
        } else {
            Thread.sleep(pollIntervalMs);
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    /** Convert WAL column values to Flink RowData. */
    private RowData convertWalColumnsToRowData(List<WalChange.ColumnValue> columns) {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        GenericRowData row = new GenericRowData(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            WalChange.ColumnValue col = columns.get(i);
            if (col.isNull()) {
                row.setField(i, null);
                continue;
            }
            // Value is already in string format from mppdb_decoding
            // Convert based on type OID
            Object value = convertColumnValueByOid(col.getTypeOid(), col.getValue());
            row.setField(i, value);
        }
        return row;
    }

    /** Convert a column value string based on PostgreSQL/GaussDB type OID. */
    private Object convertColumnValueByOid(int typeOid, String value) {
        if (value == null) {
            return null;
        }
        // Common PostgreSQL type OIDs
        // 23=int4, 20=int8, 21=int2, 16=bool, 25=text, 1043=varchar
        // 700=float4, 701=float8, 1700=numeric, 1082=date, 1114=timestamp
        switch (typeOid) {
            case 23: // int4
            case 21: // int2
                return Integer.parseInt(value);
            case 20: // int8
                return Long.parseLong(value);
            case 16: // bool
                return Boolean.parseBoolean(value);
            case 700: // float4
                return Float.parseFloat(value);
            case 701: // float8
                return Double.parseDouble(value);
            case 25: // text
            case 1043: // varchar
            case 19: // name
                return StringData.fromString(value);
            case 1700: // numeric
                java.math.BigDecimal numericBd = new java.math.BigDecimal(value);
                return DecimalData.fromBigDecimal(
                        numericBd, numericBd.precision(), numericBd.scale());
            case 1114: // timestamp
                return TimestampData.fromLocalDateTime(
                        java.time.LocalDateTime.parse(
                                value,
                                java.time.format.DateTimeFormatter.ofPattern(
                                        "yyyy-MM-dd HH:mm:ss[.SSSSSS]")));
            case 1082: // date
                return (int) java.time.LocalDate.parse(value).toEpochDay();
            default:
                // Fallback to string
                return StringData.fromString(value);
        }
    }

    /** Poll changes using ChangeDataPoller (polling-based CDC). */
    private InputStatus pollChangesFromPoller(ReaderOutput<RowData> output) throws Exception {
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
                    // For DELETE, emit the before image with DELETE RowKind
                    RowData beforeRow = event.getBefore();
                    if (beforeRow instanceof GenericRowData) {
                        ((GenericRowData) beforeRow).setRowKind(RowKind.DELETE);
                    }
                    LOG.info("Emitting DELETE event: before={}", beforeRow);
                    output.collect(beforeRow);
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
                    java.math.BigDecimal bd = rs.getBigDecimal(i);
                    if (bd != null) {
                        int p = meta.getPrecision(i);
                        int s = meta.getScale(i);
                        // Flink compact decimal supports precision <= 18
                        // Use actual precision/scale from metadata
                        value = DecimalData.fromBigDecimal(bd, p, s);
                        // fromBigDecimal returns null if bd.precision() > declared precision
                        if (value == null) {
                            // Fallback: adjust precision to fit the actual value
                            value = DecimalData.fromBigDecimal(bd, bd.precision(), s);
                        }
                    } else {
                        value = null;
                    }
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

    /**
     * Get the starting LSN for WAL streaming after snapshot.
     *
     * <p>Reads the replication slot's confirmed_flush position as the starting point. This avoids
     * replaying WAL changes that occurred before the slot was created (which are already captured
     * by the initial snapshot query).
     *
     * <p>For GaussDB standby nodes, pg_current_wal_lsn() and related WAL control functions are not
     * available (standby cannot generate WAL). The slot's confirmed_flush is the safest reliable
     * position.
     */
    private String getCurrentWalLsn() throws SQLException {
        // Always prefer the current WAL position over the slot's
        // confirmed_flush. confirmed_flush lags behind (it reflects the
        // last position that was acknowledged by a previous CDC run), so
        // using it would cause WAL to replay data that was written after
        // the previous run ended, which the snapshot already captured.
        String slotToCheck = slotName;

        // Try pg_current_wal_lsn() / pg_current_xlog_location() first
        // (different GaussDB versions use different function names).
        // Fall back to confirmed_flush only on standby nodes where WAL
        // control functions are unavailable.
        String[] walFuncs = {"SELECT pg_current_wal_lsn()", "SELECT pg_current_xlog_location()"};
        for (String walFunc : walFuncs) {
            try {
                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery(walFunc)) {
                    if (rs.next()) {
                        String lsn = rs.getString(1);
                        if (lsn != null && !lsn.isEmpty()) {
                            LOG.info("Using {} as WAL start LSN: {}", walFunc, lsn);
                            return lsn;
                        }
                    }
                }
            } catch (SQLException e) {
                LOG.debug("{} failed: {}", walFunc, e.getMessage());
            }
        }

        // Fallback: standby nodes cannot execute WAL control functions.
        // Read the slot's confirmed_flush as the next best position.
        String sql = "SELECT confirmed_flush FROM pg_replication_slots WHERE slot_name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotToCheck);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String lsn = rs.getString("confirmed_flush");
                    if (lsn != null && !lsn.isEmpty()) {
                        LOG.info(
                                "WAL functions unavailable, using slot confirmed_flush: {} (slot={})",
                                lsn,
                                slotToCheck);
                        return lsn;
                    }
                }
            }
        }
        LOG.warn("Could not determine WAL start position, WAL stream will start from 0/0");
        return null;
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (walReplicationStream != null) {
            walReplicationStream.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("Closed GaussDB connection");
        }
    }
}
