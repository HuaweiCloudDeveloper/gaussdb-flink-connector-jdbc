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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.gaussdbcdc.GaussDBCDCOptions;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A CDC SourceFunction implementation for GaussDB that captures change events from GaussDB WAL.
 *
 * <p>This source uses the legacy {@code SourceFunction} API (stable since Flink 1.0), making it
 * compatible with Flink 1.13 through 1.17.
 *
 * <p>The source works in two phases:
 *
 * <ol>
 *   <li><b>Snapshot phase</b>: Reads the initial table data via JDBC SELECT
 *   <li><b>Streaming phase</b>: Continuously captures changes via WAL logical decoding
 *       (mppdb_decoding)
 * </ol>
 *
 * <p>Usage example:
 *
 * <pre>
 * GaussDBCDCSourceFunction source = GaussDBCDCSourceFunction.builder()
 *     .hostname("localhost")
 *     .port(8000)
 *     .database("test")
 *     .tableName("student")
 *     .username("root")
 *     .password("password")
 *     .walMode(true)
 *     .build();
 * </pre>
 */
@PublicEvolving
public class GaussDBCDCSourceFunction extends RichParallelSourceFunction<RowData>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GaussDBCDCSourceFunction.class);

    /**
     * TIMESTAMP formatter accepting 0~9 digits of fractional seconds.
     *
     * <p>GaussDB strips trailing zeros from TIMESTAMP fractional parts (e.g. {@code 2026-05-11
     * 14:52:53.4848} has only 4 digits, {@code .48497} has 5 digits). A strict {@code SSSSSS}
     * pattern (exactly 6 digits) would reject them, causing silent row drops in the WAL path. Use
     * an optional variable-width fraction to accept any precision.
     */
    private static final java.time.format.DateTimeFormatter TIMESTAMP_FORMATTER =
            new java.time.format.DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter();

    // Configuration
    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final String slotName;
    private final boolean snapshotMode;
    private final int chunkSize;
    private final int connectTimeoutMs;
    private final int pollIntervalMs;
    private final boolean walMode;
    private final String decodePlugin;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final String sslMode;
    /**
     * Optional dedicated port for WAL replication streaming. Null means reuse {@link #port} for
     * replication. See {@link GaussDBCDCOptions#REPLICATION_PORT}.
     */
    private final Integer replicationPort;
    /** Output format: "raw" (RowData, default) or "json" (single STRING column with JSON). */
    private final String outputFormat;
    /** JSON sub-format when output.format=json: "debezium" (default), "canal", or "he". */
    private final String outputJsonFormat;

    /** Compiled regex pattern for table-name matching. Null means exact match on tableName. */
    private transient Pattern tablePattern;

    // Runtime state
    private transient volatile boolean running = true;
    private transient Connection connection;
    private transient String identifierQuoteString;
    private transient WalReplicationStream walReplicationStream;
    private transient ChangeDataPoller changeDataPoller;
    private transient boolean walStreamInitialized = false;

    // Cached column metadata for WAL change conversion — supports multi-table via Map
    private transient Map<String, List<String>> cachedColumnsByTable;
    private transient Map<String, String> cachedPkByTable;

    // LSN recorded after snapshot to avoid WAL replay of already-snapshot data
    private transient String snapshotStartLsn = null;
    // Last consumed WAL LSN for duplicate filtering in SQL fallback mode
    private transient String lastConsumedLsn = null;
    // LSN restored from Flink operator state. A restored source must resume from this position
    // instead of taking a new snapshot and starting from the slot's current position.
    private transient String restoredLsn = null;
    private transient int walEmitCount = 0;

    // Checkpoint state
    private transient ListState<String> offsetState;
    private transient ListState<byte[]> pollingState;
    private transient byte[] restoredPollingState;
    private transient NavigableMap<Long, String> pendingCheckpointLsns;
    private transient String lastAcknowledgedLsn;

    // Database-backed snapshot coordination. Each source subtask owns a distinct advisory lock;
    // subtask 0 uses an additional gate lock to form start/end barriers without external storage.
    private transient int snapshotCoordinationKey;
    private transient boolean snapshotCoordinationActive;
    private transient boolean snapshotTransactionActive;
    private transient long snapshotCsn = -1L;
    private transient Map<Long, PendingWalTransaction> pendingWalTransactions;
    private transient Deque<PendingWalTransaction> pendingWalTransactionOrder;
    private transient String lastCommittedWalLsn;

    private static final class PendingWalTransaction {
        private final long xid;
        private final List<WalChange> changes = new ArrayList<>();
        private boolean committed;
        private String commitLsn;
        private long commitCsn;

        private PendingWalTransaction(long xid) {
            this.xid = xid;
        }
    }

    public GaussDBCDCSourceFunction(Builder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.database = builder.database;
        this.schema = builder.schema;
        this.tableName = builder.tableName;
        this.username = builder.username;
        this.password = builder.password;
        this.slotName = builder.slotName;
        this.snapshotMode = builder.snapshotMode;
        this.chunkSize = builder.chunkSize;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.walMode = builder.walMode;
        this.decodePlugin = builder.decodePlugin;
        this.parallelDecodeNum = builder.parallelDecodeNum;
        this.decodeStyle = builder.decodeStyle;
        this.sendingBatch = builder.sendingBatch;
        this.sslMode = builder.sslMode;
        this.replicationPort = builder.replicationPort;
        this.outputFormat = builder.outputFormat;
        this.outputJsonFormat = builder.outputJsonFormat;
        validateConfiguration();
    }

    private void validateConfiguration() {
        if (!"raw".equalsIgnoreCase(outputFormat) && !"json".equalsIgnoreCase(outputFormat)) {
            throw new IllegalArgumentException(
                    "Unsupported output.format: "
                            + outputFormat
                            + ". Supported values are raw and json.");
        }
        if ("json".equalsIgnoreCase(outputFormat)
                && !"debezium".equalsIgnoreCase(outputJsonFormat)
                && !"canal".equalsIgnoreCase(outputJsonFormat)
                && !"he".equalsIgnoreCase(outputJsonFormat)) {
            throw new IllegalArgumentException(
                    "Unsupported output.json.format: "
                            + outputJsonFormat
                            + ". Supported values are debezium, canal, and he.");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunk.size must be greater than zero");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connect.timeout.ms must be greater than zero");
        }
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("poll.interval.ms must be greater than zero");
        }
        if (parallelDecodeNum < 1 || parallelDecodeNum > 20) {
            throw new IllegalArgumentException("parallel-decode-num must be between 1 and 20");
        }
        if (!"b".equalsIgnoreCase(decodeStyle)
                && !"j".equalsIgnoreCase(decodeStyle)
                && !"t".equalsIgnoreCase(decodeStyle)) {
            throw new IllegalArgumentException("decode-style must be one of b, j, or t");
        }
    }

    private String buildJdbcUrl() {
        long timeoutSeconds = Math.max(1L, (connectTimeoutMs + 999L) / 1000L);
        return String.format(
                "jdbc:gaussdb://%s:%d/%s?sslmode=%s&connectTimeout=%d",
                hostname, port, database, sslMode, timeoutSeconds);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.running = true; // transient field reset on deserialization, must re-initialize
        Class.forName("com.huawei.gaussdb.jdbc.Driver");

        String url = buildJdbcUrl();
        this.connection = DriverManager.getConnection(url, username, password);
        this.identifierQuoteString = SqlIdentifierUtils.resolveQuoteString(connection);
        this.pendingCheckpointLsns = new TreeMap<>();
        this.lastAcknowledgedLsn = null;
        this.snapshotCoordinationKey =
                java.util.Objects.hash(database, schema, slotName, tableName);
        this.snapshotCoordinationActive = false;
        this.snapshotTransactionActive = false;
        this.pendingWalTransactions = new HashMap<>();
        this.pendingWalTransactionOrder = new ArrayDeque<>();
        this.lastCommittedWalLsn = null;

        // Initialize multi-table structures
        this.cachedColumnsByTable = new HashMap<>();
        this.cachedPkByTable = new HashMap<>();

        // Compile regex pattern if table-name contains regex metacharacters
        if (tableName != null && tableName.matches(".*[.*+?^$\\[\\]()|\\\\].*")) {
            tablePattern = Pattern.compile(tableName, Pattern.CASE_INSENSITIVE);
            LOG.info(
                    "Table-name '{}' compiled as regex pattern for multi-table capture", tableName);
        }

        if (walMode) {
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
                            chunkSize,
                            replicationPort);
            if (replicationPort != null) {
                LOG.info(
                        "Using dedicated replication port {} for GaussDB WAL streaming (main JDBC port = {})",
                        replicationPort,
                        port);
            }
            LOG.info(
                    "Using WAL mode with plugin={}, parallel-decode-num={}, decode-style={}, output.format={}",
                    decodePlugin,
                    parallelDecodeNum,
                    decodeStyle,
                    outputFormat);
        } else {
            // Polling mode: discover matching tables and create poller for each
            List<String> matchedTables = discoverMatchingTables();
            if (matchedTables.isEmpty()) {
                throw new SQLException(
                        "No tables found matching pattern '" + tableName + "' in schema " + schema);
            }
            LOG.info("Discovered {} matching table(s): {}", matchedTables.size(), matchedTables);
            this.changeDataPoller =
                    new ChangeDataPoller(
                            connection,
                            database,
                            schema,
                            matchedTables,
                            outputFormat,
                            outputJsonFormat,
                            chunkSize);
            if (restoredPollingState != null) {
                changeDataPoller.restoreState(restoredPollingState);
                LOG.info("Restored polling CDC state from the latest completed checkpoint");
            }
            LOG.info("Using polling-based CDC mode for {} table(s)", matchedTables.size());
        }

        // Cache column names for WAL change conversion (needed because DELETE/UPDATE
        // may only send a subset of columns, but Flink expects full-row arity).
        // In multi-table mode, cache columns for each discovered table.
        if (walMode) {
            try {
                List<String> tablesToCache = discoverMatchingTables();
                for (String tbl : tablesToCache) {
                    cacheTableColumns(tbl);
                    // Pre-cache PK for each table
                    getPrimaryKeyColumn(tbl);
                }
                LOG.info("Cached column metadata for {} table(s)", tablesToCache.size());
            } catch (Exception e) {
                LOG.warn("Failed to cache column metadata for some tables: {}", e.getMessage());
            }
        }

        LOG.info(
                "GaussDB CDC SourceFunction opened: {}:{}/{}.{} (output.format={})",
                hostname,
                port,
                database,
                schema,
                tableName,
                outputFormat);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        if (!running) {
            return;
        }
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int sourceParallelism = getRuntimeContext().getNumberOfParallelSubtasks();

        // Polling mode has one in-memory state store and cannot safely run one poller per subtask.
        // Keep it single-active even when the surrounding job has a higher parallelism.
        if (!walMode && subtaskIndex != 0) {
            LOG.info(
                    "Polling source subtask {}/{} is idle; polling is handled by subtask 0",
                    subtaskIndex,
                    sourceParallelism);
            return;
        }

        // Phase 1: Snapshot - read initial table data
        boolean restoredSourceState = walMode ? restoredLsn != null : restoredPollingState != null;
        if (snapshotMode && !restoredSourceState) {
            List<String> tables = discoverMatchingTables();
            boolean coordinatedSnapshot = walMode;
            try {
                if (coordinatedSnapshot) {
                    beginCoordinatedSnapshot(tables);
                }
                LOG.info(
                        "Starting snapshot phase for {} table(s) matching '{}': {}",
                        tables.size(),
                        tableName,
                        tables);
                for (String tbl : tables) {
                    readSnapshot(ctx, tbl);
                    LOG.info("Snapshot completed for table {}.{}", schema, tbl);
                }
                if (coordinatedSnapshot) {
                    completeCoordinatedSnapshot();
                }
                LOG.info("Snapshot phase completed for all {} table(s)", tables.size());
            } catch (Exception e) {
                if (coordinatedSnapshot) {
                    abortCoordinatedSnapshot();
                }
                throw e;
            }
        } else if (snapshotMode) {
            if (walMode) {
                LOG.info(
                        "Skipping snapshot after checkpoint restore; WAL will resume from LSN {}",
                        restoredLsn);
            } else {
                LOG.info("Skipping snapshot after restoring polling CDC state");
            }
        }

        // Phase 2: Streaming - continuously capture changes
        // Only subtask-0 reads WAL changes; other subtasks finish after snapshot
        if (walMode) {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                runWalStreaming(ctx);
            } else {
                LOG.info(
                        "Subtask {} finished snapshot; remaining active for checkpoints while WAL streaming is handled by subtask 0",
                        getRuntimeContext().getIndexOfThisSubtask());
                // A bounded/finished source subtask makes subsequent streaming checkpoints fail
                // with "task is closing". Keep snapshot-only subtasks alive so every parallel
                // operator instance continues to acknowledge checkpoint barriers.
                while (running) {
                    try {
                        Thread.sleep(Math.max(100L, pollIntervalMs));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } else {
            runPollingStreaming(ctx);
        }
    }

    /** Phase 1: Read initial snapshot via JDBC SELECT with parallel split support. */
    private void readSnapshot(SourceContext<RowData> ctx, String tbl) throws SQLException {
        boolean activePollingSource = !walMode && changeDataPoller != null;
        int subtaskIndex = activePollingSource ? 0 : getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks =
                activePollingSource ? 1 : getRuntimeContext().getNumberOfParallelSubtasks();
        String columns = getTableColumns(tbl);
        String pkColumn = getPrimaryKeyColumn(tbl);
        String quotedTable = qualifiedTable(tbl);
        String quotedPkColumn = quoteIdentifier(pkColumn);

        boolean rangeSplittable = numSubtasks > 1 && isNumericPrimaryKey(tbl, pkColumn);
        if (numSubtasks > 1 && !rangeSplittable && subtaskIndex != 0) {
            LOG.info(
                    "Snapshot for {}.{} has a non-numeric primary key; subtask {} is idle while subtask 0 reads the table",
                    schema,
                    tbl,
                    subtaskIndex);
            return;
        }

        if (rangeSplittable) {
            // Parallel snapshot: each subtask reads a different id range
            long[] range = getIdRange(tbl, pkColumn);
            long minId = range[0];
            long maxId = range[1];
            long totalRange = maxId - minId + 1;
            long rangeSize = totalRange / numSubtasks;

            long startId = minId + (long) subtaskIndex * rangeSize;
            long endId;
            if (subtaskIndex == numSubtasks - 1) {
                endId = maxId;
            } else {
                endId = minId + (long) (subtaskIndex + 1) * rangeSize - 1;
            }

            LOG.info(
                    "Parallel snapshot: subtask {}/{}, id range [{}, {}] for table {}.{}",
                    subtaskIndex,
                    numSubtasks,
                    startId,
                    endId,
                    schema,
                    tbl);

            String sql =
                    String.format(
                            "SELECT %s FROM %s WHERE %s >= ? AND %s <= ? ORDER BY %s",
                            columns, quotedTable, quotedPkColumn, quotedPkColumn, quotedPkColumn);

            int count = 0;
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setFetchSize(chunkSize);
                stmt.setLong(1, startId);
                stmt.setLong(2, endId);
                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    while (rs.next()) {
                        RowData row = convertSnapshotRow(rs, meta, tbl);
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(row);
                        }
                        count++;
                    }
                }
            }
            LOG.info(
                    "Parallel snapshot subtask {} completed: {} rows from {}.{}",
                    subtaskIndex,
                    count,
                    schema,
                    tbl);
        } else {
            // Single subtask: read all data
            String sql =
                    String.format(
                            "SELECT %s FROM %s ORDER BY %s", columns, quotedTable, quotedPkColumn);

            int count = 0;
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setFetchSize(chunkSize);
                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    while (rs.next()) {
                        RowData row = convertSnapshotRow(rs, meta, tbl);
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(row);
                        }
                        count++;
                    }
                }
            }
            LOG.info("Snapshot read {} rows from {}.{}", count, schema, tbl);
        }
    }

    private boolean isNumericPrimaryKey(String tbl, String pkColumn) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(null, schema, tbl, null)) {
            while (rs.next()) {
                if (!pkColumn.equalsIgnoreCase(rs.getString("COLUMN_NAME"))) {
                    continue;
                }
                int type = rs.getInt("DATA_TYPE");
                switch (type) {
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.BIGINT:
                        return true;
                    default:
                        return false;
                }
            }
        }
        return false;
    }

    /** Coordinate a non-blocking parallel snapshot using one exported GaussDB MVCC snapshot. */
    private void beginCoordinatedSnapshot(List<String> tables) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        final int gateLock = -1;

        if (subtask == 0) {
            waitAndAcquireAdvisoryLock(gateLock);
            snapshotStartLsn = walReplicationStream.prepareSlotForSnapshot();
            SnapshotBoundary boundary = exportSnapshotBoundary();
            snapshotCsn = boundary.csn;
            publishSnapshotBoundary(boundary);
            waitAndAcquireAdvisoryLock(subtask);
            snapshotCoordinationActive = true;
            waitForAllSnapshotSubtasks(parallelism);
            LOG.info(
                    "Parallel snapshot barrier ready: parallelism={}, tables={}, startLsn={}, snapshotCsn={}",
                    parallelism,
                    tables.size(),
                    snapshotStartLsn,
                    snapshotCsn);
            releaseAdvisoryLock(gateLock);
        } else {
            waitForAdvisoryLockHeld(0);
            SnapshotBoundary boundary = waitForPublishedSnapshotBoundary();
            importSnapshotBoundary(boundary.snapshotId);
            waitAndAcquireAdvisoryLock(subtask);
            snapshotCoordinationActive = true;
            waitForAdvisoryLockReleased(gateLock);
        }
    }

    private void completeCoordinatedSnapshot() throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

        if (subtask == 0) {
            // Keep the exporting transaction open until every reader has finished its imported
            // snapshot. Business DML is never locked and continues using newer MVCC versions.
            for (int i = 1; i < parallelism; i++) {
                waitForAdvisoryLockReleased(i);
            }
            commitSnapshotTransaction();
            releaseAdvisoryLock(0);
        } else {
            commitSnapshotTransaction();
            releaseAdvisoryLock(subtask);
        }
        snapshotCoordinationActive = false;
    }

    private void abortCoordinatedSnapshot() {
        try {
            if (snapshotTransactionActive) {
                connection.rollback();
                snapshotTransactionActive = false;
            }
            if (connection != null && !connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            clearPublishedSnapshotBoundary();
        } catch (Exception e) {
            LOG.warn("Failed to roll back coordinated snapshot transaction: {}", e.getMessage());
        }
        if (snapshotCoordinationActive) {
            try {
                releaseAdvisoryLock(getRuntimeContext().getIndexOfThisSubtask());
                if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    releaseAdvisoryLock(-1);
                }
            } catch (Exception e) {
                LOG.warn("Failed to release snapshot advisory locks: {}", e.getMessage());
            }
        }
        snapshotCoordinationActive = false;
    }

    private SnapshotBoundary exportSnapshotBoundary() throws SQLException {
        boolean combinedExportAvailable = isSnapshotFunctionAvailable("pg_export_snapshot_and_csn");
        if (combinedExportAvailable) {
            try {
                return exportSnapshotAndCsnTogether();
            } catch (SQLException combinedFailure) {
                rollbackSnapshotExportAttempt(combinedFailure);
                LOG.warn(
                        "pg_export_snapshot_and_csn() failed; falling back to standard snapshot export: {}",
                        combinedFailure.getMessage());
            }
        }

        boolean currentCsnAvailable = isSnapshotFunctionAvailable("pg_current_csn");
        beginSnapshotExportTransaction();
        long csn = -1L;
        if (currentCsnAvailable) {
            try {
                // Pin the REPEATABLE READ transaction snapshot before exporting it. Reading a
                // moving current CSN after pg_export_snapshot() could include transactions that
                // are not visible in the exported snapshot and cause data loss during filtering.
                csn = queryCurrentCsn();
            } catch (SQLException csnFailure) {
                rollbackSnapshotExportAttempt(csnFailure);
                LOG.warn(
                        "pg_current_csn() failed; exporting a snapshot without CSN overlap filtering: {}",
                        csnFailure.getMessage());
                beginSnapshotExportTransaction();
            }
        }

        String snapshotId = querySingleText("SELECT pg_catalog.pg_export_snapshot()");
        if (csn < 0) {
            LOG.warn(
                    "GaussDB does not expose an atomic snapshot CSN; snapshot/WAL overlap filtering is disabled. "
                            + "Recovery remains lossless but duplicate events may occur at the initial snapshot boundary.");
        }
        return new SnapshotBoundary(snapshotId, csn);
    }

    private SnapshotBoundary exportSnapshotAndCsnTogether() throws SQLException {
        beginSnapshotExportTransaction();
        try (PreparedStatement export =
                        connection.prepareStatement(
                                "SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()");
                ResultSet rs = export.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("pg_export_snapshot_and_csn() returned no row");
            }
            String snapshotId = rs.getString(1);
            String csnText = rs.getString(2);
            try {
                return new SnapshotBoundary(snapshotId, Long.parseUnsignedLong(csnText, 16));
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid hexadecimal snapshot CSN: " + csnText, e);
            }
        }
    }

    private void beginSnapshotExportTransaction() throws SQLException {
        connection.setAutoCommit(false);
        snapshotTransactionActive = true;
        try (PreparedStatement isolation =
                connection.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
            isolation.execute();
        }
    }

    private boolean isSnapshotFunctionAvailable(String functionName) {
        String sql =
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p "
                        + "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
                        + "WHERE n.nspname = 'pg_catalog' AND p.proname = ? AND p.pronargs = 0)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, functionName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Could not probe GaussDB function {}(); treating it as unavailable: {}",
                    functionName,
                    e.getMessage());
            return false;
        }
    }

    private long queryCurrentCsn() throws SQLException {
        try (PreparedStatement stmt =
                        connection.prepareStatement("SELECT pg_catalog.pg_current_csn()");
                ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException("pg_current_csn() returned no row");
            }
            Object value = rs.getObject(1);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value == null) {
                throw new SQLException("pg_current_csn() returned null");
            }
            String text = value.toString().trim();
            try {
                if (text.startsWith("0x") || text.startsWith("0X")) {
                    return Long.parseUnsignedLong(text.substring(2), 16);
                }
                if (text.matches(".*[A-Fa-f].*")) {
                    return Long.parseUnsignedLong(text, 16);
                }
                return Long.parseUnsignedLong(text, 10);
            } catch (NumberFormatException e) {
                throw new SQLException("Invalid current CSN: " + text, e);
            }
        }
    }

    private String querySingleText(String sql) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new SQLException(sql + " returned no row");
            }
            String value = rs.getString(1);
            if (value == null || value.isEmpty()) {
                throw new SQLException(sql + " returned an empty value");
            }
            return value;
        }
    }

    private void rollbackSnapshotExportAttempt(SQLException originalFailure) throws SQLException {
        try {
            connection.rollback();
            snapshotTransactionActive = false;
            connection.setAutoCommit(true);
        } catch (SQLException rollbackFailure) {
            originalFailure.addSuppressed(rollbackFailure);
            throw originalFailure;
        }
    }

    private void importSnapshotBoundary(String snapshotId) throws SQLException {
        if (snapshotId == null || !snapshotId.matches("[0-9A-Fa-f-]+")) {
            throw new SQLException("Invalid exported snapshot identifier: " + snapshotId);
        }
        connection.setAutoCommit(false);
        snapshotTransactionActive = true;
        try (PreparedStatement isolation =
                        connection.prepareStatement(
                                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                PreparedStatement importSnapshot =
                        connection.prepareStatement(
                                "SET TRANSACTION SNAPSHOT '" + snapshotId + "'")) {
            isolation.execute();
            importSnapshot.execute();
        }
    }

    private void publishSnapshotBoundary(SnapshotBoundary boundary) throws SQLException {
        String applicationName =
                snapshotBoundaryPrefix()
                        + boundary.snapshotId
                        + ":"
                        + Long.toHexString(boundary.csn).toUpperCase(java.util.Locale.ROOT);
        try (PreparedStatement stmt =
                connection.prepareStatement("SELECT set_config('application_name', ?, false)")) {
            stmt.setString(1, applicationName);
            stmt.execute();
        }
    }

    private SnapshotBoundary waitForPublishedSnapshotBoundary() throws Exception {
        String sql =
                "SELECT application_name FROM pg_stat_activity "
                        + "WHERE datname = current_database() AND usename = current_user "
                        + "AND application_name LIKE ? ORDER BY backend_start DESC LIMIT 1";
        while (running) {
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, snapshotBoundaryPrefix() + "%");
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String value = rs.getString(1).substring(snapshotBoundaryPrefix().length());
                        int separator = value.lastIndexOf(':');
                        if (separator > 0) {
                            return new SnapshotBoundary(
                                    value.substring(0, separator),
                                    Long.parseUnsignedLong(value.substring(separator + 1), 16));
                        }
                    }
                }
            }
            Thread.sleep(50L);
        }
        throw new InterruptedException("Source cancelled while waiting for exported snapshot");
    }

    private void clearPublishedSnapshotBoundary() throws SQLException {
        if (connection == null || connection.isClosed()) {
            return;
        }
        try (PreparedStatement stmt =
                connection.prepareStatement("SELECT set_config('application_name', '', false)")) {
            stmt.execute();
        }
    }

    private String snapshotBoundaryPrefix() {
        return "flink_gdbc_" + Integer.toUnsignedString(snapshotCoordinationKey) + ":";
    }

    private void commitSnapshotTransaction() throws SQLException {
        if (snapshotTransactionActive) {
            connection.commit();
            snapshotTransactionActive = false;
        }
        connection.setAutoCommit(true);
    }

    private void waitForAllSnapshotSubtasks(int parallelism) throws Exception {
        for (int i = 1; i < parallelism; i++) {
            waitForAdvisoryLockHeld(i);
        }
    }

    private void waitForAdvisoryLockHeld(int lockId) throws Exception {
        while (running) {
            if (!tryAcquireAdvisoryLock(lockId)) {
                return;
            }
            releaseAdvisoryLock(lockId);
            Thread.sleep(50L);
        }
        throw new InterruptedException("Source cancelled while waiting for snapshot start barrier");
    }

    private void waitForAdvisoryLockReleased(int lockId) throws Exception {
        while (running) {
            if (tryAcquireAdvisoryLock(lockId)) {
                releaseAdvisoryLock(lockId);
                return;
            }
            Thread.sleep(50L);
        }
        throw new InterruptedException(
                "Source cancelled while waiting for snapshot completion barrier");
    }

    private void waitAndAcquireAdvisoryLock(int lockId) throws Exception {
        while (running) {
            if (tryAcquireAdvisoryLock(lockId)) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new InterruptedException(
                "Source cancelled while acquiring snapshot coordinator lock");
    }

    private boolean tryAcquireAdvisoryLock(int lockId) throws SQLException {
        String sql = "SELECT pg_try_advisory_lock(?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, snapshotCoordinationKey);
            stmt.setInt(2, lockId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void releaseAdvisoryLock(int lockId) throws SQLException {
        String sql = "SELECT pg_advisory_unlock(?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, snapshotCoordinationKey);
            stmt.setInt(2, lockId);
            stmt.execute();
        }
    }

    private static final class SnapshotBoundary {
        private final String snapshotId;
        private final long csn;

        private SnapshotBoundary(String snapshotId, long csn) {
            this.snapshotId = snapshotId;
            this.csn = csn;
        }
    }

    /** Get the min and max id of the table for parallel split calculation. */
    private long[] getIdRange(String tbl, String pkCol) throws SQLException {
        String quotedPk = quoteIdentifier(pkCol);
        String sql =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s", quotedPk, quotedPk, qualifiedTable(tbl));
        try (PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                long minId = rs.getLong(1);
                long maxId = rs.getLong(2);
                if (rs.wasNull()) {
                    // Empty table
                    return new long[] {0, -1};
                }
                return new long[] {minId, maxId};
            }
        }
        return new long[] {0, -1};
    }

    /**
     * Dynamically detect the primary key column name for a table via
     * DatabaseMetaData.getPrimaryKeys(). Falls back to "id" for backward compatibility if no
     * primary key is found. Results are cached per-table in {@link #cachedPkByTable}.
     *
     * @param tbl the table name to detect PK for
     * @return the primary key column name
     */
    private String getPrimaryKeyColumn(String tbl) throws SQLException {
        if (cachedPkByTable != null && cachedPkByTable.containsKey(tbl)) {
            return cachedPkByTable.get(tbl);
        }
        String result = detectPrimaryKeyColumn(tbl);
        if (cachedPkByTable != null) {
            cachedPkByTable.put(tbl, result);
        }
        return result;
    }

    /**
     * @deprecated Use {@link #getPrimaryKeyColumn(String)} instead. Kept for backward compatibility
     *     — delegates to {@code getPrimaryKeyColumn(tableName)}.
     */
    private String getPrimaryKeyColumn() throws SQLException {
        return getPrimaryKeyColumn(tableName);
    }

    /** Actual PK detection logic, parameterized by table name. */
    private String detectPrimaryKeyColumn(String tbl) throws SQLException {
        // Try DatabaseMetaData.getPrimaryKeys()
        try {
            if (connection == null) {
                throw new SQLException("Connection is null");
            }
            java.sql.DatabaseMetaData meta = connection.getMetaData();
            if (meta != null) {
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
                            LOG.info(
                                    "Detected primary key column for {}.{}: {}",
                                    schema,
                                    tbl,
                                    pkColumns.get(0));
                            return pkColumns.get(0);
                        }
                        if (pkColumns.size() > 1) {
                            throw unsupportedPrimaryKey(tbl, pkColumns);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            if (isUnsupportedPrimaryKey(e)) {
                throw e;
            }
            LOG.warn("Failed to detect primary key for {}.{}: {}", schema, tbl, e.getMessage());
        }
        // Fallback: try pg_index query
        PreparedStatement stmt = null;
        try {
            stmt =
                    connection.prepareStatement(
                            "SELECT a.attname FROM pg_index i "
                                    + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                                    + "JOIN pg_class c ON c.oid = i.indrelid "
                                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                                    + "WHERE n.nspname = ? AND c.relname = ? AND i.indisprimary "
                                    + "ORDER BY a.attnum");
            if (stmt == null) {
                throw new SQLException("prepareStatement returned null");
            }
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
                    LOG.info(
                            "Detected primary key column (pg_index) for {}.{}: {}",
                            schema,
                            tbl,
                            pkColumns.get(0));
                    return pkColumns.get(0);
                }
                if (pkColumns.size() > 1) {
                    throw unsupportedPrimaryKey(tbl, pkColumns);
                }
            }
        } catch (SQLException e) {
            if (isUnsupportedPrimaryKey(e)) {
                throw e;
            }
            LOG.warn(
                    "pg_index primary key detection failed for {}.{}: {}",
                    schema,
                    tbl,
                    e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                }
            }
        }
        throw new SQLException(
                "GaussDB CDC requires a primary key, but none was found for " + schema + "." + tbl);
    }

    private SQLException unsupportedPrimaryKey(String tbl, List<String> columns) {
        return new SQLException(
                "GaussDB CDC currently requires a single-column primary key for "
                        + schema
                        + "."
                        + tbl
                        + "; found composite key "
                        + columns);
    }

    private boolean isUnsupportedPrimaryKey(SQLException e) {
        return e.getMessage() != null
                && e.getMessage().startsWith("GaussDB CDC currently requires a single-column");
    }

    /** Phase 2a: WAL streaming using WalReplicationStream. */
    private void runWalStreaming(SourceContext<RowData> ctx) throws Exception {
        if (!walStreamInitialized) {
            LOG.info("Initializing WAL replication stream for incremental phase");
            // Start WAL streaming from the snapshot start LSN to avoid
            // replaying data already emitted during the initial snapshot
            String startLsn = restoredLsn != null ? restoredLsn : snapshotStartLsn;
            if (startLsn != null) {
                LOG.info(
                        "Starting WAL stream from LSN: {} (source={})",
                        startLsn,
                        restoredLsn != null ? "checkpoint" : "snapshot");
                walReplicationStream.initialize(startLsn);
                lastConsumedLsn = startLsn;
            } else {
                walReplicationStream.initialize();
            }
            walStreamInitialized = true;
            LOG.info(
                    "WAL stream initialized, starting from LSN: {}",
                    walReplicationStream.getLastLsn());

            // Do not discard the first returned batch here. The slot was established before the
            // snapshot and per-change LSN filtering removes records at or before that boundary. A
            // blind first-batch discard drops legitimate changes committed during the snapshot.
        }

        LOG.info("Entering WAL streaming loop, running={}", running);
        while (running) {
            try {
                List<WalChange> changes = walReplicationStream.readChanges(chunkSize);
                String streamLsn = walReplicationStream.getLastLsn();
                if (!changes.isEmpty()) {
                    LOG.info(
                            "Read {} changes from WAL stream, running={}, lastLsn={}",
                            changes.size(),
                            running,
                            streamLsn);
                }

                List<WalChange> committedChanges = prepareCommittedWalChanges(changes);
                String consumedLsn =
                        walReplicationStream.isUseReplicationApi()
                                ? streamLsn
                                : lastCommittedWalLsn;
                synchronized (ctx.getCheckpointLock()) {
                    for (WalChange change : committedChanges) {

                        // Filter out changes from non-target tables.
                        // WAL logical decoding captures ALL changes in the database,
                        // but we only want changes for tables matching the configured pattern.
                        String changeSchema = change.getSchema();
                        String changeTable = change.getTable();
                        if (changeSchema == null || changeTable == null) {
                            LOG.debug(
                                    "Skipping change with null schema/table: {}.{}",
                                    changeSchema,
                                    changeTable);
                            continue;
                        }
                        if (!isTableMatch(changeSchema, changeTable)) {
                            LOG.debug(
                                    "Skipping change from non-target table: {}.{}",
                                    changeSchema,
                                    changeTable);
                            continue;
                        }

                        // The replication API can replay individual WAL records from its start LSN.
                        // SQL fallback is deduplicated at COMMIT granularity in
                        // prepareCommittedWalChanges(): a transaction's data LSN may legitimately
                        // be
                        // older than lastConsumedLsn when its COMMIT arrives in a later read batch.
                        if (walReplicationStream.isUseReplicationApi()
                                && lastConsumedLsn != null
                                && change.getLsn() != null
                                && !WalReplicationStream.isLsnNewer(
                                        change.getLsn(), lastConsumedLsn)) {
                            LOG.info(
                                    "[DIAG] Filtered dup: change LSN={} <= lastConsumed={}, type={}, table={}",
                                    change.getLsn(),
                                    lastConsumedLsn,
                                    change.getType(),
                                    change.getTable());
                            continue;
                        }
                        WalChange.ChangeType changeType = change.getType();
                        RowData row = null;
                        try {
                            if ("json".equalsIgnoreCase(outputFormat)) {
                                // JSON output mode: single STRING column with all data
                                row = convertWalChangeToJson(change);
                            } else {
                                // Raw output mode: RowData with fixed columns per table
                                if (changeType == WalChange.ChangeType.INSERT) {
                                    row =
                                            convertWalColumnsToRowData(
                                                    change.getAfterColumns(), changeTable);
                                } else if (changeType == WalChange.ChangeType.UPDATE) {
                                    row =
                                            convertWalColumnsToRowData(
                                                    change.getAfterColumns(), changeTable);
                                } else if (changeType == WalChange.ChangeType.DELETE) {
                                    row =
                                            convertWalColumnsToRowData(
                                                    change.getBeforeColumns(), changeTable);
                                    if (row != null) {
                                        row.setRowKind(RowKind.DELETE);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            LOG.warn(
                                    "Failed to convert WAL change on {}.{}: {}",
                                    changeSchema,
                                    changeTable,
                                    e.getMessage());
                            continue;
                        }

                        if (row != null) {
                            ctx.collect(row);
                            // Log first few emissions to correlate with LSN filter
                            if (walEmitCount < 3) {
                                LOG.info(
                                        "[DIAG] Emitted #{}, type={}, LSN={}, row={}",
                                        walEmitCount + 1,
                                        changeType,
                                        change.getLsn(),
                                        row);
                                walEmitCount++;
                            }
                        }
                    }

                    if (consumedLsn != null
                            && (lastConsumedLsn == null
                                    || WalReplicationStream.isLsnNewer(
                                            consumedLsn, lastConsumedLsn))) {
                        // Keep event emission and the corresponding source offset atomic with
                        // Flink checkpoint snapshots. SQL fallback advances only through the
                        // contiguous COMMIT prefix prepared above.
                        lastConsumedLsn = consumedLsn;
                    }
                }

                if (changes.isEmpty()) {
                    Thread.sleep(pollIntervalMs);
                }
            } catch (InterruptedException e) {
                LOG.info("WAL streaming loop interrupted, running={}", running);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error(
                        "Error in WAL streaming loop (db restart?), reconnecting: {}",
                        e.getMessage());
                try {
                    reconnectWalStream();
                    LOG.info("WAL stream reconnected successfully, resuming streaming");
                } catch (Exception reconnectError) {
                    LOG.error(
                            "WAL reconnection failed, task will restart: {}",
                            reconnectError.getMessage());
                    throw reconnectError;
                }
            }
        }
        LOG.info("Exited WAL streaming loop, running={}", running);
    }

    /**
     * Return only transactionally committed SQL-fallback changes.
     *
     * <p>The logical slot is created immediately before exporting the MVCC snapshot. A transaction
     * that commits between those two operations is therefore present in both the snapshot and WAL.
     * GaussDB's exported CSN identifies exactly that overlap: transactions at or below the snapshot
     * CSN are discarded, while later commits are emitted. Buffering until COMMIT also ensures that
     * checkpoint offsets never split a transaction.
     */
    private List<WalChange> prepareCommittedWalChanges(List<WalChange> changes) {
        if (changes.isEmpty() || walReplicationStream.isUseReplicationApi()) {
            return changes;
        }
        if (pendingWalTransactions == null) {
            pendingWalTransactions = new HashMap<>();
        }
        if (pendingWalTransactionOrder == null) {
            pendingWalTransactionOrder = new ArrayDeque<>();
        }

        List<WalChange> committed = new ArrayList<>();
        for (WalChange change : changes) {
            if (change.getType() == WalChange.ChangeType.BEGIN) {
                if (!pendingWalTransactions.containsKey(change.getXid())) {
                    PendingWalTransaction transaction = new PendingWalTransaction(change.getXid());
                    pendingWalTransactions.put(change.getXid(), transaction);
                    pendingWalTransactionOrder.addLast(transaction);
                }
            } else if (change.isDataChange()) {
                PendingWalTransaction transaction = pendingWalTransactions.get(change.getXid());
                if (transaction == null) {
                    // Keep compatibility with decoding formats that omit transaction markers,
                    // while still preserving its order behind any unfinished transaction.
                    transaction = new PendingWalTransaction(change.getXid());
                    transaction.committed = true;
                    transaction.commitLsn = change.getLsn();
                    pendingWalTransactionOrder.addLast(transaction);
                }
                transaction.changes.add(change);
                flushCommittedWalTransactions(committed);
            } else if (change.getType() == WalChange.ChangeType.COMMIT) {
                PendingWalTransaction transaction = pendingWalTransactions.get(change.getXid());
                if (transaction == null) {
                    LOG.debug(
                            "COMMIT without buffered data (empty or replayed transaction): xid={}, lsn={}, pendingXids={}",
                            change.getXid(),
                            change.getLsn(),
                            pendingWalTransactions.keySet());
                    if (pendingWalTransactionOrder.isEmpty()) {
                        advanceLastCommittedWalLsn(change.getLsn());
                    }
                    continue;
                }
                transaction.committed = true;
                transaction.commitLsn = change.getLsn();
                transaction.commitCsn = change.getCsn();
                flushCommittedWalTransactions(committed);
            }
        }
        return committed;
    }

    /**
     * Release only the contiguous committed prefix of the decoded transaction stream.
     *
     * <p>GaussDB parallel logical decoding can return a later transaction's COMMIT while an older
     * transaction is still incomplete. Emitting or checkpointing that later transaction would let
     * recovery skip the older one. Keeping the ordered prefix here makes both event emission and
     * the SQL slot watermark transaction-safe across read batches.
     */
    private void flushCommittedWalTransactions(List<WalChange> output) {
        while (!pendingWalTransactionOrder.isEmpty()) {
            PendingWalTransaction transaction = pendingWalTransactionOrder.peekFirst();
            if (!transaction.committed) {
                return;
            }

            pendingWalTransactionOrder.removeFirst();
            pendingWalTransactions.remove(transaction.xid, transaction);

            boolean replayedTransaction =
                    lastConsumedLsn != null
                            && transaction.commitLsn != null
                            && !WalReplicationStream.isLsnNewer(
                                    transaction.commitLsn, lastConsumedLsn);
            boolean visibleInSnapshot =
                    snapshotCsn >= 0
                            && transaction.commitCsn > 0
                            && transaction.commitCsn <= snapshotCsn;

            if (visibleInSnapshot) {
                LOG.info(
                        "Discarding {} WAL changes already visible in snapshot: xid={}, commitLsn={}, commitCsn={}, snapshotCsn={}",
                        transaction.changes.size(),
                        transaction.xid,
                        transaction.commitLsn,
                        transaction.commitCsn,
                        snapshotCsn);
            } else if (replayedTransaction) {
                LOG.debug(
                        "Discarding replayed WAL transaction: xid={}, commitLsn={}, lastConsumedLsn={}",
                        transaction.xid,
                        transaction.commitLsn,
                        lastConsumedLsn);
            } else {
                output.addAll(transaction.changes);
            }
            advanceLastCommittedWalLsn(transaction.commitLsn);
        }
    }

    private void advanceLastCommittedWalLsn(String candidate) {
        if (candidate != null
                && (lastCommittedWalLsn == null
                        || WalReplicationStream.isLsnNewer(candidate, lastCommittedWalLsn))) {
            lastCommittedWalLsn = candidate;
        }
    }

    /**
     * Reconnect to GaussDB and re-establish the WAL replication stream.
     *
     * <p>Called when the WAL streaming loop encounters a connection error (e.g. database restart).
     * Closes old resources, creates fresh JDBC/WAL connections, and resumes streaming from the last
     * known LSN. This avoids a Flink task restart that would re-trigger the snapshot phase and emit
     * duplicate data.
     */
    private void reconnectWalStream() throws Exception {
        String lastReadLsn =
                walReplicationStream != null ? walReplicationStream.getLastLsn() : null;
        LOG.info(
                "Reconnecting WAL stream, lastReadLsn={}, lastSafeLsn={}, lastAcknowledgedLsn={}",
                lastReadLsn,
                lastConsumedLsn,
                lastAcknowledgedLsn);

        // Close old WAL stream (may be broken)
        if (walReplicationStream != null) {
            try {
                walReplicationStream.close();
            } catch (Exception ignored) {
                // Stream is likely already dead
            }
            walReplicationStream = null;
        }

        // Close old JDBC connection
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception ignored) {
                // Connection is likely already dead
            }
            connection = null;
        }

        // Re-establish JDBC connection
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        String url = buildJdbcUrl();
        connection = DriverManager.getConnection(url, username, password);
        identifierQuoteString = SqlIdentifierUtils.resolveQuoteString(connection);
        LOG.info("Reconnected JDBC to {}:{}/{}", hostname, port, database);

        // A reconnect does not make a fixed Flink RowType capable of schema evolution. Verify the
        // metadata instead of silently retaining stale column/PK positions or clearing the caches
        // and emitting rows with a different arity.
        validateCachedTableMetadataAfterReconnect();

        // Re-create WAL stream
        walReplicationStream =
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
                        chunkSize,
                        replicationPort);

        // Re-read from the database slot's checkpoint-acknowledged position. Advancing to the last
        // record merely read by the broken connection can skip a BEGIN/DELETE whose COMMIT had not
        // arrived yet. Replayed committed transactions are filtered at COMMIT against
        // lastConsumedLsn.
        if (pendingWalTransactions != null) {
            pendingWalTransactions.clear();
        }
        if (pendingWalTransactionOrder != null) {
            pendingWalTransactionOrder.clear();
        }
        walReplicationStream.initialize();

        LOG.info(
                "WAL stream re-initialized, starting from LSN: {}",
                walReplicationStream.getLastLsn());
    }

    /** Phase 2b: Polling-based streaming using ChangeDataPoller. */
    private void runPollingStreaming(SourceContext<RowData> ctx) throws Exception {
        if (changeDataPoller != null && restoredPollingState == null) {
            changeDataPoller.loadSnapshot();
        }

        while (running) {
            List<ChangeEvent<RowData>> events;
            // Poller cursors/snapshots and emitted rows must move atomically relative to a Flink
            // checkpoint. This may delay a checkpoint for the duration of one polling query, but it
            // does not lock source tables or block business DML.
            synchronized (ctx.getCheckpointLock()) {
                events = changeDataPoller.pollAllChanges();
                for (ChangeEvent<RowData> event : events) {
                    RowData row = null;
                    switch (event.getChangeType()) {
                        case INSERT:
                        case UPDATE:
                            row = event.getAfter();
                            break;
                        case DELETE:
                            row = event.getBefore();
                            if (row instanceof GenericRowData) {
                                ((GenericRowData) row).setRowKind(RowKind.DELETE);
                            }
                            LOG.debug("Detected DELETE event");
                            break;
                        case SNAPSHOT:
                            row = event.getAfter();
                            break;
                    }

                    if (row != null) {
                        ctx.collect(row);
                    }
                }
            }

            if (events.isEmpty()) {
                Thread.sleep(pollIntervalMs);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (snapshotCoordinationActive && connection != null) {
            try {
                // Closing the session immediately releases GaussDB session advisory/table locks
                // and unblocks the remaining snapshot subtasks during task cancellation.
                connection.close();
            } catch (SQLException e) {
                LOG.debug("Failed to close snapshot connection during cancellation", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (snapshotCoordinationActive) {
            abortCoordinatedSnapshot();
        }
        if (walReplicationStream != null) {
            walReplicationStream.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("Closed GaussDB connection");
        }
    }

    // ---- CheckpointedFunction ----

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        if (pollingState != null) {
            pollingState.clear();
        }
        if (walReplicationStream != null && walStreamInitialized) {
            String lsn =
                    lastConsumedLsn != null ? lastConsumedLsn : walReplicationStream.getLastLsn();
            if (lsn != null) {
                offsetState.add(lsn);
                if (pendingCheckpointLsns == null) {
                    pendingCheckpointLsns = new TreeMap<>();
                }
                pendingCheckpointLsns.put(context.getCheckpointId(), lsn);
                LOG.debug("Checkpointed LSN: {}", lsn);
            }
        } else if (!walMode
                && changeDataPoller != null
                && pollingState != null
                && getRuntimeContext().getIndexOfThisSubtask() == 0) {
            pollingState.add(changeDataPoller.serializeState());
            LOG.debug("Checkpointed polling CDC state");
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("gaussdb-cdc-offset-state", Types.STRING);
        // Union state makes the single WAL offset available to the new subtask 0 after rescaling.
        offsetState = context.getOperatorStateStore().getUnionListState(descriptor);
        ListStateDescriptor<byte[]> pollingDescriptor =
                new ListStateDescriptor<>(
                        "gaussdb-cdc-polling-state",
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
        // Only polling subtask 0 writes this state. Union distribution guarantees that the new
        // subtask 0 receives it after rescaling.
        pollingState = context.getOperatorStateStore().getUnionListState(pollingDescriptor);
        pendingCheckpointLsns = new TreeMap<>();

        if (context.isRestored()) {
            for (String lsn : offsetState.get()) {
                restoredLsn = lsn;
                LOG.info("Restored LSN from checkpoint: {}", lsn);
                break;
            }
            if (!walMode && pollingState != null) {
                for (byte[] state : pollingState.get()) {
                    restoredPollingState = state;
                    LOG.info("Restored polling CDC state from checkpoint");
                    break;
                }
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (walReplicationStream == null
                || !walStreamInitialized
                || pendingCheckpointLsns == null) {
            return;
        }
        Map.Entry<Long, String> completed = pendingCheckpointLsns.floorEntry(checkpointId);
        if (completed != null) {
            if (!completed.getValue().equals(lastAcknowledgedLsn)) {
                walReplicationStream.acknowledgeLsn(completed.getValue());
                lastAcknowledgedLsn = completed.getValue();
                LOG.info(
                        "Acknowledged WAL LSN {} after Flink checkpoint {} completed",
                        completed.getValue(),
                        checkpointId);
            }
            pendingCheckpointLsns.headMap(checkpointId, true).clear();
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        if (pendingCheckpointLsns != null) {
            pendingCheckpointLsns.remove(checkpointId);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

    // ---- Data conversion helpers ----

    /**
     * Discover all tables in the configured schema that match the table-name pattern. Uses regex
     * matching if table-name contains regex metacharacters, otherwise falls back to exact match.
     *
     * @return list of matching table names (never null, may be empty)
     */
    private List<String> discoverMatchingTables() throws SQLException {
        List<String> result = new ArrayList<>();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tbl = rs.getString("TABLE_NAME");
                if (tbl != null && isTableMatch(schema, tbl)) {
                    result.add(tbl);
                }
            }
        }
        return result;
    }

    /**
     * Check if a table matches the configured table-name pattern.
     *
     * <p>If tablePattern is non-null (regex mode), uses regex matching. Otherwise, does exact
     * case-insensitive match against tableName.
     *
     * @param tableSchema the schema of the change/table (must match configured schema)
     * @param tbl the table name to check
     * @return true if the table matches
     */
    private boolean isTableMatch(String tableSchema, String tbl) {
        // Schema must always match
        if (!tableSchema.equalsIgnoreCase(schema)) {
            return false;
        }
        if (tablePattern != null) {
            return tablePattern.matcher(tbl).matches();
        }
        // Exact match (backward compatible single-table mode)
        return tbl.equalsIgnoreCase(tableName);
    }

    /**
     * Cache column names for a specific table.
     *
     * @param tbl the table name
     */
    private void cacheTableColumns(String tbl) throws SQLException {
        if (cachedColumnsByTable.containsKey(tbl)) {
            return;
        }
        List<String> colNames = loadTableColumnNames(tbl);
        cachedColumnsByTable.put(tbl, colNames);
        LOG.info("Cached {} columns for table {}.{}: {}", colNames.size(), schema, tbl, colNames);
    }

    private List<String> loadTableColumnNames(String tbl) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        List<String> colNames = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, tbl, null)) {
            while (rs.next()) {
                colNames.add(rs.getString("COLUMN_NAME"));
            }
        }
        return colNames;
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

    /**
     * Fail explicitly when a reconnect observes a table definition different from the cached one.
     */
    private void validateCachedTableMetadataAfterReconnect() throws SQLException {
        if (cachedColumnsByTable != null) {
            for (Map.Entry<String, List<String>> entry : cachedColumnsByTable.entrySet()) {
                List<String> freshColumns = loadTableColumnNames(entry.getKey());
                if (!entry.getValue().equals(freshColumns)) {
                    throw schemaEvolutionException(
                            entry.getKey(),
                            "columns changed from " + entry.getValue() + " to " + freshColumns);
                }
            }
        }
        if (cachedPkByTable != null) {
            for (Map.Entry<String, String> entry : cachedPkByTable.entrySet()) {
                String freshPk = detectPrimaryKeyColumn(entry.getKey());
                if (!entry.getValue().equals(freshPk)) {
                    throw schemaEvolutionException(
                            entry.getKey(),
                            "primary key changed from " + entry.getValue() + " to " + freshPk);
                }
            }
        }
    }

    private SQLException schemaEvolutionException(String tbl, String detail) {
        return new SQLException(
                "Unsupported schema change detected for "
                        + qualifiedTableForMessage(tbl)
                        + " after WAL reconnect: "
                        + detail
                        + ". Cancel the job, update the Flink table schema, and restart it.");
    }

    private String qualifiedTableForMessage(String tbl) {
        try {
            return qualifiedTable(tbl);
        } catch (SQLException ignored) {
            return schema + "." + tbl;
        }
    }

    private String getTableColumns(String tbl) throws SQLException {
        List<String> cols = cachedColumnsByTable.get(tbl);
        if (cols == null) {
            cacheTableColumns(tbl);
            cols = cachedColumnsByTable.get(tbl);
        }
        if (cols == null || cols.isEmpty()) {
            throw new SQLException("No columns found for table " + schema + "." + tbl);
        }
        return quoteColumnList(cols);
    }

    /**
     * Convert WAL column values to RowData using cached column metadata for the specific table.
     *
     * @param columns the WAL column values
     * @param tbl the table name (for looking up cached column metadata)
     * @return RowData with full row arity
     */
    private RowData convertWalColumnsToRowData(List<WalChange.ColumnValue> columns, String tbl) {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        List<String> cachedCols = cachedColumnsByTable.get(tbl);
        int colCount = (cachedCols != null) ? cachedCols.size() : columns.size();
        GenericRowData row = new GenericRowData(colCount);

        if (cachedCols != null && !cachedCols.isEmpty()) {
            // Map WAL columns by name to the correct positions
            java.util.Map<String, WalChange.ColumnValue> colMap = new java.util.HashMap<>();
            for (WalChange.ColumnValue col : columns) {
                colMap.put(col.getColumnName(), col);
            }
            for (int i = 0; i < cachedCols.size(); i++) {
                String colName = cachedCols.get(i);
                WalChange.ColumnValue col = colMap.get(colName);
                if (col == null || col.isNull()) {
                    row.setField(i, null);
                } else {
                    Object value = convertColumnValueByOid(col.getTypeOid(), col.getValue());
                    row.setField(i, value);
                }
            }
        } else {
            // Fallback: assume WAL columns are in table order.
            // Try to cache columns for this table on the fly.
            try {
                cacheTableColumns(tbl);
                List<String> freshCols = cachedColumnsByTable.get(tbl);
                if (freshCols != null && !freshCols.isEmpty()) {
                    return convertWalColumnsToRowData(columns, tbl);
                }
            } catch (SQLException e) {
                LOG.warn("Failed to cache columns for table {}.{} on the fly", schema, tbl, e);
            }
            for (int i = 0; i < columns.size(); i++) {
                WalChange.ColumnValue col = columns.get(i);
                if (col.isNull()) {
                    row.setField(i, null);
                    continue;
                }
                Object value = convertColumnValueByOid(col.getTypeOid(), col.getValue());
                row.setField(i, value);
            }
        }
        return row;
    }

    /**
     * Convert a WalChange to a single-column RowData containing a JSON string. Used in
     * output.format=json mode for heterogeneous multi-table capture.
     *
     * <p>Custom JSON format selected by {@code output.json.format=he}:
     *
     * <pre>
     * {
     *   "database": "event_driven",
     *   "table": "sample_order_oper",
     *   "optType": "INSERT",
     *   "pkNames": ["id"],
     *   "pkValues": "722253",
     *   "es": 1783489586878,
     *   "ts": 1783489587143,
     *   "data": {"id": "722253", "name": "Alice", ...},
     *   "old": null
     * }
     * </pre>
     *
     * @param change the WAL change event
     * @return RowData with a single StringData field containing the JSON string
     */
    private RowData convertWalChangeToJson(WalChange change) {
        String json;
        if ("canal".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCanalJson(change);
        } else if ("he".equalsIgnoreCase(outputJsonFormat)) {
            json = buildCustomJson(change);
        } else {
            json = buildDebeziumJson(change);
        }
        GenericRowData row = new GenericRowData(1);
        row.setField(0, StringData.fromString(json));
        if (change.getType() == WalChange.ChangeType.DELETE) {
            row.setRowKind(RowKind.DELETE);
        }
        return row;
    }

    /** Build Debezium standard JSON format. */
    private String buildDebeziumJson(WalChange change) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        // before
        sb.append("\"before\":");
        appendColumnsAsJson(sb, change.getBeforeColumns());
        // after
        sb.append(",\"after\":");
        appendColumnsAsJson(sb, change.getAfterColumns());
        // source
        sb.append(",\"source\":{");
        sb.append("\"connector\":\"gaussdb\"");
        sb.append(",\"db\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"schema\":\"").append(escapeJson(change.getSchema())).append('"');
        sb.append(",\"table\":\"").append(escapeJson(change.getTable())).append('"');
        if (change.getLsn() != null) {
            sb.append(",\"lsn\":\"").append(escapeJson(change.getLsn())).append('"');
        }
        long now = System.currentTimeMillis();
        sb.append(",\"ts_ms\":").append(now);
        sb.append(",\"snapshot\":false");
        sb.append('}');
        // op
        String op;
        switch (change.getType()) {
            case INSERT:
                op = "c";
                break;
            case UPDATE:
                op = "u";
                break;
            case DELETE:
                op = "d";
                break;
            default:
                op = "u";
        }
        sb.append(",\"op\":\"").append(op).append('"');
        // ts_ms
        sb.append(",\"ts_ms\":").append(now);
        sb.append(",\"transaction\":null");
        sb.append('}');
        return sb.toString();
    }

    /** Build Canal standard JSON format. */
    private String buildCanalJson(WalChange change) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        // data (after columns for INSERT/UPDATE, null for DELETE)
        if (change.getType() == WalChange.ChangeType.DELETE) {
            sb.append("\"data\":null");
        } else {
            sb.append("\"data\":");
            appendColumnsAsJson(sb, change.getAfterColumns());
        }
        // old (before columns for UPDATE/DELETE, null for INSERT)
        if (change.getType() == WalChange.ChangeType.INSERT) {
            sb.append(",\"old\":null");
        } else {
            List<WalChange.ColumnValue> before = change.getBeforeColumns();
            if (before == null || before.isEmpty()) {
                sb.append(",\"old\":null");
            } else {
                sb.append(",\"old\":");
                appendColumnsAsJson(sb, before);
            }
        }
        // database
        sb.append(",\"database\":\"").append(escapeJson(database)).append('"');
        // table
        sb.append(",\"table\":\"").append(escapeJson(change.getTable())).append('"');
        // type
        sb.append(",\"type\":\"").append(change.getType().name()).append('"');
        // pkNames
        String pkCol = null;
        if (cachedPkByTable != null) {
            pkCol = cachedPkByTable.get(change.getTable());
        }
        if (pkCol != null) {
            sb.append(",\"pkNames\":[\"").append(escapeJson(pkCol)).append("\"]");
        } else {
            sb.append(",\"pkNames\":[]");
        }
        // es
        long now = System.currentTimeMillis();
        sb.append(",\"es\":").append(now);
        // ts
        sb.append(",\"ts\":").append(now);
        // isDdl
        sb.append(",\"isDdl\":false");
        // sqlType (empty for now)
        sb.append(",\"sqlType\":{}");
        // mysqlType (empty for now)
        sb.append(",\"mysqlType\":{}");
        sb.append('}');
        return sb.toString();
    }

    /** Build the custom Canal-compatible JSON format. */
    private String buildCustomJson(WalChange change) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        // database
        sb.append("\"database\":\"").append(escapeJson(database)).append('"');
        // table
        sb.append(",\"table\":\"").append(escapeJson(change.getTable())).append('"');
        // Custom envelope uses optType instead of Canal's type field.
        sb.append(",\"optType\":\"").append(change.getType().name()).append('"');
        // pkNames + pkValues
        String tbl = change.getTable();
        String pkCol = null;
        if (cachedPkByTable != null) {
            pkCol = cachedPkByTable.get(tbl);
        }
        if (pkCol != null) {
            sb.append(",\"pkNames\":[\"").append(escapeJson(pkCol)).append("\"]");
            String pkVal = extractPkValue(change, pkCol);
            if (pkVal != null) {
                sb.append(",\"pkValues\":\"").append(escapeJson(pkVal)).append('"');
            } else {
                sb.append(",\"pkValues\":null");
            }
        } else {
            sb.append(",\"pkNames\":[]");
            sb.append(",\"pkValues\":null");
        }
        // es
        long now = System.currentTimeMillis();
        sb.append(",\"es\":").append(now);
        // ts
        sb.append(",\"ts\":").append(now);
        // data (after columns for INSERT/UPDATE, null for DELETE)
        if (change.getType() == WalChange.ChangeType.DELETE) {
            sb.append(",\"data\":null");
        } else {
            sb.append(",\"data\":");
            appendColumnsAsJson(sb, change.getAfterColumns());
        }
        // old (before columns for UPDATE/DELETE, null for INSERT)
        if (change.getType() == WalChange.ChangeType.INSERT) {
            sb.append(",\"old\":null");
        } else {
            List<WalChange.ColumnValue> before = change.getBeforeColumns();
            if (before == null || before.isEmpty()) {
                sb.append(",\"old\":null");
            } else {
                sb.append(",\"old\":");
                appendColumnsAsJson(sb, before);
            }
        }
        // lsn (diagnostic)
        if (change.getLsn() != null) {
            sb.append(",\"lsn\":\"").append(escapeJson(change.getLsn())).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    /** Extract the primary key value from a WalChange. */
    private String extractPkValue(WalChange change, String pkCol) {
        // For INSERT/UPDATE, look in after columns
        List<WalChange.ColumnValue> cols = change.getAfterColumns();
        if (change.getType() == WalChange.ChangeType.DELETE) {
            cols = change.getBeforeColumns();
        }
        if (cols == null) {
            return null;
        }
        for (WalChange.ColumnValue col : cols) {
            if (pkCol.equalsIgnoreCase(col.getColumnName()) && !col.isNull()) {
                return col.getValue();
            }
        }
        return null;
    }

    /** Append a list of column values as a JSON object. */
    private void appendColumnsAsJson(StringBuilder sb, List<WalChange.ColumnValue> columns) {
        if (columns == null || columns.isEmpty()) {
            sb.append("{}");
            return;
        }
        sb.append('{');
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            WalChange.ColumnValue col = columns.get(i);
            sb.append('"').append(escapeJson(col.getColumnName())).append("\":");
            if (col.isNull()) {
                sb.append("null");
            } else {
                sb.append('"').append(escapeJson(col.getValue())).append('"');
            }
        }
        sb.append('}');
    }

    /** Escape a string for safe inclusion in a JSON string value. */
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

    /**
     * Convert a snapshot ResultSet row to RowData, respecting output.format.
     *
     * <p>In raw mode: uses convertToRowDataDynamic (fixed columns). In json mode: wraps the row
     * data into a JSON string with table name and op=INSERT.
     */
    private RowData convertSnapshotRow(ResultSet rs, ResultSetMetaData meta, String tbl)
            throws SQLException {
        if ("json".equalsIgnoreCase(outputFormat)) {
            String json;
            if ("canal".equalsIgnoreCase(outputJsonFormat)) {
                json = buildCanalSnapshotJson(rs, meta, tbl);
            } else if ("he".equalsIgnoreCase(outputJsonFormat)) {
                json = buildCustomSnapshotJson(rs, meta, tbl);
            } else {
                json = buildDebeziumSnapshotJson(rs, meta, tbl);
            }
            GenericRowData row = new GenericRowData(1);
            row.setField(0, StringData.fromString(json));
            return row;
        } else {
            return convertToRowDataDynamic(rs, meta);
        }
    }

    /** Build Debezium JSON for snapshot row (op="c" for create/read). */
    private String buildDebeziumSnapshotJson(ResultSet rs, ResultSetMetaData meta, String tbl)
            throws SQLException {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        sb.append("\"before\":null");
        sb.append(",\"after\":");
        appendResultSetAsJson(sb, rs, meta);
        sb.append(",\"source\":{");
        sb.append("\"connector\":\"gaussdb\"");
        sb.append(",\"db\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"schema\":\"").append(escapeJson(schema)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        long now = System.currentTimeMillis();
        sb.append(",\"ts_ms\":").append(now);
        sb.append(",\"snapshot\":true");
        sb.append('}');
        sb.append(",\"op\":\"c\"");
        sb.append(",\"ts_ms\":").append(now);
        sb.append(",\"transaction\":null");
        sb.append('}');
        return sb.toString();
    }

    /** Build Canal JSON for snapshot row (type=INSERT). */
    private String buildCanalSnapshotJson(ResultSet rs, ResultSetMetaData meta, String tbl)
            throws SQLException {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        sb.append("\"data\":");
        appendResultSetAsJson(sb, rs, meta);
        sb.append(",\"old\":null");
        sb.append(",\"database\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"type\":\"INSERT\"");
        // pkNames
        String pkCol = null;
        if (cachedPkByTable != null) {
            pkCol = cachedPkByTable.get(tbl);
        }
        if (pkCol != null) {
            sb.append(",\"pkNames\":[\"").append(escapeJson(pkCol)).append("\"]");
        } else {
            sb.append(",\"pkNames\":[]");
        }
        long now = System.currentTimeMillis();
        sb.append(",\"es\":").append(now);
        sb.append(",\"ts\":").append(now);
        sb.append(",\"isDdl\":false");
        sb.append(",\"sqlType\":{}");
        sb.append(",\"mysqlType\":{}");
        sb.append('}');
        return sb.toString();
    }

    /** Build custom JSON for a snapshot row (optType=INSERT). */
    private String buildCustomSnapshotJson(ResultSet rs, ResultSetMetaData meta, String tbl)
            throws SQLException {
        String pkCol = null;
        if (cachedPkByTable != null) {
            pkCol = cachedPkByTable.get(tbl);
        }
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        sb.append("\"database\":\"").append(escapeJson(database)).append('"');
        sb.append(",\"table\":\"").append(escapeJson(tbl)).append('"');
        sb.append(",\"optType\":\"INSERT\"");
        if (pkCol != null) {
            sb.append(",\"pkNames\":[\"").append(escapeJson(pkCol)).append("\"]");
            String pkVal = null;
            try {
                pkVal = rs.getString(pkCol);
            } catch (SQLException e) {
                // PK column not in result set
            }
            if (pkVal != null && !rs.wasNull()) {
                sb.append(",\"pkValues\":\"").append(escapeJson(pkVal)).append('"');
            } else {
                sb.append(",\"pkValues\":null");
            }
        } else {
            sb.append(",\"pkNames\":[]");
            sb.append(",\"pkValues\":null");
        }
        sb.append(",\"es\":").append(now);
        sb.append(",\"ts\":").append(now);
        sb.append(",\"data\":");
        appendResultSetAsJson(sb, rs, meta);
        sb.append(",\"old\":null");
        sb.append('}');
        return sb.toString();
    }

    /** Append ResultSet columns as a JSON object. */
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

    private Object convertColumnValueByOid(int typeOid, String value) {
        if (value == null) {
            return null;
        }
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
                        java.time.LocalDateTime.parse(value, TIMESTAMP_FORMATTER));
            case 1082: // date
                return (int) java.time.LocalDate.parse(value).toEpochDay();
            default:
                return StringData.fromString(value);
        }
    }

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
                    java.sql.Timestamp ts = rs.getTimestamp(i);
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

    // ---- Builder ----

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for GaussDBCDCSourceFunction. */
    public static class Builder {
        private String hostname;
        private int port = 8000;
        private String database;
        private String schema = "public";
        private String tableName;
        private String username;
        private String password;
        private String slotName = "flink_cdc_slot";
        private boolean snapshotMode = true;
        private int chunkSize = 1000;
        private int connectTimeoutMs = 30000;
        private int pollIntervalMs = 1000;
        private boolean walMode = false;
        private String decodePlugin = "mppdb_decoding";
        private int parallelDecodeNum = 1;
        private String decodeStyle = "b";
        private boolean sendingBatch = false;
        private String sslMode = GaussDBCDCOptions.SSL_MODE.defaultValue();
        private Integer replicationPort;
        private String outputFormat = "raw";
        private String outputJsonFormat = "debezium";

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        /** @deprecated Use {@link #decodePlugin(String)}. */
        @Deprecated
        public Builder pluginName(String pluginName) {
            this.decodePlugin = pluginName;
            return this;
        }

        public Builder snapshotMode(boolean snapshotMode) {
            this.snapshotMode = snapshotMode;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder pollIntervalMs(int pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        public Builder walMode(boolean walMode) {
            this.walMode = walMode;
            return this;
        }

        public Builder decodePlugin(String decodePlugin) {
            this.decodePlugin = decodePlugin;
            return this;
        }

        public Builder parallelDecodeNum(int parallelDecodeNum) {
            this.parallelDecodeNum = parallelDecodeNum;
            return this;
        }

        public Builder decodeStyle(String decodeStyle) {
            this.decodeStyle = decodeStyle;
            return this;
        }

        public Builder sendingBatch(boolean sendingBatch) {
            this.sendingBatch = sendingBatch;
            return this;
        }

        public Builder sslMode(String sslMode) {
            this.sslMode = sslMode;
            return this;
        }

        public Builder replicationPort(Integer replicationPort) {
            this.replicationPort = replicationPort;
            return this;
        }

        public Builder outputFormat(String outputFormat) {
            this.outputFormat = outputFormat;
            return this;
        }

        public Builder outputJsonFormat(String outputJsonFormat) {
            this.outputJsonFormat = outputJsonFormat;
            return this;
        }

        public GaussDBCDCSourceFunction build() {
            return new GaussDBCDCSourceFunction(this);
        }
    }
}
