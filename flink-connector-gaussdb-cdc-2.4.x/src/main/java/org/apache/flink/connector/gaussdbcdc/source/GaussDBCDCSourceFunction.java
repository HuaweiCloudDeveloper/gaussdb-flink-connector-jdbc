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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalChange;
import org.apache.flink.connector.gaussdbcdc.source.wal.WalReplicationStream;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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
public class GaussDBCDCSourceFunction extends RichSourceFunction<RowData>
        implements CheckpointedFunction, ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GaussDBCDCSourceFunction.class);

    // Configuration
    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final String slotName;
    private final String pluginName;
    private final boolean snapshotMode;
    private final int chunkSize;
    private final int connectTimeoutMs;
    private final int pollIntervalMs;
    private final boolean walMode;
    private final String decodePlugin;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;

    // Runtime state
    private transient volatile boolean running = true;
    private transient Connection connection;
    private transient WalReplicationStream walReplicationStream;
    private transient ChangeDataPoller changeDataPoller;
    private transient boolean walStreamInitialized = false;

    // Checkpoint state
    private transient ListState<String> offsetState;

    public GaussDBCDCSourceFunction(Builder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.database = builder.database;
        this.schema = builder.schema;
        this.tableName = builder.tableName;
        this.username = builder.username;
        this.password = builder.password;
        this.slotName = builder.slotName;
        this.pluginName = builder.pluginName;
        this.snapshotMode = builder.snapshotMode;
        this.chunkSize = builder.chunkSize;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.walMode = builder.walMode;
        this.decodePlugin = builder.decodePlugin;
        this.parallelDecodeNum = builder.parallelDecodeNum;
        this.decodeStyle = builder.decodeStyle;
        this.sendingBatch = builder.sendingBatch;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.huawei.gaussdb.jdbc.Driver");

        String url =
                String.format(
                        "jdbc:gaussdb://%s:%d/%s?compatibleMode=mysql", hostname, port, database);
        this.connection = DriverManager.getConnection(url, username, password);

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
                            1000);
            LOG.info(
                    "Using WAL mode with plugin={}, parallel-decode-num={}, decode-style={}",
                    decodePlugin,
                    parallelDecodeNum,
                    decodeStyle);
        } else {
            this.changeDataPoller = new ChangeDataPoller(connection, schema, tableName, "id");
            LOG.info("Using polling-based CDC mode");
        }

        LOG.info(
                "GaussDB CDC SourceFunction opened: {}:{}/{}.{}",
                hostname,
                port,
                database,
                schema,
                tableName);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        // Phase 1: Snapshot - read initial table data
        if (snapshotMode) {
            LOG.info("Starting snapshot phase for {}.{}", schema, tableName);
            readSnapshot(ctx);
            LOG.info("Snapshot phase completed for {}.{}", schema, tableName);
        }

        // Phase 2: Streaming - continuously capture changes
        // Only subtask-0 reads WAL changes; other subtasks finish after snapshot
        if (walMode) {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                runWalStreaming(ctx);
            } else {
                LOG.info(
                        "Subtask {} finished snapshot, WAL streaming handled by subtask 0",
                        getRuntimeContext().getIndexOfThisSubtask());
            }
        } else {
            runPollingStreaming(ctx);
        }
    }

    /** Phase 1: Read initial snapshot via JDBC SELECT with parallel split support. */
    private void readSnapshot(SourceContext<RowData> ctx) throws SQLException {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        String columns = getTableColumns();

        if (numSubtasks > 1) {
            // Parallel snapshot: each subtask reads a different id range
            long[] range = getIdRange();
            long minId = range[0];
            long maxId = range[1];
            long totalRange = maxId - minId + 1;
            long chunkSize = totalRange / numSubtasks;

            long startId = minId + (long) subtaskIndex * chunkSize;
            long endId;
            if (subtaskIndex == numSubtasks - 1) {
                endId = maxId;
            } else {
                endId = minId + (long) (subtaskIndex + 1) * chunkSize - 1;
            }

            LOG.info(
                    "Parallel snapshot: subtask {}/{}, id range [{}, {}]",
                    subtaskIndex,
                    numSubtasks,
                    startId,
                    endId);

            String sql =
                    String.format(
                            "SELECT %s FROM %s.%s WHERE id >= ? AND id <= ? ORDER BY id",
                            columns, schema, tableName);

            int count = 0;
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setLong(1, startId);
                stmt.setLong(2, endId);
                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    while (rs.next()) {
                        RowData row = convertToRowDataDynamic(rs, meta);
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
                    tableName);
        } else {
            // Single subtask: read all data
            String sql =
                    String.format("SELECT %s FROM %s.%s ORDER BY id", columns, schema, tableName);

            int count = 0;
            try (PreparedStatement stmt = connection.prepareStatement(sql);
                    ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                while (rs.next()) {
                    RowData row = convertToRowDataDynamic(rs, meta);
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                    }
                    count++;
                }
            }
            LOG.info("Snapshot read {} rows from {}.{}", count, schema, tableName);
        }
    }

    /** Get the min and max id of the table for parallel split calculation. */
    private long[] getIdRange() throws SQLException {
        String sql = String.format("SELECT MIN(id), MAX(id) FROM %s.%s", schema, tableName);
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

    /** Phase 2a: WAL streaming using WalReplicationStream. */
    private void runWalStreaming(SourceContext<RowData> ctx) throws Exception {
        if (!walStreamInitialized) {
            LOG.info("Initializing WAL replication stream for incremental phase");
            walReplicationStream.initialize();
            walStreamInitialized = true;
            LOG.info(
                    "WAL stream initialized, starting from LSN: {}",
                    walReplicationStream.getLastLsn());
        }

        while (running) {
            List<WalChange> changes = walReplicationStream.readChanges(1000);

            for (WalChange change : changes) {
                if (!change.isDataChange()) {
                    continue;
                }

                WalChange.ChangeType changeType = change.getType();
                RowData row = null;
                if (changeType == WalChange.ChangeType.INSERT) {
                    row = convertWalColumnsToRowData(change.getAfterColumns());
                } else if (changeType == WalChange.ChangeType.UPDATE) {
                    row = convertWalColumnsToRowData(change.getAfterColumns());
                } else if (changeType == WalChange.ChangeType.DELETE) {
                    LOG.debug("Detected DELETE on {}.{}", change.getSchema(), change.getTable());
                }

                if (row != null) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                    }
                }
            }

            if (changes.isEmpty()) {
                Thread.sleep(pollIntervalMs);
            }
        }
    }

    /** Phase 2b: Polling-based streaming using ChangeDataPoller. */
    private void runPollingStreaming(SourceContext<RowData> ctx) throws Exception {
        if (changeDataPoller != null) {
            changeDataPoller.loadSnapshot();
        }

        while (running) {
            List<ChangeEvent<RowData>> events = changeDataPoller.pollAllChanges();

            for (ChangeEvent<RowData> event : events) {
                RowData row = null;
                switch (event.getChangeType()) {
                    case INSERT:
                    case UPDATE:
                        row = event.getAfter();
                        break;
                    case DELETE:
                        LOG.debug("Detected DELETE event");
                        break;
                    case SNAPSHOT:
                        row = event.getAfter();
                        break;
                }

                if (row != null) {
                    synchronized (ctx.getCheckpointLock()) {
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

    // ---- CheckpointedFunction ----

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        offsetState.clear();
        if (walReplicationStream != null && walStreamInitialized) {
            String lsn = walReplicationStream.getLastLsn();
            if (lsn != null) {
                offsetState.add(lsn);
                LOG.debug("Checkpointed LSN: {}", lsn);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("gaussdb-cdc-offset-state", Types.STRING);
        offsetState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            // Restore LSN from checkpoint (for future use with LSN-based resumption)
            for (String lsn : offsetState.get()) {
                LOG.info("Restored LSN from checkpoint: {}", lsn);
            }
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

    // ---- Data conversion helpers ----

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
            Object value = convertColumnValueByOid(col.getTypeOid(), col.getValue());
            row.setField(i, value);
        }
        return row;
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
                        java.time.LocalDateTime.parse(
                                value,
                                java.time.format.DateTimeFormatter.ofPattern(
                                        "yyyy-MM-dd HH:mm:ss[.SSSSSS]")));
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
        private String pluginName = "mppdb_decoding";
        private boolean snapshotMode = true;
        private int chunkSize = 1000;
        private int connectTimeoutMs = 30000;
        private int pollIntervalMs = 1000;
        private boolean walMode = false;
        private String decodePlugin = "mppdb_decoding";
        private int parallelDecodeNum = 1;
        private String decodeStyle = "b";
        private boolean sendingBatch = false;

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

        public Builder pluginName(String pluginName) {
            this.pluginName = pluginName;
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

        public GaussDBCDCSourceFunction build() {
            return new GaussDBCDCSourceFunction(this);
        }
    }
}
