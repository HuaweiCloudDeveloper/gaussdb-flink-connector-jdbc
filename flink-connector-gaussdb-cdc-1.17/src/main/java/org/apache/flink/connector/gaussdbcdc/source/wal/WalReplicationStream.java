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

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * WAL replication stream for GaussDB using mppdb_decoding plugin with parallel decoding support.
 *
 * <p>This implementation supports:
 *
 * <ul>
 *   <li>mppdb_decoding plugin with binary format (decode-style='b')
 *   <li>Parallel decoding (parallel-decode-num=1~20)
 *   <li>Batch sending mode (sending-batch=true)
 *   <li>Both JDBC replication API and SQL function fallback
 * </ul>
 *
 * <p>Requirements:
 *
 * <ul>
 *   <li>wal_level = logical
 *   <li>Replication slot created with mppdb_decoding plugin
 *   <li>GaussDB JDBC driver with replication API support
 * </ul>
 */
@Internal
public class WalReplicationStream {

    private static final Logger LOG = LoggerFactory.getLogger(WalReplicationStream.class);

    private final Connection connection;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String slotName;
    private final String pluginName;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final int fetchSize;

    private String lastLsn;
    private boolean running = false;
    private boolean useReplicationApi = false;

    private final MppdbBinaryDecoder binaryDecoder;

    /** JDBC replication stream (PGReplicationStream), may be null if API unavailable. */
    private Object replicationStream;

    /** Dedicated replication connection, separate from the main data connection. */
    private Connection replicationConnection;

    public WalReplicationStream(
            Connection connection,
            String jdbcUrl,
            String username,
            String password,
            String slotName,
            String pluginName,
            int parallelDecodeNum,
            String decodeStyle,
            boolean sendingBatch,
            int fetchSize) {
        this.connection = connection;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.slotName = slotName;
        this.pluginName = pluginName;
        this.parallelDecodeNum = parallelDecodeNum;
        this.decodeStyle = decodeStyle;
        this.sendingBatch = sendingBatch;
        this.fetchSize = fetchSize;
        this.binaryDecoder = new MppdbBinaryDecoder();
    }

    /**
     * Initialize the replication stream.
     *
     * <p>Creates the replication slot if it doesn't exist, then tries to establish a streaming
     * replication connection via JDBC API. Falls back to SQL function-based polling if the
     * replication API is not available.
     */
    public void initialize() throws SQLException {
        LOG.info(
                "Initializing WAL replication stream: slot={}, plugin={}, parallel-decode-num={}, decode-style={}, sending-batch={}",
                slotName,
                pluginName,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch);

        // Check if slot exists
        if (!slotExists()) {
            createSlot();
        }

        // For SQL function mode: start from the slot's creation LSN (0/0 = from
        // beginning of slot) rather than the current LSN. Starting from current LSN
        // would skip all changes that occurred between slot creation and now.
        // For streaming replication API: the stream starts from the slot position
        // automatically, so lastLsn is only used for SQL function fallback.
        lastLsn = "0/0";
        LOG.info("Starting from LSN: {} (slot position)", lastLsn);

        // Try JDBC replication API first
        try {
            initializeReplicationApi();
            useReplicationApi = true;
            LOG.info("Using JDBC replication API for streaming");
        } catch (Exception e) {
            LOG.info(
                    "JDBC replication API not available, falling back to SQL function polling: {}",
                    e.getMessage());
            useReplicationApi = false;
        }

        running = true;
    }

    /**
     * Try to initialize using GaussDB JDBC replication API.
     *
     * <p>Creates a dedicated replication connection (with replication=database parameter) and
     * starts a logical replication stream. Falls back to SQL function polling if the replication
     * connection cannot be established (e.g., HA port not available in thread_pool mode).
     */
    private void initializeReplicationApi() throws Exception {
        // Build replication connection URL
        // GaussDB requires a separate connection with replication=database for streaming
        String replUrl = buildReplicationUrl(jdbcUrl);
        LOG.info("Attempting to create replication connection: {}", replUrl);

        // Create dedicated replication connection
        replicationConnection = DriverManager.getConnection(replUrl, username, password);

        // Check if PGConnection interface is available
        // GaussDB JDBC driver uses com.huawei.gaussdb.jdbc.PGConnection
        // instead of org.postgresql.PGConnection
        Class<?> pgConnectionClass;
        try {
            pgConnectionClass =
                    Class.forName(
                            "com.huawei.gaussdb.jdbc.PGConnection",
                            false,
                            replicationConnection.getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            // Fallback to PostgreSQL PGConnection for compatibility
            pgConnectionClass =
                    Class.forName(
                            "org.postgresql.PGConnection",
                            false,
                            replicationConnection.getClass().getClassLoader());
        }

        if (!pgConnectionClass.isInstance(replicationConnection)) {
            throw new IllegalStateException(
                    "Replication connection is not a PGConnection instance");
        }

        // Get replication API: conn.getReplicationAPI()
        Method getReplicationAPI = pgConnectionClass.getMethod("getReplicationAPI");
        Object replApi = getReplicationAPI.invoke(replicationConnection);

        // Build replication stream options
        Properties slotOptions = buildSlotOptions();

        // Create logical replication stream via reflection
        // replApi.replicationStream().logical().withSlotName(slotName).withSlotOption(...).start()
        Method replicationStreamMethod = replApi.getClass().getMethod("replicationStream");
        Object streamBuilder = replicationStreamMethod.invoke(replApi);

        Method logicalMethod = streamBuilder.getClass().getMethod("logical");
        Object logicalBuilder = logicalMethod.invoke(streamBuilder);

        Method withSlotNameMethod =
                logicalBuilder.getClass().getMethod("withSlotName", String.class);
        Object slotBuilder = withSlotNameMethod.invoke(logicalBuilder, slotName);

        // Set slot options
        if (slotOptions != null && !slotOptions.isEmpty()) {
            Method withSlotOptionMethod =
                    slotBuilder.getClass().getMethod("withSlotOption", String.class, String.class);
            for (String key : slotOptions.stringPropertyNames()) {
                withSlotOptionMethod.invoke(slotBuilder, key, slotOptions.getProperty(key));
            }
        }

        // Start the stream
        Method startMethod = slotBuilder.getClass().getMethod("start");
        replicationStream = startMethod.invoke(slotBuilder);

        LOG.info("JDBC replication stream started successfully");
    }

    /**
     * Build a replication-compatible JDBC URL from the original URL.
     *
     * <p>Adds or ensures the following parameters in the URL:
     *
     * <ul>
     *   <li>replication=database - required for logical replication streaming
     *   <li>preferQueryMode=simple - required for replication protocol
     *   <li>assumeMinServerVersion=9.4 - for replication protocol compatibility
     * </ul>
     */
    private String buildReplicationUrl(String originalUrl) {
        String url = originalUrl;

        // Remove trailing slash after database name if present
        // and ensure we can append parameters properly
        if (!url.contains("?")) {
            url += "?";
        } else {
            url += "&";
        }

        // Add replication parameters (don't duplicate if already present)
        if (!url.contains("replication=")) {
            url += "replication=database&";
        }
        if (!url.contains("preferQueryMode=")) {
            url += "preferQueryMode=simple&";
        }
        if (!url.contains("assumeMinServerVersion=")) {
            url += "assumeMinServerVersion=9.4&";
        }

        // Remove trailing & or ?
        url = url.replaceAll("[&?]$", "");

        return url;
    }

    /** Build slot options for parallel decoding. */
    private Properties buildSlotOptions() {
        Properties props = new Properties();

        if (parallelDecodeNum > 1) {
            props.setProperty("parallel-decode-num", String.valueOf(parallelDecodeNum));
            props.setProperty("decode-style", decodeStyle);
            if (sendingBatch) {
                props.setProperty("sending-batch", "1");
            }
        }

        props.setProperty("include-xids", "1");
        props.setProperty("include-timestamp", "1");

        return props;
    }

    /**
     * Read pending changes from the WAL.
     *
     * @param maxChanges maximum number of changes to read
     * @return list of WAL changes
     */
    public List<WalChange> readChanges(int maxChanges) throws SQLException {
        if (useReplicationApi) {
            return readChangesFromReplicationApi(maxChanges);
        } else {
            return readChangesFromSqlFunction(maxChanges);
        }
    }

    /** Read changes using JDBC replication API (streaming mode). */
    private List<WalChange> readChangesFromReplicationApi(int maxChanges) throws SQLException {
        List<WalChange> changes = new ArrayList<>();

        try {
            // Check if stream is still active
            Method isClosedMethod = replicationStream.getClass().getMethod("isClosed");
            boolean isClosed = (Boolean) isClosedMethod.invoke(replicationStream);
            if (isClosed) {
                LOG.warn("Replication stream is closed");
                running = false;
                return changes;
            }

            // Read pending changes: stream.readPending()
            Method readPendingMethod = replicationStream.getClass().getMethod("readPending");
            ByteBuffer buffer = (ByteBuffer) readPendingMethod.invoke(replicationStream);

            if (buffer != null && buffer.hasRemaining()) {
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                // Decode using mppdb_decoding binary decoder
                changes = binaryDecoder.decodeBatch(data);

                // Update last LSN
                if (!changes.isEmpty()) {
                    WalChange lastChange = changes.get(changes.size() - 1);
                    if (lastChange.getLsn() != null) {
                        lastLsn = lastChange.getLsn();
                    }
                }

                // Flush LSN to confirm processing
                Method flushMethod = replicationStream.getClass().getMethod("flush");
                flushMethod.invoke(replicationStream);
            }
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            throw new SQLException("Failed to read from replication stream", cause);
        } catch (Exception e) {
            throw new SQLException("Failed to read from replication stream", e);
        }

        return changes;
    }

    /** Read changes using SQL function pg_logical_slot_peek_changes (fallback mode). */
    private List<WalChange> readChangesFromSqlFunction(int maxChanges) throws SQLException {
        List<WalChange> changes = new ArrayList<>();

        // Build the SQL query with slot options
        // Note: GaussDB pg_logical_slot_peek_changes returns columns: location, xid, data
        // (not lsn like PostgreSQL). Using 'location' as alias for compatibility.
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append("'include-xids', '1'");

        if (parallelDecodeNum > 1) {
            optionBuilder
                    .append(", 'parallel-decode-num', '")
                    .append(parallelDecodeNum)
                    .append("'");
            // Note: decode-style and sending-batch are streaming-only options,
            // not supported by pg_logical_slot_peek_changes SQL function.
            // For SQL function mode, parallel-decode-num alone controls parallelism.
        }

        // Always pass NULL as the start LSN to pg_logical_slot_peek_changes.
        // GaussDB peek_changes returns empty when a specific LSN is passed after
        // pg_replication_slot_advance(). We rely on advanceSlot() to track progress
        // and always read from the slot's current position.
        String sql =
                String.format(
                        "SELECT location AS lsn, xid, data FROM pg_logical_slot_peek_changes(?, NULL, ?, %s)",
                        optionBuilder);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, slotName);
            stmt.setInt(2, maxChanges);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String lsn = rs.getString("lsn");
                    long xid = rs.getLong("xid");
                    String data = rs.getString("data");

                    if (data != null && !data.isEmpty()) {
                        // GaussDB mppdb_decoding outputs JSON format when using
                        // pg_logical_slot_peek_changes with parallel-decode-num.
                        // Binary format (decode-style='b') is only available via
                        // streaming replication API, not SQL functions.
                        WalChange change = parseChangeData(lsn, xid, data);
                        if (change != null) {
                            changes.add(change);
                        }
                        lastLsn = lsn;
                    }
                }
            }
        }

        // Advance the slot position after peeking
        if (!changes.isEmpty()) {
            advanceSlot();
        }

        return changes;
    }

    /**
     * Advance the replication slot to the last read LSN.
     *
     * <p>GaussDB uses pg_replication_slot_advance() instead of PostgreSQL's
     * pg_logical_slot_advance(). This method tries both for compatibility.
     */
    private void advanceSlot() throws SQLException {
        if (lastLsn == null) {
            return;
        }
        // Try GaussDB compatible function first, then PostgreSQL function
        String[] sqlCandidates = {
            "SELECT pg_replication_slot_advance(?, ?)", "SELECT pg_logical_slot_advance(?, ?)"
        };
        for (String sql : sqlCandidates) {
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, slotName);
                stmt.setString(2, lastLsn);
                stmt.execute();
                LOG.debug("Advanced slot {} to {} via {}", slotName, lastLsn, sql);
                return;
            } catch (SQLException e) {
                LOG.debug("Advance function not available: {} - {}", sql, e.getMessage());
            }
        }
        LOG.warn("Could not advance slot {} - no compatible function found", slotName);
    }

    /**
     * Parse change data from mppdb_decoding output.
     *
     * <p>Supports both text format (BEGIN/COMMIT/INSERT/UPDATE/DELETE) and JSON format output by
     * parallel decoding.
     */
    private WalChange parseChangeData(String lsn, long xid, String data) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setRawData(data);

        if (data.startsWith("BEGIN")) {
            change.setType(WalChange.ChangeType.BEGIN);
            // Try to parse CSN from: COMMIT ... CSN <number>
        } else if (data.startsWith("COMMIT")) {
            change.setType(WalChange.ChangeType.COMMIT);
            // Parse CSN from: COMMIT ... CSN <number>
            int csnIdx = data.indexOf("CSN ");
            if (csnIdx > 0) {
                try {
                    String csnStr = data.substring(csnIdx + 4).trim().split("\\s+")[0];
                    change.setCsn(Long.parseLong(csnStr));
                } catch (NumberFormatException e) {
                    LOG.debug("Failed to parse CSN from: {}", data);
                }
            }
        } else if (data.trim().startsWith("{")) {
            // JSON format from parallel decoding
            parseJsonChange(data, change);
        } else if (data.contains("INSERT")) {
            change.setType(WalChange.ChangeType.INSERT);
            parseTableAndColumns(data, change);
        } else if (data.contains("UPDATE")) {
            change.setType(WalChange.ChangeType.UPDATE);
            parseTableAndColumns(data, change);
        } else if (data.contains("DELETE")) {
            change.setType(WalChange.ChangeType.DELETE);
            parseTableAndColumns(data, change);
        } else {
            change.setType(WalChange.ChangeType.UNKNOWN);
        }

        return change;
    }

    /** Parse table name and column data from text format output. */
    private void parseTableAndColumns(String data, WalChange change) {
        // Text format: "INSERT: schema.table [col1=val1 col2=val2 ...]"
        try {
            int colonIdx = data.indexOf(':');
            if (colonIdx > 0) {
                String afterColon = data.substring(colonIdx + 1).trim();
                int bracketIdx = afterColon.indexOf('[');
                if (bracketIdx > 0) {
                    String tableRef = afterColon.substring(0, bracketIdx).trim();
                    int dotIdx = tableRef.lastIndexOf('.');
                    if (dotIdx > 0) {
                        change.setSchema(tableRef.substring(0, dotIdx));
                        change.setTable(tableRef.substring(dotIdx + 1));
                    } else {
                        change.setTable(tableRef);
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse table/columns from text: {}", data, e);
        }
    }

    /**
     * Parse JSON format change data from mppdb_decoding parallel decoding.
     *
     * <p>JSON format example:
     *
     * <pre>
     * {
     *   "table_name": "public.test_table",
     *   "op_type": "INSERT",
     *   "columns_name": ["id", "name", "age"],
     *   "columns_type": ["integer", "character varying", "integer"],
     *   "columns_val": ["1", "'Alice'", "25"],
     *   "old_keys_name": ["id"],
     *   "old_keys_type": ["integer"],
     *   "old_keys_val": ["1"]
     * }
     * </pre>
     */
    private void parseJsonChange(String data, WalChange change) {
        try {
            // Simple JSON parsing without external library
            String tableName = extractJsonStringField(data, "table_name");
            String opType = extractJsonStringField(data, "op_type");

            if (tableName != null) {
                int dotIdx = tableName.lastIndexOf('.');
                if (dotIdx > 0) {
                    change.setSchema(tableName.substring(0, dotIdx));
                    change.setTable(tableName.substring(dotIdx + 1));
                } else {
                    change.setTable(tableName);
                }
            }

            if (opType != null) {
                switch (opType.toUpperCase()) {
                    case "INSERT":
                        change.setType(WalChange.ChangeType.INSERT);
                        change.setAfterColumns(
                                parseJsonColumns(
                                        data, "columns_name", "columns_type", "columns_val"));
                        break;
                    case "UPDATE":
                        change.setType(WalChange.ChangeType.UPDATE);
                        change.setAfterColumns(
                                parseJsonColumns(
                                        data, "columns_name", "columns_type", "columns_val"));
                        change.setBeforeColumns(
                                parseJsonColumns(
                                        data, "old_keys_name", "old_keys_type", "old_keys_val"));
                        break;
                    case "DELETE":
                        change.setType(WalChange.ChangeType.DELETE);
                        change.setBeforeColumns(
                                parseJsonColumns(
                                        data, "old_keys_name", "old_keys_type", "old_keys_val"));
                        break;
                    default:
                        change.setType(WalChange.ChangeType.UNKNOWN);
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse JSON change data: {}", data, e);
            change.setType(WalChange.ChangeType.UNKNOWN);
        }
    }

    /** Extract a string field value from simple JSON. */
    private String extractJsonStringField(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return null;
        }
        // Find the colon after the field name
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return null;
        }
        // Find the value - could be a string or array
        int valueStart = colonIdx + 1;
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') {
            valueStart++;
        }
        if (valueStart >= json.length()) {
            return null;
        }
        if (json.charAt(valueStart) == '"') {
            // String value
            int valueEnd = json.indexOf('"', valueStart + 1);
            if (valueEnd > valueStart) {
                return json.substring(valueStart + 1, valueEnd);
            }
        }
        return null;
    }

    /** Parse column arrays from JSON change data. */
    private List<WalChange.ColumnValue> parseJsonColumns(
            String json, String namesKey, String typesKey, String valuesKey) {
        List<String> names = extractJsonArray(json, namesKey);
        List<String> types = extractJsonArray(json, typesKey);
        List<String> values = extractJsonArray(json, valuesKey);

        List<WalChange.ColumnValue> columns = new ArrayList<>();
        if (names == null) {
            return columns;
        }

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String type = (types != null && i < types.size()) ? types.get(i) : "unknown";
            String value = (values != null && i < values.size()) ? values.get(i) : null;

            // Strip quotes from values like 'Alice' -> Alice
            if (value != null && value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }

            // Map GaussDB type name to OID (approximate)
            int typeOid = mapTypeNameToOid(type);
            boolean isNull = "null".equalsIgnoreCase(value);

            columns.add(new WalChange.ColumnValue(name, typeOid, isNull ? null : value, isNull));
        }
        return columns;
    }

    /** Extract a JSON array field as a list of strings. */
    private List<String> extractJsonArray(String json, String fieldName) {
        List<String> result = new ArrayList<>();
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return result;
        }
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return result;
        }
        int arrayStart = json.indexOf('[', colonIdx);
        if (arrayStart < 0) {
            return result;
        }
        int arrayEnd = json.indexOf(']', arrayStart);
        if (arrayEnd < 0) {
            return result;
        }
        String arrayContent = json.substring(arrayStart + 1, arrayEnd);

        // Parse comma-separated quoted strings
        for (String item : arrayContent.split(",")) {
            item = item.trim();
            if (item.startsWith("\"") && item.endsWith("\"")) {
                result.add(item.substring(1, item.length() - 1));
            } else if (!item.isEmpty()) {
                result.add(item);
            }
        }
        return result;
    }

    /** Map GaussDB type name to approximate PostgreSQL type OID. */
    private int mapTypeNameToOid(String typeName) {
        if (typeName == null) {
            return 25; // text
        }
        switch (typeName.toLowerCase()) {
            case "integer":
            case "int":
            case "int4":
                return 23;
            case "bigint":
            case "int8":
                return 20;
            case "smallint":
            case "int2":
                return 21;
            case "boolean":
            case "bool":
                return 16;
            case "real":
            case "float4":
                return 700;
            case "double precision":
            case "float8":
                return 701;
            case "numeric":
            case "decimal":
                return 1700;
            case "character varying":
            case "varchar":
                return 1043;
            case "character":
            case "char":
                return 1042;
            case "text":
                return 25;
            case "timestamp without time zone":
            case "timestamp":
                return 1114;
            case "timestamp with time zone":
            case "timestamptz":
                return 1184;
            case "date":
                return 1082;
            default:
                return 25; // fallback to text
        }
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

    /** Create a new logical replication slot with mppdb_decoding plugin. */
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

    /**
     * Get the current WAL LSN position.
     *
     * <p>GaussDB uses pg_current_xlog_location() instead of PostgreSQL's pg_current_wal_lsn(). This
     * method tries both functions for compatibility.
     */
    private String getCurrentLsn() throws SQLException {
        // GaussDB compatible function (preferred)
        String[] sqlCandidates = {
            "SELECT pg_current_xlog_location()", "SELECT pg_current_wal_lsn()"
        };
        for (String sql : sqlCandidates) {
            try (PreparedStatement stmt = connection.prepareStatement(sql);
                    ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String result = rs.getString(1);
                    if (result != null) {
                        LOG.debug("Got current LSN via {}: {}", sql, result);
                        return result;
                    }
                }
            } catch (SQLException e) {
                LOG.debug("Function not available: {} - {}", sql, e.getMessage());
            }
        }
        return "0/0";
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
        if (replicationStream != null) {
            try {
                Method closeMethod = replicationStream.getClass().getMethod("close");
                closeMethod.invoke(replicationStream);
            } catch (Exception e) {
                LOG.debug("Failed to close replication stream", e);
            }
        }
        if (replicationConnection != null) {
            try {
                replicationConnection.close();
            } catch (Exception e) {
                LOG.debug("Failed to close replication connection", e);
            }
        }
        LOG.info("WAL replication stream closed");
    }

    public boolean isRunning() {
        return running;
    }

    public String getLastLsn() {
        return lastLsn;
    }

    public boolean isUseReplicationApi() {
        return useReplicationApi;
    }
}
