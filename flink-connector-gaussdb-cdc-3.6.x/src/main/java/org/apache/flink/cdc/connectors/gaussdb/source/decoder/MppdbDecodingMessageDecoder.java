/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.gaussdb.source.decoder;

import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link io.debezium.connector.postgresql.connection.MessageDecoder} implementation for GaussDB's
 * mppdb_decoding logical replication plugin.
 *
 * <p>mppdb_decoding outputs change events in JSON format when using decode-style='j'. This decoder
 * parses the JSON output and converts it into Debezium {@link ReplicationMessage} objects that can
 * be processed by the standard Debezium/CDC streaming pipeline.
 *
 * <p>JSON format (from mppdb_decoding parallel decoding):
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
public class MppdbDecodingMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(MppdbDecodingMessageDecoder.class);

    private boolean containsMetadata = false;

    @Override
    public void setContainsMetadata(boolean containsMetadata) {
        this.containsMetadata = containsMetadata;
    }

    @Override
    protected void processNotEmptyMessage(
            ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        String content = new String(data, StandardCharsets.UTF_8);

        LOG.info(
                "MppdbDecodingMessageDecoder received message ({} bytes): {}",
                data.length,
                content.substring(0, Math.min(content.length(), 300)));

        // mppdb_decoding output may contain multiple JSON objects concatenated with
        // binary framing headers. Extract JSON objects by finding { ... } patterns.
        List<String> jsonEvents = extractJsonObjects(content);

        for (String json : jsonEvents) {
            try {
                ReplicationMessage msg = parseJsonMessage(json);
                if (msg != null) {
                    processor.process(msg);
                }
            } catch (Exception e) {
                LOG.debug("Failed to parse mppdb_decoding JSON event: {}", json, e);
            }
        }
    }

    @Override
    public boolean shouldMessageBeSkipped(
            ByteBuffer buffer, Lsn lastReceivedLsn, Lsn startLsn, WalPositionLocator locator) {
        return false;
    }

    @Override
    public void close() {
        // nothing to close
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(
            ChainedLogicalStreamBuilder builder,
            Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(
            ChainedLogicalStreamBuilder builder,
            Function<Integer, Boolean> hasMinimumServerVersion) {
        return builder;
    }

    // ---- mppdb_decoding JSON parsing (from CDC 1.17 WalReplicationStream) ----

    private List<String> extractJsonObjects(String content) {
        List<String> results = new ArrayList<>();
        int i = 0;
        while (i < content.length()) {
            int jsonStart = content.indexOf('{', i);
            if (jsonStart == -1) {
                break;
            }
            int depth = 0;
            int jsonEnd = jsonStart;
            for (int j = jsonStart; j < content.length(); j++) {
                char c = content.charAt(j);
                if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                }
                if (depth == 0) {
                    jsonEnd = j;
                    break;
                }
            }
            if (depth == 0) {
                results.add(content.substring(jsonStart, jsonEnd + 1));
                i = jsonEnd + 1;
            } else {
                break;
            }
        }
        return results;
    }

    private ReplicationMessage parseJsonMessage(String json) {
        String opType = extractJsonStringField(json, "op_type");
        String tableName = extractJsonStringField(json, "table_name");

        if (opType == null) {
            return null;
        }

        String schema = null;
        String table = null;
        if (tableName != null) {
            int dotIdx = tableName.lastIndexOf('.');
            if (dotIdx > 0) {
                schema = tableName.substring(0, dotIdx);
                table = tableName.substring(dotIdx + 1);
            } else {
                table = tableName;
            }
        }

        ReplicationMessage.Operation operation;
        switch (opType.toUpperCase()) {
            case "INSERT":
                operation = ReplicationMessage.Operation.INSERT;
                break;
            case "UPDATE":
                operation = ReplicationMessage.Operation.UPDATE;
                break;
            case "DELETE":
                operation = ReplicationMessage.Operation.DELETE;
                break;
            case "BEGIN":
                operation = ReplicationMessage.Operation.BEGIN;
                break;
            case "COMMIT":
                operation = ReplicationMessage.Operation.COMMIT;
                break;
            default:
                operation = ReplicationMessage.Operation.NOOP;
                break;
        }

        List<Column> newColumns =
                parseJsonColumns(json, "columns_name", "columns_type", "columns_val");
        List<Column> oldColumns =
                parseJsonColumns(json, "old_keys_name", "old_keys_type", "old_keys_val");

        return new MppdbReplicationMessage(operation, schema, table, newColumns, oldColumns, json);
    }

    private String extractJsonStringField(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return null;
        }
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return null;
        }
        int valueStart = colonIdx + 1;
        while (valueStart < json.length() && json.charAt(valueStart) == ' ') {
            valueStart++;
        }
        if (valueStart >= json.length()) {
            return null;
        }
        if (json.charAt(valueStart) == '"') {
            int valueEnd = json.indexOf('"', valueStart + 1);
            if (valueEnd > valueStart) {
                return json.substring(valueStart + 1, valueEnd);
            }
        }
        return null;
    }

    private List<Column> parseJsonColumns(
            String json, String namesKey, String typesKey, String valuesKey) {
        List<String> names = extractJsonArray(json, namesKey);
        List<String> types = extractJsonArray(json, typesKey);
        List<String> values = extractJsonArray(json, valuesKey);

        List<Column> columns = new ArrayList<>();
        if (names == null || names.isEmpty()) {
            return columns;
        }

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String type = (types != null && i < types.size()) ? types.get(i) : "text";
            String val = (values != null && i < values.size()) ? values.get(i) : null;

            // Strip quotes from values like 'Alice' -> Alice
            if (val != null && val.startsWith("'") && val.endsWith("'")) {
                val = val.substring(1, val.length() - 1);
            }
            boolean isNull = val == null || "null".equalsIgnoreCase(val);

            columns.add(new MppdbColumn(name, type, isNull ? null : val, isNull));
        }
        return columns;
    }

    private List<String> extractJsonArray(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) {
            return Collections.emptyList();
        }
        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) {
            return Collections.emptyList();
        }
        int arrayStart = json.indexOf('[', colonIdx);
        if (arrayStart < 0) {
            return Collections.emptyList();
        }
        int arrayEnd = json.indexOf(']', arrayStart);
        if (arrayEnd < 0) {
            return Collections.emptyList();
        }
        String arrayContent = json.substring(arrayStart + 1, arrayEnd);

        List<String> result = new ArrayList<>();
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
}
