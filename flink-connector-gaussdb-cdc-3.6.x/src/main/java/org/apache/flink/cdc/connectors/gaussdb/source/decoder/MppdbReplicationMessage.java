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

package org.apache.flink.cdc.connectors.gaussdb.source.decoder;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Shared {@link ReplicationMessage} implementation for mppdb_decoding output.
 *
 * <p>Used by both JSON and binary decoders to represent a single replication change event.
 */
public class MppdbReplicationMessage implements ReplicationMessage {

    private final Operation operation;
    private final String catalogName;
    private final String schema;
    private final String table;
    private final List<Column> newColumns;
    private final List<Column> oldColumns;
    private final String rawData;

    MppdbReplicationMessage(
            Operation operation,
            String schema,
            String table,
            List<Column> newColumns,
            List<Column> oldColumns,
            String rawData) {
        this(operation, null, schema, table, newColumns, oldColumns, rawData);
    }

    MppdbReplicationMessage(
            Operation operation,
            String catalogName,
            String schema,
            String table,
            List<Column> newColumns,
            List<Column> oldColumns,
            String rawData) {
        this.operation = operation;
        this.catalogName = catalogName;
        this.schema = schema;
        this.table = table;
        this.newColumns = newColumns;
        this.oldColumns = oldColumns;
        this.rawData = rawData;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return Instant.now();
    }

    @Override
    public OptionalLong getTransactionId() {
        return OptionalLong.empty();
    }

    @Override
    public String getTable() {
        // Return fully-qualified table name (e.g., "postgres.public.test_cdc") so that
        // PostgresSchema.parse() can correctly create a TableId that matches the one
        // registered in PostgresSchema via buildAndRegisterSchema(). The registered
        // TableId includes the catalog (database name) from readSchema(), so the lookup
        // must also include it. Without the catalog, PostgresSchema.parse("public.test_cdc")
        // would produce TableId(null, "public", "test_cdc") which doesn't match
        // TableId("postgres", "public", "test_cdc").
        StringBuilder sb = new StringBuilder();
        if (catalogName != null && !catalogName.isEmpty()) {
            sb.append(catalogName).append('.');
        }
        if (schema != null && !schema.isEmpty()) {
            sb.append(schema).append('.');
        }
        sb.append(table);
        return sb.toString();
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns != null ? oldColumns : Collections.emptyList();
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns != null ? newColumns : Collections.emptyList();
    }

    @Override
    public boolean hasTypeMetadata() {
        return false;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public boolean isTransactionalMessage() {
        return operation == Operation.BEGIN || operation == Operation.COMMIT;
    }

    @Override
    public boolean shouldSchemaBeSynchronized() {
        return false;
    }

    public String getRawData() {
        return rawData;
    }
}
