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
import java.util.List;
import java.util.function.Function;

/**
 * A {@link io.debezium.connector.postgresql.connection.MessageDecoder} implementation for GaussDB's
 * mppdb_decoding logical replication plugin with binary output format (decode-style='b').
 *
 * <p>Binary format specification from GaussDB mppdb_decoding parallel decoding:
 *
 * <pre>
 * Each decoded record:
 *   4 bytes uint32  - total bytes of this record (excluding this field)
 *   8 bytes uint64  - LSN
 *   1 byte          - record type: B=BEGIN, C=COMMIT, I=INSERT, U=UPDATE, D=DELETE
 *
 * BEGIN (B):
 *   8 bytes uint64  - CSN
 *   8 bytes uint64  - first_lsn
 *   [Optional] 1 byte 'T' + 4 bytes uint32 length + timestamp string
 *   [Optional] 1 byte 'N' + 4 bytes uint32 length + username string
 *
 * COMMIT (C):
 *   [Optional] 1 byte 'X' + 8 bytes uint64 xid
 *   [Optional] 1 byte 'T' + 4 bytes uint32 length + timestamp string
 *   1 byte separator: 'P' = more records, 'F' = batch end
 *
 * INSERT/UPDATE/DELETE (I/U/D):
 *   2 bytes uint16  - schema name length
 *   N bytes         - schema name
 *   2 bytes uint16  - table name length
 *   N bytes         - table name
 *   [Optional] 1 byte 'N' = new tuple, 'O' = old tuple
 *   2 bytes uint16  - column count (attrnum)
 *   For each column:
 *     2 bytes uint16  - column name length
 *     N bytes         - column name
 *     4 bytes uint32  - column type OID
 *     4 bytes uint32  - value length (0xFFFFFFFF = NULL, 0 = empty string)
 *     N bytes         - column value (string format)
 *   1 byte separator: 'P' = more records, 'F' = batch end
 * </pre>
 *
 * <p>Reference: GaussDB logical decoding documentation and Huawei Cloud docs on logical
 * replication.
 */
public class MppdbBinaryMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(MppdbBinaryMessageDecoder.class);

    /** NULL value marker in binary format. */
    private static final long NULL_VALUE_MARKER = 0xFFFFFFFFL;

    /** Batch separator: more records pending. */
    private static final byte SEPARATOR_MORE = 'P';

    /** Batch separator: batch finished. */
    private static final byte SEPARATOR_END = 'F';

    /** New tuple marker. */
    private static final byte TUPLE_NEW = 'N';

    /** Old tuple marker. */
    private static final byte TUPLE_OLD = 'O';

    /** Timestamp optional marker. */
    private static final byte TIMESTAMP_MARKER = 'T';

    /** Username optional marker. */
    private static final byte USERNAME_MARKER = 'N';

    /** Xid optional marker. */
    private static final byte XID_MARKER = 'X';

    private boolean containsMetadata = false;

    /**
     * The expected schema name for table identification. In some mppdb_decoding output formats, the
     * schema part might not match the actual schema (e.g., using the database username instead of
     * the schema name). This field is used to correct the schema name in the decoded
     * ReplicationMessage so that Debezium's table filter and schema lookup can match correctly.
     */
    private final String expectedSchemaName;

    /** The database (catalog) name to include in the fully-qualified table identifier. */
    private final String catalogName;

    /** Creates a decoder without schema name correction. */
    public MppdbBinaryMessageDecoder() {
        this.expectedSchemaName = null;
        this.catalogName = null;
    }

    /**
     * Creates a decoder with schema name correction.
     *
     * @param expectedSchemaName the actual schema name to use when correcting schema (e.g.,
     *     "public")
     */
    public MppdbBinaryMessageDecoder(String expectedSchemaName) {
        this.expectedSchemaName = expectedSchemaName;
        this.catalogName = null;
    }

    /**
     * Creates a decoder with schema name correction and catalog name.
     *
     * @param expectedSchemaName the actual schema name to use when correcting schema (e.g.,
     *     "public")
     * @param catalogName the database name to include in the fully-qualified table identifier
     *     (e.g., "postgres")
     */
    public MppdbBinaryMessageDecoder(String expectedSchemaName, String catalogName) {
        this.expectedSchemaName = expectedSchemaName;
        this.catalogName = catalogName;
    }

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

        LOG.debug("MppdbBinaryMessageDecoder received message ({} bytes)", data.length);

        List<ReplicationMessage> messages = decodeBatch(data);
        for (ReplicationMessage msg : messages) {
            if (!msg.isTransactionalMessage()) {
                LOG.debug(
                        "MppdbBinaryMessageDecoder decoded DML: op={}, table={}",
                        msg.getOperation(),
                        msg.getTable());
            }
            processor.process(msg);
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

    // ---- Binary protocol decoding ----

    /**
     * Corrects the schema name if needed. In some mppdb_decoding outputs, the schema part might not
     * match the actual schema name (e.g., using the database username). If expectedSchemaName is
     * set and the decoded schema differs, the schema is corrected.
     */
    private String correctSchemaName(String schema) {
        if (expectedSchemaName != null && !schema.equals(expectedSchemaName)) {
            LOG.debug(
                    "Correcting schema name from '{}' to '{}' in binary decoder",
                    schema,
                    expectedSchemaName);
            return expectedSchemaName;
        }
        return schema;
    }

    /**
     * Decodes a batch of binary data from mppdb_decoding into {@link ReplicationMessage} events.
     *
     * @param data the raw binary data from the replication stream
     * @return list of decoded ReplicationMessage events
     */
    List<ReplicationMessage> decodeBatch(byte[] data) {
        List<ReplicationMessage> messages = new ArrayList<>();
        if (data == null || data.length == 0) {
            return messages;
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            if (buf.remaining() < 4) {
                break;
            }
            int totalSize = buf.getInt() & 0xFFFFFFFF;
            if (totalSize == 0) {
                break;
            }
            if (buf.remaining() < 9) {
                break;
            }
            long lsn = buf.getLong();
            String lsnStr = "0x" + Long.toHexString(lsn);
            byte typeByte = buf.get();

            try {
                ReplicationMessage msg = decodeRecord(buf, lsnStr, typeByte);
                if (msg != null) {
                    messages.add(msg);
                }
            } catch (Exception e) {
                LOG.warn("Failed to decode WAL record at LSN {}: {}", lsnStr, e.getMessage());
                int bytesRead = 9;
                int toSkip = totalSize - bytesRead;
                if (toSkip > 0 && buf.remaining() >= toSkip) {
                    buf.position(buf.position() + toSkip);
                }
            }
        }
        return messages;
    }

    private ReplicationMessage decodeRecord(ByteBuffer buf, String lsnStr, byte typeByte) {
        switch (typeByte) {
            case 'B':
                return decodeBegin(buf, lsnStr);
            case 'C':
                return decodeCommit(buf, lsnStr);
            case 'I':
                return decodeInsert(buf, lsnStr);
            case 'U':
                return decodeUpdate(buf, lsnStr);
            case 'D':
                return decodeDelete(buf, lsnStr);
            default:
                LOG.warn("Unknown WAL record type: {} at LSN {}", (char) typeByte, lsnStr);
                return null;
        }
    }

    private ReplicationMessage decodeBegin(ByteBuffer buf, String lsnStr) {
        long csn = buf.getLong();
        buf.getLong(); // first_lsn, not needed for ReplicationMessage

        String timestamp = null;
        while (buf.hasRemaining()) {
            byte marker = buf.get();
            if (marker == TIMESTAMP_MARKER) {
                timestamp = readLengthPrefixedString(buf);
            } else if (marker == USERNAME_MARKER) {
                readLengthPrefixedString(buf); // skip username
            } else {
                buf.position(buf.position() - 1);
                break;
            }
        }

        String rawData =
                timestamp != null
                        ? "BEGIN csn=" + csn + " timestamp=" + timestamp
                        : "BEGIN csn=" + csn;
        return new MppdbReplicationMessage(
                ReplicationMessage.Operation.BEGIN, null, null, null, null, rawData);
    }

    private ReplicationMessage decodeCommit(ByteBuffer buf, String lsnStr) {
        long xid = 0;
        String timestamp = null;

        while (buf.hasRemaining()) {
            byte marker = buf.get();
            if (marker == XID_MARKER) {
                xid = buf.getLong();
            } else if (marker == TIMESTAMP_MARKER) {
                timestamp = readLengthPrefixedString(buf);
            } else if (marker == SEPARATOR_MORE || marker == SEPARATOR_END) {
                buf.position(buf.position() - 1);
                break;
            } else {
                buf.position(buf.position() - 1);
                break;
            }
        }

        String rawData =
                timestamp != null
                        ? "COMMIT xid=" + xid + " timestamp=" + timestamp
                        : "COMMIT xid=" + xid;
        return new MppdbReplicationMessage(
                ReplicationMessage.Operation.COMMIT, null, null, null, null, rawData);
    }

    private ReplicationMessage decodeInsert(ByteBuffer buf, String lsnStr) {
        String schema = correctSchemaName(readUint16LengthString(buf));
        String table = readUint16LengthString(buf);

        List<Column> columns = new ArrayList<>();
        if (buf.hasRemaining()) {
            byte tupleMarker = buf.get();
            if (tupleMarker == TUPLE_NEW) {
                columns = decodeTupleColumns(buf);
            } else {
                buf.position(buf.position() - 1);
                columns = decodeTupleColumns(buf);
            }
        }

        readSeparator(buf);
        return new MppdbReplicationMessage(
                ReplicationMessage.Operation.INSERT,
                catalogName,
                schema,
                table,
                columns,
                null,
                "INSERT");
    }

    private ReplicationMessage decodeUpdate(ByteBuffer buf, String lsnStr) {
        String schema = correctSchemaName(readUint16LengthString(buf));
        String table = readUint16LengthString(buf);

        List<Column> beforeColumns = new ArrayList<>();
        List<Column> afterColumns = new ArrayList<>();

        if (buf.hasRemaining()) {
            byte tupleMarker = buf.get();
            if (tupleMarker == TUPLE_NEW) {
                afterColumns = decodeTupleColumns(buf);
                if (buf.hasRemaining()) {
                    byte nextMarker = buf.get();
                    if (nextMarker == TUPLE_OLD) {
                        beforeColumns = decodeTupleColumns(buf);
                    } else {
                        buf.position(buf.position() - 1);
                    }
                }
            } else if (tupleMarker == TUPLE_OLD) {
                beforeColumns = decodeTupleColumns(buf);
                if (buf.hasRemaining()) {
                    byte nextMarker = buf.get();
                    if (nextMarker == TUPLE_NEW) {
                        afterColumns = decodeTupleColumns(buf);
                    } else {
                        buf.position(buf.position() - 1);
                    }
                }
            } else {
                buf.position(buf.position() - 1);
                afterColumns = decodeTupleColumns(buf);
            }
        }

        readSeparator(buf);
        return new MppdbReplicationMessage(
                ReplicationMessage.Operation.UPDATE,
                catalogName,
                schema,
                table,
                afterColumns,
                beforeColumns,
                "UPDATE");
    }

    private ReplicationMessage decodeDelete(ByteBuffer buf, String lsnStr) {
        String schema = correctSchemaName(readUint16LengthString(buf));
        String table = readUint16LengthString(buf);

        List<Column> columns = new ArrayList<>();
        if (buf.hasRemaining()) {
            byte tupleMarker = buf.get();
            if (tupleMarker == TUPLE_OLD || tupleMarker == TUPLE_NEW) {
                columns = decodeTupleColumns(buf);
            } else {
                buf.position(buf.position() - 1);
                columns = decodeTupleColumns(buf);
            }
        }

        readSeparator(buf);
        return new MppdbReplicationMessage(
                ReplicationMessage.Operation.DELETE,
                catalogName,
                schema,
                table,
                null,
                columns,
                "DELETE");
    }

    /**
     * Decodes tuple columns from the buffer.
     *
     * <p>Format: 2 bytes uint16 column count, then for each column: 2 bytes name length + name, 4
     * bytes type OID, 4 bytes value length + value.
     */
    private List<Column> decodeTupleColumns(ByteBuffer buf) {
        List<Column> columns = new ArrayList<>();
        int attrNum = buf.getShort() & 0xFFFF;

        for (int i = 0; i < attrNum; i++) {
            String columnName = readUint16LengthString(buf);
            int typeOid = buf.getInt();
            long valueLength = buf.getInt() & 0xFFFFFFFFL;

            String value;
            boolean isNull;
            if (valueLength == NULL_VALUE_MARKER) {
                value = null;
                isNull = true;
            } else if (valueLength == 0) {
                value = "";
                isNull = false;
            } else {
                byte[] valueBytes = new byte[(int) valueLength];
                buf.get(valueBytes);
                value = new String(valueBytes, StandardCharsets.UTF_8);
                isNull = false;
            }

            columns.add(new MppdbColumn(columnName, typeOid, value, isNull));
        }
        return columns;
    }

    /** Reads a length-prefixed string (4 bytes uint32 length + string). */
    private String readLengthPrefixedString(ByteBuffer buf) {
        int length = buf.getInt();
        if (length <= 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Reads a uint16 length-prefixed string (2 bytes uint16 length + string). */
    private String readUint16LengthString(ByteBuffer buf) {
        int length = buf.getShort() & 0xFFFF;
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Reads and consumes the batch separator byte ('P' or 'F'). */
    private void readSeparator(ByteBuffer buf) {
        if (buf.hasRemaining()) {
            byte sep = buf.get();
            if (sep != SEPARATOR_MORE && sep != SEPARATOR_END) {
                buf.position(buf.position() - 1);
            }
        }
    }
}
