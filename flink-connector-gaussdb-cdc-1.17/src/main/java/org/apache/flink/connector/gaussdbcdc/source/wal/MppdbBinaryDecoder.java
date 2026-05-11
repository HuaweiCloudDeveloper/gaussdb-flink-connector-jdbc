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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decoder for GaussDB mppdb_decoding binary format.
 *
 * <p>This decoder parses the binary output from GaussDB's mppdb_decoding logical replication plugin
 * with parallel decoding support. The binary format specification:
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
 * <p>Reference: GaussDB logical decoding documentation.
 */
@Internal
public class MppdbBinaryDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(MppdbBinaryDecoder.class);

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

    /**
     * Decode a batch of binary data from mppdb_decoding into WalChange events.
     *
     * @param data the raw binary data from the replication stream
     * @return list of decoded WalChange events
     */
    public List<WalChange> decodeBatch(byte[] data) {
        List<WalChange> changes = new ArrayList<>();
        if (data == null || data.length == 0) {
            return changes;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);

        while (buffer.hasRemaining()) {
            // Check if batch is finished (totalSize = 0 means end of batch)
            if (buffer.remaining() < 4) {
                break;
            }

            // Mark the position before reading the record header
            int recordStartPos = buffer.position();

            int totalSize = buffer.getInt() & 0xFFFFFFFF;
            if (totalSize == 0) {
                // End of batch marker
                break;
            }

            // Determine the next record position.
            // In mppdb_decoding binary format, each record is followed by a 1-byte separator
            // ('P' = more records pending, 'F' = end of batch). The totalSize field does NOT
            // include this separator byte. So the actual record layout is:
            //   totalSize(4) + LSN(8) + type(1) + body(...) + separator(1)
            // And nextRecordPos = recordStartPos + 4 + totalSize + 1 (for separator)
            int bodyEndPos = recordStartPos + 4 + totalSize;

            // Check if there's a separator byte at bodyEndPos
            int nextRecordPos;
            if (bodyEndPos < data.length) {
                byte sepByte = data[bodyEndPos];
                if (sepByte == SEPARATOR_MORE || sepByte == SEPARATOR_END) {
                    // Separator found: next record starts after separator
                    nextRecordPos = bodyEndPos + 1;
                    if (sepByte == SEPARATOR_END) {
                        // End of batch - process this record but stop after
                        nextRecordPos = -1; // signal: end of batch after this record
                    }
                } else {
                    // No separator: next record starts right after the body
                    nextRecordPos = bodyEndPos;
                }
            } else {
                nextRecordPos = bodyEndPos;
            }

            // Read LSN (8 bytes uint64)
            if (buffer.remaining() < 9) {
                break;
            }
            long lsn = buffer.getLong();
            String lsnStr = "0x" + Long.toHexString(lsn);

            // Read record type (1 byte)
            byte typeByte = buffer.get();

            LOG.debug(
                    "Record at pos={}: totalSize={}, type={}, lsn={}, bodyEndPos={}, chosenNext={}",
                    recordStartPos,
                    totalSize,
                    (char) typeByte,
                    lsnStr,
                    bodyEndPos,
                    nextRecordPos);

            try {
                WalChange change = decodeRecord(buffer, lsnStr, typeByte);
                if (change != null) {
                    changes.add(change);
                }
            } catch (Exception e) {
                LOG.warn("Failed to decode WAL record at LSN {}: {}", lsnStr, e.getMessage());
            }

            // CRITICAL: Always position to the next record using totalSize.
            // The decode* methods may consume more or fewer bytes than the record
            // actually contains. By using totalSize + separator we guarantee correct alignment.
            if (nextRecordPos == -1) {
                // End of batch ('F' separator encountered)
                break;
            } else if (nextRecordPos <= buffer.limit()) {
                buffer.position(nextRecordPos);
            } else {
                // Not enough data for the next record, stop
                break;
            }
        }

        return changes;
    }

    private WalChange decodeRecord(ByteBuffer buffer, String lsnStr, byte typeByte) {
        switch (typeByte) {
            case 'B':
                return decodeBegin(buffer, lsnStr);
            case 'C':
                return decodeCommit(buffer, lsnStr);
            case 'I':
                return decodeInsert(buffer, lsnStr);
            case 'U':
                return decodeUpdate(buffer, lsnStr);
            case 'D':
                return decodeDelete(buffer, lsnStr);
            default:
                LOG.warn("Unknown WAL record type: {}", (char) typeByte);
                return null;
        }
    }

    private WalChange decodeBegin(ByteBuffer buffer, String lsnStr) {
        // CSN (8 bytes uint64)
        long csn = buffer.getLong();
        // first_lsn (8 bytes uint64)
        long firstLsn = buffer.getLong();

        long xid = 0;
        String timestamp = null;

        // Optional fields
        while (buffer.hasRemaining()) {
            byte marker = buffer.get();
            if (marker == TIMESTAMP_MARKER) {
                timestamp = readLengthPrefixedString(buffer);
            } else if (marker == USERNAME_MARKER) {
                // Skip username
                readLengthPrefixedString(buffer);
            } else {
                // Not an optional field, push back
                buffer.position(buffer.position() - 1);
                break;
            }
        }

        WalChange change = WalChange.begin(lsnStr, xid, csn);
        if (timestamp != null) {
            change.setRawData("BEGIN csn=" + csn + " timestamp=" + timestamp);
        }
        return change;
    }

    private WalChange decodeCommit(ByteBuffer buffer, String lsnStr) {
        long xid = 0;
        String timestamp = null;

        // Optional fields
        while (buffer.hasRemaining()) {
            byte marker = buffer.get();
            if (marker == XID_MARKER) {
                xid = buffer.getLong();
            } else if (marker == TIMESTAMP_MARKER) {
                timestamp = readLengthPrefixedString(buffer);
            } else if (marker == SEPARATOR_MORE || marker == SEPARATOR_END) {
                // Separator reached, push back for outer loop
                buffer.position(buffer.position() - 1);
                break;
            } else {
                buffer.position(buffer.position() - 1);
                break;
            }
        }

        WalChange change = WalChange.commit(lsnStr, xid);
        if (timestamp != null) {
            change.setRawData("COMMIT xid=" + xid + " timestamp=" + timestamp);
        }
        return change;
    }

    private WalChange decodeInsert(ByteBuffer buffer, String lsnStr) {
        // Schema and table name
        String schema = readUint16LengthString(buffer);
        String table = readUint16LengthString(buffer);

        // New tuple (marked with 'N')
        List<WalChange.ColumnValue> columns = new ArrayList<>();
        if (buffer.hasRemaining()) {
            byte tupleMarker = buffer.get();
            if (tupleMarker == TUPLE_NEW) {
                columns = decodeTupleColumns(buffer);
            } else {
                // No tuple marker, push back - might be column data directly
                buffer.position(buffer.position() - 1);
                columns = decodeTupleColumns(buffer);
            }
        }

        // Read separator
        readSeparator(buffer);

        return WalChange.insert(lsnStr, 0, schema, table, columns);
    }

    private WalChange decodeUpdate(ByteBuffer buffer, String lsnStr) {
        // Schema and table name
        String schema = readUint16LengthString(buffer);
        String table = readUint16LengthString(buffer);

        List<WalChange.ColumnValue> beforeColumns = new ArrayList<>();
        List<WalChange.ColumnValue> afterColumns = new ArrayList<>();

        // Update may have old tuple ('O') and new tuple ('N')
        // The new tuple comes first, then optionally old tuple
        if (buffer.hasRemaining()) {
            byte tupleMarker = buffer.get();
            if (tupleMarker == TUPLE_NEW) {
                afterColumns = decodeTupleColumns(buffer);
                // Check for old tuple
                if (buffer.hasRemaining()) {
                    byte nextMarker = buffer.get();
                    if (nextMarker == TUPLE_OLD) {
                        beforeColumns = decodeTupleColumns(buffer);
                    } else {
                        buffer.position(buffer.position() - 1);
                    }
                }
            } else if (tupleMarker == TUPLE_OLD) {
                beforeColumns = decodeTupleColumns(buffer);
                // Check for new tuple
                if (buffer.hasRemaining()) {
                    byte nextMarker = buffer.get();
                    if (nextMarker == TUPLE_NEW) {
                        afterColumns = decodeTupleColumns(buffer);
                    } else {
                        buffer.position(buffer.position() - 1);
                    }
                }
            } else {
                // No tuple marker, try to read columns directly
                buffer.position(buffer.position() - 1);
                afterColumns = decodeTupleColumns(buffer);
            }
        }

        // Read separator
        readSeparator(buffer);

        return WalChange.update(lsnStr, 0, schema, table, beforeColumns, afterColumns);
    }

    private WalChange decodeDelete(ByteBuffer buffer, String lsnStr) {
        // Schema and table name
        String schema = readUint16LengthString(buffer);
        String table = readUint16LengthString(buffer);

        // Old tuple (marked with 'O' or 'N')
        List<WalChange.ColumnValue> columns = new ArrayList<>();
        if (buffer.hasRemaining()) {
            byte tupleMarker = buffer.get();
            if (tupleMarker == TUPLE_OLD || tupleMarker == TUPLE_NEW) {
                columns = decodeTupleColumns(buffer);
            } else {
                buffer.position(buffer.position() - 1);
                columns = decodeTupleColumns(buffer);
            }
        }

        // Read separator
        readSeparator(buffer);

        return WalChange.delete(lsnStr, 0, schema, table, columns);
    }

    /**
     * Decode tuple columns from the buffer.
     *
     * <p>Format: 2 bytes uint16 column count, then for each column: 2 bytes name length + name, 4
     * bytes type OID, 4 bytes value length + value.
     */
    private List<WalChange.ColumnValue> decodeTupleColumns(ByteBuffer buffer) {
        List<WalChange.ColumnValue> columns = new ArrayList<>();

        // Column count (2 bytes uint16)
        int attrNum = buffer.getShort() & 0xFFFF;

        for (int i = 0; i < attrNum; i++) {
            // Column name length (2 bytes uint16) + name
            String columnName = readUint16LengthString(buffer);

            // Column type OID (4 bytes uint32)
            int typeOid = buffer.getInt();

            // Column value length (4 bytes uint32)
            long valueLength = buffer.getInt() & 0xFFFFFFFFL;

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
                buffer.get(valueBytes);
                value = new String(valueBytes, StandardCharsets.UTF_8);
                isNull = false;
            }

            columns.add(new WalChange.ColumnValue(columnName, typeOid, value, isNull));
        }

        return columns;
    }

    /** Read a length-prefixed string (4 bytes uint32 length + string). */
    private String readLengthPrefixedString(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length <= 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Read a uint16 length-prefixed string (2 bytes uint16 length + string). */
    private String readUint16LengthString(ByteBuffer buffer) {
        int length = buffer.getShort() & 0xFFFF;
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Read and consume the batch separator byte ('P' or 'F'). */
    private void readSeparator(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            byte sep = buffer.get();
            if (sep != SEPARATOR_MORE && sep != SEPARATOR_END) {
                // Not a separator, push back
                buffer.position(buffer.position() - 1);
            }
        }
    }
}
