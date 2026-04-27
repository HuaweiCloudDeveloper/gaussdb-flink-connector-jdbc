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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MppdbBinaryDecoder}. */
class MppdbBinaryDecoderTest {

    private final MppdbBinaryDecoder decoder = new MppdbBinaryDecoder();

    @Test
    void testDecodeEmptyData() {
        List<WalChange> changes = decoder.decodeBatch(new byte[0]);
        assertThat(changes).isEmpty();
    }

    @Test
    void testDecodeNullData() {
        List<WalChange> changes = decoder.decodeBatch(null);
        assertThat(changes).isEmpty();
    }

    @Test
    void testDecodeEndOfBatchMarker() {
        // totalSize=0 means end of batch
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(0);
        List<WalChange> changes = decoder.decodeBatch(buf.array());
        assertThat(changes).isEmpty();
    }

    @Test
    void testDecodeBeginRecord() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        // totalSize (placeholder, will fix later)
        int sizePos = buf.position();
        buf.putInt(0); // placeholder for total size

        // LSN (8 bytes)
        buf.putLong(0x0100_0000L);

        // Type: B = BEGIN
        buf.put((byte) 'B');

        // CSN (8 bytes)
        buf.putLong(1001L);

        // first_lsn (8 bytes)
        buf.putLong(0x0100_0000L);

        // Separator 'P' (more records) then 'F' (end batch) - but for BEGIN we don't need one
        // Actually, let's add a separator
        buf.put((byte) 'P');

        // Fix totalSize
        int endPos = buf.position();
        int totalSize = endPos - sizePos - 4;
        buf.putInt(sizePos, totalSize);

        // End of batch marker
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getType()).isEqualTo(WalChange.ChangeType.BEGIN);
        assertThat(changes.get(0).getCsn()).isEqualTo(1001L);
    }

    @Test
    void testDecodeCommitRecord() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        int sizePos = buf.position();
        buf.putInt(0); // placeholder

        // LSN
        buf.putLong(0x0200_0000L);

        // Type: C = COMMIT
        buf.put((byte) 'C');

        // Optional XID
        buf.put((byte) 'X');
        buf.putLong(42L);

        // Optional timestamp
        buf.put((byte) 'T');
        buf.putInt(19); // length
        buf.put("2024-01-15 10:30:00".getBytes(StandardCharsets.UTF_8));

        // Separator: end of batch
        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);

        // End of batch
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getType()).isEqualTo(WalChange.ChangeType.COMMIT);
    }

    @Test
    void testDecodeInsertRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0); // placeholder

        // LSN
        buf.putLong(0x0300_0000L);

        // Type: I = INSERT
        buf.put((byte) 'I');

        // Schema name
        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        // Table name
        byte[] tableBytes = "student".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        // New tuple marker
        buf.put((byte) 'N');

        // Column count = 2
        buf.putShort((short) 2);

        // Column 1: id (int4, OID=23)
        byte[] col1Name = "id".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1Name.length);
        buf.put(col1Name);
        buf.putInt(23); // type OID
        byte[] col1Value = "1".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col1Value.length);
        buf.put(col1Value);

        // Column 2: name (varchar, OID=1043)
        byte[] col2Name = "name".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col2Name.length);
        buf.put(col2Name);
        buf.putInt(1043); // type OID
        byte[] col2Value = "张三".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col2Value.length);
        buf.put(col2Value);

        // Separator: end of batch
        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);

        // End of batch
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        WalChange change = changes.get(0);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
        assertThat(change.getSchema()).isEqualTo("public");
        assertThat(change.getTable()).isEqualTo("student");
        assertThat(change.getAfterColumns()).hasSize(2);
        assertThat(change.getAfterColumns().get(0).getColumnName()).isEqualTo("id");
        assertThat(change.getAfterColumns().get(0).getValue()).isEqualTo("1");
        assertThat(change.getAfterColumns().get(0).getTypeOid()).isEqualTo(23);
        assertThat(change.getAfterColumns().get(1).getColumnName()).isEqualTo("name");
        assertThat(change.getAfterColumns().get(1).getValue()).isEqualTo("张三");
    }

    @Test
    void testDecodeDeleteRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0); // placeholder

        // LSN
        buf.putLong(0x0400_0000L);

        // Type: D = DELETE
        buf.put((byte) 'D');

        // Schema name
        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        // Table name
        byte[] tableBytes = "student".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        // Old tuple marker
        buf.put((byte) 'O');

        // Column count = 1
        buf.putShort((short) 1);

        // Column: id
        byte[] col1Name = "id".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1Name.length);
        buf.put(col1Name);
        buf.putInt(23);
        byte[] col1Value = "5".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col1Value.length);
        buf.put(col1Value);

        // Separator
        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0); // end of batch

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        WalChange change = changes.get(0);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.DELETE);
        assertThat(change.getBeforeColumns()).hasSize(1);
        assertThat(change.getBeforeColumns().get(0).getValue()).isEqualTo("5");
    }

    @Test
    void testDecodeUpdateRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0500_0000L);
        buf.put((byte) 'U');

        // Schema
        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        // Table
        byte[] tableBytes = "student".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        // New tuple
        buf.put((byte) 'N');
        buf.putShort((short) 1);
        byte[] col1Name = "age".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1Name.length);
        buf.put(col1Name);
        buf.putInt(23);
        byte[] col1Value = "21".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col1Value.length);
        buf.put(col1Value);

        // Old tuple
        buf.put((byte) 'O');
        buf.putShort((short) 1);
        byte[] col2Name = "age".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col2Name.length);
        buf.put(col2Name);
        buf.putInt(23);
        byte[] col2Value = "20".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col2Value.length);
        buf.put(col2Value);

        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        WalChange change = changes.get(0);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.UPDATE);
        assertThat(change.getAfterColumns()).hasSize(1);
        assertThat(change.getAfterColumns().get(0).getValue()).isEqualTo("21");
        assertThat(change.getBeforeColumns()).hasSize(1);
        assertThat(change.getBeforeColumns().get(0).getValue()).isEqualTo("20");
    }

    @Test
    void testDecodeNullColumnValue() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0600_0000L);
        buf.put((byte) 'I');

        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        byte[] tableBytes = "t1".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        buf.put((byte) 'N');
        buf.putShort((short) 1);

        byte[] colName = "nullable_col".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) colName.length);
        buf.put(colName);
        buf.putInt(25); // text OID
        buf.putInt(0xFFFFFFFF); // NULL marker

        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getAfterColumns().get(0).isNull()).isTrue();
        assertThat(changes.get(0).getAfterColumns().get(0).getValue()).isNull();
    }

    @Test
    void testDecodeEmptyStringColumn() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0700_0000L);
        buf.put((byte) 'I');

        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        byte[] tableBytes = "t1".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        buf.put((byte) 'N');
        buf.putShort((short) 1);

        byte[] colName = "empty_col".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) colName.length);
        buf.put(colName);
        buf.putInt(25);
        buf.putInt(0); // length=0 means empty string

        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getAfterColumns().get(0).isNull()).isFalse();
        assertThat(changes.get(0).getAfterColumns().get(0).getValue()).isEmpty();
    }

    @Test
    void testDecodeMultipleRecordsInBatch() {
        ByteBuffer buf = ByteBuffer.allocate(1024);

        // Record 1: INSERT
        int sizePos1 = buf.position();
        buf.putInt(0);
        buf.putLong(0x0100_0000L);
        buf.put((byte) 'I');
        byte[] schema1 = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schema1.length);
        buf.put(schema1);
        byte[] table1 = "t1".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) table1.length);
        buf.put(table1);
        buf.put((byte) 'N');
        buf.putShort((short) 1);
        byte[] col1 = "id".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1.length);
        buf.put(col1);
        buf.putInt(23);
        byte[] val1 = "1".getBytes(StandardCharsets.UTF_8);
        buf.putInt(val1.length);
        buf.put(val1);
        buf.put((byte) 'P'); // more records
        int totalSize1 = buf.position() - sizePos1 - 4;
        buf.putInt(sizePos1, totalSize1);

        // Record 2: INSERT
        int sizePos2 = buf.position();
        buf.putInt(0);
        buf.putLong(0x0200_0000L);
        buf.put((byte) 'I');
        buf.putShort((short) schema1.length);
        buf.put(schema1);
        buf.putShort((short) table1.length);
        buf.put(table1);
        buf.put((byte) 'N');
        buf.putShort((short) 1);
        buf.putShort((short) col1.length);
        buf.put(col1);
        buf.putInt(23);
        byte[] val2 = "2".getBytes(StandardCharsets.UTF_8);
        buf.putInt(val2.length);
        buf.put(val2);
        buf.put((byte) 'F'); // end of batch
        int totalSize2 = buf.position() - sizePos2 - 4;
        buf.putInt(sizePos2, totalSize2);

        // End of batch
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<WalChange> changes = decoder.decodeBatch(data);
        assertThat(changes).hasSize(2);
        assertThat(changes.get(0).getAfterColumns().get(0).getValue()).isEqualTo("1");
        assertThat(changes.get(1).getAfterColumns().get(0).getValue()).isEqualTo("2");
    }
}
