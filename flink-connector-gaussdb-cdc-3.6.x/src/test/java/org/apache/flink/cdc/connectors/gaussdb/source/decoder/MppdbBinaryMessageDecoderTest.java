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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MppdbBinaryMessageDecoder}. */
class MppdbBinaryMessageDecoderTest {

    private final MppdbBinaryMessageDecoder decoder = new MppdbBinaryMessageDecoder();

    @Test
    void testDecodeEmptyData() {
        List<ReplicationMessage> messages = decoder.decodeBatch(new byte[0]);
        assertThat(messages).isEmpty();
    }

    @Test
    void testDecodeNullData() {
        List<ReplicationMessage> messages = decoder.decodeBatch(null);
        assertThat(messages).isEmpty();
    }

    @Test
    void testDecodeEndOfBatchMarker() {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(0);
        List<ReplicationMessage> messages = decoder.decodeBatch(buf.array());
        assertThat(messages).isEmpty();
    }

    @Test
    void testDecodeBeginRecord() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0100_0000L);
        buf.put((byte) 'B');
        buf.putLong(1001L); // CSN
        buf.putLong(0x0100_0000L); // first_lsn

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0); // end of batch

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
        assertThat(messages.get(0).isTransactionalMessage()).isTrue();
    }

    @Test
    void testDecodeCommitRecord() {
        ByteBuffer buf = ByteBuffer.allocate(256);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0200_0000L);
        buf.put((byte) 'C');
        buf.put((byte) 'X');
        buf.putLong(42L);
        buf.put((byte) 'T');
        buf.putInt(19);
        buf.put("2024-01-15 10:30:00".getBytes(StandardCharsets.UTF_8));
        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
        assertThat(messages.get(0).isTransactionalMessage()).isTrue();
    }

    @Test
    void testDecodeInsertRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0300_0000L);
        buf.put((byte) 'I');

        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        byte[] tableBytes = "student".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        buf.put((byte) 'N');
        buf.putShort((short) 2);

        byte[] col1Name = "id".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1Name.length);
        buf.put(col1Name);
        buf.putInt(23);
        byte[] col1Value = "1".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col1Value.length);
        buf.put(col1Value);

        byte[] col2Name = "name".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col2Name.length);
        buf.put(col2Name);
        buf.putInt(1043);
        byte[] col2Value = "Alice".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col2Value.length);
        buf.put(col2Value);

        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        ReplicationMessage msg = messages.get(0);
        assertThat(msg.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
        assertThat(msg.getTable()).isEqualTo("student");
        assertThat(msg.getNewTupleList()).hasSize(2);
        assertThat(msg.getNewTupleList().get(0).getName()).isEqualTo("id");
        assertThat(msg.getNewTupleList().get(1).getName()).isEqualTo("name");
    }

    @Test
    void testDecodeDeleteRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0400_0000L);
        buf.put((byte) 'D');

        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

        byte[] tableBytes = "student".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) tableBytes.length);
        buf.put(tableBytes);

        buf.put((byte) 'O');
        buf.putShort((short) 1);

        byte[] col1Name = "id".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) col1Name.length);
        buf.put(col1Name);
        buf.putInt(23);
        byte[] col1Value = "5".getBytes(StandardCharsets.UTF_8);
        buf.putInt(col1Value.length);
        buf.put(col1Value);

        buf.put((byte) 'F');

        int totalSize = buf.position() - sizePos - 4;
        buf.putInt(sizePos, totalSize);
        buf.putInt(0);

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        ReplicationMessage msg = messages.get(0);
        assertThat(msg.getOperation()).isEqualTo(ReplicationMessage.Operation.DELETE);
        assertThat(msg.getOldTupleList()).hasSize(1);
    }

    @Test
    void testDecodeUpdateRecord() {
        ByteBuffer buf = ByteBuffer.allocate(512);
        int sizePos = buf.position();
        buf.putInt(0);

        buf.putLong(0x0500_0000L);
        buf.put((byte) 'U');

        byte[] schemaBytes = "public".getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) schemaBytes.length);
        buf.put(schemaBytes);

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

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        ReplicationMessage msg = messages.get(0);
        assertThat(msg.getOperation()).isEqualTo(ReplicationMessage.Operation.UPDATE);
        assertThat(msg.getNewTupleList()).hasSize(1);
        assertThat(msg.getOldTupleList()).hasSize(1);
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

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        Column col = messages.get(0).getNewTupleList().get(0);
        assertThat(col.isOptional()).isTrue();
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

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(1);
        Column col = messages.get(0).getNewTupleList().get(0);
        assertThat(col.isOptional()).isTrue();
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

        buf.putInt(0); // end of batch

        byte[] data = new byte[buf.position()];
        buf.flip();
        buf.get(data);

        List<ReplicationMessage> messages = decoder.decodeBatch(data);
        assertThat(messages).hasSize(2);
        assertThat(messages.get(0).getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
        assertThat(messages.get(1).getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
    }
}
