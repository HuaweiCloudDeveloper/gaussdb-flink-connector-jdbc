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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for GaussDBSplit. */
@Internal
public class GaussDBSplitSerializer implements SimpleVersionedSerializer<GaussDBSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(GaussDBSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeUTF(split.splitId());
            out.writeUTF(split.getSplitType().name());
            out.writeUTF(split.getTableName());

            if (split.isSnapshotSplit()) {
                out.writeLong(split.getStartId());
                out.writeLong(split.getEndId());
            } else {
                out.writeUTF(split.getSlotName() != null ? split.getSlotName() : "");
                out.writeUTF(split.getStartLsn() != null ? split.getStartLsn() : "");
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public GaussDBSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            String splitId = in.readUTF();
            GaussDBSplit.SplitType splitType = GaussDBSplit.SplitType.valueOf(in.readUTF());
            String tableName = in.readUTF();

            if (splitType == GaussDBSplit.SplitType.SNAPSHOT) {
                long startId = in.readLong();
                long endId = in.readLong();
                return new GaussDBSplit(splitId, tableName, startId, endId);
            } else {
                String slotName = in.readUTF();
                String startLsn = in.readUTF();
                return new GaussDBSplit(
                        splitId,
                        tableName,
                        slotName.isEmpty() ? null : slotName,
                        startLsn.isEmpty() ? null : startLsn);
            }
        }
    }
}
