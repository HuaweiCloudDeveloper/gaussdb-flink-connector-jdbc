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
import java.util.ArrayList;
import java.util.List;

/** Serializer for GaussDBCheckpoint. */
@Internal
public class GaussDBCheckpointSerializer implements SimpleVersionedSerializer<GaussDBCheckpoint> {

    private static final int CURRENT_VERSION = 1;
    private final GaussDBSplitSerializer splitSerializer = new GaussDBSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(GaussDBCheckpoint checkpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            // Serialize assigned splits
            out.writeInt(checkpoint.getAssignedSplits().size());
            for (GaussDBSplit split : checkpoint.getAssignedSplits()) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }

            // Serialize unassigned splits
            out.writeInt(checkpoint.getUnassignedSplits().size());
            for (GaussDBSplit split : checkpoint.getUnassignedSplits()) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }

            // Serialize lastLsn
            out.writeUTF(checkpoint.getLastLsn() != null ? checkpoint.getLastLsn() : "");

            // Serialize snapshotCompleted
            out.writeBoolean(checkpoint.isSnapshotCompleted());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public GaussDBCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            // Deserialize assigned splits
            int assignedSize = in.readInt();
            List<GaussDBSplit> assignedSplits = new ArrayList<>(assignedSize);
            for (int i = 0; i < assignedSize; i++) {
                int splitLength = in.readInt();
                byte[] splitBytes = new byte[splitLength];
                in.readFully(splitBytes);
                assignedSplits.add(
                        splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
            }

            // Deserialize unassigned splits
            int unassignedSize = in.readInt();
            List<GaussDBSplit> unassignedSplits = new ArrayList<>(unassignedSize);
            for (int i = 0; i < unassignedSize; i++) {
                int splitLength = in.readInt();
                byte[] splitBytes = new byte[splitLength];
                in.readFully(splitBytes);
                unassignedSplits.add(
                        splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
            }

            // Deserialize lastLsn
            String lastLsn = in.readUTF();

            // Deserialize snapshotCompleted
            boolean snapshotCompleted = in.readBoolean();

            return new GaussDBCheckpoint(
                    assignedSplits,
                    unassignedSplits,
                    lastLsn.isEmpty() ? null : lastLsn,
                    snapshotCompleted);
        }
    }
}
