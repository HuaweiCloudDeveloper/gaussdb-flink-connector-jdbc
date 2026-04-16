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

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link GaussDBCheckpointSerializer}. */
public class GaussDBCheckpointSerializerTest {

    private final GaussDBCheckpointSerializer serializer = new GaussDBCheckpointSerializer();

    @Test
    public void testSerializeDeserializeCheckpoint() throws IOException {
        List<GaussDBSplit> assignedSplits = new ArrayList<>();
        assignedSplits.add(new GaussDBSplit("split-0", "student", 1L, 100L));

        List<GaussDBSplit> unassignedSplits = new ArrayList<>();
        unassignedSplits.add(new GaussDBSplit("split-1", "student", 101L, 200L));

        GaussDBCheckpoint original =
                new GaussDBCheckpoint(assignedSplits, unassignedSplits, "0/12345", true);

        byte[] serialized = serializer.serialize(original);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        GaussDBCheckpoint deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(original.getAssignedSplits().size(), deserialized.getAssignedSplits().size());
        assertEquals(
                original.getUnassignedSplits().size(), deserialized.getUnassignedSplits().size());
        assertEquals(original.getLastLsn(), deserialized.getLastLsn());
        assertEquals(original.isSnapshotCompleted(), deserialized.isSnapshotCompleted());
    }

    @Test
    public void testSerializeDeserializeEmptyCheckpoint() throws IOException {
        GaussDBCheckpoint original =
                new GaussDBCheckpoint(new ArrayList<>(), new ArrayList<>(), null, false);

        byte[] serialized = serializer.serialize(original);
        GaussDBCheckpoint deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(0, deserialized.getAssignedSplits().size());
        assertEquals(0, deserialized.getUnassignedSplits().size());
        assertNull(deserialized.getLastLsn());
        assertEquals(false, deserialized.isSnapshotCompleted());
    }

    @Test
    public void testSerializeDeserializeWithMultipleSplits() throws IOException {
        List<GaussDBSplit> assignedSplits = new ArrayList<>();
        assignedSplits.add(new GaussDBSplit("split-0", "student", 1L, 100L));
        assignedSplits.add(new GaussDBSplit("split-1", "student", 101L, 200L));
        assignedSplits.add(new GaussDBSplit("split-2", "student", 201L, 300L));

        List<GaussDBSplit> unassignedSplits = new ArrayList<>();
        unassignedSplits.add(new GaussDBSplit("split-3", "student", 301L, 400L));
        unassignedSplits.add(new GaussDBSplit("split-4", "student", 401L, 500L));

        GaussDBCheckpoint original =
                new GaussDBCheckpoint(assignedSplits, unassignedSplits, "1/ABC", true);

        byte[] serialized = serializer.serialize(original);
        GaussDBCheckpoint deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(3, deserialized.getAssignedSplits().size());
        assertEquals(2, deserialized.getUnassignedSplits().size());
        assertEquals("1/ABC", deserialized.getLastLsn());
        assertTrue(deserialized.isSnapshotCompleted());
    }

    @Test
    public void testGetVersion() {
        assertEquals(1, serializer.getVersion());
    }
}
