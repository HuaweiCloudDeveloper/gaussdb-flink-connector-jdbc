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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link GaussDBSplitSerializer}. */
public class GaussDBSplitSerializerTest {

    private final GaussDBSplitSerializer serializer = new GaussDBSplitSerializer();

    @Test
    public void testSerializeDeserializeSnapshotSplit() throws IOException {
        GaussDBSplit original = new GaussDBSplit("split-0", "student", 1L, 1000L);

        byte[] serialized = serializer.serialize(original);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        GaussDBSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(original.splitId(), deserialized.splitId());
        assertEquals(original.getTableName(), deserialized.getTableName());
        assertEquals(original.getStartId(), deserialized.getStartId());
        assertEquals(original.getEndId(), deserialized.getEndId());
        assertEquals(original.getSplitType(), deserialized.getSplitType());
        assertTrue(deserialized.isSnapshotSplit());
    }

    @Test
    public void testSerializeDeserializeStreamSplit() throws IOException {
        GaussDBSplit original =
                new GaussDBSplit("stream-split", "student", "flink_slot", "0/12345");

        byte[] serialized = serializer.serialize(original);
        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        GaussDBSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(original.splitId(), deserialized.splitId());
        assertEquals(original.getTableName(), deserialized.getTableName());
        assertEquals(original.getSlotName(), deserialized.getSlotName());
        assertEquals(original.getStartLsn(), deserialized.getStartLsn());
        assertEquals(original.getSplitType(), deserialized.getSplitType());
        assertTrue(deserialized.isStreamSplit());
    }

    @Test
    public void testSerializeDeserializeStreamSplitWithNullLsn() throws IOException {
        GaussDBSplit original = new GaussDBSplit("stream-split", "student", "flink_slot", null);

        byte[] serialized = serializer.serialize(original);
        GaussDBSplit deserialized = serializer.deserialize(serializer.getVersion(), serialized);

        assertNull(deserialized.getStartLsn());
        assertEquals("flink_slot", deserialized.getSlotName());
    }

    @Test(expected = IOException.class)
    public void testDeserializeInvalidVersion() throws IOException {
        byte[] data = new byte[] {0, 1, 2, 3};
        serializer.deserialize(999, data); // Invalid version
    }

    @Test
    public void testGetVersion() {
        assertEquals(1, serializer.getVersion());
    }
}
