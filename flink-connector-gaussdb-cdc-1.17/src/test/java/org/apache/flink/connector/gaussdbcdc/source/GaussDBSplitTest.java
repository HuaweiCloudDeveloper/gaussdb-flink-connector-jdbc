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

import static org.junit.Assert.*;

/** Unit tests for {@link GaussDBSplit}. */
public class GaussDBSplitTest {

    @Test
    public void testSnapshotSplit() {
        GaussDBSplit split = new GaussDBSplit("split-0", "student", 1L, 1000L);

        assertEquals("split-0", split.splitId());
        assertEquals("student", split.getTableName());
        assertEquals(1L, split.getStartId().longValue());
        assertEquals(1000L, split.getEndId().longValue());
        assertTrue(split.isSnapshotSplit());
        assertFalse(split.isStreamSplit());
        assertNull(split.getSlotName());
        assertNull(split.getStartLsn());
    }

    @Test
    public void testStreamSplit() {
        GaussDBSplit split = new GaussDBSplit("stream-split", "student", "flink_slot", "0/12345");

        assertEquals("stream-split", split.splitId());
        assertEquals("student", split.getTableName());
        assertFalse(split.isSnapshotSplit());
        assertTrue(split.isStreamSplit());
        assertEquals("flink_slot", split.getSlotName());
        assertEquals("0/12345", split.getStartLsn());
        assertNull(split.getStartId());
        assertNull(split.getEndId());
    }

    @Test
    public void testSplitTypeEnum() {
        assertEquals(GaussDBSplit.SplitType.SNAPSHOT, GaussDBSplit.SplitType.valueOf("SNAPSHOT"));
        assertEquals(GaussDBSplit.SplitType.STREAM, GaussDBSplit.SplitType.valueOf("STREAM"));
    }

    @Test
    public void testSplitToString() {
        GaussDBSplit snapshotSplit = new GaussDBSplit("split-0", "student", 1L, 1000L);
        String str = snapshotSplit.toString();

        assertNotNull(str);
        assertTrue(str.contains("split-0"));
        assertTrue(str.contains("SNAPSHOT"));
        assertTrue(str.contains("student"));
    }

    @Test
    public void testStreamSplitToString() {
        GaussDBSplit streamSplit =
                new GaussDBSplit("stream-split", "student", "flink_slot", "0/12345");
        String str = streamSplit.toString();

        assertNotNull(str);
        assertTrue(str.contains("stream-split"));
        assertTrue(str.contains("STREAM"));
        assertTrue(str.contains("flink_slot"));
    }
}
