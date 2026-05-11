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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link GaussDBCheckpoint}. */
public class GaussDBCheckpointTest {

    @Test
    public void testCheckpointWithSnapshotCompleted() {
        List<GaussDBSplit> assignedSplits = new ArrayList<>();
        assignedSplits.add(new GaussDBSplit("split-0", "student", 1L, 100L));

        List<GaussDBSplit> unassignedSplits = new ArrayList<>();
        unassignedSplits.add(new GaussDBSplit("split-1", "student", 101L, 200L));

        GaussDBCheckpoint checkpoint =
                new GaussDBCheckpoint(assignedSplits, unassignedSplits, "0/12345", true);

        assertNotNull(checkpoint.getAssignedSplits());
        assertEquals(1, checkpoint.getAssignedSplits().size());
        assertNotNull(checkpoint.getUnassignedSplits());
        assertEquals(1, checkpoint.getUnassignedSplits().size());
        assertEquals("0/12345", checkpoint.getLastLsn());
        assertTrue(checkpoint.isSnapshotCompleted());
    }

    @Test
    public void testCheckpointWithSnapshotNotCompleted() {
        List<GaussDBSplit> assignedSplits = new ArrayList<>();
        List<GaussDBSplit> unassignedSplits = new ArrayList<>();
        unassignedSplits.add(new GaussDBSplit("split-0", "student", 1L, 100L));

        GaussDBCheckpoint checkpoint =
                new GaussDBCheckpoint(assignedSplits, unassignedSplits, null, false);

        assertNotNull(checkpoint.getAssignedSplits());
        assertEquals(0, checkpoint.getAssignedSplits().size());
        assertNotNull(checkpoint.getUnassignedSplits());
        assertEquals(1, checkpoint.getUnassignedSplits().size());
        assertNull(checkpoint.getLastLsn());
        assertFalse(checkpoint.isSnapshotCompleted());
    }

    @Test
    public void testCheckpointWithEmptyLists() {
        GaussDBCheckpoint checkpoint =
                new GaussDBCheckpoint(new ArrayList<>(), new ArrayList<>(), "0/67890", true);

        assertNotNull(checkpoint.getAssignedSplits());
        assertEquals(0, checkpoint.getAssignedSplits().size());
        assertNotNull(checkpoint.getUnassignedSplits());
        assertEquals(0, checkpoint.getUnassignedSplits().size());
        assertEquals("0/67890", checkpoint.getLastLsn());
        assertTrue(checkpoint.isSnapshotCompleted());
    }

    @Test
    public void testCheckpointWithMultipleSplits() {
        List<GaussDBSplit> assignedSplits = new ArrayList<>();
        assignedSplits.add(new GaussDBSplit("split-0", "student", 1L, 100L));
        assignedSplits.add(new GaussDBSplit("split-1", "student", 101L, 200L));
        assignedSplits.add(new GaussDBSplit("split-2", "student", 201L, 300L));

        List<GaussDBSplit> unassignedSplits = new ArrayList<>();
        unassignedSplits.add(new GaussDBSplit("split-3", "student", 301L, 400L));
        unassignedSplits.add(new GaussDBSplit("split-4", "student", 401L, 500L));

        GaussDBCheckpoint checkpoint =
                new GaussDBCheckpoint(assignedSplits, unassignedSplits, "1/ABC", true);

        assertEquals(3, checkpoint.getAssignedSplits().size());
        assertEquals(2, checkpoint.getUnassignedSplits().size());
        assertTrue(checkpoint.isSnapshotCompleted());
    }

    @Test
    public void testCheckpointToString() {
        GaussDBCheckpoint checkpoint =
                new GaussDBCheckpoint(new ArrayList<>(), new ArrayList<>(), "0/12345", true);

        String str = checkpoint.toString();
        assertNotNull(str);
        assertTrue(str.contains("GaussDBCheckpoint"));
    }
}
