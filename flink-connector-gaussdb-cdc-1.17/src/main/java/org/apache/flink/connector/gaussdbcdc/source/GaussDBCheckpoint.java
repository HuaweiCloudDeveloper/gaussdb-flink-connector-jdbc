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

import java.io.Serializable;
import java.util.List;

/** Checkpoint state for GaussDB CDC source. */
@Internal
public class GaussDBCheckpoint implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<GaussDBSplit> assignedSplits;
    private final List<GaussDBSplit> unassignedSplits;
    private final String lastLsn;
    private final boolean snapshotCompleted;

    public GaussDBCheckpoint(
            List<GaussDBSplit> assignedSplits,
            List<GaussDBSplit> unassignedSplits,
            String lastLsn,
            boolean snapshotCompleted) {
        this.assignedSplits = assignedSplits;
        this.unassignedSplits = unassignedSplits;
        this.lastLsn = lastLsn;
        this.snapshotCompleted = snapshotCompleted;
    }

    public List<GaussDBSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public List<GaussDBSplit> getUnassignedSplits() {
        return unassignedSplits;
    }

    public String getLastLsn() {
        return lastLsn;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public String toString() {
        return "GaussDBCheckpoint{"
                + "assignedSplits="
                + assignedSplits
                + ", unassignedSplits="
                + unassignedSplits
                + ", lastLsn='"
                + lastLsn
                + '\''
                + ", snapshotCompleted="
                + snapshotCompleted
                + '}';
    }
}
