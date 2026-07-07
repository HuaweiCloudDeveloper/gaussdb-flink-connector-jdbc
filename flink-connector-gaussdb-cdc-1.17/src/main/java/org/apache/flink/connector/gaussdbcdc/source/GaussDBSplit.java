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
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/** A split representing either a snapshot chunk or a WAL stream. */
@Internal
public class GaussDBSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** Type of source split. */
    public enum SplitType {
        SNAPSHOT, // Initial snapshot chunk
        STREAM // WAL streaming
    }

    private final String splitId;
    private final SplitType splitType;
    private final String tableName;

    // For snapshot splits
    private final Long startId;
    private final Long endId;

    // For stream splits
    private final String slotName;
    private final String startLsn;

    // Snapshot split constructor
    public GaussDBSplit(String splitId, String tableName, long startId, long endId) {
        this.splitId = splitId;
        this.splitType = SplitType.SNAPSHOT;
        this.tableName = tableName;
        this.startId = startId;
        this.endId = endId;
        this.slotName = null;
        this.startLsn = null;
    }

    // Stream split constructor
    public GaussDBSplit(String splitId, String tableName, String slotName, String startLsn) {
        this.splitId = splitId;
        this.splitType = SplitType.STREAM;
        this.tableName = tableName;
        this.startId = null;
        this.endId = null;
        this.slotName = slotName;
        this.startLsn = startLsn;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public SplitType getSplitType() {
        return splitType;
    }

    public String getTableName() {
        return tableName;
    }

    public Long getStartId() {
        return startId;
    }

    public Long getEndId() {
        return endId;
    }

    public String getSlotName() {
        return slotName;
    }

    public String getStartLsn() {
        return startLsn;
    }

    public boolean isSnapshotSplit() {
        return splitType == SplitType.SNAPSHOT;
    }

    public boolean isStreamSplit() {
        return splitType == SplitType.STREAM;
    }

    @Override
    public String toString() {
        return "GaussDBSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", splitType="
                + splitType
                + ", tableName='"
                + tableName
                + '\''
                + ", startId="
                + startId
                + ", endId="
                + endId
                + ", slotName='"
                + slotName
                + '\''
                + ", startLsn='"
                + startLsn
                + '\''
                + '}';
    }
}
