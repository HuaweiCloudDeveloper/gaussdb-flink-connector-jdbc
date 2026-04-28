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

/**
 * Represents a change event from GaussDB.
 *
 * @param <T> The type of data (before/after row)
 */
@Internal
public class ChangeEvent<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Type of change event. */
    public enum ChangeType {
        /** Insert event - new row inserted. */
        INSERT,

        /** Update event - existing row updated. */
        UPDATE,

        /** Delete event - row deleted. */
        DELETE,

        /** Snapshot read - initial data. */
        SNAPSHOT
    }

    private final ChangeType changeType;
    private final String tableName;
    private final T before;
    private final T after;
    private final long timestamp;

    // Constructor for INSERT
    public static <T> ChangeEvent<T> insert(String tableName, T after, long timestamp) {
        return new ChangeEvent<>(ChangeType.INSERT, tableName, null, after, timestamp);
    }

    // Constructor for UPDATE
    public static <T> ChangeEvent<T> update(String tableName, T before, T after, long timestamp) {
        return new ChangeEvent<>(ChangeType.UPDATE, tableName, before, after, timestamp);
    }

    // Constructor for DELETE
    public static <T> ChangeEvent<T> delete(String tableName, T before, long timestamp) {
        return new ChangeEvent<>(ChangeType.DELETE, tableName, before, null, timestamp);
    }

    // Constructor for SNAPSHOT
    public static <T> ChangeEvent<T> snapshot(String tableName, T data, long timestamp) {
        return new ChangeEvent<>(ChangeType.SNAPSHOT, tableName, null, data, timestamp);
    }

    private ChangeEvent(
            ChangeType changeType, String tableName, T before, T after, long timestamp) {
        this.changeType = changeType;
        this.tableName = tableName;
        this.before = before;
        this.after = after;
        this.timestamp = timestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public String getTableName() {
        return tableName;
    }

    public T getBefore() {
        return before;
    }

    public T getAfter() {
        return after;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isInsert() {
        return changeType == ChangeType.INSERT;
    }

    public boolean isUpdate() {
        return changeType == ChangeType.UPDATE;
    }

    public boolean isDelete() {
        return changeType == ChangeType.DELETE;
    }

    public boolean isSnapshot() {
        return changeType == ChangeType.SNAPSHOT;
    }

    @Override
    public String toString() {
        return "ChangeEvent{"
                + "changeType="
                + changeType
                + ", tableName='"
                + tableName
                + '\''
                + ", before="
                + before
                + ", after="
                + after
                + ", timestamp="
                + timestamp
                + '}';
    }
}
