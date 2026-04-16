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

package org.apache.flink.connector.gaussdbcdc.source.wal;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Map;

/** Represents a single change from the WAL (Write-Ahead Log). */
@Internal
public class WalChange {

    public enum ChangeType {
        INSERT,
        UPDATE,
        DELETE,
        BEGIN,
        COMMIT,
        UNKNOWN
    }

    private String lsn;
    private long xid;
    private ChangeType type;
    private String schema;
    private String table;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private String rawData;

    public WalChange() {
        this.before = new HashMap<>();
        this.after = new HashMap<>();
    }

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String lsn) {
        this.lsn = lsn;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public ChangeType getType() {
        return type;
    }

    public void setType(ChangeType type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public String getRawData() {
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public boolean isDataChange() {
        return type == ChangeType.INSERT || type == ChangeType.UPDATE || type == ChangeType.DELETE;
    }

    @Override
    public String toString() {
        return "WalChange{"
                + "lsn='"
                + lsn
                + '\''
                + ", xid="
                + xid
                + ", type="
                + type
                + ", schema='"
                + schema
                + '\''
                + ", table='"
                + table
                + '\''
                + '}';
    }
}
