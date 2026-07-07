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

import java.util.ArrayList;
import java.util.List;

/** Represents a single change from the WAL (Write-Ahead Log). */
@Internal
public class WalChange {

    /** Type of WAL change event. */
    public enum ChangeType {
        INSERT,
        UPDATE,
        DELETE,
        BEGIN,
        COMMIT,
        UNKNOWN
    }

    /** Represents a single column value in a WAL change event. */
    public static class ColumnValue {
        private final String columnName;
        private final int typeOid;
        private final String value;
        private final boolean isNull;

        public ColumnValue(String columnName, int typeOid, String value, boolean isNull) {
            this.columnName = columnName;
            this.typeOid = typeOid;
            this.value = value;
            this.isNull = isNull;
        }

        public String getColumnName() {
            return columnName;
        }

        public int getTypeOid() {
            return typeOid;
        }

        public String getValue() {
            return value;
        }

        public boolean isNull() {
            return isNull;
        }

        @Override
        public String toString() {
            return "ColumnValue{"
                    + "columnName='"
                    + columnName
                    + '\''
                    + ", typeOid="
                    + typeOid
                    + ", value='"
                    + (isNull ? "NULL" : value)
                    + '\''
                    + '}';
        }
    }

    private String lsn;
    private long xid;
    private ChangeType type;
    private String schema;
    private String table;
    private List<ColumnValue> beforeColumns;
    private List<ColumnValue> afterColumns;
    private String rawData;
    private long csn;

    public WalChange() {
        this.beforeColumns = new ArrayList<>();
        this.afterColumns = new ArrayList<>();
    }

    public static WalChange begin(String lsn, long xid, long csn) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setType(ChangeType.BEGIN);
        change.setCsn(csn);
        return change;
    }

    public static WalChange commit(String lsn, long xid) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setType(ChangeType.COMMIT);
        return change;
    }

    public static WalChange insert(
            String lsn, long xid, String schema, String table, List<ColumnValue> columns) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setType(ChangeType.INSERT);
        change.setSchema(schema);
        change.setTable(table);
        change.setAfterColumns(columns);
        return change;
    }

    public static WalChange update(
            String lsn,
            long xid,
            String schema,
            String table,
            List<ColumnValue> beforeColumns,
            List<ColumnValue> afterColumns) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setType(ChangeType.UPDATE);
        change.setSchema(schema);
        change.setTable(table);
        change.setBeforeColumns(beforeColumns);
        change.setAfterColumns(afterColumns);
        return change;
    }

    public static WalChange delete(
            String lsn, long xid, String schema, String table, List<ColumnValue> columns) {
        WalChange change = new WalChange();
        change.setLsn(lsn);
        change.setXid(xid);
        change.setType(ChangeType.DELETE);
        change.setSchema(schema);
        change.setTable(table);
        change.setBeforeColumns(columns);
        return change;
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

    public List<ColumnValue> getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(List<ColumnValue> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public List<ColumnValue> getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(List<ColumnValue> afterColumns) {
        this.afterColumns = afterColumns;
    }

    public long getCsn() {
        return csn;
    }

    public void setCsn(long csn) {
        this.csn = csn;
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
