/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements... */

package org.apache.flink.connector.jdbc.gaussdb.table;

/** Field object for GaussDB. */
public class GaussdbFieldObject {
    private final int index;
    private final Object value;

    public GaussdbFieldObject(int index, Object value) {
        this.index = index;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public Object getValue() {
        return value;
    }
}
