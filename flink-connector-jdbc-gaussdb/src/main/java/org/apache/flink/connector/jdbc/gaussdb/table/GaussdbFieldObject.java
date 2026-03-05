package org.apache.flink.connector.jdbc.gaussdb.table;

/*** GaussdbFieldObject. */

public class GaussdbFieldObject {
    
    private final Object field;
    
    private final int index;

    public GaussdbFieldObject(int index, Object field) {
        this.index = index;
        this.field = field;
    }

    public Object getField() {
        return field;
    }

    public int getIndex() {
        return index;
    }
}
