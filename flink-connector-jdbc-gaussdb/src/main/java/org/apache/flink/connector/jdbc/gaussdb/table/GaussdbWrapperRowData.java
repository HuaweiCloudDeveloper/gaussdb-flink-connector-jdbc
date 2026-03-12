package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/** RowData 包装类. */
public class GaussdbWrapperRowData implements RowData {

    private RowData rowData;
    private Integer[] rowIndexes;

    public GaussdbWrapperRowData(RowData rowData, Integer[] rowIndexes) {
        this.rowData = rowData;
        this.rowIndexes = rowIndexes;
    }

    public RowData getRowData() {
        return this.rowData;
    }

    public Integer getFieldIndex(int index) {
        return rowIndexes[index];
    }

    @Override
    public int getArity() {
        return rowIndexes.length;
    }

    @Override
    public RowKind getRowKind() {
        return rowData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        rowData.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return rowData.isNullAt(rowIndexes[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return rowData.getBoolean(rowIndexes[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return rowData.getByte(rowIndexes[pos]);
    }

    @Override
    public short getShort(int pos) {
        return rowData.getShort(rowIndexes[pos]);
    }

    @Override
    public int getInt(int pos) {
        return rowData.getInt(rowIndexes[pos]);
    }

    @Override
    public long getLong(int pos) {
        return rowData.getLong(rowIndexes[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return rowData.getFloat(rowIndexes[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return rowData.getDouble(rowIndexes[pos]);
    }

    @Override
    public StringData getString(int pos) {
        return rowData.getString(rowIndexes[pos]);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return rowData.getDecimal(rowIndexes[pos], precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return rowData.getTimestamp(rowIndexes[pos], precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return rowData.getRawValue(rowIndexes[pos]);
    }

    @Override
    public byte[] getBinary(int pos) {
        return rowData.getBinary(rowIndexes[pos]);
    }

    @Override
    public ArrayData getArray(int pos) {
        return rowData.getArray(rowIndexes[pos]);
    }

    @Override
    public MapData getMap(int pos) {
        return rowData.getMap(rowIndexes[pos]);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return rowData.getRow(rowIndexes[pos], numFields);
    }
}
