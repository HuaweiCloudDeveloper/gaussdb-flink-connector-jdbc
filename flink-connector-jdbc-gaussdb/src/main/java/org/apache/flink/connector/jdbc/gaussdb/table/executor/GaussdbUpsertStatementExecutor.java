package org.apache.flink.connector.jdbc.gaussdb.table.executor;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.gaussdb.database.dialect.GaussdbDialectConverter;
import org.apache.flink.connector.jdbc.gaussdb.table.GaussdbExtendOptions;
import org.apache.flink.connector.jdbc.gaussdb.table.GaussdbFieldObject;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Gaussdb upsert statement executor. */
public class GaussdbUpsertStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private final JdbcDialect dialect;
    private final GaussdbExtendOptions options;
    private final String tableName;
    private final String[] fieldNames;
    private final String[] keyFields;
    private Connection connection;
    private FieldNamedPreparedStatement updateStatement;
    private final LogicalType[] fieldTypes;

    // Buffer for ignoreNullWhenUpdate mode: each entry = [fieldNames, reducedRowData]
    private final List<Object[]> bufferedRows;

    public GaussdbUpsertStatementExecutor(
            JdbcDmlOptions opt, GaussdbExtendOptions ept, LogicalType[] fieldTypes) {
        this.dialect = opt.getDialect();
        this.tableName = opt.getTableName();
        this.fieldNames = opt.getFieldNames();
        this.keyFields = opt.getKeyFields().orElse(null);
        this.options = ept;
        this.fieldTypes = fieldTypes;
        this.bufferedRows = options.isIgnoreNullWhenUpdate() ? new ArrayList<>() : null;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
        if (!options.isIgnoreNullWhenUpdate()) {
            // All rows share the same SQL — create statement once for reuse
            String sql = dialect.getUpsertStatement(tableName, fieldNames, keyFields).get();
            this.updateStatement =
                    FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames);
        }
    }

    @Override
    public void addToBatch(RowData rowData) throws SQLException {
        GenericRowData genericRowData = toGenericRowData(rowData);

        if (options.isIgnoreNullWhenUpdate()) {
            // Determine non-null column set for this row
            Map<String, GaussdbFieldObject> indexFieldData = new LinkedHashMap<>();
            for (int i = 0; i < genericRowData.getArity(); i++) {
                if (!genericRowData.isNullAt(i)) {
                    indexFieldData.put(
                            fieldNames[i],
                            new GaussdbFieldObject(i, genericRowData.getField(i)));
                }
            }
            String[] newFieldNames = indexFieldData.keySet().toArray(new String[0]);
            GenericRowData reduced =
                    GenericRowData.ofKind(
                            genericRowData.getRowKind(), indexFieldData.values().toArray());
            bufferedRows.add(new Object[] {newFieldNames, reduced});
        } else {
            // Set values directly using setObject() — avoids incompatible
            // JdbcSerializationConverter.serialize() on MRS jdbc-core 3.2.x
            setRowValues(genericRowData, updateStatement, fieldNames);
            updateStatement.addBatch();
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        if (options.isIgnoreNullWhenUpdate()) {
            executeBuffered();
        } else {
            if (updateStatement != null) {
                updateStatement.executeBatch();
            }
        }
    }

    /**
     * Group buffered rows by column set, then execute each group with its own PreparedStatement.
     * Different rows may have different non-null column sets when ignoreNullWhenUpdate is true,
     * producing different UPSERT SQL — each group gets its own batch.
     */
    private void executeBuffered() throws SQLException {
        if (bufferedRows.isEmpty()) {
            return;
        }

        // Group rows by column set (keyed by comma-joined field names)
        Map<String, List<GenericRowData>> groups = new LinkedHashMap<>();
        Map<String, String[]> columnSets = new LinkedHashMap<>();

        for (Object[] entry : bufferedRows) {
            String[] cols = (String[]) entry[0];
            GenericRowData row = (GenericRowData) entry[1];
            String key = String.join(",", cols);
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            columnSets.putIfAbsent(key, cols);
        }

        // Execute each group with its own statement
        for (Map.Entry<String, List<GenericRowData>> group : groups.entrySet()) {
            String[] cols = columnSets.get(group.getKey());
            List<GenericRowData> rows = group.getValue();

            String sql = dialect.getUpsertStatement(tableName, cols, keyFields).get();
            FieldNamedPreparedStatement stmt =
                    FieldNamedPreparedStatement.prepareStatement(connection, sql, cols);

            for (GenericRowData row : rows) {
                setRowValues(row, stmt, cols);
                stmt.addBatch();
            }

            stmt.executeBatch();
            stmt.close();
        }

        bufferedRows.clear();
    }

    /**
     * Set RowData fields directly on the FieldNamedPreparedStatement using setObject().
     * This avoids calling JdbcSerializationConverter.serialize() which has incompatible
     * method signatures between jdbc-core 3.2.x (MRS: 4-arg) and 3.3.x (3-arg).
     */
    private void setRowValues(GenericRowData row, FieldNamedPreparedStatement stmt, String[] cols)
            throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            if (row.isNullAt(i)) {
                stmt.setNull(i, java.sql.Types.NULL);
            } else {
                Object val = row.getField(i);
                // Unwrap GaussdbFieldObject if present (used by ignoreNullWhenUpdate path)
                if (val instanceof GaussdbFieldObject) {
                    val = ((GaussdbFieldObject) val).getField();
                }
                stmt.setObject(i, GaussdbDialectConverter.toJdbcObject(val));
            }
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (updateStatement != null) {
            updateStatement.close();
            updateStatement = null;
        }
        if (bufferedRows != null) {
            bufferedRows.clear();
        }
    }

    /** Convert any RowData implementation (BinaryRowData, etc.) to GenericRowData. */
    private GenericRowData toGenericRowData(RowData rowData) {
        if (rowData instanceof GenericRowData) {
            return (GenericRowData) rowData;
        }
        GenericRowData result = new GenericRowData(rowData.getArity());
        for (int i = 0; i < rowData.getArity(); i++) {
            if (!rowData.isNullAt(i)) {
                RowData.FieldGetter getter = RowData.createFieldGetter(fieldTypes[i], i);
                result.setField(i, getter.getFieldOrNull(rowData));
            }
        }
        result.setRowKind(rowData.getRowKind());
        return result;
    }
}
