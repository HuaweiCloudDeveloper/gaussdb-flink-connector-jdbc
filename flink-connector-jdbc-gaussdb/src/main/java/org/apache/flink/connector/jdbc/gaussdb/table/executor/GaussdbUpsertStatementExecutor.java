package org.apache.flink.connector.jdbc.gaussdb.table.executor;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.connector.jdbc.gaussdb.table.GaussdbExtendOptions;
import org.apache.flink.connector.jdbc.gaussdb.table.GaussdbFieldObject;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/*** Gaussdb upsert statement executor.*/

public class GaussdbUpsertStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private JdbcDialect dialect;
    private GaussdbExtendOptions options;
    private String tableName;
    private String[] fieldNames;
    private String[] keyFields;
    private Connection connection;
    private FieldNamedPreparedStatement updateStatement;
    private JdbcDialectConverter updateSetter;

    public GaussdbUpsertStatementExecutor(
            JdbcDmlOptions opt, GaussdbExtendOptions ept, LogicalType[] fieldTypes) {
        this.dialect = opt.getDialect();
        this.tableName = opt.getTableName();
        this.fieldNames = opt.getFieldNames();
        this.keyFields = opt.getKeyFields().orElse(null);
        this.options = ept;
        this.updateSetter = dialect.getRowConverter(RowType.of(fieldTypes));
    }

    @Override
    public void prepareStatements(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void addToBatch(RowData rowData) throws SQLException {
        GenericRowData genericRowData = (GenericRowData) rowData;
        String[] newFieldNames;
        if (options.isIgnoreNullWhenUpdate()) {
            Map<String, GaussdbFieldObject> indexFieldData = new LinkedHashMap<>();
            for (int i = 0; i < genericRowData.getArity(); i++) {
                if (!genericRowData.isNullAt(i)) {
                    indexFieldData.put(
                            fieldNames[i], new GaussdbFieldObject(i, genericRowData.getField(i)));
                }
            }
            newFieldNames = indexFieldData.keySet().toArray(new String[0]);
            genericRowData =
                    GenericRowData.ofKind(
                            genericRowData.getRowKind(), indexFieldData.values().toArray());
        } else {
            newFieldNames = fieldNames;
        }
        String sql = dialect.getUpsertStatement(tableName, newFieldNames, keyFields).get();
        updateStatement =
                FieldNamedPreparedStatement.prepareStatement(connection, sql, newFieldNames);
        updateSetter.toExternal(genericRowData, updateStatement);
        updateStatement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        if (updateStatement != null) {
            updateStatement.executeBatch();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (updateStatement != null) {
            updateStatement.close();
        }
    }
}
