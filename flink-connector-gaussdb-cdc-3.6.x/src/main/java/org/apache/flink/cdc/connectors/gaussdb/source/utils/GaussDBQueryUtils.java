/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.rowToArray;

/** Query-related Utilities for GaussDB CDC source. */
public class GaussDBQueryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBQueryUtils.class);

    private GaussDBQueryUtils() {}

    /**
     * Returns the current LSN offset of the GaussDB server. Uses pg_current_xlog_location() for
     * GaussDB compatibility.
     */
    public static GaussDBOffset currentOffset(PostgresConnection jdbc) throws SQLException {
        // GaussDB uses pg_current_xlog_location() instead of pg_current_wal_lsn()
        final String query = "SELECT pg_current_xlog_location()";
        return jdbc.queryAndMap(
                query,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                "No result returned after running query [" + query + "]");
                    }
                    String lsnStr = rs.getString(1);
                    Lsn lsn = Lsn.valueOf(lsnStr);
                    return new GaussDBOffset(lsn.asLong(), null, null);
                });
    }

    /**
     * Returns the committed LSN offset for the given replication slot. Uses
     * pg_current_xlog_location() for GaussDB compatibility.
     */
    public static GaussDBOffset committedOffset(
            PostgresConnection jdbc, String slotName, String pluginName) throws SQLException {
        // GaussDB uses pg_current_xlog_location() instead of pg_current_wal_lsn()
        final String query =
                "SELECT pg_current_xlog_location(), slot_name FROM pg_replication_slots "
                        + "WHERE slot_name = ? AND plugin = ?";
        return jdbc.prepareQueryAndMap(
                query,
                ps -> {
                    ps.setString(1, slotName);
                    ps.setString(2, pluginName);
                },
                rs -> {
                    if (rs.next()) {
                        String lsnStr = rs.getString(1);
                        Lsn lsn = Lsn.valueOf(lsnStr);
                        return new GaussDBOffset(lsn.asLong(), null, null);
                    }
                    return GaussDBOffset.INITIAL_OFFSET;
                });
    }

    /** List all tables in the given database that match the table filter. */
    public static List<TableId> listTables(
            String database, JdbcConnection jdbc, Tables.TableFilter tableFilter)
            throws SQLException {
        final List<TableId> tableIds = new ArrayList<>();
        // Query tables from GaussDB (PG-compatible catalog queries)
        final String query =
                "SELECT n.nspname AS schema_name, c.relname AS table_name "
                        + "FROM pg_class c "
                        + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        + "WHERE c.relkind = 'r' "
                        + "AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'cstore', 'db4ai', 'sqladvisor', 'dbe_perf', 'snapshot', 'pkg_service') "
                        + "ORDER BY n.nspname, c.relname";

        jdbc.query(
                query,
                rs -> {
                    while (rs.next()) {
                        String schemaName = rs.getString(1);
                        String tableName = rs.getString(2);
                        TableId tableId = new TableId(database, schemaName, tableName);
                        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                            tableIds.add(tableId);
                        }
                    }
                });
        LOG.debug(
                "listTables for database={} discovered {} matching tables",
                database,
                tableIds.size());
        return tableIds;
    }

    /** Query table schemas for the given table IDs. */
    public static Map<TableId, TableChanges.TableChange> queryTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds) throws Exception {
        Map<TableId, TableChanges.TableChange> schemas = new HashMap<>();
        for (TableId tableId : tableIds) {
            TableChanges.TableChange tableChange = queryTableSchema(jdbc, tableId);
            if (tableChange != null) {
                schemas.put(tableId, tableChange);
            }
        }
        return schemas;
    }

    /** Query table schema for a single table. */
    public static TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        try {
            Tables tables = new Tables();
            jdbc.readSchema(tables, tableId.catalog(), tableId.schema(), null, null, false);
            // readSchema may create TableId without catalog, so try matching with and without
            // catalog
            Table table = tables.forTable(tableId);
            if (table == null) {
                table = tables.forTable(null, tableId.schema(), tableId.table());
            }
            if (table == null) {
                table = tables.forTable(tableId.catalog(), tableId.schema(), tableId.table());
            }
            if (table != null) {
                TableChanges changes = new TableChanges();
                changes.create(table);
                return changes.iterator().next();
            }
            LOG.warn(
                    "Table {} not found after readSchema. Available tables: {}",
                    tableId,
                    tables.tableIds());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query schema for table " + tableId, e);
        }
        return null;
    }

    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column column)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quoteForMinMax(column), quoteForMinMax(column), quote(tableId));
        return jdbc.queryAndMap(
                minMaxQuery,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        minMaxQuery));
                    }
                    return rowToArray(rs, 2);
                });
    }

    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        final String query =
                "SELECT reltuples::bigint"
                        + " FROM pg_class c"
                        + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = ?"
                        + " AND c.relname = ?";
        return jdbc.prepareQueryAndMap(
                query,
                ps -> {
                    ps.setString(1, tableId.schema());
                    ps.setString(2, tableId.table());
                },
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    LOG.info("queryApproximateRowCnt: {} => {}", query, rs.getLong(1));
                    return rs.getLong(1);
                });
    }

    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column column, Object excludedLowerBound)
            throws SQLException {
        final String query =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > %s",
                        quoteForMinMax(column),
                        quote(tableId),
                        quote(column.name()),
                        castParam(column));
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumn.name());
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= %s ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quoteForMinMax(splitColumn),
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        castParam(splitColumn),
                        quotedColumn,
                        chunkSize);
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, includedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    public static String buildSplitScanQuery(
            org.apache.flink.table.types.logical.RowType pkRowType,
            TableId tableId,
            boolean isFirstSplit,
            boolean isLastSplit,
            List<String> columnNames,
            List<String> uuidFields) {
        return buildSplitScanQuery(
                pkRowType, tableId, isFirstSplit, isLastSplit, columnNames, uuidFields, null);
    }

    public static String buildSplitScanQuery(
            org.apache.flink.table.types.logical.RowType pkRowType,
            TableId tableId,
            boolean isFirstSplit,
            boolean isLastSplit,
            List<String> columnNames,
            List<String> uuidFields,
            List<String> extraConditions) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ", uuidFields);
            sql.append(" AND NOT (");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ", uuidFields);
            sql.append(")");
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ", uuidFields);
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ", uuidFields);
            sql.append(" AND NOT (");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ", uuidFields);
            sql.append(")");
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ", uuidFields);
            condition = sql.toString();
        }

        return buildSelectWithRowLimits(
                tableId,
                columnNames == null ? "*" : String.join(",", columnNames),
                Optional.ofNullable(condition),
                Optional.empty());
    }

    public static PreparedStatement readTableSplitDataStatement(
            JdbcConnection jdbc,
            String sql,
            boolean isFirstSplit,
            boolean isLastSplit,
            Object[] splitStart,
            Object[] splitEnd,
            int primaryKeyNum,
            int fetchSize) {
        try {
            final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
            if (isFirstSplit && isLastSplit) {
                return statement;
            }
            if (isFirstSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitEnd[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                }
            } else if (isLastSplit) {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                }
            } else {
                for (int i = 0; i < primaryKeyNum; i++) {
                    statement.setObject(i + 1, splitStart[i]);
                    statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
                    statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
                }
            }
            return statement;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build the split data read statement.", e);
        }
    }

    public static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String quoteForMinMax(Column column) {
        String quoteColumn = quote(column.name());
        return isUUID(column) ? castToText(quoteColumn) : quoteColumn;
    }

    private static String castParam(Column column) {
        return castParam(isUUID(column));
    }

    private static String castParam(boolean isUUID) {
        return isUUID ? castToUuid("?") : "?";
    }

    private static String castToUuid(String value) {
        return String.format("(%s)::uuid", value);
    }

    private static String castToText(String value) {
        return String.format("(%s)::text", value);
    }

    public static boolean isUUID(Column column) {
        return column.typeName().equals("uuid");
    }

    public static String quote(TableId tableId) {
        return tableId.toQuotedString('"');
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
            throws SQLException {
        final Connection connection = jdbc.connection();
        connection.setAutoCommit(false);
        final PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private static void addPrimaryKeyColumnsToCondition(
            org.apache.flink.table.types.logical.RowType pkRowType,
            StringBuilder sql,
            String predicate,
            List<String> uuidFields) {
        for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
                fieldNamesIt.hasNext(); ) {
            String fieldName = fieldNamesIt.next();
            boolean isUUID = uuidFields.contains(fieldName);
            sql.append(quote(fieldName)).append(predicate).append(castParam(isUUID));
            if (fieldNamesIt.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String buildSelectWithRowLimits(
            TableId tableId,
            String projection,
            Optional<String> condition,
            Optional<String> orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quote(tableId));
        if (condition.isPresent()) {
            sql.append(" WHERE ").append(condition.get());
        }
        if (orderBy.isPresent()) {
            sql.append(" ORDER BY ").append(orderBy.get());
        }
        return sql.toString();
    }
}
