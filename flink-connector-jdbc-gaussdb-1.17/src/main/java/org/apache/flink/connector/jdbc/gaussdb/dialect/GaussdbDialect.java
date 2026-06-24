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

package org.apache.flink.connector.jdbc.gaussdb.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JDBC dialect for GaussDB (compatible with PostgreSQL).
 *
 * <p>Notes: Based on PostgresDialect for Flink 1.17.
 */
@Internal
public class GaussdbDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    public GaussdbDialect() {}

    @Override
    public AbstractJdbcRowConverter getRowConverter(RowType rowType) {
        return new GaussdbRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        // Auto-detect which GaussDB JDBC driver is on the classpath.
        // gaussdbjdbc.jar registers com.huawei.gaussdb.jdbc.Driver (jdbc:gaussdb://)
        // gsjdbc4.jar registers org.postgresql.Driver (jdbc:postgresql://)
        try {
            Class.forName("com.huawei.gaussdb.jdbc.Driver");
            return Optional.of("com.huawei.gaussdb.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // fall through
        }
        try {
            Class.forName("org.postgresql.Driver");
            return Optional.of("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            // fall through to default
        }
        return Optional.of("com.huawei.gaussdb.jdbc.Driver");
    }

    @Override
    public String dialectName() {
        return "GaussDB";
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 1000));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 6));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.ARRAY);
    }

    /**
     * GaussDB upsert query using ON DUPLICATE KEY UPDATE syntax.
     *
     * <p>GaussDB does not support PostgreSQL's ON CONFLICT syntax. Always use ON DUPLICATE KEY
     * UPDATE which is GaussDB's native upsert syntax.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        // GaussDB does not support ON CONFLICT, always use ON DUPLICATE KEY UPDATE
        final Set<String> uniqueKeyFieldsSet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !uniqueKeyFieldsSet.contains(f))
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return Optional.of(
                this.getInsertIntoStatement(tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }
}
