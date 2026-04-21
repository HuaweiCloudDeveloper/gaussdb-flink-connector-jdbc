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

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GaussdbDialect}. */
class GaussdbDialectTest {

    @Test
    void testDialectName() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.dialectName()).isEqualTo("GaussDB");
    }

    @Test
    void testDriverName() {
        GaussdbDialect dialect = new GaussdbDialect();
        Optional<String> driverName = dialect.defaultDriverName();
        assertThat(driverName).isPresent();
        assertThat(driverName.get()).isEqualTo("com.huawei.gaussdb.jdbc.Driver");
    }

    @Test
    void testLimitClause() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.getLimitClause(100)).isEqualTo("LIMIT 100");
    }

    @Test
    void testQuoteIdentifier() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.quoteIdentifier("my_table")).isEqualTo("my_table");
    }

    @Test
    void testInsertIntoStatement() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name", "age"};
        String sql = dialect.getInsertIntoStatement("test_table", fieldNames);
        assertThat(sql)
                .isEqualTo("INSERT INTO test_table(id, name, age) VALUES (:id, :name, :age)");
    }

    @Test
    void testUpsertStatement() {
        // GaussDB always uses ON DUPLICATE KEY UPDATE (does not support ON CONFLICT)
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeys = {"id"};
        Optional<String> upsertSql =
                dialect.getUpsertStatement("test_table", fieldNames, uniqueKeys);
        assertThat(upsertSql).isPresent();
        assertThat(upsertSql.get()).contains("ON DUPLICATE KEY UPDATE");
        assertThat(upsertSql.get()).contains("name=VALUES(name)");
        assertThat(upsertSql.get()).contains("age=VALUES(age)");
    }

    @Test
    void testUpsertStatementAlwaysOnDuplicateKey() {
        // GaussDB always uses ON DUPLICATE KEY UPDATE regardless of mode
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeys = {"id"};
        Optional<String> upsertSql =
                dialect.getUpsertStatement("test_table", fieldNames, uniqueKeys);
        assertThat(upsertSql).isPresent();
        assertThat(upsertSql.get()).contains("ON DUPLICATE KEY UPDATE");
        assertThat(upsertSql.get()).contains("name=VALUES(name)");
        assertThat(upsertSql.get()).contains("age=VALUES(age)");
        // GaussDB does NOT support ON CONFLICT
        assertThat(upsertSql.get()).doesNotContain("ON CONFLICT");
    }

    @Test
    void testDeleteStatement() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] keyFields = {"id"};
        String sql = dialect.getDeleteStatement("test_table", keyFields);
        assertThat(sql).isEqualTo("DELETE FROM test_table WHERE id = :id");
    }

    @Test
    void testSelectStatement() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name"};
        String sql = dialect.getSelectFromStatement("test_table", fieldNames, new String[0]);
        assertThat(sql).isEqualTo("SELECT id, name FROM test_table");
    }

    @Test
    void testRowConverter() {
        GaussdbDialect dialect = new GaussdbDialect();
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType()
                        },
                        new String[] {"id", "name"});
        assertThat(dialect.getRowConverter(rowType)).isNotNull();
    }

    @Test
    void testDialectLoader() {
        // Test that the dialect can be loaded via SPI
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        "jdbc:gaussdb://localhost:5432/test",
                        Thread.currentThread().getContextClassLoader());
        assertThat(dialect).isInstanceOf(GaussdbDialect.class);
        assertThat(dialect.dialectName()).isEqualTo("GaussDB");
    }

    @Test
    void testSupportedTypes() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.supportedTypes()).isNotEmpty();
        assertThat(dialect.supportedTypes())
                .contains(
                        org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER,
                        org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR,
                        org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT);
    }

    @Test
    void testDecimalPrecisionRange() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.decimalPrecisionRange()).isPresent();
    }

    @Test
    void testTimestampPrecisionRange() {
        GaussdbDialect dialect = new GaussdbDialect();
        assertThat(dialect.timestampPrecisionRange()).isPresent();
    }

    @Test
    void testUpsertStatementWithMultipleUniqueKeys() {
        // GaussDB always uses ON DUPLICATE KEY UPDATE
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "user_id", "name", "age"};
        String[] uniqueKeys = {"id", "user_id"};
        Optional<String> upsertSql =
                dialect.getUpsertStatement("test_table", fieldNames, uniqueKeys);
        assertThat(upsertSql).isPresent();
        assertThat(upsertSql.get()).contains("ON DUPLICATE KEY UPDATE");
        // Unique keys should not be in UPDATE clause
        assertThat(upsertSql.get()).doesNotContain("id=VALUES(id)");
        assertThat(upsertSql.get()).doesNotContain("user_id=VALUES(user_id)");
        // Non-unique fields should be in UPDATE clause
        assertThat(upsertSql.get()).contains("name=VALUES(name)");
        assertThat(upsertSql.get()).contains("age=VALUES(age)");
    }

    @Test
    void testSelectWithWhereClause() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name"};
        String[] conditionFields = {"id"};
        String sql = dialect.getSelectFromStatement("test_table", fieldNames, conditionFields);
        assertThat(sql).isEqualTo("SELECT id, name FROM test_table WHERE id = :id");
    }

    @Test
    void testSelectWithMultipleWhereConditions() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] conditionFields = {"name", "age"};
        String sql = dialect.getSelectFromStatement("test_table", fieldNames, conditionFields);
        assertThat(sql)
                .isEqualTo(
                        "SELECT id, name, age FROM test_table WHERE name = :name AND age = :age");
    }

    @Test
    void testDeleteWithMultipleKeys() {
        GaussdbDialect dialect = new GaussdbDialect();
        String[] keyFields = {"id", "user_id"};
        String sql = dialect.getDeleteStatement("test_table", keyFields);
        assertThat(sql).isEqualTo("DELETE FROM test_table WHERE id = :id AND user_id = :user_id");
    }

    @Test
    void testDialectFactoryAcceptsURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:gaussdb://localhost:5432/test")).isTrue();
        assertThat(factory.acceptsURL("jdbc:postgresql://localhost:5432/test")).isFalse();
        assertThat(factory.acceptsURL("jdbc:mysql://localhost:3306/test")).isFalse();
    }

    @Test
    void testDialectFactoryCreate() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        JdbcDialect dialect = factory.create();
        assertThat(dialect).isInstanceOf(GaussdbDialect.class);
        assertThat(dialect.dialectName()).isEqualTo("GaussDB");
    }
}
