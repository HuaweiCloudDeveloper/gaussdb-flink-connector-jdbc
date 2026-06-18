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

package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.connector.jdbc.gaussdb.GaussdbTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The ITCase for GaussDB JDBC Dynamic Table Sink. */
public class GaussdbJdbcDynamicTableSinkITCase implements GaussdbTestBase {

    @Test
    public void testInsertInto() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "test_insert_sink";

        // Create sink table
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  age INT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        tableName,
                        getMetadata().getJdbcUrl(),
                        tableName,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Create source data
        List<Row> data =
                Arrays.asList(
                        Row.of(1, "Alice", 20), Row.of(2, "Bob", 25), Row.of(3, "Charlie", 30));

        Table sourceTable = tEnv.fromValues(data).as("id", "name", "age");
        tEnv.createTemporaryView("source_table", sourceTable);

        // Execute insert
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table", tableName))
                .await();

        // Verify data
        List<Row> results = queryTable(tableName);
        assertThat(results).hasSize(3);
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.of(1, "Alice", 20), Row.of(2, "Bob", 25), Row.of(3, "Charlie", 30));

        // Cleanup
        dropTable(tableName);
    }

    @Test
    public void testUpsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "test_upsert_sink";

        // Create sink table with upsert mode
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  user_id STRING,"
                                + "  user_name STRING,"
                                + "  balance DECIMAL(18, 2),"
                                + "  PRIMARY KEY (user_id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        tableName,
                        getMetadata().getJdbcUrl(),
                        tableName,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Insert initial data
        List<Row> initialData =
                Arrays.asList(
                        Row.of("user1", "Tom", new BigDecimal("100.00")),
                        Row.of("user2", "Jerry", new BigDecimal("200.00")));

        Table sourceTable1 = tEnv.fromValues(initialData).as("user_id", "user_name", "balance");
        tEnv.createTemporaryView("source_table1", sourceTable1);
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table1", tableName))
                .await();

        // Upsert data - update existing and insert new
        List<Row> upsertData =
                Arrays.asList(
                        Row.of("user1", "Tom Updated", new BigDecimal("150.00")),
                        Row.of("user3", "New User", new BigDecimal("300.00")));

        Table sourceTable2 = tEnv.fromValues(upsertData).as("user_id", "user_name", "balance");
        tEnv.createTemporaryView("source_table2", sourceTable2);
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table2", tableName))
                .await();

        // Verify data
        List<Row> results = queryTable(tableName);
        assertThat(results).hasSize(3);
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.of("user1", "Tom Updated", new BigDecimal("150.00")),
                        Row.of("user2", "Jerry", new BigDecimal("200.00")),
                        Row.of("user3", "New User", new BigDecimal("300.00")));

        // Cleanup
        dropTable(tableName);
    }

    @Test
    public void testAppendMode() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "test_append_sink";

        // Create sink table without primary key (append mode)
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  num BIGINT,"
                                + "  ts TIMESTAMP(3)"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        tableName,
                        getMetadata().getJdbcUrl(),
                        tableName,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Insert data
        List<Row> data =
                Arrays.asList(
                        Row.of(1, 1L, Timestamp.valueOf("1970-01-01 00:00:00.001")),
                        Row.of(2, 2L, Timestamp.valueOf("1970-01-01 00:00:00.002")),
                        Row.of(3, 2L, Timestamp.valueOf("1970-01-01 00:00:00.003")));

        Table sourceTable = tEnv.fromValues(data).as("id", "num", "ts");
        tEnv.createTemporaryView("source_table", sourceTable);
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table", tableName))
                .await();

        // Verify data
        List<Row> results = queryTableWithTimestamp(tableName);
        assertThat(results).hasSize(3);

        // Cleanup
        dropTable(tableName);
    }

    @Test
    public void testIgnoreNullWhenUpdate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        String tableName = "test_ignore_null_sink";

        // Create sink table with ignoreNullWhenUpdate option
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  age INT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver',"
                                + "  'ignore-null-when-update' = 'true'"
                                + ")",
                        tableName,
                        getMetadata().getJdbcUrl(),
                        tableName,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Insert initial data
        List<Row> initialData = Arrays.asList(Row.of(1, "Alice", 20));

        Table sourceTable1 = tEnv.fromValues(initialData).as("id", "name", "age");
        tEnv.createTemporaryView("source_table1", sourceTable1);
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table1", tableName))
                .await();

        // Update with null value - name should not be updated
        List<Row> updateData = Arrays.asList(Row.of(1, null, 25));

        Table sourceTable2 = tEnv.fromValues(updateData).as("id", "name", "age");
        tEnv.createTemporaryView("source_table2", sourceTable2);
        tEnv.executeSql(String.format("INSERT INTO %s SELECT * FROM source_table2", tableName))
                .await();

        // Verify data - name should remain "Alice" because null is ignored
        List<Row> results = queryTable(tableName);
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(Row.of(1, "Alice", 25));

        // Cleanup
        dropTable(tableName);
    }

    private List<Row> queryTable(String tableName) throws SQLException {
        List<Row> results = new ArrayList<>();
        try (Connection conn =
                        DriverManager.getConnection(
                                getMetadata().getJdbcUrl(),
                                getMetadata().getUsername(),
                                getMetadata().getPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = rs.getObject(i + 1);
                }
                results.add(Row.of(values));
            }
        }
        return results;
    }

    private List<Row> queryTableWithTimestamp(String tableName) throws SQLException {
        List<Row> results = new ArrayList<>();
        try (Connection conn =
                        DriverManager.getConnection(
                                getMetadata().getJdbcUrl(),
                                getMetadata().getUsername(),
                                getMetadata().getPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

            while (rs.next()) {
                results.add(Row.of(rs.getInt(1), rs.getLong(2), rs.getTimestamp(3)));
            }
        }
        return results;
    }

    private void dropTable(String tableName) throws SQLException {
        try (Connection conn =
                        DriverManager.getConnection(
                                getMetadata().getJdbcUrl(),
                                getMetadata().getUsername(),
                                getMetadata().getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
