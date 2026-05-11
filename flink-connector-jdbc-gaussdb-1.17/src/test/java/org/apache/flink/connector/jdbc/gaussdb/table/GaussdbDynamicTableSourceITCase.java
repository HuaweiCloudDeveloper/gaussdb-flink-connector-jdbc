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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The ITCase for GaussDB JDBC Dynamic Table Source. */
public class GaussdbDynamicTableSourceITCase implements GaussdbTestBase {

    private static final String TEST_TABLE = "test_source_table";

    @BeforeEach
    void setUp() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                getMetadata().getJdbcUrl(),
                                getMetadata().getUsername(),
                                getMetadata().getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE "
                            + TEST_TABLE
                            + " (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
            stmt.execute("INSERT INTO " + TEST_TABLE + " VALUES (1, 'Alice', 20), (2, 'Bob', 25)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn =
                        DriverManager.getConnection(
                                getMetadata().getJdbcUrl(),
                                getMetadata().getUsername(),
                                getMetadata().getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
        }
    }

    @Test
    public void testSelect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // Create source table
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  age INT"
                                + ") WITH ("
                                + "  'connector' = 'jdbc',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        TEST_TABLE,
                        getMetadata().getJdbcUrl(),
                        TEST_TABLE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Query data
        Table result = tEnv.sqlQuery("SELECT * FROM " + TEST_TABLE);
        List<Row> results = new ArrayList<>();
        result.execute().collect().forEachRemaining(results::add);

        assertThat(results).hasSize(2);
        assertThat(results).containsExactlyInAnyOrder(Row.of(1, "Alice", 20), Row.of(2, "Bob", 25));
    }

    @Test
    public void testFilterPushDown() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // Create source table
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  age INT"
                                + ") WITH ("
                                + "  'connector' = 'jdbc',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        TEST_TABLE,
                        getMetadata().getJdbcUrl(),
                        TEST_TABLE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Query with filter
        Table result = tEnv.sqlQuery("SELECT * FROM " + TEST_TABLE + " WHERE id = 1");
        List<Row> results = new ArrayList<>();
        result.execute().collect().forEachRemaining(results::add);

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(Row.of(1, "Alice", 20));
    }

    @Test
    public void testProjectionPushDown() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // Create source table
        String createTableDDL =
                String.format(
                        "CREATE TABLE %s ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  age INT"
                                + ") WITH ("
                                + "  'connector' = 'jdbc',"
                                + "  'url' = '%s',"
                                + "  'table-name' = '%s',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'driver' = 'com.huawei.gaussdb.jdbc.Driver'"
                                + ")",
                        TEST_TABLE,
                        getMetadata().getJdbcUrl(),
                        TEST_TABLE,
                        getMetadata().getUsername(),
                        getMetadata().getPassword());

        tEnv.executeSql(createTableDDL);

        // Query with projection
        Table result = tEnv.sqlQuery("SELECT id, name FROM " + TEST_TABLE);
        List<Row> results = new ArrayList<>();
        result.execute().collect().forEachRemaining(results::add);

        assertThat(results).hasSize(2);
        assertThat(results).containsExactlyInAnyOrder(Row.of(1, "Alice"), Row.of(2, "Bob"));
    }
}
