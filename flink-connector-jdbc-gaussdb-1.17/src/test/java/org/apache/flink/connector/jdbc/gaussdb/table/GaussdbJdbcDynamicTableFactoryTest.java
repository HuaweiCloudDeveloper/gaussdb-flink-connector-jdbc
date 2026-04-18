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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.gaussdb.dialect.GaussdbDialect;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussdbJdbcDynamicTableFactory}. */
class GaussdbJdbcDynamicTableFactoryTest {

    private final GaussdbJdbcDynamicTableFactory factory = new GaussdbJdbcDynamicTableFactory();

    @Test
    void testFactoryIdentifier() {
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb");
    }

    @Test
    void testRequiredOptions() {
        Set<ConfigOption<?>> requiredOptions = factory.requiredOptions();
        assertThat(requiredOptions).isNotNull();
        assertThat(requiredOptions).hasSize(2);
    }

    @Test
    void testOptionalOptions() {
        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertThat(optionalOptions).isNotNull();
        assertThat(optionalOptions).hasSize(8);
    }

    @Test
    void testImplementsSourceFactory() {
        assertThat(factory).isInstanceOf(DynamicTableSourceFactory.class);
    }

    @Test
    void testImplementsSinkFactory() {
        assertThat(factory).isInstanceOf(DynamicTableSinkFactory.class);
    }

    @Test
    void testGetJdbcReadOptions() throws Exception {
        Method method =
                GaussdbJdbcDynamicTableFactory.class.getDeclaredMethod(
                        "getJdbcReadOptions", org.apache.flink.configuration.ReadableConfig.class);
        method.setAccessible(true);

        Configuration config = new Configuration();
        config.set(org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_FETCH_SIZE, 100);
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_AUTO_COMMIT, true);

        Object result = method.invoke(factory, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetJdbcOptions() throws Exception {
        Method method =
                GaussdbJdbcDynamicTableFactory.class.getDeclaredMethod(
                        "getJdbcOptions",
                        org.apache.flink.configuration.ReadableConfig.class,
                        ClassLoader.class);
        method.setAccessible(true);

        Configuration config = new Configuration();
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL,
                "jdbc:gaussdb://localhost:8000/test?compatibleMode=mysql");
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME, "mytable");
        config.set(org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME, "user");
        config.set(org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD, "pass");

        Object result =
                method.invoke(factory, config, Thread.currentThread().getContextClassLoader());
        assertThat(result).isInstanceOf(InternalJdbcConnectionOptions.class);

        InternalJdbcConnectionOptions jdbcOpts = (InternalJdbcConnectionOptions) result;
        assertThat(jdbcOpts.getDbURL())
                .isEqualTo("jdbc:gaussdb://localhost:8000/test?compatibleMode=mysql");
        assertThat(jdbcOpts.getTableName()).isEqualTo("mytable");
        assertThat(jdbcOpts.getDialect()).isInstanceOf(GaussdbDialect.class);
    }

    @Test
    void testGetJdbcExecutionOptions() throws Exception {
        Method method =
                GaussdbJdbcDynamicTableFactory.class.getDeclaredMethod(
                        "getJdbcExecutionOptions",
                        org.apache.flink.configuration.ReadableConfig.class);
        method.setAccessible(true);

        Configuration config = new Configuration();
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions
                        .SINK_BUFFER_FLUSH_MAX_ROWS,
                1000);
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions
                        .SINK_BUFFER_FLUSH_INTERVAL,
                java.time.Duration.ofSeconds(5));
        config.set(org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES, 3);

        Object result = method.invoke(factory, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetJdbcDmlOptions() throws Exception {
        // First create jdbcOptions
        Method getJdbcOptionsMethod =
                GaussdbJdbcDynamicTableFactory.class.getDeclaredMethod(
                        "getJdbcOptions",
                        org.apache.flink.configuration.ReadableConfig.class,
                        ClassLoader.class);
        getJdbcOptionsMethod.setAccessible(true);

        Configuration config = new Configuration();
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL,
                "jdbc:gaussdb://localhost:8000/test");
        config.set(
                org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME, "mytable");

        InternalJdbcConnectionOptions jdbcOpts =
                (InternalJdbcConnectionOptions)
                        getJdbcOptionsMethod.invoke(
                                factory, config, Thread.currentThread().getContextClassLoader());

        Method getDmlMethod =
                GaussdbJdbcDynamicTableFactory.class.getDeclaredMethod(
                        "getJdbcDmlOptions",
                        InternalJdbcConnectionOptions.class,
                        org.apache.flink.table.types.DataType.class,
                        int[].class);
        getDmlMethod.setAccessible(true);

        org.apache.flink.table.types.DataType dataType =
                org.apache.flink.table.api.DataTypes.ROW(
                        org.apache.flink.table.api.DataTypes.FIELD(
                                "id", org.apache.flink.table.api.DataTypes.INT()),
                        org.apache.flink.table.api.DataTypes.FIELD(
                                "name", org.apache.flink.table.api.DataTypes.VARCHAR(255)));

        // Test with primary key
        Object result = getDmlMethod.invoke(factory, jdbcOpts, dataType, new int[] {0});
        assertThat(result).isNotNull();

        // Test without primary key
        Object resultNoKey = getDmlMethod.invoke(factory, jdbcOpts, dataType, new int[] {});
        assertThat(resultNoKey).isNotNull();
    }

    @Test
    void testIdentifierConstant() {
        assertThat(GaussdbJdbcDynamicTableFactory.IDENTIFIER).isEqualTo("gaussdb");
    }
}
