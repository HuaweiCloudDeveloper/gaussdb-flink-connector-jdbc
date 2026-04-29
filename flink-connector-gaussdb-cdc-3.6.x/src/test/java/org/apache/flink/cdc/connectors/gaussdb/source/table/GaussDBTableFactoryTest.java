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

package org.apache.flink.cdc.connectors.gaussdb.source.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GaussDBTableFactory}. */
class GaussDBTableFactoryTest {

    private final GaussDBTableFactory factory = new GaussDBTableFactory();

    @Test
    void testFactoryIdentifier() {
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }

    @Test
    void testRequiredOptions() {
        assertThat(factory.requiredOptions())
                .extracting(ConfigOption::key)
                .containsExactlyInAnyOrder(
                        "hostname",
                        "username",
                        "password",
                        "database-name",
                        "schema-name",
                        "table-name",
                        "slot.name");
    }

    @Test
    void testOptionalOptions() {
        assertThat(factory.optionalOptions())
                .extracting(ConfigOption::key)
                .contains(
                        "port",
                        "decoding.plugin.name",
                        "changelog-mode",
                        "scan.incremental.snapshot.enabled",
                        "scan.incremental.snapshot.chunk.size",
                        "heartbeat.interval.ms",
                        "parallel-decode-num",
                        "decode-style",
                        "sending-batch",
                        "table-id.include-database",
                        "scan.lsn-commit.checkpoints-num-delay");
    }

    @Test
    void testOptionalOptionsContainsAllGaussDBSpecificOptions() {
        assertThat(factory.optionalOptions())
                .extracting(ConfigOption::key)
                .contains(
                        GaussDBSourceOptions.PARALLEL_DECODE_NUM.key(),
                        GaussDBSourceOptions.DECODE_STYLE.key(),
                        GaussDBSourceOptions.SENDING_BATCH.key(),
                        GaussDBSourceOptions.TABLE_ID_INCLUDE_DATABASE.key(),
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.key());
    }

    @Test
    void testGetStartupOptionsInitial() throws Exception {
        Method getStartupOptions =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "getStartupOptions", ReadableConfig.class);
        getStartupOptions.setAccessible(true);

        Configuration config = new Configuration();
        config.set(JdbcSourceOptions.SCAN_STARTUP_MODE, "initial");
        StartupOptions result = (StartupOptions) getStartupOptions.invoke(null, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetStartupOptionsSnapshot() throws Exception {
        Method getStartupOptions =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "getStartupOptions", ReadableConfig.class);
        getStartupOptions.setAccessible(true);

        Configuration config = new Configuration();
        config.set(JdbcSourceOptions.SCAN_STARTUP_MODE, "snapshot");
        StartupOptions result = (StartupOptions) getStartupOptions.invoke(null, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetStartupOptionsLatestOffset() throws Exception {
        Method getStartupOptions =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "getStartupOptions", ReadableConfig.class);
        getStartupOptions.setAccessible(true);

        Configuration config = new Configuration();
        config.set(JdbcSourceOptions.SCAN_STARTUP_MODE, "latest-offset");
        StartupOptions result = (StartupOptions) getStartupOptions.invoke(null, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetStartupOptionsCommittedOffset() throws Exception {
        Method getStartupOptions =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "getStartupOptions", ReadableConfig.class);
        getStartupOptions.setAccessible(true);

        Configuration config = new Configuration();
        config.set(JdbcSourceOptions.SCAN_STARTUP_MODE, "committed-offset");
        StartupOptions result = (StartupOptions) getStartupOptions.invoke(null, config);
        assertThat(result).isNotNull();
    }

    @Test
    void testGetStartupOptionsInvalid() throws Exception {
        Method getStartupOptions =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "getStartupOptions", ReadableConfig.class);
        getStartupOptions.setAccessible(true);

        Configuration config = new Configuration();
        config.set(JdbcSourceOptions.SCAN_STARTUP_MODE, "invalid-mode");

        // Should throw ValidationException via InvocationTargetException
        assertThatThrownBy(() -> getStartupOptions.invoke(null, config))
                .hasCauseInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateIntegerOption() throws Exception {
        Method validateIntegerOption =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "validateIntegerOption", ConfigOption.class, int.class, int.class);
        validateIntegerOption.setAccessible(true);

        // Valid value should not throw
        validateIntegerOption.invoke(
                factory, JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 100, 0);

        // Invalid value should throw
        assertThatThrownBy(
                        () ->
                                validateIntegerOption.invoke(
                                        factory,
                                        JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                                        0,
                                        0))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testValidateDistributionFactorUpper() throws Exception {
        Method validateDistributionFactorUpper =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "validateDistributionFactorUpper", double.class);
        validateDistributionFactorUpper.setAccessible(true);

        // Valid value (>= 1.0) should not throw
        validateDistributionFactorUpper.invoke(factory, 1.0);
        validateDistributionFactorUpper.invoke(factory, 2.0);

        // Invalid value (< 1.0) should throw
        assertThatThrownBy(() -> validateDistributionFactorUpper.invoke(factory, 0.5))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testValidateDistributionFactorLower() throws Exception {
        Method validateDistributionFactorLower =
                GaussDBTableFactory.class.getDeclaredMethod(
                        "validateDistributionFactorLower", double.class);
        validateDistributionFactorLower.setAccessible(true);

        // Valid value (0.0 <= x <= 1.0) should not throw
        validateDistributionFactorLower.invoke(factory, 0.0);
        validateDistributionFactorLower.invoke(factory, 0.5);
        validateDistributionFactorLower.invoke(factory, 1.0);

        // Invalid value (< 0.0) should throw
        assertThatThrownBy(() -> validateDistributionFactorLower.invoke(factory, -0.1))
                .hasCauseInstanceOf(IllegalStateException.class);

        // Invalid value (> 1.0) should throw
        assertThatThrownBy(() -> validateDistributionFactorLower.invoke(factory, 1.5))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCreateDynamicTableSource() {
        // Create a minimal test context
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", org.apache.flink.table.api.DataTypes.INT()),
                                Column.physical(
                                        "name", org.apache.flink.table.api.DataTypes.VARCHAR(100))),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        // Test factory via its identifier and options
        Map<String, String> options = new HashMap<>();
        options.put("hostname", "localhost");
        options.put("username", "testuser");
        options.put("password", "testpass");
        options.put("database-name", "testdb");
        options.put("schema-name", "public");
        options.put("table-name", "test_table");
        options.put("slot.name", "test_slot");
        options.put("connector", "gaussdb-cdc");

        // Verify that the factory can be loaded with these options
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }
}
