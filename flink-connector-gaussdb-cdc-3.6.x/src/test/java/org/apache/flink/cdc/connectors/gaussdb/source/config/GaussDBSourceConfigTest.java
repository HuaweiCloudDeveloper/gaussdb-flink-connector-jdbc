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

package org.apache.flink.cdc.connectors.gaussdb.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBSourceConfig}. */
class GaussDBSourceConfigTest {

    private GaussDBSourceConfig createTestConfig() {
        Properties props = new Properties();
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("plugin.name", "mppdb_decoding");
        props.setProperty("database.server.name", "gaussdb_cdc_source");
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.dbname", "testdb");
        props.setProperty("database.user", "testuser");
        props.setProperty("database.password", "testpass");
        props.setProperty("database.port", "8000");
        props.setProperty("slot.name", "flink_slot");
        props.setProperty("snapshot.mode", "never");

        Configuration dbzConfig = Configuration.from(props);

        return new GaussDBSourceConfig(
                0,
                StartupOptions.initial(),
                Collections.singletonList("testdb"),
                Collections.singletonList("public"),
                Collections.singletonList("public.test_table"),
                8096,
                1000,
                1000.0d,
                0.05d,
                false,
                false,
                props,
                dbzConfig,
                "com.huawei.gaussdb.jdbc.Driver",
                "localhost",
                8000,
                "testuser",
                "testpass",
                1024,
                "UTC",
                Duration.ofSeconds(30),
                3,
                10,
                null,
                false,
                false,
                3,
                false,
                true,
                1,
                "b",
                false,
                null);
    }

    @Test
    void testGetSubtaskId() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getSubtaskId()).isEqualTo(0);
    }

    @Test
    void testGetLsnCommitCheckpointsDelay() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getLsnCommitCheckpointsDelay()).isEqualTo(3);
    }

    @Test
    void testIsIncludeDatabaseInTableId() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.isIncludeDatabaseInTableId()).isTrue();
    }

    @Test
    void testGetParallelDecodeNum() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getParallelDecodeNum()).isEqualTo(1);
    }

    @Test
    void testGetDecodeStyle() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getDecodeStyle()).isEqualTo("b");
    }

    @Test
    void testIsSendingBatch() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.isSendingBatch()).isFalse();
    }

    @Test
    void testGetSlotNameForBackfillTask() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getSlotNameForBackfillTask()).isEqualTo("flink_slot_0");
    }

    @Test
    void testGetJdbcUrl() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getJdbcUrl()).isEqualTo("jdbc:gaussdb://localhost:8000/testdb");
    }

    @Test
    void testGetDbzConnectorConfig() {
        GaussDBSourceConfig config = createTestConfig();
        PostgresConnectorConfig pgConfig = config.getDbzConnectorConfig();
        assertThat(pgConfig).isNotNull();
        assertThat(pgConfig.getLogicalName()).isEqualTo("gaussdb_cdc_source");
    }

    @Test
    void testGetHostname() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getHostname()).isEqualTo("localhost");
    }

    @Test
    void testGetPort() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getPort()).isEqualTo(8000);
    }

    @Test
    void testGetDatabaseList() {
        GaussDBSourceConfig config = createTestConfig();
        assertThat(config.getDatabaseList()).containsExactly("testdb");
    }
}
