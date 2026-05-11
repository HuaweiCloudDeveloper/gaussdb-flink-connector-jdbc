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

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GaussDBSourceConfigFactory}. */
class GaussDBSourceConfigFactoryTest {

    private GaussDBSourceConfigFactory factory;

    @BeforeEach
    void setUp() {
        factory = new GaussDBSourceConfigFactory();
        factory.hostname("localhost");
        factory.database("testdb");
        factory.username("testuser");
        factory.password("testpass");
        factory.port(8000);
        factory.tableList("public.test_table");
    }

    @Test
    void testCreateDefaultConfig() {
        GaussDBSourceConfig config = factory.create(0);

        assertThat(config).isNotNull();
        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(8000);
        assertThat(config.getDatabaseList()).isEqualTo(Collections.singletonList("testdb"));
        assertThat(config.getParallelDecodeNum()).isEqualTo(1);
        assertThat(config.getDecodeStyle()).isEqualTo("b");
        assertThat(config.isSendingBatch()).isFalse();
        assertThat(config.isIncludeDatabaseInTableId()).isTrue();
        assertThat(config.getLsnCommitCheckpointsDelay()).isEqualTo(0);
    }

    @Test
    void testCreateConfigWithCustomValues() {
        factory.decodingPluginName("pgoutput");
        factory.slotName("custom_slot");
        factory.heartbeatInterval(Duration.ofSeconds(10));
        factory.setParallelDecodeNum(4);
        factory.setDecodeStyle("j");
        factory.setSendingBatch(true);
        factory.setIncludeDatabaseInTableId(false);
        factory.setLsnCommitCheckpointsDelay(5);

        GaussDBSourceConfig config = factory.create(0);

        assertThat(config.getParallelDecodeNum()).isEqualTo(4);
        assertThat(config.getDecodeStyle()).isEqualTo("j");
        assertThat(config.isSendingBatch()).isTrue();
        assertThat(config.isIncludeDatabaseInTableId()).isFalse();
        assertThat(config.getLsnCommitCheckpointsDelay()).isEqualTo(5);
    }

    @Test
    void testCreateConfigWithSchemaList() {
        factory.schemaList(new String[] {"public", "test_schema"});

        GaussDBSourceConfig config = factory.create(0);
        assertThat(config).isNotNull();
    }

    @Test
    void testCreateConfigSetsDebeziumProperties() {
        GaussDBSourceConfig config = factory.create(0);
        Properties dbzProps = config.getDbzProperties();

        // plugin.name is set to "decoderbufs" for Debezium LogicalDecoder enum compatibility;
        // the real plugin name (mppdb_decoding) is stored in "real.plugin.name".
        assertThat(dbzProps.getProperty("plugin.name")).isEqualTo("decoderbufs");
        assertThat(dbzProps.getProperty("real.plugin.name")).isEqualTo("mppdb_decoding");
        assertThat(dbzProps.getProperty("database.server.name")).isEqualTo("gaussdb_cdc_source");
        assertThat(dbzProps.getProperty("database.hostname")).isEqualTo("localhost");
        assertThat(dbzProps.getProperty("database.dbname")).isEqualTo("testdb");
        assertThat(dbzProps.getProperty("database.user")).isEqualTo("testuser");
        assertThat(dbzProps.getProperty("database.port")).isEqualTo("8000");
        assertThat(dbzProps.getProperty("snapshot.mode")).isEqualTo("never");
        assertThat(dbzProps.getProperty("database.driver"))
                .isEqualTo("com.huawei.gaussdb.jdbc.Driver");
    }

    @Test
    void testCreateConfigWithParallelDecode() {
        factory.setParallelDecodeNum(4);
        factory.setDecodeStyle("j");
        factory.setSendingBatch(true);

        GaussDBSourceConfig config = factory.create(0);
        Properties dbzProps = config.getDbzProperties();

        // parallel-decode-num, decode-style, sending-batch are passed via slot.stream.params
        String streamParams = dbzProps.getProperty("slot.stream.params");
        assertThat(streamParams).isNotNull();
        assertThat(streamParams).contains("parallel-decode-num=4");
        assertThat(streamParams).contains("decode-style=j");
        assertThat(streamParams).contains("sending-batch=1");
        assertThat(streamParams).contains("include-xids=1");
        assertThat(streamParams).contains("include-timestamp=1");
    }

    @Test
    void testCreateConfigWithoutParallelDecode() {
        factory.setParallelDecodeNum(1);

        GaussDBSourceConfig config = factory.create(0);
        Properties dbzProps = config.getDbzProperties();

        // slot.stream.params should not be set when parallel-decode-num=1
        assertThat(dbzProps.getProperty("slot.stream.params")).isNull();
    }

    @Test
    void testCreateConfigWithSlotName() {
        factory.slotName("my_slot");
        GaussDBSourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty(SLOT_NAME.name())).isEqualTo("my_slot");
    }

    @Test
    void testGetSlotNameForBackfillTask() {
        factory.slotName("flink_slot");
        GaussDBSourceConfig config = factory.create(2);

        assertThat(config.getSlotNameForBackfillTask()).isEqualTo("flink_slot_2");
    }

    @Test
    void testGetJdbcUrl() {
        GaussDBSourceConfig config = factory.create(0);

        assertThat(config.getJdbcUrl()).isEqualTo("jdbc:gaussdb://localhost:8000/testdb");
    }

    @Test
    void testGetDbzConnectorConfig() {
        GaussDBSourceConfig config = factory.create(0);
        PostgresConnectorConfig pgConfig = config.getDbzConnectorConfig();

        assertThat(pgConfig).isNotNull();
    }

    @Test
    void testMissingHostnameThrows() {
        GaussDBSourceConfigFactory emptyFactory = new GaussDBSourceConfigFactory();
        emptyFactory.database("testdb");
        emptyFactory.username("testuser");
        emptyFactory.password("testpass");

        assertThatThrownBy(() -> emptyFactory.create(0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testCreateConfigWithSubtaskId() {
        GaussDBSourceConfig config0 = factory.create(0);
        GaussDBSourceConfig config1 = factory.create(1);

        assertThat(config0.getSubtaskId()).isEqualTo(0);
        assertThat(config1.getSubtaskId()).isEqualTo(1);
    }
}
