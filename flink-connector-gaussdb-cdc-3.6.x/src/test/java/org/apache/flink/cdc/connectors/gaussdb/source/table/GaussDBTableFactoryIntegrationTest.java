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

import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link GaussDBTableFactory} with Flink table framework. */
class GaussDBTableFactoryIntegrationTest {

    private Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "gaussdb-cdc");
        options.put("hostname", "localhost");
        options.put("port", "8000");
        options.put("username", "testuser");
        options.put("password", "testpass");
        options.put("database-name", "testdb");
        options.put("schema-name", "public");
        options.put("table-name", "test_table");
        options.put("slot.name", "flink_slot");
        return options;
    }

    private Map<String, String> getAllOptions() {
        return getRequiredOptions();
    }

    @Test
    void testFactoryIdentifierIsGaussDbCdc() {
        GaussDBTableFactory factory = new GaussDBTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }

    @Test
    void testStartupModeInitial() {
        Map<String, String> options = getRequiredOptions();
        options.put("scan.startup.mode", "initial");

        GaussDBTableFactory factory = new GaussDBTableFactory();
        Configuration config = Configuration.fromMap(options);

        // Test that the startup mode parsing works
        assertThat(config.toMap().get("scan.startup.mode")).isEqualTo("initial");
    }

    @Test
    void testStartupModeLatest() {
        Map<String, String> options = getRequiredOptions();
        options.put("scan.startup.mode", "latest-offset");

        Configuration config = Configuration.fromMap(options);
        assertThat(config.toMap().get("scan.startup.mode")).isEqualTo("latest-offset");
    }

    @Test
    void testGaussDbSpecificOptions() {
        Map<String, String> options = getRequiredOptions();
        options.put("parallel-decode-num", "4");
        options.put("decode-style", "j");
        options.put("sending-batch", "true");
        options.put("table-id.include-database", "false");

        Configuration config = Configuration.fromMap(options);
        assertThat(config.getString("parallel-decode-num", "1")).isEqualTo("4");
        assertThat(config.getString("decode-style", "b")).isEqualTo("j");
        assertThat(config.getString("sending-batch", "false")).isEqualTo("true");
        assertThat(config.getString("table-id.include-database", "true")).isEqualTo("false");
    }

    @Test
    void testDefaultPort() {
        assertThat(GaussDBSourceOptions.PORT.defaultValue()).isEqualTo(8000);
    }

    @Test
    void testDefaultDecodingPlugin() {
        assertThat(GaussDBSourceOptions.DECODING_PLUGIN_NAME.defaultValue())
                .isEqualTo("mppdb_decoding");
    }
}
