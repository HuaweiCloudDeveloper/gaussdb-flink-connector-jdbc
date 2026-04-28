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

package org.apache.flink.connector.gaussdbcdc.table;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Comprehensive tests for {@link GaussDBCDCTableSourceFactory}. */
class GaussDBCDCTableSourceFactoryComprehensiveTest {

    private GaussDBCDCTableSourceFactory factory;

    @BeforeEach
    void setUp() {
        factory = new GaussDBCDCTableSourceFactory();
    }

    @Test
    void testFactoryIdentifierIsGaussdbCdc() {
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }

    @Test
    void testRequiredOptionsContainsHostname() {
        Set<String> requiredNames = extractOptionNames(factory.requiredOptions());
        assertThat(requiredNames).contains("hostname");
    }

    @Test
    void testRequiredOptionsContainsDatabase() {
        Set<String> requiredNames = extractOptionNames(factory.requiredOptions());
        assertThat(requiredNames).contains("database");
    }

    @Test
    void testRequiredOptionsContainsTableName() {
        Set<String> requiredNames = extractOptionNames(factory.requiredOptions());
        assertThat(requiredNames).contains("table-name");
    }

    @Test
    void testRequiredOptionsContainsUsername() {
        Set<String> requiredNames = extractOptionNames(factory.requiredOptions());
        assertThat(requiredNames).contains("username");
    }

    @Test
    void testRequiredOptionsContainsPassword() {
        Set<String> requiredNames = extractOptionNames(factory.requiredOptions());
        assertThat(requiredNames).contains("password");
    }

    @Test
    void testOptionalOptionsContainsPort() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("port");
    }

    @Test
    void testOptionalOptionsContainsSchema() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("schema");
    }

    @Test
    void testOptionalOptionsContainsWalMode() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("wal.mode");
    }

    @Test
    void testOptionalOptionsContainsParallelDecodeNum() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("parallel-decode-num");
    }

    @Test
    void testOptionalOptionsContainsDecodeStyle() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("decode-style");
    }

    @Test
    void testOptionalOptionsContainsSendingBatch() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("sending-batch");
    }

    @Test
    void testOptionalOptionsContainsSlotName() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("slot.name");
    }

    @Test
    void testOptionalOptionsContainsDecodePlugin() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("decode.plugin");
    }

    @Test
    void testOptionalOptionsContainsSnapshotMode() {
        Set<String> optionalNames = extractOptionNames(factory.optionalOptions());
        assertThat(optionalNames).contains("snapshot.mode");
    }

    private Set<String> extractOptionNames(Set<ConfigOption<?>> options) {
        Set<String> names = new HashSet<>();
        for (ConfigOption<?> option : options) {
            names.add(option.key());
        }
        return names;
    }
}
