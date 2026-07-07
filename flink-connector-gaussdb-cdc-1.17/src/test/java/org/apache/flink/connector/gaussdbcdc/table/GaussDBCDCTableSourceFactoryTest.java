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
import org.apache.flink.connector.gaussdbcdc.GaussDBCDCOptions;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBCDCTableSourceFactory}. */
class GaussDBCDCTableSourceFactoryTest {

    private final GaussDBCDCTableSourceFactory factory = new GaussDBCDCTableSourceFactory();

    @Test
    void testFactoryIdentifier() {
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }

    @Test
    void testRequiredOptions() {
        Set<ConfigOption<?>> requiredOptions = factory.requiredOptions();
        assertThat(requiredOptions).isNotNull();
        assertThat(requiredOptions).contains(GaussDBCDCOptions.HOSTNAME);
        assertThat(requiredOptions).contains(GaussDBCDCOptions.DATABASE);
        assertThat(requiredOptions).contains(GaussDBCDCOptions.TABLE_NAME);
        assertThat(requiredOptions).contains(GaussDBCDCOptions.USERNAME);
        assertThat(requiredOptions).contains(GaussDBCDCOptions.PASSWORD);
    }

    @Test
    void testOptionalOptions() {
        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertThat(optionalOptions).isNotNull();
        assertThat(optionalOptions).contains(GaussDBCDCOptions.PORT);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.SCHEMA);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.SLOT_NAME);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.PLUGIN_NAME);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.SNAPSHOT_MODE);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.CHUNK_SIZE);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.CONNECT_TIMEOUT_MS);
        assertThat(optionalOptions).contains(GaussDBCDCOptions.POLL_INTERVAL_MS);
    }
}
