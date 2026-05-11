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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;

import io.debezium.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBConnectionPoolFactory}. */
class GaussDBConnectionPoolFactoryTest {

    @Test
    void testGetJdbcUrl() {
        GaussDBConnectionPoolFactory factory = new GaussDBConnectionPoolFactory();
        JdbcSourceConfig sourceConfig = mock(JdbcSourceConfig.class);
        when(sourceConfig.getHostname()).thenReturn("localhost");
        when(sourceConfig.getPort()).thenReturn(8000);
        when(sourceConfig.getDatabaseList())
                .thenReturn(java.util.Collections.singletonList("testdb"));

        String jdbcUrl = factory.getJdbcUrl(sourceConfig);
        assertThat(jdbcUrl).isEqualTo("jdbc:gaussdb://localhost:8000/testdb");
    }

    @Test
    void testGetJdbcUrlPattern() {
        assertThat(GaussDBConnectionPoolFactory.JDBC_URL_PATTERN)
                .isEqualTo("jdbc:gaussdb://%s:%s/%s");
    }

    @Test
    void testGetPoolId() {
        GaussDBConnectionPoolFactory factory = new GaussDBConnectionPoolFactory();
        JdbcConfiguration config = mock(JdbcConfiguration.class);
        when(config.getHostname()).thenReturn("localhost");
        when(config.getPort()).thenReturn(8000);
        when(config.getUser()).thenReturn("testuser");
        when(config.getDatabase()).thenReturn("testdb");

        var poolId = factory.getPoolId(config, "gaussdb-pool");
        assertThat(poolId).isNotNull();
    }
}
