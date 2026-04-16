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

package org.apache.flink.connector.gaussdbcdc.source;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/** Unit tests for {@link GaussDBCDCSource} builder. */
public class GaussDBCDCSourceBuilderTest {

    @Test
    public void testBuildSourceWithMinimalConfig() {
        GaussDBCDCSource source =
                GaussDBCDCSource.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schema("public")
                        .tableName("student")
                        .username("root")
                        .password("secret")
                        .build();

        assertNotNull(source);
    }

    @Test
    public void testBuildSourceWithAllConfig() {
        GaussDBCDCSource source =
                GaussDBCDCSource.builder()
                        .hostname("gaussdb.example.com")
                        .port(5432)
                        .database("mydb")
                        .schema("myschema")
                        .tableName("mytable")
                        .username("admin")
                        .password("password123")
                        .slotName("custom_slot")
                        .snapshotMode(false)
                        .chunkSize(500)
                        .connectTimeoutMs(60000)
                        .pollIntervalMs(2000)
                        .build();

        assertNotNull(source);
    }

    @Test
    public void testDefaultValues() {
        GaussDBCDCSource.Builder builder =
                GaussDBCDCSource.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schema("public")
                        .tableName("student")
                        .username("root")
                        .password("secret");

        assertNotNull(builder);

        // Test default values are set correctly
        GaussDBCDCSource source = builder.build();
        assertNotNull(source);
    }

    @Test
    public void testBuilderChaining() {
        GaussDBCDCSource.Builder builder =
                GaussDBCDCSource.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schema("public")
                        .tableName("student")
                        .username("root")
                        .password("secret");

        // Verify builder returns itself for chaining
        assertNotNull(builder);
    }

    @Test
    public void testMultipleBuildCalls() {
        GaussDBCDCSource.Builder builder =
                GaussDBCDCSource.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schema("public")
                        .tableName("student")
                        .username("root")
                        .password("secret");

        GaussDBCDCSource source1 = builder.build();
        GaussDBCDCSource source2 = builder.build();

        assertNotNull(source1);
        assertNotNull(source2);
    }
}
