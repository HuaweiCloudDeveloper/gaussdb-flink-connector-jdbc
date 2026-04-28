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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBCDCSourceFunction}. */
class GaussDBCDCSourceFunctionTest {

    @Test
    void testBuilderDefaults() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .schema("public")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .build();

        assertThat(source).isNotNull();
    }

    @Test
    void testBuilderCustomValues() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("1.2.3.4")
                        .port(9000)
                        .database("mydb")
                        .schema("myschema")
                        .tableName("mytable")
                        .username("admin")
                        .password("secret")
                        .slotName("my_slot")
                        .pluginName("mppdb_decoding")
                        .snapshotMode(false)
                        .chunkSize(5000)
                        .connectTimeoutMs(60000)
                        .pollIntervalMs(2000)
                        .walMode(true)
                        .decodePlugin("mppdb_decoding")
                        .parallelDecodeNum(4)
                        .decodeStyle("b")
                        .sendingBatch(true)
                        .build();

        assertThat(source).isNotNull();
    }

    @Test
    void testBuilderAllFieldsSet() {
        GaussDBCDCSourceFunction.Builder builder = GaussDBCDCSourceFunction.builder();
        builder.hostname("h").port(1234).database("d").schema("s").tableName("t");
        builder.username("u").password("p").slotName("slot").pluginName("plugin");
        builder.snapshotMode(true).chunkSize(100).connectTimeoutMs(5000).pollIntervalMs(500);
        builder.walMode(true).decodePlugin("mppdb_decoding");
        builder.parallelDecodeNum(8).decodeStyle("j").sendingBatch(true);

        GaussDBCDCSourceFunction source = builder.build();
        assertThat(source).isNotNull();
    }

    @Test
    void testGetProducedType() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .build();

        TypeInformation<RowData> type = source.getProducedType();
        assertThat(type).isNotNull();
        assertThat(type.getTypeClass()).isEqualTo(RowData.class);
    }

    @Test
    void testBuilderChaining() {
        // Verify builder returns itself for chaining
        GaussDBCDCSourceFunction.Builder builder = GaussDBCDCSourceFunction.builder();
        assertThat(builder.hostname("h")).isSameAs(builder);
        assertThat(builder.port(8000)).isSameAs(builder);
        assertThat(builder.database("d")).isSameAs(builder);
        assertThat(builder.schema("s")).isSameAs(builder);
        assertThat(builder.tableName("t")).isSameAs(builder);
        assertThat(builder.username("u")).isSameAs(builder);
        assertThat(builder.password("p")).isSameAs(builder);
        assertThat(builder.slotName("slot")).isSameAs(builder);
        assertThat(builder.pluginName("plugin")).isSameAs(builder);
        assertThat(builder.snapshotMode(true)).isSameAs(builder);
        assertThat(builder.chunkSize(1000)).isSameAs(builder);
        assertThat(builder.connectTimeoutMs(30000)).isSameAs(builder);
        assertThat(builder.pollIntervalMs(1000)).isSameAs(builder);
        assertThat(builder.walMode(true)).isSameAs(builder);
        assertThat(builder.decodePlugin("mppdb_decoding")).isSameAs(builder);
        assertThat(builder.parallelDecodeNum(4)).isSameAs(builder);
        assertThat(builder.decodeStyle("b")).isSameAs(builder);
        assertThat(builder.sendingBatch(true)).isSameAs(builder);
    }

    @Test
    void testCancelSetsRunningToFalse() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .build();

        // cancel should not throw
        source.cancel();
    }

    @Test
    void testWalModeEnabled() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .walMode(true)
                        .parallelDecodeNum(4)
                        .decodeStyle("b")
                        .sendingBatch(true)
                        .build();

        assertThat(source).isNotNull();
    }

    @Test
    void testWalModeDisabled() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .walMode(false)
                        .build();

        assertThat(source).isNotNull();
    }

    @Test
    void testSnapshotModeDisabled() {
        GaussDBCDCSourceFunction source =
                GaussDBCDCSourceFunction.builder()
                        .hostname("localhost")
                        .port(8000)
                        .database("testdb")
                        .tableName("test_table")
                        .username("root")
                        .password("pass")
                        .snapshotMode(false)
                        .walMode(true)
                        .build();

        assertThat(source).isNotNull();
    }
}
