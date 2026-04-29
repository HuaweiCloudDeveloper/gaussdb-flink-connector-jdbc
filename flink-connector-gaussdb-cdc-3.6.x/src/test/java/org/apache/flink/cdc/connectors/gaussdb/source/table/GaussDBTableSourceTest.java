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

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBTableSource}. */
class GaussDBTableSourceTest {

    private GaussDBTableSource createTestSource() {
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical(
                                        "id", org.apache.flink.table.api.DataTypes.INT().notNull()),
                                Column.physical(
                                        "name", org.apache.flink.table.api.DataTypes.VARCHAR(255))),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        return new GaussDBTableSource(
                schema,
                8000,
                "localhost",
                "testdb",
                "public",
                "test_table",
                "testuser",
                "testpass",
                "mppdb_decoding",
                "flink_slot",
                DebeziumChangelogMode.ALL,
                new Properties(),
                true,
                8096,
                1000,
                1024,
                Duration.ofSeconds(30),
                3,
                10,
                1000.0d,
                0.05d,
                Duration.ofSeconds(30),
                StartupOptions.initial(),
                null,
                false,
                false,
                false,
                3,
                false,
                1,
                "b",
                false,
                true);
    }

    @Test
    void testCopy() {
        GaussDBTableSource source = createTestSource();
        DynamicTableSource copy = source.copy();

        assertThat(copy).isInstanceOf(GaussDBTableSource.class);
        assertThat(copy).isNotSameAs(source);
        assertThat(copy).isEqualTo(source);
    }

    @Test
    void testEqualsSameObject() {
        GaussDBTableSource source = createTestSource();
        assertThat(source).isEqualTo(source);
    }

    @Test
    void testEqualsDifferentObject() {
        GaussDBTableSource source1 = createTestSource();
        GaussDBTableSource source2 = createTestSource();
        assertThat(source1).isEqualTo(source2);
    }

    @Test
    void testNotEqualsNull() {
        GaussDBTableSource source = createTestSource();
        assertThat(source).isNotEqualTo(null);
    }

    @Test
    void testNotEqualsDifferentClass() {
        GaussDBTableSource source = createTestSource();
        assertThat(source).isNotEqualTo("not a source");
    }

    @Test
    void testHashCode() {
        GaussDBTableSource source1 = createTestSource();
        GaussDBTableSource source2 = createTestSource();
        assertThat(source1.hashCode()).isEqualTo(source2.hashCode());
    }

    @Test
    void testAsSummaryString() {
        GaussDBTableSource source = createTestSource();
        assertThat(source.asSummaryString()).isEqualTo("GaussDB-CDC");
    }

    @Test
    void testGetChangelogModeAll() {
        GaussDBTableSource source = createTestSource();
        ChangelogMode mode = source.getChangelogMode();
        assertThat(mode).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testGetChangelogModeUpsert() {
        ResolvedSchema schemaWithPk =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical(
                                        "id", org.apache.flink.table.api.DataTypes.INT().notNull()),
                                Column.physical(
                                        "name", org.apache.flink.table.api.DataTypes.VARCHAR(255))),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        GaussDBTableSource source =
                new GaussDBTableSource(
                        schemaWithPk,
                        8000,
                        "localhost",
                        "testdb",
                        "public",
                        "test_table",
                        "testuser",
                        "testpass",
                        "mppdb_decoding",
                        "flink_slot",
                        DebeziumChangelogMode.UPSERT,
                        new Properties(),
                        true,
                        8096,
                        1000,
                        1024,
                        Duration.ofSeconds(30),
                        3,
                        10,
                        1000.0d,
                        0.05d,
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        false,
                        false,
                        false,
                        3,
                        false,
                        1,
                        "b",
                        false,
                        true);

        ChangelogMode mode = source.getChangelogMode();
        assertThat(mode).isEqualTo(ChangelogMode.upsert());
    }

    @Test
    void testNotEqualsDifferentPort() {
        GaussDBTableSource source1 = createTestSource();
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical(
                                        "id", org.apache.flink.table.api.DataTypes.INT().notNull()),
                                Column.physical(
                                        "name", org.apache.flink.table.api.DataTypes.VARCHAR(255))),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));
        GaussDBTableSource source2 =
                new GaussDBTableSource(
                        schema,
                        5432,
                        "localhost",
                        "testdb",
                        "public",
                        "test_table",
                        "testuser",
                        "testpass",
                        "mppdb_decoding",
                        "flink_slot",
                        DebeziumChangelogMode.ALL,
                        new Properties(),
                        true,
                        8096,
                        1000,
                        1024,
                        Duration.ofSeconds(30),
                        3,
                        10,
                        1000.0d,
                        0.05d,
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        false,
                        false,
                        false,
                        3,
                        false,
                        1,
                        "b",
                        false,
                        true);
        assertThat(source1).isNotEqualTo(source2);
    }
}
