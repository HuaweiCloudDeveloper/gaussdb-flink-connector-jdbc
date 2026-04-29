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
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTableAdapter;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtilAdapter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link GaussDBTableSource} created by {@link GaussDBTableFactory}.
 *
 * <p>Migrated from official flink-cdc PostgreSQLTableFactoryTest with GaussDB-specific adaptations.
 */
class GaussDBTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", org.apache.flink.table.api.DataTypes.INT()),
                            Column.physical(
                                    "name", org.apache.flink.table.api.DataTypes.VARCHAR(100))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final ResolvedSchema SCHEMA_WITHOUT_PRIMARY_KEY =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", org.apache.flink.table.api.DataTypes.INT()),
                            Column.physical(
                                    "name", org.apache.flink.table.api.DataTypes.VARCHAR(100))),
                    Collections.emptyList(),
                    null);

    private static final String HOSTNAME = "localhost";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpass";
    private static final String DATABASE = "testdb";
    private static final String TABLE = "test_table";
    private static final String SCHEMA_NAME = "public";
    private static final String SLOT_NAME = "flink_slot";
    private static final Properties PROPERTIES = new Properties();

    /** Test that all common properties are correctly parsed into a GaussDBTableSource. */
    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        GaussDBTableSource expectedSource =
                new GaussDBTableSource(
                        SCHEMA,
                        8000,
                        HOSTNAME,
                        DATABASE,
                        SCHEMA_NAME,
                        TABLE,
                        USERNAME,
                        PASSWORD,
                        "mppdb_decoding",
                        SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        1,
                        "b",
                        false,
                        true);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    /** Test that optional properties are correctly overridden. */
    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "5432");
        options.put("decoding.plugin.name", "pgoutput");
        options.put("debezium.snapshot.mode", "never");
        options.put("changelog-mode", "upsert");
        options.put("scan.incremental.snapshot.backfill.skip", "true");
        options.put("scan.newly-added-table.enabled", "true");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.put("snapshot.mode", "never");
        GaussDBTableSource expectedSource =
                new GaussDBTableSource(
                        SCHEMA,
                        5432,
                        HOSTNAME,
                        DATABASE,
                        SCHEMA_NAME,
                        TABLE,
                        USERNAME,
                        PASSWORD,
                        "pgoutput",
                        SLOT_NAME,
                        DebeziumChangelogMode.UPSERT,
                        dbzProperties,
                        false,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        true,
                        true,
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        1,
                        "b",
                        false,
                        true);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    /** Test enabling parallel read source with specific chunk/fetch sizes. */
    @Test
    void testEnableParallelReadSource() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("scan.incremental.snapshot.chunk.size", "8000");
        properties.put("scan.snapshot.fetch.size", "100");
        properties.put("connect.timeout", "45s");

        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        GaussDBTableSource expectedSource =
                new GaussDBTableSource(
                        SCHEMA,
                        8000,
                        HOSTNAME,
                        DATABASE,
                        SCHEMA_NAME,
                        TABLE,
                        USERNAME,
                        PASSWORD,
                        "mppdb_decoding",
                        SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        true,
                        8000,
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        100,
                        Duration.ofSeconds(45),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        1,
                        "b",
                        false,
                        true);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    /** Test startup from latest offset. */
    @Test
    void testStartupFromLatestOffset() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("scan.startup.mode", "latest-offset");

        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        GaussDBTableSource expectedSource =
                new GaussDBTableSource(
                        SCHEMA,
                        8000,
                        HOSTNAME,
                        DATABASE,
                        SCHEMA_NAME,
                        TABLE,
                        USERNAME,
                        PASSWORD,
                        "mppdb_decoding",
                        SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        true,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        Duration.ofSeconds(30),
                        StartupOptions.latest(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        1,
                        "b",
                        false,
                        true);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    /** Test GaussDB-specific options: parallel-decode-num, decode-style, sending-batch. */
    @Test
    void testGaussDBSpecificOptions() {
        Map<String, String> properties = getAllOptions();
        properties.put("scan.incremental.snapshot.enabled", "true");
        properties.put("parallel-decode-num", "4");
        properties.put("decode-style", "j");
        properties.put("sending-batch", "true");
        properties.put("table-id.include-database", "false");

        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        GaussDBTableSource expectedSource =
                new GaussDBTableSource(
                        SCHEMA,
                        8000,
                        HOSTNAME,
                        DATABASE,
                        SCHEMA_NAME,
                        TABLE,
                        USERNAME,
                        PASSWORD,
                        "mppdb_decoding",
                        SLOT_NAME,
                        DebeziumChangelogMode.ALL,
                        PROPERTIES,
                        true,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        CHUNK_META_GROUP_SIZE.defaultValue(),
                        SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        CONNECT_TIMEOUT.defaultValue(),
                        CONNECT_MAX_RETRIES.defaultValue(),
                        CONNECTION_POOL_SIZE.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue(),
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue(),
                        Duration.ofSeconds(30),
                        StartupOptions.initial(),
                        null,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue(),
                        SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED.defaultValue(),
                        4,
                        "j",
                        true,
                        false);
        assertThat(actualSource).isEqualTo(expectedSource);
    }

    /** Test validation of required options, illegal values, and unsupported options. */
    @Test
    void testValidation() {
        // validate illegal port
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("port", "123b");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Could not parse value '123b' for key 'port'.");

        // validate missing required options
        Factory factory = new GaussDBTableFactory();
        for (ConfigOption<?> requiredOption : factory.requiredOptions()) {
            Map<String, String> properties = getAllOptions();
            properties.remove(requiredOption.key());

            assertThatThrownBy(() -> createTableSource(SCHEMA, properties))
                    .hasStackTraceContaining(
                            "Missing required options are:\n\n" + requiredOption.key());
        }

        // validate unsupported option
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("unknown", "abc");
                            createTableSource(properties);
                        })
                .hasStackTraceContaining("Unsupported options:\n\nunknown");
    }

    /** Test that upsert mode without primary key throws an error. */
    @Test
    void testUpsertModeWithoutPrimaryKeyError() {
        assertThatThrownBy(
                        () -> {
                            Map<String, String> properties = getAllOptions();
                            properties.put("changelog-mode", "upsert");
                            createTableSource(SCHEMA_WITHOUT_PRIMARY_KEY, properties);
                        })
                .hasStackTraceContaining(
                        "Primary key must be present when upsert mode is selected.");
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "gaussdb-cdc");
        options.put("hostname", HOSTNAME);
        options.put("username", USERNAME);
        options.put("password", PASSWORD);
        options.put("database-name", DATABASE);
        options.put("schema-name", SCHEMA_NAME);
        options.put("table-name", TABLE);
        options.put("slot.name", SLOT_NAME);
        options.put("scan.incremental.snapshot.enabled", String.valueOf(false));
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtilAdapter.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTableAdapter.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        schema),
                new Configuration(),
                GaussDBTableSourceFactoryTest.class.getClassLoader(),
                false);
    }
}
