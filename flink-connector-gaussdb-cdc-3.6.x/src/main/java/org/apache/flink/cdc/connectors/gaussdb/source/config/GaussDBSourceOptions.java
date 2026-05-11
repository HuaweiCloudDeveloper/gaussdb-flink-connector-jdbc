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

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBSourceBuilder;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link GaussDBSourceBuilder.GaussDBIncrementalSource}. */
public class GaussDBSourceOptions extends JdbcSourceOptions {

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(8000)
                    .withDescription("Integer port number of the GaussDB database server.");

    public static final ConfigOption<Integer> REPLICATION_PORT =
            ConfigOptions.key("replication.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional port used exclusively for replication (streaming decode) connections. "
                                    + "When GaussDB is running with 'enable_thread_pool=on', replication "
                                    + "connections must go through the HA port (typically the normal port + 1), "
                                    + "while normal gsql/JDBC queries are rejected on that port. "
                                    + "If set, this port is used when opening replication connections; "
                                    + "the 'port' option is still used for all other JDBC queries. "
                                    + "If not set, the 'port' option is used for both.");

    public static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("mppdb_decoding")
                    .withDescription(
                            "The name of the GaussDB logical decoding plug-in. "
                                    + "GaussDB uses 'mppdb_decoding' (default). "
                                    + "Also supports 'pgoutput' for PG-compatible mode.");

    public static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the GaussDB logical decoding slot that was created for streaming changes.");

    public static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(DebeziumChangelogMode.class)
                    .defaultValue(DebeziumChangelogMode.ALL)
                    .withDescription(
                            "The changelog mode used for encoding streaming changes.\n"
                                    + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                    + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key.");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Default is true for GaussDB CDC 3.x.");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracking the latest available replication slot offsets");

    public static final ConfigOption<Integer> SCAN_LSN_COMMIT_CHECKPOINTS_DELAY =
            ConfigOptions.key("scan.lsn-commit.checkpoints-num-delay")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The number of checkpoint delays before starting to commit the LSN offsets.");

    // GaussDB-specific mppdb_decoding parameters

    public static final ConfigOption<Integer> PARALLEL_DECODE_NUM =
            ConfigOptions.key("parallel-decode-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of parallel decoder threads for logical decoding. "
                                    + "Range 1-20, 1 means serial decoding (default). "
                                    + "Only effective when decoding.plugin.name=mppdb_decoding.");

    public static final ConfigOption<String> DECODE_STYLE =
            ConfigOptions.key("decode-style")
                    .stringType()
                    .defaultValue("b")
                    .withDescription(
                            "Decode output format for parallel decoding. "
                                    + "'b' = binary (default), 'j' = json, 't' = text. "
                                    + "Only effective when parallel-decode-num > 1.");

    public static final ConfigOption<Boolean> SENDING_BATCH =
            ConfigOptions.key("sending-batch")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to batch send decoded results. "
                                    + "When true, results are accumulated to 1MB before sending. "
                                    + "Only effective when parallel-decode-num > 1.");

    public static final ConfigOption<Boolean> TABLE_ID_INCLUDE_DATABASE =
            ConfigOptions.key("table-id.include-database")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to include database in the generated Table ID.\n"
                                    + "If set to true (default), the Table ID will be in the format (database, schema, table).\n"
                                    + "If set to false, the Table ID will be in the format (schema, table).");
}
