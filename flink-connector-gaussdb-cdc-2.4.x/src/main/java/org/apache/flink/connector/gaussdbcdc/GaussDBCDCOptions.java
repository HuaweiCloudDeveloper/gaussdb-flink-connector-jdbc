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

package org.apache.flink.connector.gaussdbcdc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for GaussDB CDC Connector. */
@PublicEvolving
public class GaussDBCDCOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the GaussDB database server.");

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
                            "Optional dedicated port for WAL logical replication streaming."
                                    + " Only effective when wal.mode=true. When GaussDB runs with"
                                    + " enable_thread_pool=on, the main data port (e.g. 8000) only"
                                    + " serves regular JDBC and rejects replication=database; the HA"
                                    + " port (e.g. 8001) must be used for WAL streaming. Set this"
                                    + " option to the HA port in that scenario. If unset, the main"
                                    + " 'port' is reused for replication.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the GaussDB database to use.");

    public static final ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("Name of the GaussDB schema to use.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the GaussDB database to use when connecting to the GaussDB database.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the GaussDB database.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table name of the GaussDB table to monitor. "
                                    + "Supports regex matching for multi-table capture, e.g. "
                                    + "'orders_.*' matches all tables starting with 'orders_'. "
                                    + "When no regex metacharacters are present, exact match "
                                    + "is used (backward compatible with single-table mode).");

    public static final ConfigOption<String> OUTPUT_FORMAT =
            ConfigOptions.key("output.format")
                    .stringType()
                    .defaultValue("raw")
                    .withDescription(
                            "Output format for CDC events. "
                                    + "'raw' (default): outputs RowData with fixed columns "
                                    + "(requires homogeneous table schemas). "
                                    + "'json': outputs a single STRING column containing JSON "
                                    + "with table name, operation type, and column values "
                                    + "(supports heterogeneous tables with different schemas).");

    public static final ConfigOption<String> OUTPUT_JSON_FORMAT =
            ConfigOptions.key("output.json.format")
                    .stringType()
                    .defaultValue("debezium")
                    .withDescription(
                            "JSON output format when output.format=json. "
                                    + "'debezium' (default): Debezium standard format "
                                    + "(before/after/source/op/ts_ms), compatible with Flink CDC. "
                                    + "'canal': Canal standard format "
                                    + "(data/old/database/table/type/es/ts/pkNames/isDdl/sqlType). "
                                    + "'haier': Haier customized Canal format "
                                    + "(data/old/database/table/optType/es/ts/pkNames/pkValues).");

    public static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .defaultValue("flink_cdc_slot")
                    .withDescription("Name of the GaussDB logical decoding slot.");

    public static final ConfigOption<String> PLUGIN_NAME =
            ConfigOptions.key("plugin.name")
                    .stringType()
                    .defaultValue("pgoutput")
                    .withDescription("Name of the GaussDB logical decoding plugin.");

    public static final ConfigOption<Boolean> SNAPSHOT_MODE =
            ConfigOptions.key("snapshot.mode")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to take a snapshot of the table before streaming changes.");

    public static final ConfigOption<Integer> CHUNK_SIZE =
            ConfigOptions.key("chunk.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of rows to read in each chunk during snapshot.");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT_MS =
            ConfigOptions.key("connect.timeout.ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Maximum time to wait for connection to GaussDB (in milliseconds).");

    public static final ConfigOption<Integer> POLL_INTERVAL_MS =
            ConfigOptions.key("poll.interval.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Time to wait between polling for new changes (in milliseconds).");

    public static final ConfigOption<String> DECODE_PLUGIN =
            ConfigOptions.key("decode.plugin")
                    .stringType()
                    .defaultValue("mppdb_decoding")
                    .withDescription(
                            "Logical decoding plugin name. "
                                    + "Supported: mppdb_decoding (default), pgoutput.");

    public static final ConfigOption<Integer> PARALLEL_DECODE_NUM =
            ConfigOptions.key("parallel-decode-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Number of parallel decoder threads for logical decoding. "
                                    + "Range 1-20, 1 means serial decoding (default).");

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

    public static final ConfigOption<Boolean> WAL_MODE =
            ConfigOptions.key("wal.mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use WAL logical decoding for change capture. "
                                    + "When false (default), uses polling-based CDC. "
                                    + "When true, uses WAL logical replication stream.");

    public static final ConfigOption<String> SSL_MODE =
            ConfigOptions.key("sslmode")
                    .stringType()
                    .defaultValue("prefer")
                    .withDescription(
                            "SSL mode for GaussDB connections. "
                                    + "Supported values: disable, allow, prefer, require, verify-ca, verify-full. "
                                    + "Default is 'prefer'.");

    private GaussDBCDCOptions() {}
}
