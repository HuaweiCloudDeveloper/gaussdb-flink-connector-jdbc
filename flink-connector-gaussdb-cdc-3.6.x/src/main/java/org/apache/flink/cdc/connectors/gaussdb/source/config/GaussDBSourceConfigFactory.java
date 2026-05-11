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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory to create Configuration for GaussDB source. */
public class GaussDBSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String JDBC_DRIVER = "com.huawei.gaussdb.jdbc.Driver";

    private Duration heartbeatInterval = GaussDBSourceOptions.HEARTBEAT_INTERVAL.defaultValue();

    private String pluginName = "mppdb_decoding";

    private String slotName = "flink";

    private String database;

    private List<String> schemaList;

    private int lsnCommitCheckpointsDelay;

    private boolean includeDatabaseInTableId =
            GaussDBSourceOptions.TABLE_ID_INCLUDE_DATABASE.defaultValue();

    private int parallelDecodeNum = GaussDBSourceOptions.PARALLEL_DECODE_NUM.defaultValue();

    private String decodeStyle = GaussDBSourceOptions.DECODE_STYLE.defaultValue();

    private boolean sendingBatch = GaussDBSourceOptions.SENDING_BATCH.defaultValue();

    /**
     * Optional dedicated port for replication connections. When non-null, replication connections
     * use this port instead of {@link #port}. Useful for GaussDB with {@code
     * enable_thread_pool=on}, where replication must go through the HA port while regular JDBC
     * queries must use the main port.
     */
    private Integer replicationPort;

    /** Creates a new {@link GaussDBSourceConfig} for the given subtask. */
    @Override
    public GaussDBSourceConfig create(int subtaskId) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("connector.class", PostgresConnector.class.getCanonicalName());
        // Use decoderbufs for Debezium's LogicalDecoder enum compatibility.
        // decoderbufs is chosen over pgoutput because:
        // 1. LogicalDecoder.DECODERBUFS is a valid enum constant (passes validation)
        // 2. initPublication() only runs for PGOUTPUT, so it's automatically skipped
        // 3. GaussDB does not support pg_publication, so avoiding PGOUTPUT is essential
        // The real plugin name (e.g., mppdb_decoding) is stored separately
        // and used for slot operations and query functions.
        props.setProperty("plugin.name", "decoderbufs");
        props.setProperty("real.plugin.name", pluginName);
        props.setProperty("database.server.name", "gaussdb_cdc_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.dbname", checkNotNull(database));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("slot.name", checkNotNull(slotName));

        // GaussDB JDBC driver class
        props.setProperty("database.driver", JDBC_DRIVER);

        // Database history
        props.setProperty(
                "database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        // TCP keep-alive
        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatInterval.toMillis()));
        props.setProperty("include.schema.changes", String.valueOf(includeSchemaChanges));

        // mppdb_decoding specific parameters
        // These must be passed as 'slot.stream.params' (key1=value1;key2=value2 format)
        // so Debezium passes them to START_REPLICATION SLOT command, which tells
        // GaussDB's mppdb_decoding plugin to use parallel decoding and binary format.
        // See: io.debezium.connector.postgresql.PostgresConnectorConfig.STREAM_PARAMS
        if (parallelDecodeNum > 1) {
            StringBuilder streamParamsBuilder = new StringBuilder();
            streamParamsBuilder.append("parallel-decode-num").append('=').append(parallelDecodeNum);
            streamParamsBuilder.append(';').append("decode-style").append('=').append(decodeStyle);
            if (sendingBatch) {
                streamParamsBuilder.append(';').append("sending-batch=1");
            }
            streamParamsBuilder.append(';').append("include-xids=1");
            streamParamsBuilder.append(';').append("include-timestamp=1");
            props.setProperty("slot.stream.params", streamParamsBuilder.toString());
        }

        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }

        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        // Override user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        // GaussDB source handles snapshot via StartupMode, not Debezium
        props.setProperty("snapshot.mode", "never");

        Configuration dbzConfiguration = Configuration.from(props);
        return new GaussDBSourceConfig(
                subtaskId,
                startupOptions,
                Collections.singletonList(database),
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                JDBC_DRIVER,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst,
                includeDatabaseInTableId,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch,
                replicationPort);
    }

    public void schemaList(String[] schemaList) {
        this.schemaList = Arrays.asList(schemaList);
    }

    public void decodingPluginName(String name) {
        this.pluginName = name;
    }

    public void database(String database) {
        this.database = database;
    }

    public void slotName(String slotName) {
        this.slotName = slotName;
    }

    public void heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    public void setLsnCommitCheckpointsDelay(int lsnCommitCheckpointsDelay) {
        this.lsnCommitCheckpointsDelay = lsnCommitCheckpointsDelay;
    }

    public void setIncludeDatabaseInTableId(boolean includeDatabaseInTableId) {
        this.includeDatabaseInTableId = includeDatabaseInTableId;
    }

    public void setParallelDecodeNum(int parallelDecodeNum) {
        this.parallelDecodeNum = parallelDecodeNum;
    }

    public void setDecodeStyle(String decodeStyle) {
        this.decodeStyle = decodeStyle;
    }

    public void setSendingBatch(boolean sendingBatch) {
        this.sendingBatch = sendingBatch;
    }

    public void setReplicationPort(Integer replicationPort) {
        this.replicationPort = replicationPort;
    }
}
