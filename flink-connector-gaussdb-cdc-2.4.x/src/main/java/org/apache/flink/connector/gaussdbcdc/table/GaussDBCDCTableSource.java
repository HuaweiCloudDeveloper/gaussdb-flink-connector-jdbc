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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.gaussdbcdc.source.GaussDBCDCSourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.DataType;

/** Table source for GaussDB CDC using SourceFunction API (Flink 1.13~1.17 compatible). */
@Internal
public class GaussDBCDCTableSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final String slotName;
    private final boolean snapshotMode;
    private final int pollIntervalMs;
    private final boolean walMode;
    private final String decodePlugin;
    private final int parallelDecodeNum;
    private final String decodeStyle;
    private final boolean sendingBatch;
    private final String sslMode;
    private final DataType physicalRowDataType;

    public GaussDBCDCTableSource(
            String hostname,
            int port,
            String database,
            String schema,
            String tableName,
            String username,
            String password,
            String slotName,
            boolean snapshotMode,
            int pollIntervalMs,
            boolean walMode,
            String decodePlugin,
            int parallelDecodeNum,
            String decodeStyle,
            boolean sendingBatch,
            String sslMode,
            DataType physicalRowDataType) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.slotName = slotName;
        this.snapshotMode = snapshotMode;
        this.pollIntervalMs = pollIntervalMs;
        this.walMode = walMode;
        this.decodePlugin = decodePlugin;
        this.parallelDecodeNum = parallelDecodeNum;
        this.decodeStyle = decodeStyle;
        this.sendingBatch = sendingBatch;
        this.sslMode = sslMode;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        GaussDBCDCSourceFunction sourceFunction =
                GaussDBCDCSourceFunction.builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .schema(schema)
                        .tableName(tableName)
                        .username(username)
                        .password(password)
                        .slotName(slotName)
                        .snapshotMode(snapshotMode)
                        .pollIntervalMs(pollIntervalMs)
                        .walMode(walMode)
                        .decodePlugin(decodePlugin)
                        .parallelDecodeNum(parallelDecodeNum)
                        .decodeStyle(decodeStyle)
                        .sendingBatch(sendingBatch)
                        .sslMode(sslMode)
                        .build();

        // SourceFunctionProvider is available since Flink 1.11, compatible with 1.13~1.17
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new GaussDBCDCTableSource(
                hostname,
                port,
                database,
                schema,
                tableName,
                username,
                password,
                slotName,
                snapshotMode,
                pollIntervalMs,
                walMode,
                decodePlugin,
                parallelDecodeNum,
                decodeStyle,
                sendingBatch,
                sslMode,
                physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "GaussDB-CDC";
    }
}
