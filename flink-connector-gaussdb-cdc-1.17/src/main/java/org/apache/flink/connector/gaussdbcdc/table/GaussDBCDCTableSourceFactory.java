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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.gaussdbcdc.GaussDBCDCOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/** Factory for creating GaussDB CDC table source. */
@Internal
public class GaussDBCDCTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "gaussdb-cdc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GaussDBCDCOptions.HOSTNAME);
        options.add(GaussDBCDCOptions.DATABASE);
        options.add(GaussDBCDCOptions.TABLE_NAME);
        options.add(GaussDBCDCOptions.USERNAME);
        options.add(GaussDBCDCOptions.PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GaussDBCDCOptions.PORT);
        options.add(GaussDBCDCOptions.SCHEMA);
        options.add(GaussDBCDCOptions.SLOT_NAME);
        options.add(GaussDBCDCOptions.PLUGIN_NAME);
        options.add(GaussDBCDCOptions.SNAPSHOT_MODE);
        options.add(GaussDBCDCOptions.CHUNK_SIZE);
        options.add(GaussDBCDCOptions.CONNECT_TIMEOUT_MS);
        options.add(GaussDBCDCOptions.POLL_INTERVAL_MS);
        options.add(GaussDBCDCOptions.WAL_MODE);
        options.add(GaussDBCDCOptions.DECODE_PLUGIN);
        options.add(GaussDBCDCOptions.PARALLEL_DECODE_NUM);
        options.add(GaussDBCDCOptions.DECODE_STYLE);
        options.add(GaussDBCDCOptions.SENDING_BATCH);
        options.add(GaussDBCDCOptions.SSL_MODE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String hostname = config.get(GaussDBCDCOptions.HOSTNAME);
        int port = config.get(GaussDBCDCOptions.PORT);
        String database = config.get(GaussDBCDCOptions.DATABASE);
        String schema = config.get(GaussDBCDCOptions.SCHEMA);
        String tableName = config.get(GaussDBCDCOptions.TABLE_NAME);
        String username = config.get(GaussDBCDCOptions.USERNAME);
        String password = config.get(GaussDBCDCOptions.PASSWORD);
        String slotName = config.get(GaussDBCDCOptions.SLOT_NAME);
        boolean snapshotMode = config.get(GaussDBCDCOptions.SNAPSHOT_MODE);
        int pollIntervalMs = config.get(GaussDBCDCOptions.POLL_INTERVAL_MS);
        boolean walMode = config.get(GaussDBCDCOptions.WAL_MODE);
        String decodePlugin = config.get(GaussDBCDCOptions.DECODE_PLUGIN);
        int parallelDecodeNum = config.get(GaussDBCDCOptions.PARALLEL_DECODE_NUM);
        String decodeStyle = config.get(GaussDBCDCOptions.DECODE_STYLE);
        boolean sendingBatch = config.get(GaussDBCDCOptions.SENDING_BATCH);
        String sslMode = config.get(GaussDBCDCOptions.SSL_MODE);

        DataType physicalRowDataType = context.getPhysicalRowDataType();

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
}
