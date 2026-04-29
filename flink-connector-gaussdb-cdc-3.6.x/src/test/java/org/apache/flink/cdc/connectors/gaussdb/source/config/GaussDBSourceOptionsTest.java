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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBSourceOptions}. */
class GaussDBSourceOptionsTest {

    @Test
    void testPortDefaultValue() {
        assertThat(GaussDBSourceOptions.PORT.defaultValue()).isEqualTo(8000);
    }

    @Test
    void testDecodingPluginNameDefaultValue() {
        assertThat(GaussDBSourceOptions.DECODING_PLUGIN_NAME.defaultValue())
                .isEqualTo("mppdb_decoding");
    }

    @Test
    void testSlotNameHasNoDefault() {
        assertThat(GaussDBSourceOptions.SLOT_NAME.hasDefaultValue()).isFalse();
    }

    @Test
    void testChangelogModeDefaultValue() {
        assertThat(GaussDBSourceOptions.CHANGELOG_MODE.defaultValue())
                .isEqualTo(org.apache.flink.cdc.debezium.table.DebeziumChangelogMode.ALL);
    }

    @Test
    void testScanIncrementalSnapshotEnabledDefaultValue() {
        assertThat(GaussDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED.defaultValue()).isTrue();
    }

    @Test
    void testHeartbeatIntervalDefaultValue() {
        assertThat(GaussDBSourceOptions.HEARTBEAT_INTERVAL.defaultValue())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testScanLsnCommitCheckpointsDelayDefaultValue() {
        assertThat(GaussDBSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY.defaultValue())
                .isEqualTo(3);
    }

    @Test
    void testParallelDecodeNumDefaultValue() {
        assertThat(GaussDBSourceOptions.PARALLEL_DECODE_NUM.defaultValue()).isEqualTo(1);
    }

    @Test
    void testDecodeStyleDefaultValue() {
        assertThat(GaussDBSourceOptions.DECODE_STYLE.defaultValue()).isEqualTo("b");
    }

    @Test
    void testSendingBatchDefaultValue() {
        assertThat(GaussDBSourceOptions.SENDING_BATCH.defaultValue()).isFalse();
    }

    @Test
    void testTableIdIncludeDatabaseDefaultValue() {
        assertThat(GaussDBSourceOptions.TABLE_ID_INCLUDE_DATABASE.defaultValue()).isTrue();
    }

    @Test
    void testPortKey() {
        assertThat(GaussDBSourceOptions.PORT.key()).isEqualTo("port");
    }

    @Test
    void testDecodingPluginNameKey() {
        assertThat(GaussDBSourceOptions.DECODING_PLUGIN_NAME.key())
                .isEqualTo("decoding.plugin.name");
    }

    @Test
    void testSlotNameKey() {
        assertThat(GaussDBSourceOptions.SLOT_NAME.key()).isEqualTo("slot.name");
    }

    @Test
    void testParallelDecodeNumKey() {
        assertThat(GaussDBSourceOptions.PARALLEL_DECODE_NUM.key()).isEqualTo("parallel-decode-num");
    }

    @Test
    void testDecodeStyleKey() {
        assertThat(GaussDBSourceOptions.DECODE_STYLE.key()).isEqualTo("decode-style");
    }

    @Test
    void testSendingBatchKey() {
        assertThat(GaussDBSourceOptions.SENDING_BATCH.key()).isEqualTo("sending-batch");
    }
}
