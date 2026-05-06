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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBCDCTableSource}. */
class GaussDBCDCTableSourceTest {

    @Test
    void testGetChangelogMode() {
        GaussDBCDCTableSource source = createTestSource();
        ChangelogMode mode = source.getChangelogMode();
        assertThat(mode).isNotNull();
    }

    @Test
    void testGetScanRuntimeProvider() {
        GaussDBCDCTableSource source = createTestSource();
        ScanTableSource.ScanRuntimeProvider provider = source.getScanRuntimeProvider(null);
        assertThat(provider).isNotNull();
    }

    @Test
    void testCopy() {
        GaussDBCDCTableSource source = createTestSource();
        DynamicTableSource copy = source.copy();
        assertThat(copy).isNotNull();
        assertThat(copy).isInstanceOf(GaussDBCDCTableSource.class);
    }

    @Test
    void testAsSummaryString() {
        GaussDBCDCTableSource source = createTestSource();
        assertThat(source.asSummaryString()).isEqualTo("GaussDB-CDC");
    }

    private GaussDBCDCTableSource createTestSource() {
        return new GaussDBCDCTableSource(
                "localhost",
                5432,
                "testdb",
                "public",
                "test_table",
                "user",
                "pass",
                "slot1",
                true,
                1000,
                false,
                "mppdb_decoding",
                1,
                "b",
                false,
                "prefer",
                DataTypes.STRING());
    }
}
