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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBCDCTableSourceFactory} createDynamicTableSource method. */
class GaussDBCDCTableSourceFactoryCreateTest {

    @Test
    void testCreateDynamicTableSourceWithMinimalOptions() {
        Map<String, String> options = createMinimalOptions();
        DynamicTableSource source = createTableSource(options);

        assertThat(source).isInstanceOf(GaussDBCDCTableSource.class);
    }

    @Test
    void testCreateDynamicTableSourceWithAllOptions() {
        Map<String, String> options = createMinimalOptions();
        options.put("port", "9000");
        options.put("schema", "myschema");
        options.put("slot.name", "my_slot");
        options.put("wal.mode", "true");
        options.put("decode.plugin", "mppdb_decoding");
        options.put("parallel-decode-num", "4");
        options.put("decode-style", "b");
        options.put("sending-batch", "true");
        options.put("snapshot.mode", "false");
        options.put("poll.interval.ms", "2000");
        options.put("chunk.size", "5000");
        options.put("connect.timeout.ms", "60000");

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isInstanceOf(GaussDBCDCTableSource.class);
    }

    @Test
    void testCreateDynamicTableSourceWithWalModeEnabled() {
        Map<String, String> options = createMinimalOptions();
        options.put("wal.mode", "true");
        options.put("decode.plugin", "mppdb_decoding");
        options.put("parallel-decode-num", "8");

        DynamicTableSource source = createTableSource(options);
        assertThat(source).isInstanceOf(GaussDBCDCTableSource.class);
    }

    @Test
    void testCreateDynamicTableSourceCopyIsEqual() {
        Map<String, String> options = createMinimalOptions();
        GaussDBCDCTableSource source = (GaussDBCDCTableSource) createTableSource(options);
        GaussDBCDCTableSource copy = (GaussDBCDCTableSource) source.copy();

        assertThat(copy).isNotSameAs(source);
        assertThat(copy.asSummaryString()).isEqualTo(source.asSummaryString());
    }

    @Test
    void testCreateDynamicTableSourceReturnsCorrectChangelogMode() {
        Map<String, String> options = createMinimalOptions();
        GaussDBCDCTableSource source = (GaussDBCDCTableSource) createTableSource(options);

        assertThat(source.getChangelogMode()).isNotNull();
    }

    @Test
    void testCreateDynamicTableSourceReturnsSourceFunctionProvider() {
        Map<String, String> options = createMinimalOptions();
        GaussDBCDCTableSource source = (GaussDBCDCTableSource) createTableSource(options);

        assertThat(source.getScanRuntimeProvider(null))
                .isInstanceOf(org.apache.flink.table.connector.source.SourceFunctionProvider.class);
    }

    @Test
    void testFactoryIdentifierMatches() {
        GaussDBCDCTableSourceFactory factory = new GaussDBCDCTableSourceFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-cdc");
    }

    @Test
    void testRequiredOptionsCount() {
        GaussDBCDCTableSourceFactory factory = new GaussDBCDCTableSourceFactory();
        assertThat(factory.requiredOptions()).hasSize(5);
    }

    @Test
    void testOptionalOptionsCount() {
        GaussDBCDCTableSourceFactory factory = new GaussDBCDCTableSourceFactory();
        assertThat(factory.optionalOptions()).hasSize(13);
    }

    // ---- Helper methods ----

    private Map<String, String> createMinimalOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("hostname", "localhost");
        options.put("database", "testdb");
        options.put("table-name", "test_table");
        options.put("username", "root");
        options.put("password", "pass");
        return options;
    }

    private DynamicTableSource createTableSource(Map<String, String> options) {
        GaussDBCDCTableSourceFactory factory = new GaussDBCDCTableSourceFactory();

        // Create a mock Context that works with FactoryUtil
        DynamicTableFactory.Context context = mock(DynamicTableFactory.Context.class);

        // Create a ResolvedCatalogTable
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.INT().notNull()),
                                Column.physical("name", DataTypes.VARCHAR(100)),
                                Column.physical("age", DataTypes.INT())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

        CatalogTable catalogTable =
                CatalogTable.of(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .fromResolvedSchema(resolvedSchema)
                                .build(),
                        "test",
                        new ArrayList<>(),
                        options);

        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(catalogTable, resolvedSchema);

        ObjectIdentifier identifier = ObjectIdentifier.of("catalog", "database", "table");

        when(context.getObjectIdentifier()).thenReturn(identifier);
        when(context.getCatalogTable()).thenReturn(resolvedCatalogTable);
        when(context.getPhysicalRowDataType()).thenReturn(resolvedSchema.toPhysicalRowDataType());

        return factory.createDynamicTableSource(context);
    }
}
