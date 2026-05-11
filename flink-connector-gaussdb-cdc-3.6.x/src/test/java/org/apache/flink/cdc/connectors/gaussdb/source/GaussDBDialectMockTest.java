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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link GaussDBDialect} with mocked connections. */
class GaussDBDialectMockTest {

    private GaussDBSourceConfig sourceConfig;
    private GaussDBDialect dialect;

    @BeforeEach
    void setUp() {
        GaussDBSourceConfigFactory factory = new GaussDBSourceConfigFactory();
        factory.hostname("localhost");
        factory.database("testdb");
        factory.username("testuser");
        factory.password("testpass");
        factory.port(8000);
        factory.tableList("public.test_table");
        factory.slotName("test_slot");
        sourceConfig = factory.create(0);
        dialect = new GaussDBDialect(sourceConfig);
    }

    @Test
    void testOpenJdbcConnectionFails() {
        // openJdbcConnection will fail because there's no real DB
        assertThatThrownBy(() -> dialect.openJdbcConnection(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDisplayCurrentOffsetFails() {
        assertThatThrownBy(() -> dialect.displayCurrentOffset(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDisplayCommittedOffsetFails() {
        assertThatThrownBy(() -> dialect.displayCommittedOffset(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDiscoverDataCollectionsFails() {
        assertThatThrownBy(() -> dialect.discoverDataCollections(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testDiscoverDataCollectionSchemasFails() {
        assertThatThrownBy(() -> dialect.discoverDataCollectionSchemas(sourceConfig))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testQueryTableSchemaWithNullJdbc() {
        JdbcConnection jdbc = mock(JdbcConnection.class);
        TableId tableId = new TableId("testdb", "public", "nonexistent");
        // This should return null because table doesn't exist
        io.debezium.relational.history.TableChanges.TableChange result =
                dialect.queryTableSchema(jdbc, tableId);
        // The method catches SQLException and returns null
        assertThat(result).isNull();
    }

    @Test
    void testRemoveSlotFails() {
        // This should return false because no DB connection
        boolean result = dialect.removeSlot("nonexistent_slot");
        assertThat(result).isFalse();
    }

    @Test
    void testGetSlotNameAndPluginName() {
        assertThat(dialect.getSlotName()).isEqualTo("test_slot");
        assertThat(dialect.getPluginName()).isEqualTo("mppdb_decoding");
    }

    @Test
    void testCreateFetchTaskContext() {
        var context = dialect.createFetchTaskContext(sourceConfig);
        assertThat(context).isInstanceOf(GaussDBSourceFetchTaskContext.class);
    }

    @Test
    void testIsIncludeDataCollectionWithMatchingTable() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        boolean result = dialect.isIncludeDataCollection(sourceConfig, tableId);
        assertThat(result).isTrue();
    }

    @Test
    void testIsIncludeDataCollectionWithNonMatchingTable() {
        TableId tableId = new TableId("testdb", "public", "other_table");
        boolean result = dialect.isIncludeDataCollection(sourceConfig, tableId);
        assertThat(result).isFalse();
    }
}
