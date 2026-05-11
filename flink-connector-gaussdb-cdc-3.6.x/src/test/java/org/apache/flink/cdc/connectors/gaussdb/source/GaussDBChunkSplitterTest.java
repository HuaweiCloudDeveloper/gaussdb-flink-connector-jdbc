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

import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfigFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBChunkSplitter}. */
class GaussDBChunkSplitterTest {

    private GaussDBSourceConfig sourceConfig;

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
    }

    @Test
    void testCreateChunkSplitterWithDefaultState() {
        GaussDBDialect dialect = new GaussDBDialect(sourceConfig);
        ChunkSplitter splitter = dialect.createChunkSplitter(sourceConfig);
        assertThat(splitter).isInstanceOf(GaussDBChunkSplitter.class);
    }

    @Test
    void testCreateChunkSplitterWithState() {
        GaussDBDialect dialect = new GaussDBDialect(sourceConfig);
        ChunkSplitter splitter =
                dialect.createChunkSplitter(
                        sourceConfig, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
        assertThat(splitter).isInstanceOf(GaussDBChunkSplitter.class);
    }
}
