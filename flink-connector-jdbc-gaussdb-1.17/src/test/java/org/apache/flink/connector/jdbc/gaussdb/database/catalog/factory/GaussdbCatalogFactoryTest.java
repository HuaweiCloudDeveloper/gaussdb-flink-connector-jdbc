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

package org.apache.flink.connector.jdbc.gaussdb.database.catalog.factory;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussdbCatalogFactory}. */
class GaussdbCatalogFactoryTest {

    private final GaussdbCatalogFactory factory = new GaussdbCatalogFactory();

    @Test
    void testFactoryIdentifier() {
        assertThat(factory.factoryIdentifier()).isEqualTo("gaussdb-catalog");
    }

    @Test
    void testRequiredOptions() {
        Set<ConfigOption<?>> requiredOptions = factory.requiredOptions();
        assertThat(requiredOptions).isNotNull();
        assertThat(requiredOptions).hasSize(4);
    }

    @Test
    void testOptionalOptions() {
        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertThat(optionalOptions).isNotNull();
        assertThat(optionalOptions).hasSize(1); // PROPERTY_VERSION
    }
}
