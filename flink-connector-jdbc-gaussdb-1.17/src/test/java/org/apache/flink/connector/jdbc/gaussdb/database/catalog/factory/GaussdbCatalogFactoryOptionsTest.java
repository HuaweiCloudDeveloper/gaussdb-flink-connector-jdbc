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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussdbCatalogFactoryOptions}. */
class GaussdbCatalogFactoryOptionsTest {

    @Test
    void testIdentifier() {
        assertThat(GaussdbCatalogFactoryOptions.IDENTIFIER).isEqualTo("gaussdb-catalog");
    }

    @Test
    void testDefaultDatabaseOption() {
        assertThat(GaussdbCatalogFactoryOptions.DEFAULT_DATABASE.key())
                .isEqualTo("default-database");
        assertThat(GaussdbCatalogFactoryOptions.DEFAULT_DATABASE.hasDefaultValue()).isFalse();
    }

    @Test
    void testUsernameOption() {
        assertThat(GaussdbCatalogFactoryOptions.USERNAME.key()).isEqualTo("username");
        assertThat(GaussdbCatalogFactoryOptions.USERNAME.hasDefaultValue()).isFalse();
    }

    @Test
    void testPasswordOption() {
        assertThat(GaussdbCatalogFactoryOptions.PASSWORD.key()).isEqualTo("password");
        assertThat(GaussdbCatalogFactoryOptions.PASSWORD.hasDefaultValue()).isFalse();
    }

    @Test
    void testBaseUrlOption() {
        assertThat(GaussdbCatalogFactoryOptions.BASE_URL.key()).isEqualTo("base-url");
        assertThat(GaussdbCatalogFactoryOptions.BASE_URL.hasDefaultValue()).isFalse();
    }
}
