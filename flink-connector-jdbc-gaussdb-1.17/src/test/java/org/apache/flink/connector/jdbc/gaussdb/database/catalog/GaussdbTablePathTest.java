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

package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GaussdbTablePath}. */
class GaussdbTablePathTest {

    @Test
    void testFromFlinkTableNameWithSchema() {
        GaussdbTablePath path = GaussdbTablePath.fromFlinkTableName("myschema.mytable");
        assertThat(path.getPgSchemaName()).isEqualTo("myschema");
        assertThat(path.getPgTableName()).isEqualTo("mytable");
        assertThat(path.getFullPath()).isEqualTo("myschema.mytable");
    }

    @Test
    void testFromFlinkTableNameWithoutSchema() {
        GaussdbTablePath path = GaussdbTablePath.fromFlinkTableName("mytable");
        assertThat(path.getPgSchemaName()).isEqualTo("public");
        assertThat(path.getPgTableName()).isEqualTo("mytable");
        assertThat(path.getFullPath()).isEqualTo("public.mytable");
    }

    @Test
    void testToFlinkTableName() {
        String tableName = GaussdbTablePath.toFlinkTableName("myschema", "mytable");
        assertThat(tableName).isEqualTo("myschema.mytable");
    }

    @Test
    void testConstructor() {
        GaussdbTablePath path = new GaussdbTablePath("testschema", "testtable");
        assertThat(path.getPgSchemaName()).isEqualTo("testschema");
        assertThat(path.getPgTableName()).isEqualTo("testtable");
    }

    @Test
    void testToString() {
        GaussdbTablePath path = new GaussdbTablePath("schema", "table");
        assertThat(path.toString()).isEqualTo("schema.table");
    }

    @Test
    void testEquals() {
        GaussdbTablePath path1 = new GaussdbTablePath("schema", "table");
        GaussdbTablePath path2 = new GaussdbTablePath("schema", "table");
        GaussdbTablePath path3 = new GaussdbTablePath("schema2", "table");

        assertThat(path1).isEqualTo(path2);
        assertThat(path1).isNotEqualTo(path3);
        assertThat(path1).isEqualTo(path1);
        assertThat(path1).isNotEqualTo(null);
        assertThat(path1).isNotEqualTo("schema.table");
    }

    @Test
    void testHashCode() {
        GaussdbTablePath path1 = new GaussdbTablePath("schema", "table");
        GaussdbTablePath path2 = new GaussdbTablePath("schema", "table");

        assertThat(path1.hashCode()).isEqualTo(path2.hashCode());
    }

    @Test
    void testInvalidTableNameWithMultipleDots() {
        assertThatThrownBy(() -> GaussdbTablePath.fromFlinkTableName("a.b.c"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name 'a.b.c' is not valid");
    }

    @Test
    void testNullSchemaName() {
        assertThatThrownBy(() -> new GaussdbTablePath(null, "table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullTableName() {
        assertThatThrownBy(() -> new GaussdbTablePath("schema", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptySchemaName() {
        assertThatThrownBy(() -> new GaussdbTablePath("", "table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptyTableName() {
        assertThatThrownBy(() -> new GaussdbTablePath("schema", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
