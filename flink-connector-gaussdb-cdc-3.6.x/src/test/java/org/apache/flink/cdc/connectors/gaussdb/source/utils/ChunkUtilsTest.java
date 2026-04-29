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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link ChunkUtils}. */
class ChunkUtilsTest {

    private Column createColumn(String name, String typeName) {
        Column column = mock(Column.class);
        when(column.name()).thenReturn(name);
        when(column.typeName()).thenReturn(typeName);
        when(column.isOptional()).thenReturn(true);
        when(column.length()).thenReturn(4);
        when(column.scale()).thenReturn(Optional.of(0));
        return column;
    }

    @Test
    void testGetSplitColumnFromPrimaryKey() {
        Column pkCol = createColumn("id", "int4");
        Table table = mock(Table.class);
        when(table.primaryKeyColumns()).thenReturn(Collections.singletonList(pkCol));
        when(table.columns()).thenReturn(Collections.singletonList(pkCol));
        when(table.id()).thenReturn(new TableId("db", "public", "test_table"));

        Column result = ChunkUtils.getSplitColumn(table, null);
        assertThat(result.name()).isEqualTo("id");
    }

    @Test
    void testGetSplitColumnWithChunkKeyColumn() {
        Column idCol = createColumn("id", "int4");
        Column nameCol = createColumn("name", "varchar");
        Table table = mock(Table.class);
        when(table.primaryKeyColumns()).thenReturn(Collections.singletonList(idCol));
        when(table.columns()).thenReturn(Arrays.asList(idCol, nameCol));
        when(table.id()).thenReturn(new TableId("db", "public", "test_table"));

        Column result = ChunkUtils.getSplitColumn(table, "name");
        assertThat(result.name()).isEqualTo("name");
    }

    @Test
    void testGetSplitColumnWithInvalidChunkKeyColumn() {
        Column idCol = createColumn("id", "int4");
        Table table = mock(Table.class);
        when(table.primaryKeyColumns()).thenReturn(Collections.singletonList(idCol));
        when(table.columns()).thenReturn(Collections.singletonList(idCol));
        when(table.id()).thenReturn(new TableId("db", "public", "test_table"));

        assertThatThrownBy(() -> ChunkUtils.getSplitColumn(table, "nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not find column nonexistent");
    }

    @Test
    void testGetSplitColumnWithNoPrimaryKey() {
        Table table = mock(Table.class);
        when(table.primaryKeyColumns()).thenReturn(Collections.emptyList());
        when(table.id()).thenReturn(new TableId("db", "public", "test_table"));

        assertThatThrownBy(() -> ChunkUtils.getSplitColumn(table, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No primary key column found");
    }

    @Test
    void testGetSplitType() {
        Column column = createColumn("id", "int4");
        assertThat(ChunkUtils.getSplitType(column)).isNotNull();
    }

    @Test
    void testGetSplitDataType() {
        Column column = createColumn("id", "int4");
        assertThat(ChunkUtils.getSplitDataType(column)).isNotNull();
    }
}
