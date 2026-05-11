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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Utility class for chunk splitting in GaussDB CDC source. */
public class ChunkUtils {

    private ChunkUtils() {}

    /**
     * Get the split column from the table. If chunkKeyColumn is specified, use it; otherwise, use
     * the first primary key column.
     */
    public static Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        List<Column> primaryKeyColumns = table.primaryKeyColumns();
        if (chunkKeyColumn != null) {
            Optional<Column> targetColumn =
                    table.columns().stream()
                            .filter(c -> c.name().equals(chunkKeyColumn))
                            .findFirst();
            if (targetColumn.isPresent()) {
                return targetColumn.get();
            }
            throw new IllegalArgumentException(
                    "Can not find column " + chunkKeyColumn + " in table " + table.id());
        }
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "No primary key column found in table "
                            + table.id()
                            + ". Please specify 'chunk-key-column' option.");
        }
        return primaryKeyColumns.get(0);
    }

    /** Get the split type (RowType) from a column. */
    public static RowType getSplitType(Column splitColumn) {
        DataType dataType = getSplitDataType(splitColumn);
        return RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {dataType.getLogicalType()},
                new String[] {splitColumn.name()});
    }

    /** Get the split data type from a column. */
    public static DataType getSplitDataType(Column splitColumn) {
        return GaussDBTypeUtils.fromDbzColumn(splitColumn);
    }
}
