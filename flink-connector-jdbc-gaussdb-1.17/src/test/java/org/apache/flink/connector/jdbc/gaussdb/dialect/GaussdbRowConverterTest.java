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

package org.apache.flink.connector.jdbc.gaussdb.dialect;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GaussdbRowConverter}. */
class GaussdbRowConverterTest {

    @Test
    void testConverterName() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType()
                        },
                        new String[] {"id"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter.converterName()).isEqualTo("GaussDB");
    }

    @Test
    void testConverterWithMultipleFields() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(),
                            DataTypes.STRING().getLogicalType(),
                            DataTypes.BIGINT().getLogicalType(),
                            DataTypes.DOUBLE().getLogicalType(),
                            DataTypes.BOOLEAN().getLogicalType()
                        },
                        new String[] {"id", "name", "count", "price", "active"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isNotNull();
        assertThat(converter.converterName()).isEqualTo("GaussDB");
    }

    @Test
    void testConverterInheritsFromAbstractJdbcRowConverter() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType()
                        },
                        new String[] {"id"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isInstanceOf(AbstractJdbcRowConverter.class);
    }

    @Test
    void testConverterWithDecimalType() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(),
                            DataTypes.DECIMAL(18, 2).getLogicalType()
                        },
                        new String[] {"id", "amount"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isNotNull();
    }

    @Test
    void testConverterWithTimestampType() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(),
                            DataTypes.TIMESTAMP(3).getLogicalType()
                        },
                        new String[] {"id", "created_at"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isNotNull();
    }

    @Test
    void testConverterWithDateType() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(), DataTypes.DATE().getLogicalType()
                        },
                        new String[] {"id", "birth_date"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isNotNull();
    }

    @Test
    void testConverterWithBinaryType() {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            DataTypes.INT().getLogicalType(),
                            DataTypes.VARBINARY(100).getLogicalType()
                        },
                        new String[] {"id", "data"});
        GaussdbRowConverter converter = new GaussdbRowConverter(rowType);
        assertThat(converter).isNotNull();
    }
}
