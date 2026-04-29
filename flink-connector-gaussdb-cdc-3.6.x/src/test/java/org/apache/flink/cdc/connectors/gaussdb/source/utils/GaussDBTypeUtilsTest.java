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

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBTypeUtils}. */
class GaussDBTypeUtilsTest {

    private Column createColumn(String typeName, boolean optional, int length, int scale) {
        Column column = mock(Column.class);
        when(column.typeName()).thenReturn(typeName);
        when(column.isOptional()).thenReturn(optional);
        when(column.length()).thenReturn(length);
        when(column.scale()).thenReturn(Optional.of(scale));
        return column;
    }

    private Column createColumn(String typeName, boolean optional, int length) {
        return createColumn(typeName, optional, length, 0);
    }

    @Test
    void testBooleanType() {
        Column col = createColumn("bool", true, 1);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("BOOLEAN");
    }

    @Test
    void testBooleanArrayType() {
        Column col = createColumn("_bool", true, 1);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testByteaType() {
        Column col = createColumn("bytea", true, 1);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("VARBINARY");
    }

    @Test
    void testSmallIntType() {
        Column col = createColumn("int2", true, 2);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("SMALLINT");
    }

    @Test
    void testSmallSerialType() {
        Column col = createColumn("smallserial", false, 2);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("SMALLINT");
    }

    @Test
    void testIntegerType() {
        Column col = createColumn("int4", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("INTEGER");
    }

    @Test
    void testSerialType() {
        Column col = createColumn("serial", false, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("INTEGER");
    }

    @Test
    void testBigIntType() {
        Column col = createColumn("int8", true, 8);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("BIGINT");
    }

    @Test
    void testBigSerialType() {
        Column col = createColumn("bigserial", false, 8);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("BIGINT");
    }

    @Test
    void testRealType() {
        Column col = createColumn("float4", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("FLOAT");
    }

    @Test
    void testDoublePrecisionType() {
        Column col = createColumn("float8", true, 8);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("DOUBLE");
    }

    @Test
    void testNumericWithPrecision() {
        Column col = createColumn("numeric", true, 10, 2);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("DECIMAL");
    }

    @Test
    void testNumericWithoutPrecision() {
        Column col = mock(Column.class);
        when(col.typeName()).thenReturn("numeric");
        when(col.isOptional()).thenReturn(true);
        when(col.length()).thenReturn(0);
        when(col.scale()).thenReturn(Optional.of(0));
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("DECIMAL");
    }

    @Test
    void testCharType() {
        Column col = createColumn("bpchar", true, 10);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("CHAR");
    }

    @Test
    void testCharacterType() {
        Column col = createColumn("character", true, 10);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("CHAR");
    }

    @Test
    void testVarcharType() {
        Column col = createColumn("varchar", true, 255);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("VARCHAR");
    }

    @Test
    void testTextType() {
        Column col = createColumn("text", true, Integer.MAX_VALUE);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("VARCHAR");
    }

    @Test
    void testUuidType() {
        Column col = createColumn("uuid", true, 16);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("VARCHAR");
    }

    @Test
    void testTimestampType() {
        Column col = createColumn("timestamp", true, 6, 6);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name())
                .isEqualTo("TIMESTAMP_WITHOUT_TIME_ZONE");
    }

    @Test
    void testTimestamptzType() {
        Column col = createColumn("timestamptz", true, 6, 6);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name())
                .isEqualTo("TIMESTAMP_WITH_LOCAL_TIME_ZONE");
    }

    @Test
    void testDateType() {
        Column col = createColumn("date", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("DATE");
    }

    @Test
    void testTimeType() {
        Column col = createColumn("time", true, 8, 6);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name())
                .isEqualTo("TIME_WITHOUT_TIME_ZONE");
    }

    @Test
    void testUnsupportedType() {
        Column col = createColumn("geometry", true, 1);
        assertThatThrownBy(() -> GaussDBTypeUtils.fromDbzColumn(col))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Doesn't support GaussDB type 'geometry' yet");
    }

    @Test
    void testNotNullableColumn() {
        Column col = createColumn("int4", false, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().isNullable()).isFalse();
    }

    @Test
    void testNullableColumn() {
        Column col = createColumn("int4", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().isNullable()).isTrue();
    }

    @Test
    void testIntArrayType() {
        Column col = createColumn("_int4", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testBigIntArrayType() {
        Column col = createColumn("_int8", true, 8);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testFloatArrayType() {
        Column col = createColumn("_float4", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testDoubleArrayType() {
        Column col = createColumn("_float8", true, 8);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testTextArrayType() {
        Column col = createColumn("_text", true, 1);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testVarcharArrayType() {
        Column col = createColumn("_varchar", true, 255);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testDateArrayType() {
        Column col = createColumn("_date", true, 4);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }

    @Test
    void testTimestampArrayType() {
        Column col = createColumn("_timestamp", true, 6, 6);
        DataType result = GaussDBTypeUtils.fromDbzColumn(col);
        assertThat(result.getLogicalType().getTypeRoot().name()).isEqualTo("ARRAY");
    }
}
