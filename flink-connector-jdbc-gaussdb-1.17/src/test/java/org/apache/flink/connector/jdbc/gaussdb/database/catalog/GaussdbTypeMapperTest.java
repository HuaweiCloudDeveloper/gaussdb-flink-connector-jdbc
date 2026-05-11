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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussdbTypeMapper}. */
class GaussdbTypeMapperTest {

    private final GaussdbTypeMapper mapper = new GaussdbTypeMapper();
    private final ObjectPath tablePath = new ObjectPath("testdb", "testtable");

    @Test
    void testBooleanMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("bool", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.BOOLEAN());
    }

    @Test
    void testSmallintMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("int2", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.SMALLINT());
    }

    @Test
    void testSmallserialMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("smallserial", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.SMALLINT());
    }

    @Test
    void testIntegerMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("int4", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.INT());
    }

    @Test
    void testSerialMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("serial", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.INT());
    }

    @Test
    void testBigintMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("int8", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testBigserialMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("bigserial", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testRealMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("float4", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.FLOAT());
    }

    @Test
    void testDoublePrecisionMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("float8", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testNumericWithPrecisionMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("numeric", 10, 2);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(10, 2));
    }

    @Test
    void testNumericWithoutPrecisionMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("numeric", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(38, 18));
    }

    @Test
    void testCharMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("bpchar", 10, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.CHAR(10));
    }

    @Test
    void testVarcharMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("varchar", 255, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.VARCHAR(255));
    }

    @Test
    void testTextMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("text", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testTimestampMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("timestamp", 0, 6);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(6));
    }

    @Test
    void testTimestamptzMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("timestamptz", 0, 3);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
    }

    @Test
    void testDateMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("date", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    @Test
    void testTimeMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("time", 0, 6);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.TIME(6));
    }

    @Test
    void testByteaMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("bytea", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testUnsupportedTypeMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("unsupported_type", 0, 0);
        assertThatThrownBy(() -> mapper.mapping(tablePath, metadata, 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Flink doesn't support GaussDB type 'unsupported_type'");
    }

    // Array type mappings

    @Test
    void testBooleanArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_bool", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.BOOLEAN()));
    }

    @Test
    void testSmallintArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_int2", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.SMALLINT()));
    }

    @Test
    void testIntegerArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_int4", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.INT()));
    }

    @Test
    void testBigintArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_int8", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.BIGINT()));
    }

    @Test
    void testRealArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_float4", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.FLOAT()));
    }

    @Test
    void testDoublePrecisionArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_float8", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.DOUBLE()));
    }

    @Test
    void testNumericArrayWithPrecisionMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_numeric", 10, 2);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)));
    }

    @Test
    void testNumericArrayWithoutPrecisionMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_numeric", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.DECIMAL(38, 18)));
    }

    @Test
    void testCharArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_bpchar", 10, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.CHAR(10)));
    }

    @Test
    void testCharacterArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_character", 10, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.CHAR(10)));
    }

    @Test
    void testVarcharArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_varchar", 255, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.VARCHAR(255)));
    }

    @Test
    void testTextArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_text", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
    }

    @Test
    void testTimestampArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_timestamp", 0, 6);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.TIMESTAMP(6)));
    }

    @Test
    void testTimestamptzArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_timestamptz", 0, 3);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)));
    }

    @Test
    void testDateArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_date", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.DATE()));
    }

    @Test
    void testTimeArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_time", 0, 6);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.TIME(6)));
    }

    @Test
    void testByteaArrayMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("_bytea", 0, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.ARRAY(DataTypes.BYTES()));
    }

    @Test
    void testCharacterMapping() throws SQLException {
        ResultSetMetaData metadata = createMockMetadata("character", 10, 0);
        DataType result = mapper.mapping(tablePath, metadata, 1);
        assertThat(result).isEqualTo(DataTypes.CHAR(10));
    }

    private ResultSetMetaData createMockMetadata(String typeName, int precision, int scale)
            throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnTypeName(1)).thenReturn(typeName);
        when(metadata.getPrecision(1)).thenReturn(precision);
        when(metadata.getScale(1)).thenReturn(scale);
        return metadata;
    }
}
