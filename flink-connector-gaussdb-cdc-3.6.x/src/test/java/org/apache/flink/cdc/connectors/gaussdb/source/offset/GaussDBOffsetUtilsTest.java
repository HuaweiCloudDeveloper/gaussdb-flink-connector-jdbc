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

package org.apache.flink.cdc.connectors.gaussdb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link GaussDBOffsetUtils}. */
class GaussDBOffsetUtilsTest {

    @Test
    void testGetPostgresOffsetContextWithValidOffset() {
        PostgresOffsetContext.Loader loader = mock(PostgresOffsetContext.Loader.class);
        PostgresOffsetContext mockContext = mock(PostgresOffsetContext.class);
        when(loader.load(any())).thenReturn(mockContext);

        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "123456789");
        offsetMap.put(SourceInfo.TXID_KEY, "100");
        Offset offset = new GaussDBOffset(offsetMap);

        PostgresOffsetContext result = GaussDBOffsetUtils.getPostgresOffsetContext(loader, offset);
        assertThat(result).isEqualTo(mockContext);
    }

    @Test
    void testGetPostgresOffsetContextWithNullOffset() {
        PostgresOffsetContext.Loader loader = mock(PostgresOffsetContext.Loader.class);
        assertThatThrownBy(() -> GaussDBOffsetUtils.getPostgresOffsetContext(loader, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("offset is null");
    }

    @Test
    void testGetPostgresOffsetContextWithNullValue() {
        PostgresOffsetContext.Loader loader = mock(PostgresOffsetContext.Loader.class);
        PostgresOffsetContext mockContext = mock(PostgresOffsetContext.class);
        when(loader.load(any())).thenReturn(mockContext);

        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "123456789");
        offsetMap.put(SourceInfo.TXID_KEY, null);
        Offset offset = new GaussDBOffset(offsetMap);

        PostgresOffsetContext result = GaussDBOffsetUtils.getPostgresOffsetContext(loader, offset);
        assertThat(result).isEqualTo(mockContext);
    }
}
