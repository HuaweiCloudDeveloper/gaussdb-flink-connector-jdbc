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

import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GaussDBOffsetFactory}. */
class GaussDBOffsetFactoryTest {

    private final GaussDBOffsetFactory factory = new GaussDBOffsetFactory();

    @Test
    void testNewOffsetFromMap() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put("lsn", "123456789");
        GaussDBOffset offset = (GaussDBOffset) factory.newOffset(offsetMap);
        assertThat(offset).isNotNull();
        assertThat(offset.getLsn()).isNotNull();
    }

    @Test
    void testNewOffsetFromFilenameAndPositionThrows() {
        assertThatThrownBy(() -> factory.newOffset("file.log", 100L))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("not supported");
    }

    @Test
    void testNewOffsetFromPositionThrows() {
        assertThatThrownBy(() -> factory.newOffset(100L))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("not supported");
    }

    @Test
    void testCreateTimestampOffsetThrows() {
        assertThatThrownBy(() -> factory.createTimestampOffset(1000L))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("not supported");
    }

    @Test
    void testCreateInitialOffset() {
        GaussDBOffset offset = (GaussDBOffset) factory.createInitialOffset();
        assertThat(offset).isEqualTo(GaussDBOffset.INITIAL_OFFSET);
    }

    @Test
    void testCreateNoStoppingOffset() {
        GaussDBOffset offset = (GaussDBOffset) factory.createNoStoppingOffset();
        assertThat(offset).isEqualTo(GaussDBOffset.NO_STOPPING_OFFSET);
    }
}
