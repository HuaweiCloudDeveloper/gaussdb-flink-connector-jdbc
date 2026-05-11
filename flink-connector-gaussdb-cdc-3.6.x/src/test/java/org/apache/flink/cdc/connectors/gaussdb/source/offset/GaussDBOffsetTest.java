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

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussDBOffset}. */
class GaussDBOffsetTest {

    @Test
    void testInitialOffset() {
        assertThat(GaussDBOffset.INITIAL_OFFSET).isNotNull();
        assertThat(GaussDBOffset.INITIAL_OFFSET.getLsn()).isEqualTo(Lsn.INVALID_LSN);
    }

    @Test
    void testNoStoppingOffset() {
        assertThat(GaussDBOffset.NO_STOPPING_OFFSET).isNotNull();
        assertThat(GaussDBOffset.NO_STOPPING_OFFSET.getLsn()).isEqualTo(Lsn.NO_STOPPING_LSN);
    }

    @Test
    void testConstructorWithLsnOnly() {
        long lsnValue = 123456789L;
        GaussDBOffset offset = new GaussDBOffset(lsnValue, null, null);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(lsnValue));
        assertThat(offset.getTxid()).isNull();
    }

    @Test
    void testConstructorWithLsnAndTxId() {
        long lsnValue = 123456789L;
        long txId = 100L;
        GaussDBOffset offset = new GaussDBOffset(lsnValue, txId, null);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(lsnValue));
        assertThat(offset.getTxid()).isEqualTo(txId);
    }

    @Test
    void testConstructorWithAllParams() {
        long lsnValue = 123456789L;
        long txId = 100L;
        Instant commitTs = Instant.now();
        GaussDBOffset offset = new GaussDBOffset(lsnValue, txId, commitTs);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(lsnValue));
        assertThat(offset.getTxid()).isEqualTo(txId);
    }

    @Test
    void testConstructorWithOffsetMap() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "123456789");
        offsetMap.put(SourceInfo.TXID_KEY, "100");
        GaussDBOffset offset = new GaussDBOffset(offsetMap);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(123456789L));
        assertThat(offset.getTxid()).isEqualTo(100L);
    }

    @Test
    void testOfSourceRecord() {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, 123456789L);
        offsetMap.put(SourceInfo.TXID_KEY, 100L);
        GaussDBOffset offset = GaussDBOffset.of(offsetMap);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(123456789L));
        assertThat(offset.getTxid()).isEqualTo(100L);
    }

    @Test
    void testOfWithNullValues() {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, 123456789L);
        offsetMap.put(SourceInfo.TXID_KEY, null);
        GaussDBOffset offset = GaussDBOffset.of(offsetMap);
        assertThat(offset.getLsn()).isEqualTo(Lsn.valueOf(123456789L));
        assertThat(offset.getTxid()).isNull();
    }

    @Test
    void testCompareTo() {
        GaussDBOffset offset1 = new GaussDBOffset(100L, null, null);
        GaussDBOffset offset2 = new GaussDBOffset(200L, null, null);
        GaussDBOffset offset3 = new GaussDBOffset(100L, null, null);

        assertThat(offset1.compareTo(offset2)).isLessThan(0);
        assertThat(offset2.compareTo(offset1)).isGreaterThan(0);
        assertThat(offset1.compareTo(offset3)).isEqualTo(0);
    }

    @Test
    void testEquals() {
        GaussDBOffset offset1 = new GaussDBOffset(100L, null, null);
        GaussDBOffset offset2 = new GaussDBOffset(100L, null, null);
        GaussDBOffset offset3 = new GaussDBOffset(200L, null, null);

        assertThat(offset1).isEqualTo(offset2);
        assertThat(offset1).isNotEqualTo(offset3);
        assertThat(offset1).isNotEqualTo(null);
        assertThat(offset1).isNotEqualTo("not an offset");
    }

    @Test
    void testEqualsSameObject() {
        GaussDBOffset offset = new GaussDBOffset(100L, null, null);
        assertThat(offset).isEqualTo(offset);
    }

    @Test
    void testToString() {
        GaussDBOffset offset = new GaussDBOffset(123456L, null, null);
        String str = offset.toString();
        assertThat(str).contains("GaussDBOffset");
        assertThat(str).contains("lsn=");
    }

    @Test
    void testGetTxidWithNullValue() {
        GaussDBOffset offset = new GaussDBOffset(100L, null, null);
        assertThat(offset.getTxid()).isNull();
    }

    @Test
    void testGetTxidWithValue() {
        GaussDBOffset offset = new GaussDBOffset(100L, 42L, null);
        assertThat(offset.getTxid()).isEqualTo(42L);
    }

    @Test
    void testInitialOffsetIsNotNoStopping() {
        assertThat(GaussDBOffset.INITIAL_OFFSET).isNotEqualTo(GaussDBOffset.NO_STOPPING_OFFSET);
    }
}
