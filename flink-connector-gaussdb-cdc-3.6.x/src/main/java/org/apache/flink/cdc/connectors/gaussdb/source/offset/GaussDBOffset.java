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

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.time.Conversions;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The offset for GaussDB, compatible with PostgreSQL LSN format. */
public class GaussDBOffset extends Offset {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(GaussDBOffset.class);

    public static final GaussDBOffset INITIAL_OFFSET =
            new GaussDBOffset(Lsn.INVALID_LSN.asLong(), null, Instant.MIN);
    public static final GaussDBOffset NO_STOPPING_OFFSET =
            new GaussDBOffset(Lsn.NO_STOPPING_LSN.asLong(), null, Instant.MAX);

    public GaussDBOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public GaussDBOffset(Long lsn, Long txId, Instant lastCommitTs) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, lsn.toString());
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, txId.toString());
        }
        if (lastCommitTs != null) {
            offsetMap.put(
                    SourceInfo.TIMESTAMP_USEC_KEY,
                    String.valueOf(Conversions.toEpochMicros(lastCommitTs)));
        }
        this.offset = offsetMap;
    }

    public static GaussDBOffset of(SourceRecord dataRecord) {
        return of(dataRecord.sourceOffset());
    }

    public static GaussDBOffset of(Map<String, ?> offsetMap) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new GaussDBOffset(offsetStrMap);
    }

    public Lsn getLsn() {
        return Lsn.valueOf(Long.valueOf(this.offset.get(SourceInfo.LSN_KEY)));
    }

    @Nullable
    public Long getTxid() {
        String txid = this.offset.get(SourceInfo.TXID_KEY);
        return txid == null ? null : Long.valueOf(txid);
    }

    @Override
    public int compareTo(Offset o) {
        GaussDBOffset rhs = (GaussDBOffset) o;
        return this.getLsn().compareTo(rhs.getLsn());
    }

    @Override
    public String toString() {
        return "GaussDBOffset{lsn=" + getLsn() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GaussDBOffset)) {
            return false;
        }
        GaussDBOffset that = (GaussDBOffset) o;
        return offset.equals(that.offset);
    }
}
