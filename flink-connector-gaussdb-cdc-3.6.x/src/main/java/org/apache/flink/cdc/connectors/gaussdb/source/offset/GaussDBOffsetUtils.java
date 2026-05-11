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
import io.debezium.connector.postgresql.connection.Lsn;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Utils for handling {@link GaussDBOffset}. */
public class GaussDBOffsetUtils {

    public static PostgresOffsetContext getPostgresOffsetContext(
            PostgresOffsetContext.Loader loader, Offset offset) {

        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // all the keys happen to be long type for PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap = new HashMap<>();
        for (String key : offsetStrMap.keySet()) {
            String value = offsetStrMap.get(key);
            if (value != null) {
                offsetMap.put(key, Long.parseLong(value));
            }
        }
        // For latest-offset / specific-offset modes, lsn_proc and lsn_commit may not
        // be present in the offset but are required by PostgresOffsetContext.Loader.
        // Fall back to the main lsn value when they are missing.
        if (!offsetMap.containsKey("lsn_proc") && offsetMap.containsKey("lsn")) {
            offsetMap.put("lsn_proc", offsetMap.get("lsn"));
        }
        if (!offsetMap.containsKey("lsn_commit") && offsetMap.containsKey("lsn")) {
            offsetMap.put("lsn_commit", offsetMap.get("lsn"));
        }
        if (!offsetMap.containsKey("lsn") && offsetMap.containsKey("lsn_proc")) {
            offsetMap.put("lsn", offsetMap.get("lsn_proc"));
        }
        // Last-resort fallback: PostgresOffsetContext.Loader.load() requires
        // "lsn", "lsn_proc", "lsn_commit", and "ts_usec" keys. If any are still
        // missing (e.g., empty offset map from checkpoint restore), fill with safe
        // defaults. Lsn.INVALID_LSN = 0 means the WAL position locator will match
        // the first available message, effectively starting from the slot's position.
        long invalidLsn = Lsn.INVALID_LSN.asLong();
        if (!offsetMap.containsKey("lsn")) {
            offsetMap.put("lsn", invalidLsn);
        }
        if (!offsetMap.containsKey("lsn_proc")) {
            offsetMap.put("lsn_proc", invalidLsn);
        }
        if (!offsetMap.containsKey("lsn_commit")) {
            offsetMap.put("lsn_commit", invalidLsn);
        }
        if (!offsetMap.containsKey("ts_usec")) {
            offsetMap.put("ts_usec", 0L);
        }
        return loader.load(offsetMap);
    }
}
